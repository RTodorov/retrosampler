// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// allocBatchTraces is the distinct-trace count in allocBatch, and so the
// number of fragments one processTraces call hands to the shard set.
const allocBatchTraces = 10

// allocBatch builds a reusable multi-trace batch for the gate.
func allocBatch() ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	ids := [allocBatchTraces][16]byte{
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
	}
	for _, id := range ids {
		for range 5 {
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(id)
			sp.SetName("op")
		}
	}
	return td
}

// TestProcessTracesZeroAllocs gates the full processor hot path
// (ADR-004 r2): pooled fragmenter, reusable routing closure, and shard
// handoff at 0 bookkeeping allocations/span steady-state.
//
// The Gosched is load-bearing: AllocsPerRun pins GOMAXPROCS to 1 and
// Offer never blocks, so without a yield the shard workers never run,
// the free rings drain, and most of the measured batch takes the
// queue-full shed early-return instead of the copy+handoff path
// (measured that way: 690-975 of 1010 offers shed, and the gate then
// passes even against an allocating Offer). Yielding also pulls the
// workers' own append path into the measured window, by design: their
// allocations count toward this budget too. The shed and pool-miss
// budgets below hold the measurement to the real path.
//
// A batch is allocBatchTraces fragments spread over GOMAXPROCS shards,
// so unlike the shards gate this one does lean on the workers recycling
// buffers, and the shed count is a budget rather than a flat 0. Its
// per-shard demand is an order of magnitude lighter, though: 0 shed
// measured across every run here, loaded and unloaded, against a 5%
// budget.
func TestProcessTracesZeroAllocs(t *testing.T) {
	const nRuns = 100

	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.SegmentSize = 1 << 30 // no roll during measurement
	// The set runs GOMAXPROCS shards, and shards.New refuses a watermark
	// under the unreclaimable Shards x SegmentSize floor, so with 1 GiB
	// segments the budget scales with the machine rather than pinning the
	// shard count this gate exercises. Nothing preallocates it.
	cfg.DiskBudget = 2 * int64(runtime.GOMAXPROCS(0)) * int64(cfg.SegmentSize)
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	// sync.Pool drops one Put in four on purpose under the race detector
	// (go1.25 sync/pool.go Put), and every suite here runs with -race, so
	// a pool round-trip is not allocation-free in that mode: the forced
	// misses measured ~9 allocs/batch. Point New at one pre-built entry so
	// a forced miss recycles instead of building a fragmenter. What is
	// measured stays processTraces exactly as production runs it; only the
	// detector's sabotage is priced out.
	//
	// Handing every miss the same entry would also hide a real regression
	// that stops returning entries to the pool, so misses are counted and
	// budgeted below. Sharing one entry is safe only because this test
	// drives processTraces sequentially; the counter is unsynchronised for
	// the same reason.
	misses := 0
	shared := newPooledFrag()
	p.fragPool.New = func() any { misses++; return shared }
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(context.Background())) }()

	td := allocBatch()
	ctx := context.Background()
	for range 200 {
		_, err := p.processTraces(ctx, td)
		require.NoError(t, err)
		runtime.Gosched()
	}

	set := p.set.Load()
	before, missesBefore := set.Stats(), misses
	avg := testing.AllocsPerRun(nRuns, func() {
		_, _ = p.processTraces(ctx, td)
		runtime.Gosched()
	})
	after := set.Stats()

	assert.Zero(t, avg, "ADR-004 r2: 0 allocs/span through fragment+route+enqueue")
	assert.Zero(t, after.AppendErrors, "every handed-off fragment must land")
	// AllocsPerRun makes one unmeasured warm-up call on top of nRuns. A
	// worker that misses its turn can cost a shard one ring slot, so the
	// budget is 5% rather than 0; the hollow mode this guards against
	// sheds most of the window.
	shed := (after.ShedQueueFull - before.ShedQueueFull) + (after.ShedFloor - before.ShedFloor)
	assert.Less(t, shed, uint64(allocBatchTraces*(nRuns+1)/20),
		"measurement must ride the copy+handoff path, not the queue-full shed")
	// The race detector's forced drops miss ~25% of the time; a path that
	// stopped returning entries to the pool would miss 100%. Half the
	// window separates the two cleanly.
	assert.Less(t, misses-missesBefore, (nRuns+1)/2,
		"measurement must ride pool hits, not New")
}
