// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// shedTotal sums every shed rung. A gate watching only one of them could
// ride an early return through the others and still report green.
func shedTotal(st shards.Stats) uint64 {
	return st.ShedQueueFull + st.ShedFloorProtected + st.ShedNothingReclaimable
}

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

// pinShardFixture fixes the shard count and the disk budget the alloc
// gates run against, so the measured topology is the same everywhere.
//
// The budget was GOMAXPROCS-scaled, which made it a machine-dependent
// fixture at 1 GiB segments: shards.New refuses a watermark at or under
// the unreclaimable Shards x SegmentSize floor, and 2 x GOMAXPROCS
// segments at 80% is 1.6 x GOMAXPROCS — under the floor on a two-core
// runner, where start() then failed outright. Pinned to 4, the budget
// clears the floor for every shard count start() can resolve
// (min(GOMAXPROCS, 4)). Nothing preallocates it.
func pinShardFixture(cfg *Config) {
	cfg.Shards = 4
	cfg.DiskBudget = 2 * 4 * int64(cfg.SegmentSize)
}

// warmups is the batch count every alloc gate pushes through before it
// measures, growing each recycled handoff buffer, the fragmenter's
// scratch, and the workers' own high-water marks past anything the
// measured window can ask for.
const warmups = 200

// warmPace is how many warm-up batches run between pace calls. A burst
// that size must not be able to drain a shard's free ring on its own, or
// the pacing cannot hold the refusal count down.
const warmPace = 25

// warmDeadline bounds how long one warm-up batch may go on being refused
// before the test calls the set stuck rather than loaded.
const warmDeadline = 30 * time.Second

// warmUp runs the warm-up batches batch(n) yields until each one is
// ACCEPTED, and returns how many refusals it rode through on the way.
//
// Retrying is what an overloaded upstream does with errOverloaded, and it
// is the only shape that makes the warm-up's guarantee unconditional: the
// measurement needs warmups accepted batches to have grown every recycled
// buffer, and a run that merely SENT that many can arrive with the
// buffers cold. Budgeting refusals instead was tried and is load-bound —
// under `go test ./...` package parallelism the shards fall behind and
// both gates measured 22-70 refusals per 200 batches, well past any
// budget that still means anything.
//
// pace, when non-nil, runs every warmPace batches with the batches sent
// and refused so far. A caller whose warm-up makes the workers do real
// work — a keep flushes a whole trace, where a healthy fragment is one
// append — uses it to let them catch up instead of burying the free
// rings and then retrying against them.
func warmUp(t *testing.T, p *retroProcessor, batch func(n uint64) ptrace.Traces,
	pace func(sent, refused uint64),
) uint64 {
	t.Helper()
	ctx := context.Background()
	var refused uint64
	for n := range uint64(warmups) {
		td := batch(n)
		deadline := systemClock().Add(warmDeadline)
		for {
			_, err := p.processTraces(ctx, td)
			if errors.Is(err, processorhelper.ErrSkipProcessingData) {
				break
			}
			require.ErrorIs(t, err, errOverloaded,
				"a warm-up batch is either accepted or refused for overload")
			refused++
			require.False(t, systemClock().After(deadline),
				"warm-up batch %d refused for %s: the set is wedged, not loaded", n, warmDeadline)
			runtime.Gosched()
		}
		runtime.Gosched()
		if pace != nil && (n+1)%warmPace == 0 {
			pace(n+1, refused)
		}
	}
	return refused
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
// A batch is allocBatchTraces fragments spread over the shards, so
// unlike the shards gate this one does lean on the workers recycling
// buffers, and the shed count is a budget rather than a flat 0. Its
// per-shard demand is an order of magnitude lighter, though: 0 shed
// measured across every run here, loaded and unloaded, against a 5%
// budget.
//
// Detection is inside the measured window: KeepOnError defaults on, so
// the detector runs over every span of the batch, and the baseline
// closure over every group. allocBatch is healthy and the default rate
// is 0, so both answer no throughout and no keep is enqueued — the keep
// path has its own gate below.
func TestProcessTracesZeroAllocs(t *testing.T) {
	const nRuns = 100

	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.SegmentSize = 1 << 30 // no roll during measurement
	pinShardFixture(cfg)
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = consumertest.NewNop()
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
	shared := p.newPooledFrag()
	p.fragPool.New = func() any { misses++; return shared }
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(context.Background())) }()

	td := allocBatch()
	ctx := context.Background()
	warmUp(t, p, func(uint64) ptrace.Traces { return td }, nil)

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
	assert.Less(t, shedTotal(after)-shedTotal(before), uint64(allocBatchTraces*(nRuns+1)/20),
		"measurement must ride the copy+handoff path, not the queue-full shed")
	// The race detector's forced drops miss ~25% of the time; a path that
	// stopped returning entries to the pool would miss 100%. Half the
	// window separates the two cleanly.
	assert.Less(t, misses-missesBefore, (nRuns+1)/2,
		"measurement must ride pool hits, not New")
}

// keepBatchSpans is the span count in one keep-path batch. They share a
// trace, so a batch costs its shard exactly two handoff buffers: one
// fragment, one verdict.
const keepBatchSpans = 5

// keepID builds a distinct trace id for run n. tag separates id spaces:
// the measured runs must not reuse a warm-up id, or the keep would be a
// duplicate and the fragment would take the decided-arrival forward
// path, which allocates a job by design.
func keepID(tag byte, n uint64) pcommon.TraceID {
	var id pcommon.TraceID
	id[0] = tag
	binary.LittleEndian.PutUint64(id[8:], n+1)
	return id
}

// keepBatch builds a single-trace batch of error spans under id — the
// keep-on-error built-in fires on every one of them.
func keepBatch(id pcommon.TraceID) ptrace.Traces {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for range keepBatchSpans {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(id)
		sp.SetName("op")
		sp.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

// TestProcessTracesKeepPathZeroAllocs gates the keep half of the hot
// path (ADR-004 r2): fragment + detect + offer + keep-enqueue, at 0
// allocations, for a batch where every span carries error status.
//
// What is NOT in the budget is the flush side — the worker's job build,
// the per-fragment copies, the decided set's growth, everything the
// flusher then does. ADR-004 scopes the zero-alloc rule to ingest and
// exempts the flush path, so those allocations are excluded here by
// construction rather than by budget, and the construction is worth
// spelling out because AllocsPerRun cannot express it: it reads
// process-wide MemStats, so a worker that runs inside the measured
// window charges its allocations to this number no matter which
// goroutine they happen on (the gate above leans on exactly that, since
// the append path it measures is itself alloc-free).
//
// So the window is sized to keep the workers asleep. AllocsPerRun pins
// GOMAXPROCS to 1, ingest never blocks, and nothing here reaches a
// preemption point, so a worker woken by a queue send stays runnable-
// but-not-running for the whole window. It only has to not run out of
// buffers meanwhile: nRuns+1 runs take two of the shard layer's 64
// per-shard handoff buffers each, and 2*(nRuns+1) is under 64, so even
// the degenerate routing where every measured id hashes to one shard
// cannot empty a ring. The assertions below hold the run to that shape:
// KeptLocal is the worker-side keep counter, so its staying put is the
// proof no worker ran, and the shed counters staying put is the proof
// the measurement rode the real path rather than an early return.
func TestProcessTracesKeepPathZeroAllocs(t *testing.T) {
	const nRuns = 25
	const warmTag, measureTag = 0x0A, 0x0B

	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.SegmentSize = 1 << 30 // no roll during measurement
	pinShardFixture(cfg)
	sink := new(consumertest.TracesSink)
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = sink
	// Same pool treatment as the gate above: the race detector drops one
	// Put in four, and a forced miss must recycle rather than build.
	var misses uint64
	shared := p.newPooledFrag()
	p.fragPool.New = func() any { misses++; return shared }
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(context.Background())) }()

	ctx := context.Background()
	set := p.set.Load()
	require.NotNil(t, set)
	// Every verdict handled means its handoff buffer is back on its free
	// ring, so pacing on that count keeps the warm-up from burying the
	// rings and starting the measurement against a drained set. A refused
	// batch may have lost its verdict with it, hence the bound rather than
	// an equality. The flusher is left to finish on its own: at-least-once
	// delivery makes the sink's count a range, so it is no signal here.
	drained := func(sent, refused uint64) {
		require.Eventually(t, func() bool { return set.Stats().KeptLocal+refused >= sent },
			30*time.Second, time.Millisecond,
			"workers must keep up with the warm-up")
	}
	refused := warmUp(t, p, func(n uint64) ptrace.Traces {
		return keepBatch(keepID(warmTag, n))
	}, drained)
	drained(warmups, refused)
	// The pool budget is read over the warm-up rather than the measured
	// window: the detector's drops are random, and 200-odd calls hold the
	// ~25% expected rate to a tight number where a 26-call window's tail
	// reaches half of it (13-15 of 26 measured over 60 runs). A path that
	// stopped returning entries to the pool would miss every call either
	// way. Retries are calls too, so the window is what warmUp actually ran.
	assert.Less(t, misses, (warmups+refused)/2, "the path must ride pool hits, not New")

	batches := make([]ptrace.Traces, nRuns+1) // AllocsPerRun adds a warm-up call
	for n := range uint64(len(batches)) {
		batches[n] = keepBatch(keepID(measureTag, n))
	}
	before := set.Stats()
	i := 0
	avg := testing.AllocsPerRun(nRuns, func() {
		_, _ = p.processTraces(ctx, batches[i])
		i++
	})
	after := set.Stats()

	assert.Zero(t, avg, "ADR-004 r2: 0 allocs/span through fragment+detect+offer+keep")
	assert.Equal(t, before.KeptLocal, after.KeptLocal,
		"the measured window is producer-only: a worker running inside it would "+
			"charge the exempt flush-side allocations to this budget")
	assert.Equal(t, shedTotal(before), shedTotal(after),
		"measurement must ride the copy+handoff path, not a shed early return")
}
