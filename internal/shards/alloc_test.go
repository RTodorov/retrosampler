// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOfferZeroAllocs gates the stage-2 routing hot path (ADR-004 r2):
// hashing, free-ring handoff, and the fragment copy must cost 0
// allocations once every recycled buffer has grown to the high-water
// fragment size. Workers run concurrently; their append path is
// zero-alloc warm as well (stage-1 gate), so the global measurement
// stays clean. Tick is inert during measurement.
//
// The Gosched is load-bearing, not decoration. AllocsPerRun pins
// GOMAXPROCS to 1 for the measurement and Offer never blocks, so a bare
// offer loop starves the workers: the free ring empties after queueDepth
// offers per shard and every later Offer takes the queue-full shed
// early-return, which returns before the copy. Measured that way the
// gate is hollow — verified by mutation, an allocating
// `append([]byte(nil), frag...)` copy still reported 0 without the
// yield, and 1 alloc/Offer with it. The shed-budget assertion below
// holds the measurement to the real path.
func TestOfferZeroAllocs(t *testing.T) {
	const nIDs, nRuns = 64, 100

	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.SegmentSize = 1 << 30 // no roll during measurement
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	frag := bytes.Repeat([]byte{0xAB}, 512)
	ids := make([][16]byte, nIDs)
	for n := range uint64(nIDs) {
		ids[n] = testID(n)
	}
	now := clk.Now()
	for range 200 { // warm every free-ring buffer past 512 bytes
		for _, id := range ids {
			s.Offer(id, frag, now)
			runtime.Gosched()
		}
	}

	before := s.Stats()
	avg := testing.AllocsPerRun(nRuns, func() {
		for _, id := range ids {
			s.Offer(id, frag, now)
			runtime.Gosched()
		}
	})
	after := s.Stats()

	assert.Zero(t, avg, "ADR-004 r2: 0 allocs on route+enqueue")
	assert.Zero(t, after.AppendErrors, "every handed-off fragment must land")
	// AllocsPerRun makes one unmeasured warm-up call on top of nRuns. A
	// worker that misses its turn can cost a shard one ring slot, so the
	// budget is 5% rather than 0; the hollow mode this guards against
	// sheds 100%.
	assert.Less(t, after.ShedQueueFull-before.ShedQueueFull, uint64(nIDs*(nRuns+1)/20),
		"measurement must ride the copy+handoff path, not the queue-full shed")
}
