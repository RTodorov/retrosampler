// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOfferZeroAllocs gates the stage-2 routing hot path (ADR-004 r2):
// hashing, free-ring handoff, and the fragment copy must cost 0
// allocations once every recycled buffer has grown to the high-water
// fragment size.
//
// Shape matters here, because the obvious shape gates nothing.
// AllocsPerRun pins GOMAXPROCS to 1 and Offer never blocks, so a loop
// that offers faster than the workers drain empties the free ring and
// every later Offer takes the queue-full shed early-return — which
// returns before the copy. Mutation-verified: with the ring empty, an
// allocating `append([]byte(nil), frag...)` copy still measured 0.
// Worse, whether the ring empties depends on whether the workers win CPU
// and disk, so the same test flips between gating and not: under `go
// test ./...` package parallelism a 64-offers-per-run version shed all
// 6464 offers in some runs and none in others.
//
// So the measured window is sized to need no recycling at all: one Offer
// per run, ids rotating, 101 calls over 4 shards — about 25 per shard
// against a 64-buffer ring. The ring is full when the measurement starts
// (waited for below) and cannot run dry, whatever the workers are doing.
// That makes the shed count deterministically 0 rather than a budget,
// and the gate load-independent.
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
	// Warm every free-ring buffer past 512 bytes: the first queueDepth
	// offers to a shard take all of its buffers, so one pass suffices, and
	// the rest warms the workers' own high-water marks (index arena,
	// segment directory) well past anything the measurement can trigger.
	for range 200 {
		for _, id := range ids {
			s.Offer(id, frag, now)
		}
	}
	require.Eventually(t, func() bool {
		for _, sh := range s.shards {
			if len(sh.free) < queueDepth {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Millisecond,
		"workers must recycle every handoff buffer before the measurement")

	before := s.Stats()
	i := 0
	avg := testing.AllocsPerRun(nRuns, func() {
		s.Offer(ids[i%nIDs], frag, now)
		i++
	})
	after := s.Stats()

	assert.Zero(t, avg, "ADR-004 r2: 0 allocs on route+enqueue")
	assert.Zero(t, after.AppendErrors, "every handed-off fragment must land")
	assert.Equal(t, before.ShedQueueFull, after.ShedQueueFull,
		"measurement must ride the copy+handoff path, not the queue-full shed")
	assert.Equal(t, floorShed(before), floorShed(after),
		"measurement must ride the copy+handoff path, not the floor shed")
}
