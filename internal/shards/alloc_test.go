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

// TestKeepZeroAllocs gates the keep path end to end (ADR-004 r2) for the
// delivery that repeats: a DUPLICATE verdict — local re-detection on the
// upstream retry, a bus echo, or this instance's own broadcast coming
// back. Every one of those costs an enqueue and a worker dequeue, and
// under a bus fanning out to N instances they outnumber first verdicts.
//
// Duplicates are the case that can be flat zero on both sides. The
// worker answers one from the decided set's table alone — no job, no
// Collect, no fragment copies — so unlike the fresh keep below there is
// nothing here that ADR-004 exempts, and the whole path is gated rather
// than only the producer's half.
//
// The measured window therefore has to CONTAIN the worker's half rather
// than exclude it: each run spins on the duplicate counter until the
// worker has answered, so its dequeue, its lookup and its buffer recycle
// are all inside the number. AllocsPerRun pins GOMAXPROCS to 1, so the
// yield is what lets the worker run at all.
func TestKeepZeroAllocs(t *testing.T) {
	const nIDs, nRuns = 64, 100
	const reason = 1

	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.SegmentSize = 1 << 30 // no roll during measurement
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	ids := make([][16]byte, nIDs)
	for n := range uint64(nIDs) {
		ids[n] = testID(n)
	}
	now := clk.Now()
	// Decide every id once, so every keep the measurement makes is a
	// duplicate. The window is a frozen hour and the tick an hour away, so
	// nothing evicts an entry back to fresh underneath the measurement.
	for _, id := range ids {
		require.True(t, s.Keep(id, reason, now))
	}
	require.Eventually(t, func() bool { return s.Stats().KeptLocal == nIDs },
		30*time.Second, time.Millisecond, "every id must be decided before the measurement")
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
	i, refused := 0, 0
	avg := testing.AllocsPerRun(nRuns, func() {
		want := s.Stats().DuplicateKeeps + 1
		if !s.Keep(ids[i%nIDs], reason, now) {
			refused++
			i++
			return
		}
		for s.Stats().DuplicateKeeps < want {
			runtime.Gosched()
		}
		i++
	})
	after := s.Stats()

	assert.Zero(t, avg, "ADR-004 r2: 0 allocs on a duplicate keep, enqueue through worker dedup")
	assert.Zero(t, refused, "a refused keep would leave its run unmeasured")
	assert.Equal(t, before.KeptLocal, after.KeptLocal,
		"every measured keep must be a duplicate: a fresh mark builds a flush job, "+
			"which allocates by design")
	assert.Equal(t, before.DuplicateKeeps+nRuns+1, after.DuplicateKeeps,
		"AllocsPerRun adds one warm-up call, and every call must have reached a worker")
	assert.Equal(t, shedTotal(before), shedTotal(after),
		"measurement must ride the enqueue, not a shed early return")

	// The fresh keep is informational, never a gate: it collects the
	// trace's buffered fragments and builds a flush job, and ADR-004
	// exempts the flush path from the zero-alloc rule. Recorded so the
	// exempt cost is visible next to the gated one rather than unmeasured.
	fresh := uint64(0)
	freshAvg := testing.AllocsPerRun(nRuns, func() {
		want := s.Stats().KeptLocal + 1
		if !s.Keep(testID(nIDs+fresh), reason, now) {
			fresh++
			return
		}
		for s.Stats().KeptLocal < want {
			runtime.Gosched()
		}
		fresh++
	})
	t.Logf("fresh keep (ungated, ADR-004 flush-path exemption): %v allocs/op", freshAvg)
}
