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
// than only the producer's half. Every trace in the fixture holds
// fragments on disk, which is what makes that a claim and not a
// tautology: the worker must answer from the table even when there IS
// data it could have collected.
//
// The measured window therefore has to CONTAIN the worker's half rather
// than exclude it: each run spins on the duplicate counter until the
// worker has answered, so its dequeue, its lookup and its buffer recycle
// are all inside the number. AllocsPerRun pins GOMAXPROCS to 1, so the
// yield is what lets the worker run at all.
func TestKeepZeroAllocs(t *testing.T) {
	const nIDs, nRuns = 64, 100
	const reason = 1
	// fragsPerTrace is what every trace here holds on disk before any
	// verdict — a trace whose spans arrived in a few batches, which is the
	// case a keep actually meets.
	const fragsPerTrace = 3

	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.SegmentSize = 1 << 30 // no roll during measurement
	// A flusher that keeps up, without a goroutine to drain it: the channel
	// holds every job this test can produce, so sendJob takes its
	// non-blocking send. A nil or full channel would park each job in the
	// shard's pending map instead — a different path, and one that would
	// quietly replace the flush cost measured below with the cost of
	// deferring it.
	opts.Flush = make(chan *FlushJob, 2*(nIDs+nRuns+1))
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	ids := make([][16]byte, nIDs)
	for n := range uint64(nIDs) {
		ids[n] = testID(n)
	}
	// AllocsPerRun makes one unmeasured warm-up call on top of nRuns, so
	// the fresh set needs one id more than it measures.
	freshIDs := make([][16]byte, nRuns+1)
	for n := range uint64(len(freshIDs)) {
		freshIDs[n] = testID(nIDs + n)
	}
	now := clk.Now()
	frag := bytes.Repeat([]byte{0xAB}, 512)
	// buffer offers one trace's fragments, waiting out a full ring rather
	// than failing on it: this producer outruns the workers by an order of
	// magnitude, so a shed here means the ring needs a moment, not that the
	// path is broken. Every fragment has to land — both measurements below
	// are about what a verdict does with data that is already on disk.
	buffer := func(id [16]byte) {
		t.Helper()
		for range fragsPerTrace {
			accepted := false
			for attempt := 0; attempt < 1<<20 && !accepted; attempt++ {
				if accepted = s.Offer(id, frag, now); !accepted {
					runtime.Gosched()
				}
			}
			require.True(t, accepted, "shard never freed a buffer for the fixture")
		}
	}
	for _, id := range ids {
		buffer(id)
	}
	for _, id := range freshIDs {
		buffer(id)
	}

	// Decide every id of the first set once, so every keep the measurement
	// makes is a duplicate. The window is a frozen hour and the tick an
	// hour away, so nothing evicts an entry back to fresh underneath the
	// measurement.
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

	// The fresh keep is informational, never a gate: marking a new verdict
	// sends the worker to collect the trace's buffered fragments and build
	// a flush job, and ADR-004 exempts the flush path from the zero-alloc
	// rule. It is measured the same way as the duplicate — spinning until
	// the worker has answered — so the flush-side allocations are inside
	// the window, which is exactly what makes the figure worth recording:
	// AllocsPerRun reads process-wide MemStats, so they count wherever
	// they happen.
	//
	// The fixture is what makes it a true cost. Each of these traces has
	// fragsPerTrace fragments on disk and a flush channel with room, so
	// the number covers one job, a Frags slice grown to fragsPerTrace, and
	// a copy per fragment. Against an unbuffered fixture the same code
	// collects nothing and reports ~1 alloc — a job built over an empty
	// trace, which is not a cost anything in production pays.
	//
	// Recorded rather than asserted so an optimisation of the flush path
	// cannot fail the suite.
	k, freshRefused := 0, 0
	freshAvg := testing.AllocsPerRun(nRuns, func() {
		want := s.Stats().KeptLocal + 1
		if !s.Keep(freshIDs[k], reason, now) {
			freshRefused++
			k++
			return
		}
		for s.Stats().KeptLocal < want {
			runtime.Gosched()
		}
		k++
	})
	assert.Zero(t, freshRefused, "a refused keep would leave its run unmeasured")
	t.Logf("fresh keep over %d buffered fragments (ungated, ADR-004 flush-path "+
		"exemption): %v allocs/op", fragsPerTrace, freshAvg)
}
