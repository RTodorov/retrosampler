// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitKept polls until the keep counters reach want, failing on timeout.
func waitKept(t *testing.T, s *Set, wantLocal, wantBus, wantDup uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		st := s.Stats()
		return st.KeptLocal == wantLocal && st.KeptBus == wantBus &&
			st.DuplicateKeeps == wantDup
	}, 5*time.Second, time.Millisecond)
}

// ADR-008 r5: duplicate keeps — local, bus, self-delivery — mark once
// and count the rest as duplicates. No duplicate decision side effects.
func TestKeepIdempotency(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	s := mustNew(t, testOptions(t.TempDir(), clk))
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(1)
	s.Offer(id, []byte("frag"), clk.Now())
	require.True(t, s.Keep(id, 1, clk.Now()))
	require.True(t, s.Keep(id, 1, clk.Now()))
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	waitKept(t, s, 1, 0, 2)
}

// ADR-008 r4: a keep with no buffered spans still records decided —
// the decided set IS the pending-keeps set — and evicts after W.
func TestKeepWithNoBufferedSpansRecordsDecided(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(9)
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	waitKept(t, s, 0, 1, 0)
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	waitKept(t, s, 0, 1, 1) // second delivery is a duplicate: still pending
	// The mirror is tick-published and starts at zero, so the eviction
	// below only means anything once the entry has been seen standing.
	require.Eventually(t, func() bool { return s.DecidedEntries() == 1 },
		5*time.Second, time.Millisecond, "a keep with no spans is still a decided entry")

	clk.Advance(opts.Window + time.Second)
	require.Eventually(t, func() bool { return s.DecidedEntries() == 0 },
		5*time.Second, time.Millisecond, "decided entry evicts after W")
	// After eviction the same id decides fresh (late duplicate beyond W
	// is out of contract; this pins the eviction really happened).
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	waitKept(t, s, 0, 2, 1)
}

func TestKeepFromBusAborts(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	wedge := make(chan struct{}) // releasable, so goleak stays clean
	opts.dequeueHook = func() { <-wedge }
	s := mustNew(t, opts)
	// The released workers drain and the retried shutdown succeeds. As
	// cleanup rather than a tail statement, goleak stays clean however
	// the assertions below land.
	t.Cleanup(func() {
		close(wedge)
		require.NoError(t, s.Shutdown(context.Background()))
	})

	// Exhaust one shard's free ring so KeepFromBus must block.
	id := testID(1)
	for range queueDepth + 1 {
		s.Offer(id, []byte("x"), clk.Now())
	}
	abort := make(chan struct{})
	close(abort)
	assert.False(t, s.KeepFromBus(id, 1, clk.Now(), abort),
		"closed abort channel unblocks a ring-starved bus keep")

	// Wedged workers: shutdown honours its deadline (stage-2 contract).
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, s.Shutdown(ctx))
}

// A bus keep waits for a buffer rather than shedding: a broadcast
// verdict has no upstream left to re-detect and retry it.
func TestKeepFromBusWaitsForARecycledBuffer(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	wedge := make(chan struct{})
	opts.dequeueHook = func() { <-wedge }
	s := mustNew(t, opts)
	// Releasing from cleanup too keeps a failed assertion from wedging
	// the shutdown that follows it.
	release := sync.OnceFunc(func() { close(wedge) })
	t.Cleanup(func() {
		release()
		require.NoError(t, s.Shutdown(context.Background()))
	})

	id := testID(1)
	for range queueDepth {
		require.True(t, s.Offer(id, []byte("x"), clk.Now()))
	}
	done := make(chan bool, 1)
	go func() { done <- s.KeepFromBus(id, 1, clk.Now(), nil) }()
	select {
	case <-done:
		require.Fail(t, "a ring-starved bus keep must block, never shed")
	case <-time.After(50 * time.Millisecond):
	}

	release() // the workers drain, recycling buffers
	select {
	case ok := <-done:
		assert.True(t, ok, "the keep lands once a buffer frees")
	case <-time.After(5 * time.Second):
		require.Fail(t, "a freed buffer never woke the blocked bus keep")
	}
	waitKept(t, s, 0, 1, 0)
}

// A local keep refused for want of a free buffer is a hard false: the
// caller must be able to fail the batch rather than lose the verdict.
func TestKeepRefusesWhenRingIsExhausted(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	wedge := make(chan struct{})
	opts.dequeueHook = func() { <-wedge }
	s := mustNew(t, opts)
	defer func() {
		close(wedge)
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	id := testID(1)
	for range queueDepth {
		require.True(t, s.Offer(id, []byte("x"), clk.Now()))
	}
	assert.False(t, s.Keep(id, 1, clk.Now()),
		"no free buffer: the local keep is refused, never silently dropped")
	assert.Positive(t, s.Stats().ShedQueueFull, "the refusal is counted")
}

// The window floor rung governs new data volume; a keep decides data
// already buffered, so it must pass a shedding shard (ADR-008 r4).
func TestFloorRefusesOffersButNotKeeps(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Shards = 1
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	s.shards[0].atFloorCause.Store(floorProtected)
	id := testID(4)
	assert.False(t, s.Offer(id, []byte("frag"), clk.Now()), "the floor sheds ingest")
	require.True(t, s.Keep(id, 1, clk.Now()), "the floor never refuses a keep")
	waitKept(t, s, 1, 0, 0)
	assert.Equal(t, uint64(1), s.Stats().ShedFloorProtected)
}

// A recycled buffer still carries the last event's kind, so Offer must
// stamp evFrag: a fragment handed off on a buffer a keep had used would
// otherwise be replayed as a keep and never reach disk.
func TestOfferStampsItsKindOverARecycledKeep(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(dir, clk)
	opts.Shards = 1
	s := mustNew(t, opts)

	id := testID(3)
	// Run every buffer in the ring through the keep path, so whichever
	// one the Offer below draws carries evKeep.
	for range queueDepth {
		require.True(t, s.Keep(id, 1, clk.Now()))
	}
	require.Eventually(t, func() bool { return len(s.shards[0].free) == queueDepth },
		5*time.Second, time.Millisecond, "every keep buffer is recycled")

	require.True(t, s.Offer(id, []byte("frag"), clk.Now()))
	require.NoError(t, s.Shutdown(context.Background()))
	assert.Equal(t, uint64(1), collectAll(t, dir, opts, [][16]byte{id}),
		"the fragment takes the append path, not the keep path")
}

// After Shutdown the queues are gone; a keep must report refusal rather
// than claim a verdict nothing will ever act on.
func TestKeepAfterShutdownIsRefused(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	s := mustNew(t, testOptions(t.TempDir(), clk))
	require.NoError(t, s.Shutdown(context.Background()))

	id := testID(7)
	assert.False(t, s.Keep(id, 1, clk.Now()))
	assert.False(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	st := s.Stats()
	assert.Zero(t, st.KeptLocal)
	assert.Zero(t, st.KeptBus)
	assert.Zero(t, st.ShedQueueFull, "post-shutdown keeps are ignored, not counted as shed")
}
