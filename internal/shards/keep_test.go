// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
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

// assertNoJobWithPublish fails if anything already queued owes a
// broadcast. The caller must first wait for the keep under test to have
// been handled — a suppressed verdict sends nothing at all, so an empty
// channel is the expected outcome, not evidence of a race.
func assertNoJobWithPublish(t *testing.T, ch <-chan *FlushJob) {
	t.Helper()
	for {
		select {
		case j := <-ch:
			assert.Zero(t, j.Need&NeedPublish, "a suppressed keep must not broadcast")
		default:
			return
		}
	}
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

// ADR-008 r1: the baseline verdict is deterministic on every instance,
// so broadcasting it would only manufacture duplicates. Its keep flushes
// this instance's fragments and stops there.
func TestKeepLocalOnlyFlushesWithoutPublish(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(7)
	require.True(t, s.Offer(id, []byte("frag"), clk.Now()))
	require.True(t, s.KeepLocalOnly(id, bus.ReasonBaseline, clk.Now()))

	j := recvJob(t, flush)
	assert.Equal(t, NeedFlush, j.Need&(NeedFlush|NeedPublish),
		"a baseline keep flushes but must never owe a broadcast (ADR-008 r1)")
	assert.Equal(t, bus.ReasonBaseline, j.Reason)
	require.Len(t, j.Frags, 1, "the flush half of the verdict still collects")
	assert.Equal(t, uint64(1), s.Stats().KeptLocal, "baseline counts as a local decision")
}

// The ACCEPTED stage-4 gap (ADR-010 r5), pinned so a future escalation
// change announces itself here: a trace decided as baseline — which
// ADR-008 r3 never publishes — suppresses a later error keep entirely,
// including its broadcast. Spans still flush (decided-arrival forward);
// only the publish is lost, at baseline-rate odds. Peers therefore
// expire their fragments of a trace this instance kept. That is the
// accepted trade, not an oversight: escalating a decided trace would
// need the decided set to retain its reason and re-open for a stronger
// one, which ADR-008 r5 deliberately does not do.
func TestKeepAfterBaselineIsDuplicate(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(8)
	require.True(t, s.Offer(id, []byte("frag"), clk.Now()))
	require.True(t, s.KeepLocalOnly(id, bus.ReasonBaseline, clk.Now()))
	recvJob(t, flush) // the baseline job, so only the error keep's is left

	require.True(t, s.Keep(id, bus.ReasonError, clk.Now()))
	// One local keep and one duplicate: the error verdict was absorbed,
	// never counted a second time and never acted on.
	waitKept(t, s, 1, 0, 1)
	assertNoJobWithPublish(t, flush)
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

// The same skew through Keep would pin a decided entry past W: the
// deadline is decide time + W, and the ring evicts from its head in
// insertion order, so one future-stamped entry holds every later one
// behind it. The worker clamps the verdict's stamp to its own clock, so
// the deadline is local-now+W.
func TestFutureStampedKeepDeadlineClamps(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(10)
	require.True(t, s.Keep(id, bus.ReasonError, clk.Now().Add(time.Hour)))
	waitKept(t, s, 1, 0, 0)
	assert.Equal(t, uint64(1), s.Stats().ClampedStamps,
		"the skewed verdict is clamped, and the clamp is counted")
	// The mirror is tick-published and starts at zero, so the eviction
	// below only means anything once the entry has been seen standing.
	require.Eventually(t, func() bool { return s.DecidedEntries() == 1 },
		5*time.Second, time.Millisecond, "the verdict stands as a decided entry")

	clk.Advance(opts.Window + time.Second)
	require.Eventually(t, func() bool { return s.DecidedEntries() == 0 },
		5*time.Second, 10*time.Millisecond,
		"decided eviction runs on decide-time+W from the local clock")
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

// Every keep that reported true is acted on even when Shutdown races the
// keeping goroutines. A true promises the event reached its shard
// worker, and the worker charges every one it handles to exactly one
// counter — a keep to KeptLocal, KeptBus or DuplicateKeeps, a retry to
// FlushRetries — so each identity below must close once Shutdown has
// returned and every worker is gone. Without the quiesce an event could
// pass the intake check and send onto a worker that had already drained:
// true returned, verdict dropped, and for a bus keep no upstream left to
// re-detect it.
//
// The abort channel doubles as a watchdog. Both blocking entry points
// wait on the free ring, so the quiesce must hold the workers up long
// enough to refill it; if one ever waited past their exit, the timer
// firing is the evidence.
func TestKeepAndRetryVsShutdownConservation(t *testing.T) {
	const goroutines, perG, nIDs = 8, 200, 32
	for round := range 20 {
		clk := newFakeClock(time.Unix(1000, 0))
		s := mustNew(t, testOptions(t.TempDir(), clk))
		for n := range uint64(nIDs) {
			require.True(t, s.Offer(testID(n), []byte("frag"), clk.Now()))
		}

		wedged := make(chan struct{})
		watchdog := time.AfterFunc(30*time.Second, func() { close(wedged) })

		// Seeded before the racers, so every round has something to
		// conserve: at round 0 Shutdown beats all eight goroutines to the
		// intake flag, and both identities would hold 0 against 0.
		var keeps, retries atomic.Uint64
		require.True(t, s.Keep(testID(0), 1, clk.Now()))
		require.True(t, s.KeepFromBus(testID(1), 1, clk.Now(), wedged))
		require.True(t, s.Retry(testID(2), 1, NeedFlush, 0, wedged))
		keeps.Add(2)
		retries.Add(1)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := range uint64(goroutines) {
			wg.Add(1)
			go func(g uint64) {
				defer wg.Done()
				<-start
				for n := range uint64(perG) {
					// Every entry point races: Keep sheds on a full ring,
					// KeepFromBus and Retry block on one.
					id := testID((g*perG + n) % nIDs)
					switch n % 3 {
					case 0:
						if s.Keep(id, 1, clk.Now()) {
							keeps.Add(1)
						}
					case 1:
						if s.KeepFromBus(id, 1, clk.Now(), wedged) {
							keeps.Add(1)
						}
					default:
						if s.Retry(id, 1, NeedFlush, 0, wedged) {
							retries.Add(1)
						}
					}
				}
			}(g)
		}
		close(start)
		time.Sleep(time.Duration(round) * 100 * time.Microsecond) // vary the race window
		require.NoError(t, s.Shutdown(context.Background()))
		wg.Wait()
		require.True(t, watchdog.Stop(), "a blocking enqueue waited past the quiesce")

		st := s.Stats()
		require.Positive(t, keeps.Load(), "round %d: no keep accepted, so nothing to conserve", round)
		require.Positive(t, retries.Load(), "round %d: no retry accepted, so nothing to conserve", round)
		require.Equal(t, keeps.Load(), st.KeptLocal+st.KeptBus+st.DuplicateKeeps,
			"round %d: every accepted keep is acted on exactly once", round)
		require.Equal(t, retries.Load(), st.FlushRetries,
			"round %d: every accepted retry is acted on exactly once", round)
	}
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
