// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recvJob(t *testing.T, ch <-chan *FlushJob) *FlushJob {
	t.Helper()
	select {
	case j := <-ch:
		return j
	case <-time.After(5 * time.Second):
		t.Fatal("no flush job arrived")
		return nil
	}
}

// A fresh local keep collects every buffered fragment into one job that
// needs publish+flush; a bus keep needs flush only.
//
// This is also the pin on the same-queue FIFO ordering property: the
// fragments are offered before the keep, so they must be on disk by the
// time the keep's Collect runs. Nothing else orders them — the shard's
// one typed queue does, which is why keeps ride it instead of taking a
// side channel (ADR-007 r2).
func TestFreshKeepProducesFlushJob(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(1)
	require.True(t, s.Offer(id, []byte("frag-a"), clk.Now()))
	require.True(t, s.Offer(id, []byte("frag-b"), clk.Now()))
	require.True(t, s.Keep(id, 1, clk.Now()))

	j := recvJob(t, flush)
	assert.Equal(t, id, j.ID)
	assert.Equal(t, byte(1), j.Reason)
	assert.Equal(t, NeedPublish|NeedFlush, j.Need)
	require.Len(t, j.Frags, 2)
	assert.Equal(t, "frag-a", string(j.Frags[0]))
	assert.Equal(t, "frag-b", string(j.Frags[1]))

	id2 := testID(2)
	require.True(t, s.Offer(id2, []byte("frag-c"), clk.Now()))
	require.True(t, s.KeepFromBus(id2, 1, clk.Now(), nil))
	j2 := recvJob(t, flush)
	assert.Equal(t, NeedFlush, j2.Need, "bus keeps never re-publish")
	require.Len(t, j2.Frags, 1)
	assert.Equal(t, "frag-c", string(j2.Frags[0]), "a bus keep collects on the same FIFO terms")
}

// A local keep whose fragments were refused still broadcasts: the job
// goes out publish-only with zero fragments.
func TestZeroFragmentLocalKeepStillPublishes(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	require.True(t, s.Keep(testID(5), 1, clk.Now()))
	j := recvJob(t, flush)
	assert.Equal(t, NeedPublish|NeedFlush, j.Need)
	assert.Empty(t, j.Frags)
}

// Spans arriving after the decision flush straight through (ADR-008 r3):
// appended to the buffer AND forwarded as a single-fragment job.
func TestDecidedArrivalForwards(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Tick = 10 * time.Millisecond // Retry below lands via the tick
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(3)
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	// A bus keep with zero buffered fragments and no publish owed sends
	// NO job — it only records the pending decision.
	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, flush)

	require.True(t, s.Offer(id, []byte("late"), clk.Now()))
	j := recvJob(t, flush)
	assert.Equal(t, NeedFlush, j.Need)
	require.Len(t, j.Frags, 1)
	assert.Equal(t, "late", string(j.Frags[0]))

	// And the fragment is ALSO on disk: a Retry re-collects it (the
	// retry parks in pend and replays on the next tick).
	require.True(t, s.Retry(id, 1, NeedFlush, 0, nil))
	j2 := recvJob(t, flush)
	require.Len(t, j2.Frags, 1)
	assert.Equal(t, "late", string(j2.Frags[0]))
	assert.Equal(t, uint64(1), s.Stats().FlushRetries, "the retry is counted")
}

// The natural bound on retries is segment expiry: an intent whose
// fragments have aged out drains without flushing and without hanging —
// the ADR-007 partial-trace consequence.
func TestPendingIntentDrainsWhenFragmentsExpire(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // keep the channel full so the intent parks
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(6)
	require.True(t, s.Offer(id, []byte("doomed"), clk.Now()))
	require.True(t, s.Keep(id, 1, clk.Now()))
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond)

	clk.Advance(2 * time.Minute) // fragments AND the decided entry age out
	// Expiring the active segment takes two ticks — buffer.Expire rolls
	// it on the first and deletes it on the second — and a rolled segment
	// still reads. Unblocking mid-way would let the retry collect a
	// fragment that is aged out but not yet gone, which is a different
	// contract from the one below.
	require.Eventually(t, func() bool { return s.DiskBytesTotal() == 0 },
		5*time.Second, time.Millisecond, "the doomed segment is deleted, not merely rolled")
	assert.Positive(t, s.Stats().ExpiredBytes, "the reclaimed bytes are reported")

	<-flush // unblock the channel
	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond, "expired intent drains")
	select {
	case j := <-flush:
		assert.Empty(t, j.Frags, "an expired trace must not flush spans")
	default: // no job at all is equally correct
	}
}

// Parking MERGES need-bits rather than replacing them, and both parked
// intents reach the flusher.
//
// The merge is what keeps a broadcast alive: a local keep parks owing
// publish+flush, and a later fragment for that now-decided trace parks
// owing flush alone. Overwriting there would drop NeedPublish, so the
// trace would flush locally and never reach the bus — peers would expire
// their fragments of a trace this instance kept, and nothing would count
// the loss. The reason follows the same rule from the other side: the
// forward carries none (a fragment holds no verdict), so the keep's must
// survive the merge.
//
// Two traces park in the one shard, which pins that neither is stranded
// by the other's presence. It does NOT pin how many intents one tick
// retries: the two jobs are collected across ticks under a 5-second
// bound, so a loop retrying a single intent per tick passes here too.
func TestParkedIntentsMergeNeedBits(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // occupy the only slot, so every handoff parks
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Shards = 1 // both traces land in the same pend map
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	merged, plain := testID(11), testID(12)
	require.True(t, s.Offer(merged, []byte("before"), clk.Now()))
	require.True(t, s.Keep(merged, 7, clk.Now())) // parks publish+flush
	// Same trace, now decided: the forward parks flush-only on top.
	require.True(t, s.Offer(merged, []byte("after"), clk.Now()))
	require.True(t, s.Offer(plain, []byte("other"), clk.Now()))
	require.True(t, s.Keep(plain, 9, clk.Now()))

	// Every handoff buffer back in the ring means every event above has
	// been handled — and with the channel full, parked. Waiting on
	// PendingFlushes would not do: it counts traces, so it reaches 2
	// before the second event for merged has necessarily landed.
	sh := s.shards[0]
	require.Eventually(t, func() bool { return len(sh.free) == queueDepth },
		5*time.Second, time.Millisecond, "every event is handled and parked")

	// One free slot, so the retries drain one per tick.
	<-flush
	jobs := make(map[[16]byte]*FlushJob, 2)
	for range 2 {
		j := recvJob(t, flush)
		jobs[j.ID] = j
	}
	require.Contains(t, jobs, merged)
	require.Contains(t, jobs, plain, "the second parked intent is not stranded by the first")

	assert.Equal(t, NeedPublish|NeedFlush, jobs[merged].Need,
		"the forward's flush-only park must not drop the keep's publish")
	assert.Equal(t, byte(7), jobs[merged].Reason, "first nonzero reason wins the merge")
	assert.Len(t, jobs[merged].Frags, 2, "the retry re-collects the whole trace")
	assert.Equal(t, NeedPublish|NeedFlush, jobs[plain].Need)

	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond, "both intents drain")
}

// Parked intents drain oldest-first: with room for exactly one job per
// tick, three parked intents come out in park order across ticks. Map
// iteration order (the old pendScratch snapshot) cannot pass this.
func TestParkedIntentsDrainOldestFirst(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // pre-fill: every send parks until we drain one
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Shards = 1 // all three ids park in the one queue
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	ids := []([16]byte){testID(1), testID(2), testID(3)}
	for _, id := range ids {
		require.True(t, s.Offer(id, []byte("frag"), clk.Now()))
		require.True(t, s.Retry(id, 1, NeedFlush, 0, nil))
	}
	// Retry only enqueues, so the parks are still in flight here. The
	// gauge reaching 3 is what says all three are in the queue, in the
	// order the one work channel handed them over.
	require.Eventually(t, func() bool { return s.PendingFlushes() == 3 },
		5*time.Second, time.Millisecond, "all three intents are parked before the slot frees")

	<-flush // free the slot; ticks may now send one job at a time
	for _, want := range ids {
		j := recvJob(t, flush)
		assert.Equal(t, want, j.ID, "oldest parked intent drains first")
	}
}

// A merge into an intent already waiting in pendq keeps its ONE place in
// line: the entry's queue-membership bit is what stops the second park
// from enqueueing the id again. Without it the queue grows a slot per
// park EVENT — every re-park of a wedged intent, every late fragment for
// a decided trace — while pend still holds the single entry all of them
// merged into, so pendq outgrows the work it indexes and the drain walks
// tombstones to reach live intents.
//
// White-box because a duplicate slot is invisible from outside: both
// name the same id, so a drain serves the entry at the first and skips
// the second as a tombstone. The count in pendq is the whole claim.
func TestMergedParkQueuesTheIDOnce(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	s := &Set{opts: Options{Now: clk.Now, Window: time.Hour}}
	sh := &shard{pend: make(map[[16]byte]pendReq)}

	id := testID(31)
	deadline := clk.Now().Add(time.Minute).UnixNano()
	sh.park(s, id, 7, NeedPublish|NeedFlush, deadline) // a local keep
	sh.park(s, id, 0, NeedFlush, 0)                    // a late fragment's forward
	sh.park(s, id, 0, NeedFlush, 0)                    // and another

	require.Len(t, sh.pendq, 1, "one queue slot per id, however many parks merge into it")
	require.Len(t, sh.pend, 1)
	req := sh.pend[id]
	assert.Equal(t, NeedPublish|NeedFlush, req.need, "the merge keeps both bits")
	assert.Equal(t, deadline, req.deadline, "and the deadline its publish arrived with")
	assert.Equal(t, byte(7), req.reason, "first nonzero reason still wins")
	assert.True(t, req.queued, "the entry knows it is in line")
}

// compactPendq bounds pendq's STORAGE, not merely its length. A
// saturation episode appends a slot per park event, so the backing array
// grows with events rather than with the intents it indexes, and
// reslicing to zero hands that peak array back to the same shard for the
// life of the process — the pendScratch it replaced could not, being
// sized by peak entries. The release is what stops one burst from
// becoming the baseline.
func TestCompactPendqReleasesTheEpisodePeakArray(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	s := &Set{opts: Options{Now: clk.Now, Window: time.Hour}}
	const episode = 8 * pendqShrinkCap

	sh := &shard{pend: make(map[[16]byte]pendReq)}
	for i := range uint64(episode) {
		sh.park(s, testID(i), 1, NeedFlush, 0)
	}
	peak := cap(sh.pendq)
	require.Greater(t, peak, pendqShrinkCap, "the fixture builds an oversized array")

	sh.pendqHead = len(sh.pendq) // the whole episode drains, nothing re-parks
	sh.compactPendq()
	require.Empty(t, sh.pendq, "an emptied queue holds no entries")
	require.Zero(t, sh.pendqHead)
	assert.Less(t, cap(sh.pendq), peak/4, "and does not keep the peak array either")

	// A partial drain moves the live tail down onto an array sized to it,
	// and settles: the shrink leaves length within a size class of
	// capacity, so it cannot retrigger and reallocate tick after tick.
	const live = 16
	sh = &shard{pend: make(map[[16]byte]pendReq)}
	for i := range uint64(episode) {
		sh.park(s, testID(i), 1, NeedFlush, 0)
	}
	sh.pendqHead = len(sh.pendq) - live
	sh.compactPendq()
	require.Len(t, sh.pendq, live, "the live tail survives the compaction")
	require.Zero(t, sh.pendqHead)
	assert.LessOrEqual(t, cap(sh.pendq), pendqShrinkCap, "on an array sized to it")

	fitted := cap(sh.pendq)
	sh.compactPendq()
	assert.Equal(t, fitted, cap(sh.pendq), "a second pass reallocates nothing")
}

// A full flush channel parks the intent per shard; the tick retries it
// with the fragments still on disk (ADR-007: retries are free until
// segment expiry).
func TestFullFlushChannelPendsAndTickRetries(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	blocker := &FlushJob{}
	flush <- blocker // occupy the only slot
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(4)
	require.True(t, s.Offer(id, []byte("parked"), clk.Now()))
	require.True(t, s.Keep(id, 1, clk.Now()))
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond, "job parked while the channel is full")

	<-flush // unblock
	j := recvJob(t, flush)
	assert.Equal(t, id, j.ID)
	assert.Equal(t, NeedPublish|NeedFlush, j.Need, "parking preserves what the job owed")
	require.Len(t, j.Frags, 1)
	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond)
}

// A parked NeedPublish past its deadline (~keep time + W) is dropped and
// counted (ADR-011 r3): past W no peer fragment survives, so the
// broadcast can cause nothing. NeedFlush needs no deadline — an expired
// trace's Collect comes back empty and the intent drains (pinned above).
func TestParkedPublishDroppedPastWindow(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // full channel: the keep's job parks
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(21)
	require.True(t, s.Keep(id, 1, clk.Now())) // zero-frag keep: publish-only job
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond, "publish intent parked")

	clk.Advance(2 * time.Minute) // past the deadline
	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond, "past-W publish intent dropped")
	assert.Equal(t, uint64(1), s.Stats().PublishesAbandoned, "the drop is counted")

	<-flush // free the channel: nothing may arrive now
	select {
	case j := <-flush:
		t.Fatalf("abandoned intent still produced a job: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}
}

// Inside W the parked publish retries and delivers: the deadline must
// not fire early. That it also does not fire LATE than it should — the
// re-park keeping the first deadline rather than extending it — needs a
// clock that moves between re-parks, and is pinned below.
func TestParkedPublishInsideWindowRetries(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{}
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(22)
	require.True(t, s.Keep(id, 1, clk.Now()))
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond)

	clk.Advance(30 * time.Second) // half W: still live
	<-flush
	j := recvJob(t, flush)
	assert.Equal(t, id, j.ID)
	assert.NotZero(t, j.Need&NeedPublish, "inside W the publish survives")
	assert.Zero(t, s.Stats().PublishesAbandoned)
}

// The deadline is stamped once and survives every re-park, so a publish
// failing over and over ages out on schedule (ADR-011 r3). The drain
// DELETES the entry before it replays it, so each cycle rebuilds it from
// scratch: a park that re-derived the deadline from the current clock
// would push the horizon out by W on every tick and the intent would
// never expire, which is the unbounded pending map this deadline exists
// to close.
//
// The two tests above cannot see that, and neither can any test whose
// clock stands still while the intent re-parks — a re-stamp reproduces
// the same number. So the clock moves in two steps here, and the probe
// is what makes the first one land: its keep is back-dated so its
// decided entry expires between the steps, and its eviction is proof
// that a tick read the intermediate clock (and, under a re-stamping
// park, would have carried the deadline past the final one). It rides
// the bus origin so it owes no publish and parks no intent of its own,
// and it is enqueued first so the decided ring's insertion order stays
// deadline order.
func TestParkedPublishDeadlineSurvivesRepark(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // full channel: every cycle re-parks
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Shards = 1 // one pend map, one decided ring
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	probe, id := testID(24), testID(25)
	require.True(t, s.KeepFromBus(probe, 1, clk.Now().Add(-45*time.Second), nil)) // expires at 1015
	require.True(t, s.Keep(id, 1, clk.Now()))                                     // deadline 1060
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 && s.DecidedEntries() == 2 },
		5*time.Second, time.Millisecond, "the publish intent is parked and both keeps are decided")

	clk.Advance(30 * time.Second) // 1030: re-parks now happen at this reading
	require.Eventually(t, func() bool { return s.DecidedEntries() == 1 },
		5*time.Second, time.Millisecond, "a tick has run against the intermediate clock")

	clk.Advance(31 * time.Second) // 1061: past the original deadline, short of a re-stamped one
	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond, "the intent ages against its first deadline, not its last failure")
	assert.Equal(t, uint64(1), s.Stats().PublishesAbandoned)
}

// The deadline belongs to the publish intent, not to the pend entry it
// rides in, and the two have different lifetimes: an entry deduped by
// trace id outlives any one intent, so a deadline left behind on it
// would be applied to whatever publish arrives next.
//
// Here the entry is a flush-only intent wedged by a downstream outage.
// Its trace is decided from the bus, and back-dated so the decision
// expires while the fragments are still live — which is what lets the
// trace be re-detected locally afterwards, on a NEW keep owing a NEW
// broadcast. Merging that into the waiting entry must install the fresh
// deadline; adopting the old one, already in the past, abandons a
// broadcast that never got a single attempt and counts it as aged out.
func TestStaleFlushEntryCannotAbandonAFreshPublish(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // full channel: the flush intent stays parked
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Shards = 1
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(26)
	require.True(t, s.Offer(id, []byte("live"), clk.Now()))                    // expires at 1060
	require.True(t, s.KeepFromBus(id, 1, clk.Now().Add(-50*time.Second), nil)) // decided until 1010
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond, "the flush intent is parked on the wedged channel")

	clk.Advance(15 * time.Second) // 1015: the decision has expired, the fragment has not
	require.Eventually(t, func() bool { return s.DecidedEntries() == 0 },
		5*time.Second, time.Millisecond, "the trace can be decided again")
	require.Equal(t, int64(1), s.PendingFlushes(), "and its flush intent is still waiting")

	require.True(t, s.Keep(id, 1, clk.Now())) // a fresh local keep: deadline 1075
	// The merge is the whole subject, so the keep has to be handled while
	// the channel is still full. Every handoff buffer back in the ring is
	// what says so; unwedging first would let its job go straight out and
	// the test would pass without ever merging anything.
	sh := s.shards[0]
	require.Eventually(t, func() bool { return len(sh.free) == queueDepth },
		5*time.Second, time.Millisecond, "the fresh keep is parked, not sent")

	<-flush // unwedge: the merged intent goes out
	j := recvJob(t, flush)
	assert.NotZero(t, j.Need&NeedPublish, "the fresh broadcast survives the merge")
	assert.Zero(t, s.Stats().PublishesAbandoned, "nothing here is past its deadline")
}

// Abandonment is the other way an entry outlives its publish intent: the
// flush half survives it. Stripping the bit clears the deadline with it,
// so what remains states what it is — a plain flush intent, Deadline 0,
// as FlushJob documents — rather than a job announcing a publish horizon
// it no longer owes to a flusher that hands the value back through the
// public Retry. The trace can then be re-detected once its decision ages
// out and broadcast again on its own fresh deadline.
//
// The fragment lands late in the window on purpose: the entry has to
// survive the abandonment tick, which it does only while its Collect
// still returns something.
func TestAbandonedIntentDoesNotPoisonTheNextPublish(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 1)
	flush <- &FlushJob{} // full channel throughout: every intent parks
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Shards = 1
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(27)
	require.True(t, s.Keep(id, 1, clk.Now())) // decided until 1060, publish deadline 1060
	require.Eventually(t, func() bool { return s.PendingFlushes() == 1 },
		5*time.Second, time.Millisecond)

	clk.Advance(50 * time.Second)
	require.True(t, s.Offer(id, []byte("late"), clk.Now())) // still collectable at 1061

	clk.Advance(11 * time.Second) // 1061: the publish ages out, the decision with it
	require.Eventually(t, func() bool {
		return s.Stats().PublishesAbandoned == 1 && s.DecidedEntries() == 0
	}, 5*time.Second, time.Millisecond, "the first publish is abandoned on schedule")
	require.Equal(t, int64(1), s.PendingFlushes(), "its flush half is still parked")

	<-flush // let the remainder through
	j := recvJob(t, flush)
	assert.Equal(t, NeedFlush, j.Need, "only the flush half survived")
	assert.Zero(t, j.Deadline, "and it carries no deadline for a publish it no longer owes")

	flush <- &FlushJob{} // wedge again, so the re-detection has to park
	require.True(t, s.Keep(id, 1, clk.Now()))
	sh := s.shards[0]
	require.Eventually(t, func() bool { return len(sh.free) == queueDepth },
		5*time.Second, time.Millisecond, "the second keep is parked, not sent")

	<-flush
	j2 := recvJob(t, flush)
	assert.NotZero(t, j2.Need&NeedPublish, "the second broadcast is not the first one's to abandon")
	assert.Equal(t, uint64(1), s.Stats().PublishesAbandoned, "and it is not counted as aged out")
}

// The deadline sweep is not starved by a blocked drain: with a nil flush
// channel (permanently "full" — nothing can ever send), a parked publish
// intent past its deadline is still abandoned on the tick.
//
// The subject sits BEHIND a blocker on purpose, and that is what gives
// the pin its teeth. The gated drain stops at the head every tick, so
// the intent under test is never visited by it at all: only a sweep on a
// schedule of its own can abandon it. Folding the deadline check back
// into the drain walk — the obvious simplification, one pass instead of
// two — leaves it parked past W forever, and that fold is what this
// fails on (measured, with the fold applied). Behind an ungated drain
// the same fold passed, because that drain reached every intent every
// tick; the gate is what promoted this from documentation to a pin.
func TestSweepAbandonsWhileDrainIsBlocked(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = nil // every send parks; the drain can never progress
	opts.Shards = 1  // one queue, so the blocker is genuinely in front
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	// blocker holds the head: its fragment collects, its send finds the
	// channel full, and the drain stops there for good.
	blocker, id := testID(8), testID(9)
	require.True(t, s.Offer(blocker, []byte("frag"), clk.Now()))
	require.True(t, s.Offer(id, []byte("frag"), clk.Now()))
	require.True(t, s.Retry(blocker, 1, NeedFlush, 0, nil))
	deadline := clk.Now().Add(time.Minute).UnixNano()
	require.True(t, s.Retry(id, 1, NeedPublish|NeedFlush, deadline, nil))
	require.Eventually(t, func() bool { return s.Stats().PublishesAbandoned == 0 && s.PendingFlushes() == 2 },
		5*time.Second, time.Millisecond, "both intents are parked, the publish inside its deadline")

	clk.Advance(2 * time.Minute) // past the deadline
	require.Eventually(t, func() bool { return s.Stats().PublishesAbandoned == 1 },
		5*time.Second, time.Millisecond,
		"the sweep abandons on schedule though the drain never reaches the intent")
	require.Eventually(t, func() bool { return s.PendingFlushes() == 2 },
		5*time.Second, time.Millisecond,
		"the flush half of the intent survives the abandonment")
}

// The drain is gated by the flush channel, not sized by the backlog:
// with the channel permanently full and 40 intents parked in ONE shard,
// each tick performs at most one whole-trace Collect. The ungated drain
// did 40 per tick — retrying the entire backlog against a flusher that
// refused its first job — so this is the O(pending) amplification pin.
//
// Shards is 1 so the whole backlog sits behind one gate: the claim is
// per shard per tick, and four workers would spend four Collects a tick
// for the same reason one spends one.
func TestBlockedDrainCollectsAtMostOncePerTick(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	var collects atomic.Int64
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = nil // permanently full: nothing ever sends
	opts.Shards = 1
	opts.Tick = 10 * time.Millisecond
	opts.collectHook = func() { collects.Add(1) }
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	const parked = 40
	for i := range uint64(parked) {
		id := testID(i + 1)
		// Each intent must hold a fragment: one whose Collect comes back
		// empty owes nothing, and the drain walks past it instead of
		// parking on the gate. Offer sheds on a momentarily full ring, so
		// it is retried rather than required outright.
		accepted := false
		for attempt := 0; attempt < 1<<20 && !accepted; attempt++ {
			if accepted = s.Offer(id, []byte("frag"), clk.Now()); !accepted {
				runtime.Gosched()
			}
		}
		require.True(t, accepted, "shard never freed a buffer for the fixture")
		require.True(t, s.Retry(id, 1, NeedFlush, 0, nil))
	}
	require.Eventually(t, func() bool { return s.PendingFlushes() == parked },
		5*time.Second, time.Millisecond, "the whole backlog is parked before the measurement")

	base := collects.Load()            // enqueueing does not Collect; only keep() and the drain do
	time.Sleep(150 * time.Millisecond) // ~15 ticks of wall clock
	got := collects.Load() - base
	require.Positive(t, got, "the drain is alive — it tries its head intent")
	require.Less(t, got, int64(parked),
		"a blocked tick costs ~1 Collect, never O(pending); 15 ungated ticks would be ~600")
}

// A keep near the window boundary still flushes: the deadline mechanics
// must not round a live keep away (spec §9 W-mechanics pin).
func TestKeepNearWindowBoundaryFlushes(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	flush := make(chan *FlushJob, 16)
	opts := testOptions(t.TempDir(), clk)
	opts.Flush = flush
	opts.Window = time.Minute
	opts.WindowFloor = time.Second
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	id := testID(23)
	require.True(t, s.Offer(id, []byte("old-but-live"), clk.Now()))
	clk.Advance(59 * time.Second) // 0.98W later the keep still lands
	require.True(t, s.KeepFromBus(id, 1, clk.Now(), nil))
	j := recvJob(t, flush)
	require.Len(t, j.Frags, 1)
	assert.Equal(t, "old-but-live", string(j.Frags[0]))
}
