// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"context"
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
	require.True(t, s.Retry(id, 1, NeedFlush, nil))
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
	require.Len(t, j.Frags, 1)
	require.Eventually(t, func() bool { return s.PendingFlushes() == 0 },
		5*time.Second, time.Millisecond)
}
