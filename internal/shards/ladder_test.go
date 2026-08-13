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

// wedgedSet builds a single-shard Set whose worker blocks before every
// dequeue until release is closed.
func wedgedSet(t *testing.T, dir string, clk *fakeClock) (s *Set, release chan struct{}) {
	t.Helper()
	release = make(chan struct{})
	opts := testOptions(dir, clk)
	opts.Shards = 1
	opts.dequeueHook = func() { <-release }
	return mustNew(t, opts), release
}

func TestQueueFullShedsAndCounts(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	s, release := wedgedSet(t, dir, clk)

	frag := make([]byte, 100)
	var ids [][16]byte
	const extra = 5
	for n := range uint64(queueDepth + extra) {
		id := testID(n)
		ids = append(ids, id)
		s.Offer(id, frag, clk.Now())
	}
	assert.Equal(t, uint64(extra), s.Stats().ShedQueueFull,
		"a full queue sheds exactly the overflow, each one counted")

	close(release)
	require.NoError(t, s.Shutdown(context.Background()))
	opts := testOptions(dir, clk)
	opts.Shards = 1
	assert.Equal(t, uint64(queueDepth), collectAll(t, dir, opts, ids),
		"every accepted fragment survives the drain")
}

func TestShutdownHonoursDeadlineWhileWedged(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	s, release := wedgedSet(t, dir, clk)
	s.Offer(testID(1), []byte("x"), clk.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a wedged worker must not hold Shutdown past its deadline")

	// A retry after the wedge clears completes the drain (and lets
	// goleak see every worker exit).
	close(release)
	require.NoError(t, s.Shutdown(context.Background()))
}

func TestTickExpiresWindowAndReportsDiskBytes(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(dir, clk)
	opts.Shards = 1
	opts.Window = time.Minute
	opts.SegmentSize = 4096
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	frag := make([]byte, 1024)
	for n := range uint64(20) { // ~5 segments of data
		s.Offer(testID(n), frag, clk.Now())
	}
	require.Eventually(t, func() bool { return s.DiskBytesTotal() > 0 },
		time.Second, 5*time.Millisecond, "ticks report disk usage globally")

	clk.Advance(2 * time.Minute) // everything is now past the window
	require.Eventually(t, func() bool {
		// Window expiry deletes finalized segments; the stale active
		// segment rolls and goes on a later tick. Fully drained =
		// nothing left in the accounting.
		return s.DiskBytesTotal() == 0
	}, time.Second, 5*time.Millisecond, "window expiry reclaims everything eventually")
}

func TestWatermarkEarlyExpiryShrinksEffectiveWindow(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(10000, 0))
	opts := testOptions(dir, clk)
	opts.Shards = 1
	opts.Window = time.Hour // window expiry must never fire in this test
	opts.SegmentSize = 4096
	opts.DiskBudget = 64 << 10
	opts.WatermarkPct = 50 // limit: 32 KiB
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	// ~64 KiB of data stamped 30 minutes ago: well inside the window,
	// well past the floor — only the watermark can reclaim it.
	old := clk.Now().Add(-30 * time.Minute)
	frag := make([]byte, 1024)
	for n := range uint64(64) {
		s.Offer(testID(n), frag, old)
	}

	// Ingestion ramps up over several ticks, so usage sits under the
	// watermark on its way up too: only a sacrifice already counted
	// proves the watermark, and not the ramp, is what capped it.
	require.Eventually(t, func() bool {
		return s.Stats().EarlyExpiredSegments > 0 &&
			s.DiskBytesTotal() > 0 && s.DiskBytesTotal() <= 32<<10
	}, time.Second, 5*time.Millisecond, "early expiry pulls usage under the watermark")
	st := s.Stats()
	assert.Positive(t, st.EarlyExpiredSegments, "sacrificed segments are counted")
	assert.Less(t, s.EffectiveWindow(), opts.Window,
		"the gauge shows the window the watermark actually left standing")
}

// idsForShard returns count distinct trace IDs that all route to shard i
// of n, so a test can load one shard while starving another.
func idsForShard(i, n, count int) [][16]byte {
	ids := make([][16]byte, 0, count)
	for seq := uint64(0); len(ids) < count; seq++ {
		if id := testID(seq); shardFor(id, n) == i {
			ids = append(ids, id)
		}
	}
	return ids
}

// Rung 2 sheds for two different reasons and the counters must tell them
// apart: the next sacrifice candidate is floor-protected, or there is no
// finalized segment to sacrifice at all. One over-watermark Set shows
// both at once, since the ladder's disk total is global while the
// candidates are per shard — the loaded shard holds protected data, the
// idle shard holds nothing reclaimable.
func TestFloorProtectedDataMakesOfferShed(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(10000, 0))
	opts := testOptions(dir, clk)
	opts.Shards = 2
	opts.Window = time.Hour
	opts.SegmentSize = 4096
	// 32 KiB budget: the 16 KiB watermark clears the two shards' 8 KiB
	// active-segment floor, and the ~32 KiB loaded below goes over it.
	opts.DiskBudget = 32 << 10
	opts.WatermarkPct = 50
	opts.WindowFloor = 45 * time.Minute // 30-minute-old data is floor-protected
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	loaded := idsForShard(0, opts.Shards, 32)
	idle := idsForShard(1, opts.Shards, 1)[0]

	old := clk.Now().Add(-30 * time.Minute)
	frag := make([]byte, 1024)
	for _, id := range loaded { // ~32 KiB: over the 16 KiB limit
		s.Offer(id, frag, old)
	}

	require.Eventually(t, func() bool {
		s.Offer(loaded[0], frag, clk.Now())
		return s.Stats().ShedFloorProtected > 0
	}, time.Second, 5*time.Millisecond,
		"over watermark with floor-protected candidates: ingest sheds, counted")
	// One byte at a time: the idle shard must stay short of a segment
	// roll, or it acquires a finalized segment and changes cause.
	require.Eventually(t, func() bool {
		s.Offer(idle, []byte("x"), clk.Now())
		return s.Stats().ShedNothingReclaimable > 0
	}, time.Second, 5*time.Millisecond,
		"over watermark with no finalized segment: the shed reports its own cause")
	assert.Zero(t, s.Stats().EarlyExpiredSegments,
		"floor-protected segments are never sacrificed")
}

// A future-stamped fragment (producer clock ahead of ours) makes
// now-tMax negative; EffectiveWindow must clamp to zero, not go
// negative (echoes the ADR-008 r7 skew clamp).
func TestEffectiveWindowClampsFutureStampedData(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Shards = 1
	opts.Tick = 10 * time.Millisecond
	opts.SegmentSize = 1 << 10 // tiny: first append rolls a finalized segment
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	future := clk.Now().Add(time.Hour)
	s.Offer(testID(1), bytes.Repeat([]byte{1}, 2<<10), future)
	s.Offer(testID(2), []byte("x"), future) // forces the roll past SegmentSize

	require.Eventually(t, func() bool {
		w := s.EffectiveWindow()
		return w < opts.Window && w >= 0
	}, 5*time.Second, time.Millisecond, "tick must observe the finalized future segment")
	assert.GreaterOrEqual(t, s.EffectiveWindow(), time.Duration(0))
}

// The stage-3 carry-over, closed: a future tMax pinned its segment against
// BOTH Expire and the watermark rung until real time caught up with the
// skew. The worker now clamps the stamp to its own clock at append, so a
// future-stamped fragment ages out on the normal window.
func TestFutureStampedFragmentIsReclaimable(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	opts.Shards = 1
	opts.Window = time.Minute
	opts.SegmentSize = 4096
	opts.WindowFloor = time.Second
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	// An hour of skew against a one-minute window: the stamp alone would
	// hold this data for 61 minutes of local time.
	future := clk.Now().Add(time.Hour)
	frag := make([]byte, 1024)
	for n := range uint64(20) { // ~5 segments, so finalized ones exist
		require.True(t, s.Offer(testID(n), frag, future))
	}
	// Every handoff buffer back in the ring means every offer above has
	// been appended — and so stamped. Advancing the clock while one was
	// still queued would clamp that straggler to the NEW now and pin it
	// honestly, testing nothing.
	sh := s.shards[0]
	require.Eventually(t, func() bool { return len(sh.free) == queueDepth },
		5*time.Second, time.Millisecond, "every offered fragment is appended")
	require.Eventually(t, func() bool { return s.DiskBytesTotal() > 0 },
		5*time.Second, time.Millisecond, "the tick reports the fragments on disk")

	clk.Advance(2 * opts.Window)
	require.Eventually(t, func() bool { return s.DiskBytesTotal() == 0 },
		5*time.Second, 10*time.Millisecond,
		"a clamped stamp must expire on the window, not on the skewed future")
}
