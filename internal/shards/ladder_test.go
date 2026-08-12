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

func TestFloorProtectedDataMakesOfferShed(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(10000, 0))
	opts := testOptions(dir, clk)
	opts.Shards = 1
	opts.Window = time.Hour
	opts.SegmentSize = 4096
	// 16 KiB budget: the 8 KiB watermark clears the 4 KiB active-segment
	// floor, and the ~16 KiB offered below goes straight over it.
	opts.DiskBudget = 16 << 10
	opts.WatermarkPct = 50
	opts.WindowFloor = 45 * time.Minute // 30-minute-old data is floor-protected
	opts.Tick = 10 * time.Millisecond
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	old := clk.Now().Add(-30 * time.Minute)
	frag := make([]byte, 1024)
	for n := range uint64(16) { // ~16 KiB: over the 4 KiB limit
		s.Offer(testID(n), frag, old)
	}

	require.Eventually(t, func() bool {
		s.Offer(testID(999), frag, clk.Now())
		return s.Stats().ShedFloor > 0
	}, time.Second, 5*time.Millisecond,
		"over watermark with floor-protected candidates: ingest sheds, counted")
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
