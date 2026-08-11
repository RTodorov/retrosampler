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
