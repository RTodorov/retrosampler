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

// TestStaticCensusAlive is the positive half of the goroutine census
// (ADR-007 r7): goleak proves the count is zero AFTER shutdown, and
// nothing proved that exactly Shards workers were running BEFORE it — a
// worker that died silently, or was never started, would leave its shard
// mute and every gate above still green, because Offer never blocks and
// a mute shard's fragments simply queue.
//
// The census is structural, not a count. runtime.NumGoroutine deltas
// around New cannot be that proof: the test binary's own goroutines
// (the race detector's, the testing package's, a previous test's
// stragglers) move underneath the measurement, so the number is only
// ever an upper bound with noise on top, and a delta that happens to
// read 4 says nothing about WHICH four goroutines are alive. What is
// asserted instead is what a census is actually for: one shard per
// configured worker, none of them exited, and each one individually
// answering work addressed to it.
func TestStaticCensusAlive(t *testing.T) {
	const wantShards = 4

	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(t.TempDir(), clk)
	require.Equal(t, wantShards, opts.Shards, "the fixture is what the census counts")
	s := mustNew(t, opts)
	defer func() { require.NoError(t, s.Shutdown(context.Background())) }()

	require.Len(t, s.shards, wantShards, "one shard per configured worker")
	for i, sh := range s.shards {
		select {
		case <-sh.done:
			t.Fatalf("shard %d worker has already exited", i)
		default:
		}
	}

	// Liveness, shard by shard: route a fragment to each one by construction
	// and wait for its worker to hand the buffer back. A wedged or dead
	// worker holds its buffer forever, so the ring never refills and this
	// fails on the shard that is actually mute.
	now := clk.Now()
	for i := range s.shards {
		id, ok := idForShard(i, len(s.shards))
		require.True(t, ok, "no trace id found routing to shard %d", i)
		require.True(t, s.Offer(id, []byte("census"), now))
		sh := s.shards[i]
		require.Eventually(t, func() bool { return len(sh.free) == queueDepth },
			30*time.Second, time.Millisecond,
			"shard %d must dequeue its fragment and recycle the buffer", i)
	}
	assert.Zero(t, s.Stats().AppendErrors, "every censused fragment must land")
	assert.Zero(t, shedTotal(s.Stats()), "the census must ride the handoff, not a shed")
}

// idForShard finds a trace id that routes to shard i of n, searching the
// same testID space the rest of the suite uses.
func idForShard(i, n int) ([16]byte, bool) {
	for k := range uint64(1024) {
		if id := testID(k); shardFor(id, n) == i {
			return id, true
		}
	}
	return [16]byte{}, false
}
