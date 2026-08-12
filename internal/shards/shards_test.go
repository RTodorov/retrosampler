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

func TestShardForSpreadsAndIsStable(t *testing.T) {
	counts := make([]int, 4)
	for n := range uint64(4096) {
		i := shardFor(testID(n), 4)
		require.Equal(t, i, shardFor(testID(n), 4), "routing is deterministic")
		require.Less(t, i, len(counts), "shard index in bounds")
		counts[i]++
	}
	for i, c := range counts {
		assert.Greater(t, c, 512, "shard %d starved: %d of 4096", i, c)
	}
}

func TestNewValidatesOptions(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	for name, mut := range map[string]func(*Options){
		"zero shards":     func(o *Options) { o.Shards = 0 },
		"zero window":     func(o *Options) { o.Window = 0 },
		"zero budget":     func(o *Options) { o.DiskBudget = 0 },
		"pct zero":        func(o *Options) { o.WatermarkPct = 0 },
		"pct over 100":    func(o *Options) { o.WatermarkPct = 101 },
		"zero floor":      func(o *Options) { o.WindowFloor = 0 },
		"floor at window": func(o *Options) { o.WindowFloor = o.Window },
		"nil clock":       func(o *Options) { o.Now = nil },
		// 4 shards x 1 MiB active segments are unreclaimable, so a 4 MiB
		// budget puts the watermark under that floor for good.
		"watermark under the active-segment floor": func(o *Options) { o.DiskBudget = 4 << 20 },
	} {
		t.Run(name, func(t *testing.T) {
			opts := testOptions(t.TempDir(), clk)
			mut(&opts)
			_, err := New(opts)
			assert.Error(t, err)
		})
	}
}

// TestNewWatermarkFloorCliffIsExact pins the boundary either side: below
// it the Set would shed 100% of ingest forever, so New must refuse rather
// than start something silently useless.
func TestNewWatermarkFloorCliffIsExact(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	// testOptions is 4 shards x 1 MiB segments: a 4194304-byte floor.
	// At WatermarkPct 80 the watermark (DiskBudget/100*WatermarkPct, the
	// tick's own expression) first clears it at DiskBudget 5242900 —
	// 52429*80 = 4194320 — one step above 5242800's 4194240.
	for name, tc := range map[string]struct {
		budget int64
		ok     bool
	}{
		"one step under the floor": {5242800, false},
		"one step over the floor":  {5242900, true},
	} {
		t.Run(name, func(t *testing.T) {
			opts := testOptions(t.TempDir(), clk)
			require.Equal(t, int64(4194304), int64(opts.Shards)*int64(opts.SegmentSize),
				"the budgets above are computed against this floor")
			opts.DiskBudget = tc.budget
			s, err := New(opts)
			if !tc.ok {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, s.Shutdown(context.Background()))
		})
	}
}

func TestConservationSequential(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(dir, clk)
	s := mustNew(t, opts)

	var ids [][16]byte
	frag := make([]byte, 200)
	for n := range uint64(100) {
		id := testID(n)
		ids = append(ids, id)
		s.Offer(id, frag, clk.Now())
	}
	require.NoError(t, s.Shutdown(context.Background()))

	st := s.Stats()
	got := collectAll(t, dir, opts, ids)
	assert.Equal(t, uint64(100), got+st.ShedQueueFull+st.ShedFloor,
		"every offered fragment is buffered or counted as shed")
	assert.Zero(t, st.AppendErrors)
}

func TestConservationConcurrent(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(dir, clk)
	s := mustNew(t, opts)

	const goroutines, perG = 8, 500
	var wg sync.WaitGroup
	for g := range uint64(goroutines) {
		wg.Add(1)
		go func(g uint64) {
			defer wg.Done()
			frag := make([]byte, 200)
			for n := range uint64(perG) {
				s.Offer(testID(g*perG+n), frag, clk.Now())
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, s.Shutdown(context.Background()))

	var ids [][16]byte
	for n := range uint64(goroutines * perG) {
		ids = append(ids, testID(n))
	}
	st := s.Stats()
	got := collectAll(t, dir, opts, ids)
	assert.Equal(t, uint64(goroutines*perG), got+st.ShedQueueFull+st.ShedFloor,
		"concurrent ingest: every fragment buffered or counted as shed")
}

func TestShutdownIsIdempotent(t *testing.T) {
	clk := newFakeClock(time.Unix(1000, 0))
	s := mustNew(t, testOptions(t.TempDir(), clk))
	require.NoError(t, s.Shutdown(context.Background()))
	require.NoError(t, s.Shutdown(context.Background()), "second shutdown must not panic or hang")
	s.Offer(testID(1), []byte("x"), clk.Now())
	st := s.Stats()
	assert.Zero(t, st.ShedQueueFull, "post-shutdown offers are ignored, not counted as shed")
	assert.Zero(t, st.ShedFloor, "post-shutdown offers are ignored, not counted as floor shed")
}
