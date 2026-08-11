// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/buffer"
)

// fakeClock is a mutable injected clock, safe for concurrent use.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t0 time.Time) *fakeClock { return &fakeClock{t: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testID builds a distinct trace ID from n without signed conversions.
func testID(n uint64) (id [16]byte) {
	binary.LittleEndian.PutUint64(id[:8], n)
	id[15] = 1
	return id
}

// testOptions returns Options sized so tests never trip the overload
// ladder unless they mean to.
func testOptions(dir string, clk *fakeClock) Options {
	return Options{
		Dir:          dir,
		Shards:       4,
		Window:       time.Hour,
		SegmentSize:  1 << 20,
		DiskBudget:   1 << 40,
		WatermarkPct: 80,
		WindowFloor:  time.Minute,
		Now:          clk.Now,
		Tick:         time.Hour,
	}
}

func mustNew(t *testing.T, opts Options) *Set {
	t.Helper()
	s, err := New(opts)
	require.NoError(t, err)
	return s
}

// collectAll reopens every shard directory of a shut-down Set and counts
// the buffered fragments across the given ids.
func collectAll(t *testing.T, dir string, opts Options, ids [][16]byte) uint64 {
	t.Helper()
	var total uint64
	for i := range opts.Shards {
		b, err := buffer.Open(shardDir(dir, i),
			buffer.Options{Window: opts.Window, SegmentSize: opts.SegmentSize},
			opts.Now())
		require.NoError(t, err)
		for _, id := range ids {
			require.NoError(t, b.Collect(id, func([]byte) { total++ }))
		}
		require.NoError(t, b.Close())
	}
	return total
}
