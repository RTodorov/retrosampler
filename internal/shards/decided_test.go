// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecidedSetMarkHasEvict(t *testing.T) {
	d := newDecidedSet()
	require.True(t, d.mark(testID(1), 100))
	require.False(t, d.mark(testID(1), 150), "second mark is a duplicate")
	require.True(t, d.has(testID(1)))
	require.False(t, d.has(testID(2)))
	require.True(t, d.mark(testID(2), 200))
	assert.Equal(t, 2, d.len())

	assert.Equal(t, 1, d.evict(100), "deadline <= now evicts")
	assert.False(t, d.has(testID(1)))
	assert.True(t, d.has(testID(2)))
	require.True(t, d.mark(testID(1), 300), "evicted id can be re-decided")
	assert.Equal(t, 2, d.evict(1000))
	assert.Zero(t, d.len())
}

func TestDecidedSetHeadCompaction(t *testing.T) {
	d := newDecidedSet()
	// Churn far past any initial capacity: memory must track the live
	// population, not the historical insert count.
	deadline := int64(10)
	for i := range uint64(100_000) {
		require.True(t, d.mark(testID(i), deadline))
		d.evict(deadline - 10)
		deadline++
	}
	assert.LessOrEqual(t, d.len(), 10)
	assert.LessOrEqual(t, cap(d.ring), 4096,
		"ring must compact under churn, not grow with total inserts")
}

// detRand is a splitmix64 state. The differential run must replay
// identically on every machine and Go version, which a stdlib generator
// only promises for as long as its algorithm holds still.
type detRand uint64

func (r *detRand) next() uint64 {
	*r += 0x9E3779B97F4A7C15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// TestDecidedSetMatchesReferenceModel differentially tests the table
// against the obvious map-and-slice model. The id space is far smaller
// than the operation count and the table outgrows its initial size, so
// the run exercises what the model has no equivalent of: colliding probe
// chains, lookups that must survive a tombstone mid-chain, tombstone
// reuse on re-decide, rehashing, and the head compaction that rebases
// every sequence number the table holds.
func TestDecidedSetMatchesReferenceModel(t *testing.T) {
	const (
		ops    = 200_000
		idSpan = 4096
	)
	d := newDecidedSet()
	live := make(map[[16]byte]int64, idSpan)
	var order [][16]byte // insertion order, mirroring the ring

	// Deadline jitter and clock steps are drawn from fixed int64 tables,
	// so the whole run stays reproducible and conversion-free.
	jitter := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 64}
	steps := []int64{0, 1, 2, 3, 5, 8}
	rnd := detRand(1)
	now := int64(0)
	for op := range ops {
		id := testID(rnd.next() % idSpan)
		switch rnd.next() % 4 {
		case 0, 1:
			// Deadlines stay monotone: the set's contract is that
			// insertion order is deadline order.
			deadline := now + jitter[rnd.next()%lenU64(len(jitter))]
			_, dup := live[id]
			if got := d.mark(id, deadline); got != !dup {
				t.Fatalf("op %d: mark(%x) = %v, model says duplicate=%v",
					op, id, got, dup)
			}
			if !dup {
				live[id] = deadline
				order = append(order, id)
			}
		case 2:
			_, want := live[id]
			if got := d.has(id); got != want {
				t.Fatalf("op %d: has(%x) = %v, want %v", op, id, got, want)
			}
		default:
			now += steps[rnd.next()%lenU64(len(steps))]
			want := 0
			for len(order) > 0 && live[order[0]] <= now {
				delete(live, order[0])
				order = order[1:]
				want++
			}
			if got := d.evict(now); got != want {
				t.Fatalf("op %d: evict(%d) = %d, want %d", op, now, got, want)
			}
		}
		if got := d.len(); got != len(live) {
			t.Fatalf("op %d: len = %d, model has %d", op, got, len(live))
		}
	}

	require.NotEmpty(t, live, "the run must end with a live population")
	for id, deadline := range live {
		require.True(t, d.has(id), "live id %x (deadline %d) went missing",
			id, deadline)
	}
	// Everything else in the id space must read as undecided.
	for n := range uint64(idSpan) {
		id := testID(n)
		if _, ok := live[id]; !ok {
			require.False(t, d.has(id), "evicted id %x still reads decided", id)
		}
	}
}

// TestDecidedSetBudgetPerEntry gates the ADR-006-style memory budget:
// <= 48 bytes per decided entry at a pinned 1M-entry population — the
// set holds GLOBAL keeps (every broadcast lands in every instance), so
// at 2000 keeps/s x 300s this is the steady-state shape.
func TestDecidedSetBudgetPerEntry(t *testing.T) {
	const entries = 1_000_000
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	d := newDecidedSet()
	deadline := int64(0)
	for i := range uint64(entries) {
		d.mark(testID(i), deadline)
		deadline++
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	perEntry := float64(after.HeapAlloc-before.HeapAlloc) / float64(entries)
	t.Logf("decided set: %.1f B/entry", perEntry)
	assert.LessOrEqual(t, perEntry, 48.0, "decided-set budget: 48 B/entry")
	runtime.KeepAlive(d)
}
