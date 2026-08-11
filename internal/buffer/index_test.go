// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexPutGetChains(t *testing.T) {
	x := newIndex()
	a, b := [16]byte{1}, [16]byte{2}
	x.put(a, loc{gen: 1, off: 0, length: 10})
	x.put(b, loc{gen: 1, off: 34, length: 5})
	x.put(a, loc{gen: 2, off: 0, length: 7})

	var got []loc
	for i := x.head(a); i >= 0; i = x.at(i).next {
		l := x.at(i)
		l.next = 0
		got = append(got, l)
	}
	assert.Equal(t, []loc{{gen: 1, off: 0, length: 10}, {gen: 2, off: 0, length: 7}}, got)
	assert.Equal(t, int32(-1), x.head([16]byte{9}))
	assert.Equal(t, 2, x.live())
}

func TestIndexGrowKeepsAllEntries(t *testing.T) {
	x := newIndex()
	for i := range 10_000 {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[:8], lenU64(i))
		x.put(id, loc{gen: 1, off: lenU32(i), length: 1})
	}
	assert.Equal(t, 10_000, x.live())
	for i := range 10_000 {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[:8], lenU64(i))
		h := x.head(id)
		require.GreaterOrEqual(t, h, int32(0))
		assert.Equal(t, lenU32(i), x.at(h).off)
	}
}

func TestIndexSweepFreesDeadTraces(t *testing.T) {
	x := newIndex()
	old, live := [16]byte{1}, [16]byte{2}
	x.put(old, loc{gen: 1, off: 0, length: 1})
	x.put(live, loc{gen: 1, off: 24, length: 1})
	x.put(live, loc{gen: 5, off: 0, length: 1})

	for range 64 { // cursor is rotating; sweep everything
		x.sweep(1024, 3)
	}
	assert.Equal(t, int32(-1), x.head(old), "all locs gen<3: dead")
	require.GreaterOrEqual(t, x.head(live), int32(0), "tail gen 5: alive")
	assert.Equal(t, 1, x.live())

	// freed arena slots are reused, not appended
	before := cap(x.arena)
	x.put([16]byte{7}, loc{gen: 6, off: 0, length: 1})
	assert.Equal(t, before, cap(x.arena))
}

func TestIndexReuseAfterSweepThenReinsert(t *testing.T) {
	x := newIndex()
	id := [16]byte{1}
	x.put(id, loc{gen: 1, off: 0, length: 1})
	for range 64 {
		x.sweep(1024, 2)
	}
	require.Equal(t, int32(-1), x.head(id))
	x.put(id, loc{gen: 3, off: 9, length: 1}) // tombstone slot must be reusable
	h := x.head(id)
	require.GreaterOrEqual(t, h, int32(0))
	assert.Equal(t, uint32(9), x.at(h).off)
}

// TestHashKeyDistributesAdversarialIDs guards against a Fibonacci-hashing
// pitfall: multiplication's carry propagates low-to-high, so a product's low
// bits depend only on the low bits of its input. Two constructions that
// collapse a hash extracting low bits from a low-8-bytes-only input into a
// single start slot:
//   - shared low 8 id bytes, varying high 8 (a hash that reads only id[:8]
//     hashes every one of these identically)
//   - shared high 8 id bytes and low 4 of the low 8, varying only the top 4
//     bytes of the low 8 (id[:8] as a uint64 is then i<<32 for varying i;
//     any extraction of LOW bits from (i<<32)*seed is 0 for every i, since
//     those bits never receive a carry from the zero low half)
//
// Real trace IDs carry a time prefix in their first bytes, which lands in
// the low bits of LE.Uint64(id[:8]) — this is exactly the second
// construction, and is why the fix must read the high bits of the product.
func TestHashKeyDistributesAdversarialIDs(t *testing.T) {
	const n = 10_000
	x := newIndex()
	// A representative grown table size, set directly so the measurement
	// isolates hash quality from put's load-factor-triggered growth.
	x.slots = make([]slot, 32768)

	distinct := func(mk func(i int) [16]byte) int {
		seen := make(map[int]struct{}, n)
		for i := range n {
			seen[x.startSlot(mk(i))] = struct{}{}
		}
		return len(seen)
	}

	sharedLow := distinct(func(i int) [16]byte {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[8:], lenU64(i))
		return id
	})
	assert.Greater(t, sharedLow, n/2,
		"hash must depend on the high 8 id bytes, not just the low 8")

	topBytesVary := distinct(func(i int) [16]byte {
		var id [16]byte
		binary.LittleEndian.PutUint32(id[4:8], lenU32(i))
		return id
	})
	assert.Greater(t, topBytesVary, n/2,
		"hash must expose high-order product bits: varying only the low half's top bytes must not collide")
}

// TestIndexPutGetAdversarialIDs is the correctness half of the same guard:
// table operations must stay correct across both adversarial id families
// from TestHashKeyDistributesAdversarialIDs, at a scale (10k) that would
// visibly thrash under a collapsed single probe chain.
func TestIndexPutGetAdversarialIDs(t *testing.T) {
	const n = 10_000
	check := func(mk func(i int) [16]byte) {
		x := newIndex()
		for i := range n {
			x.put(mk(i), loc{gen: 1, off: lenU32(i), length: 1})
		}
		for i := range n {
			h := x.head(mk(i))
			require.GreaterOrEqual(t, h, int32(0))
			assert.Equal(t, lenU32(i), x.at(h).off)
		}
	}
	check(func(i int) [16]byte {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[8:], lenU64(i))
		return id
	})
	check(func(i int) [16]byte {
		var id [16]byte
		binary.LittleEndian.PutUint32(id[4:8], lenU32(i))
		return id
	})
}

// TestIndexStructSizes pins the slot/loc layouts that memoryBytes' 24/16
// constants assume (ADR-006 r5 index budget).
func TestIndexStructSizes(t *testing.T) {
	assert.Equal(t, uintptr(24), unsafe.Sizeof(slot{}))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(loc{}))
}

func TestIndexMemoryBytes(t *testing.T) {
	x := newIndex()
	assert.Equal(t, int64(cap(x.slots))*24, x.memoryBytes(), "fresh index: empty arena")

	x.put([16]byte{1}, loc{gen: 1, off: 0, length: 1})
	x.put([16]byte{1}, loc{gen: 2, off: 0, length: 1})
	want := int64(cap(x.slots))*24 + int64(cap(x.arena))*16
	assert.Equal(t, want, x.memoryBytes())
}
