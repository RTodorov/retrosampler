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
