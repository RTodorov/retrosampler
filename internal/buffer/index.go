// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"encoding/binary"
	"math"
)

const initialSlots = 1024

// hashSeed is the multiplicative hash constant (Fibonacci hashing).
const hashSeed = 0x9E3779B97F4A7C15

// lenU64 converts a non-negative int to uint64, with guard for negative
// input (gosec G115). Returns 0 for negative input.
func lenU64(n int) uint64 {
	if n < 0 {
		return 0
	}
	// Guard for gosec: n >= 0 here, so the conversion cannot change sign.
	return uint64(n)
}

// tableMask returns n-1 as a uint64 bitmask for an open-addressing table of
// size n (a power of two, always >= 1). Returns 0 for n <= 0.
func tableMask(n int) uint64 {
	if n <= 0 {
		return 0
	}
	// Guard for gosec: n > 0 here, so the subtraction cannot go negative.
	return uint64(n) - 1
}

// probeIdx converts a masked probe position (always < len(x.slots), a
// bounded int) to an int slot index. Returns 0 if out of int range.
func probeIdx(v uint64) int {
	if v > math.MaxInt {
		return 0
	}
	// Guard for gosec: v <= MaxInt here, so the conversion is exact.
	return int(v)
}

// arenaIdx converts a non-negative arena length to an int32 index, with
// guard for overflow (gosec G115). Returns 0 for negative or out-of-range
// input; the arena cannot realistically approach 2^31 entries within the
// index's memory budget.
func arenaIdx(n int) int32 {
	if n < 0 || n > math.MaxInt32 {
		return 0
	}
	// Guard for gosec: n is in [0, MaxInt32] here, so the conversion is exact.
	return int32(n)
}

// loc locates one fragment record within a segment: its generation, byte
// offset of the record header, and fragment length. next chains locs
// belonging to the same trace, in append (gen) order; -1 ends the chain.
// Real arena index, no offset (only slot.head/tail carry the +1 offset).
type loc struct {
	gen, off, length uint32
	next             int32
}

// slot is one open-addressing table slot. head/tail store the trace's loc
// chain as an arena index+1 (0 = empty, -1 = tombstone, else arena index+1);
// head is the oldest loc, tail the newest.
type slot struct {
	id         [16]byte
	head, tail int32
}

// index is a compact, single-writer trace index: an open-addressing table
// of slot mapping trace ID to a chain of locs in a flat arena, with a free
// list for reclaiming tombstoned chains (ADR-006 r5).
type index struct {
	slots     []slot
	arena     []loc
	free      int32 // arena index of the free list head, -1 if empty
	liveCount int   // backs live(); named liveCount to avoid a field/method clash
	tombs     int
	cursor    int // sweep's rotating start slot
}

// newIndex returns an empty index with its initial table capacity.
func newIndex() *index {
	return &index{
		slots: make([]slot, initialSlots),
		free:  -1,
	}
}

// hashKey computes the table hash for id from its low 8 bytes (trace IDs
// are generated random, so the low bytes alone give a good distribution).
func hashKey(id [16]byte) uint64 {
	return binary.LittleEndian.Uint64(id[:8]) * hashSeed
}

// findSlot linear-probes the table for id. If id is present, it returns its
// slot index and found=true. Otherwise it returns the slot at which id
// should be inserted (the first tombstone seen, or else the terminating
// empty slot) and found=false.
func (x *index) findSlot(id [16]byte) (i int, found bool) {
	mask := tableMask(len(x.slots))
	start := hashKey(id) & mask
	tomb := -1
	for p := uint64(0); p < lenU64(len(x.slots)); p++ {
		j := probeIdx((start + p) & mask)
		s := &x.slots[j]
		switch {
		case s.head == 0: // empty: probe ends here
			if tomb >= 0 {
				return tomb, false
			}
			return j, false
		case s.head == -1: // tombstone: remember first, keep probing
			if tomb < 0 {
				tomb = j
			}
		case s.id == id:
			return j, true
		}
	}
	// Table saturated with tombstones/occupied slots (shouldn't happen: put
	// rehashes before load factor allows this). Fall back to a tombstone.
	return tomb, false
}

// alloc pops a loc off the free list, or appends to the arena if empty, and
// returns its arena index.
func (x *index) alloc(l loc) int32 {
	l.next = -1
	if x.free >= 0 {
		i := x.free
		x.free = x.arena[i].next
		x.arena[i] = l
		return i
	}
	x.arena = append(x.arena, l)
	return arenaIdx(len(x.arena) - 1)
}

// put appends l to id's loc chain, creating the chain (and its slot) if id
// is not yet present.
func (x *index) put(id [16]byte, l loc) {
	if (x.liveCount+x.tombs)*4 >= 3*len(x.slots) {
		x.grow()
	}
	i, found := x.findSlot(id)
	if found {
		s := &x.slots[i]
		n := x.alloc(l)
		x.arena[s.tail-1].next = n
		s.tail = n + 1
		return
	}
	s := &x.slots[i]
	wasTomb := s.head == -1
	n := x.alloc(l)
	s.id = id
	s.head = n + 1
	s.tail = n + 1
	if wasTomb {
		x.tombs--
	}
	x.liveCount++
}

// head returns the arena index of id's oldest loc, or -1 if id is absent.
func (x *index) head(id [16]byte) int32 {
	i, found := x.findSlot(id)
	if !found {
		return -1
	}
	return x.slots[i].head - 1
}

// at returns the loc at arena index i (a real index, not offset).
func (x *index) at(i int32) loc {
	return x.arena[i]
}

// live returns the number of traces currently present in the index.
func (x *index) live() int {
	return x.liveCount
}

// freeChain walks a chain from its head (real arena index) and pushes every
// node onto the free list.
func (x *index) freeChain(head int32) {
	for i := head; i >= 0; {
		next := x.arena[i].next
		x.arena[i].next = x.free
		x.free = i
		i = next
	}
}

// sweep examines up to n slots starting at the rotating cursor, wrapping,
// and advances the cursor by n. A trace whose newest loc (tail) has
// gen < minGen is dead: its slot is tombstoned and its chain freed. The
// dead check looks only at the tail because chains are appended in gen
// order, so the tail always holds the newest loc.
func (x *index) sweep(n int, minGen uint32) {
	total := len(x.slots)
	limit := min(n, total)
	for k := range limit {
		i := (x.cursor + k) % total
		s := &x.slots[i]
		if s.head <= 0 { // empty or already tombstoned
			continue
		}
		if x.arena[s.tail-1].gen < minGen {
			x.freeChain(s.head - 1)
			s.head = -1
			s.tail = 0
			x.liveCount--
			x.tombs++
		}
	}
	x.cursor = (x.cursor + n) % total
}

// grow picks the smallest power-of-two table size with live*4 < 3*size and
// rehashes into it, dropping tombstones.
func (x *index) grow() {
	size := 1
	for 3*size <= 4*x.liveCount {
		size <<= 1
	}
	x.rehash(size)
}

// rehash rebuilds the table at the given size, re-inserting live slots only.
// Arena indices are untouched: chains keep pointing at the same loc nodes.
func (x *index) rehash(size int) {
	old := x.slots
	x.slots = make([]slot, size)
	x.liveCount = 0
	x.tombs = 0
	for _, s := range old {
		if s.head <= 0 {
			continue
		}
		i, _ := x.findSlot(s.id)
		x.slots[i] = s
		x.liveCount++
	}
}

// memoryBytes estimates the index's resident memory: the table and the
// arena, at their allocated (not live) capacity.
func (x *index) memoryBytes() int64 {
	return int64(cap(x.slots))*24 + int64(cap(x.arena))*16
}
