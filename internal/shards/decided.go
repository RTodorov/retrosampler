// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// decidedInitialSlots is the decided set's initial table size and
// decidedInitialRing its initial ring capacity. Both stay powers of two.
const (
	decidedInitialSlots = 1024
	decidedInitialRing  = 1024
)

// A slot holds a live entry's sequence number + 1, which leaves 0 to mark
// an empty slot and all-ones to mark a tombstone. Neither sentinel can
// collide with a real reference: sequence numbers start at 0 and the set
// would have to record 2^64 keeps to reach the tombstone.
const (
	decidedEmpty = 0
	decidedTomb  = math.MaxUint64
)

// decidedEntry is one decided trace: its id and eviction deadline
// (decide time + W, unix nanos).
type decidedEntry struct {
	id       [16]byte
	deadline int64
}

// decidedSet is the per-shard decided/pending-keeps set (ADR-008 r3-r5):
// worker-owned, no locks.
//
// Lookup goes through an open-addressing table in the buffer index's
// idiom (ADR-006 r5), but a slot holds only a sequence number: each id
// and deadline is stored once, in an insertion-ordered ring. That split
// is what buys the memory budget — a Go map keyed by trace ID costs
// 37.7 B/entry at a 1M population before the ring is counted at all,
// where the narrow slots cost 16.8.
//
// Eviction pops the ring head: deadlines are monotone per shard, so head
// order is deadline order, making expiry exact-W and O(1) amortized. It
// is time-based ONLY. A capacity-evicted entry would re-open a decided
// trace to duplicate flushes, which is a correctness bug, not a cache
// miss.
type decidedSet struct {
	// slots is the open-addressing table, indexed by an id's Fibonacci
	// hash: decidedEmpty, decidedTomb, or a live entry's sequence
	// number + 1.
	slots []uint64

	// ring holds the live entries in insertion order. ring[i] carries
	// sequence number baseSeq+i, so a slot's reference survives head
	// compaction — which shifts the ring and rebases baseSeq — without
	// the table being touched. Entries below head are already evicted.
	ring    []decidedEntry
	head    int
	baseSeq uint64

	// tombs counts tombstoned slots, which rehashing clears.
	tombs int
}

func newDecidedSet() *decidedSet {
	return &decidedSet{
		slots: make([]uint64, decidedInitialSlots),
		ring:  make([]decidedEntry, 0, decidedInitialRing),
	}
}

// lenU64 converts a non-negative length, index, or count to uint64, with
// a guard for negative input (gosec G115). Returns 0 for negative input.
// Structural invariant: every caller passes a slice length, an index into
// one, or a counter, so the input is never negative.
func lenU64(n int) uint64 {
	if n < 0 {
		return 0
	}
	// Guard above: n >= 0 here, so the conversion cannot change sign.
	return uint64(n)
}

// tableMask returns n-1 as a uint64 probe mask for an open-addressing
// table of size n (a power of two, always >= 1). Returns 0 for n <= 0.
// Structural invariant: input is len(d.slots), which newDecidedSet seeds
// non-empty and rehash only ever resizes.
func tableMask(n int) uint64 {
	if n <= 0 {
		return 0
	}
	// Guard above: n > 0 here, so the subtraction cannot go negative.
	return uint64(n) - 1
}

// fibHash computes id's Fibonacci hash, folding the low and high 8 bytes
// together with XOR first: real trace IDs often carry a time prefix,
// which would otherwise stay invisible to a hash reading only the other
// half. Same hash family as shardFor and the buffer index. The high bits
// are the well-mixed ones, so startSlot reduces from the top.
func fibHash(id [16]byte) uint64 {
	return (binary.LittleEndian.Uint64(id[:8]) ^ binary.LittleEndian.Uint64(id[8:])) * fibSeed
}

// startSlot returns the slot at which id's probe sequence begins: the
// table-sized index carved from the high bits of its hash.
func (d *decidedSet) startSlot(id [16]byte) uint64 {
	mask := tableMask(len(d.slots))
	shift := 64 - bits.Len64(mask)
	return (fibHash(id) >> shift) & mask
}

// findSlot linear-probes the table for id. If id is present it returns
// the slot holding it and found=true; otherwise the slot id belongs in
// (the first tombstone seen, else the terminating empty slot) and
// found=false.
func (d *decidedSet) findSlot(id [16]byte) (i uint64, found bool) {
	mask := tableMask(len(d.slots))
	start := d.startSlot(id)
	tomb, haveTomb := uint64(0), false
	for p := uint64(0); p <= mask; p++ {
		j := (start + p) & mask
		switch ref := d.slots[j]; {
		case ref == decidedEmpty: // probe ends here
			if haveTomb {
				return tomb, false
			}
			return j, false
		case ref == decidedTomb: // remember the first, keep probing
			if !haveTomb {
				tomb, haveTomb = j, true
			}
		case d.ring[ref-1-d.baseSeq].id == id:
			return j, true
		}
	}
	// Table saturated (shouldn't happen: mark rehashes before the load
	// factor allows it). Fall back to a tombstone.
	return tomb, false
}

// mark records id as decided until deadline. It reports false — and
// records nothing — when id is already decided (duplicate keep).
func (d *decidedSet) mark(id [16]byte, deadline int64) bool {
	if (d.len()+d.tombs)*4 >= 3*len(d.slots) {
		d.rehash()
	}
	i, found := d.findSlot(id)
	if found {
		return false
	}
	if d.slots[i] == decidedTomb {
		d.tombs--
	}
	d.slots[i] = d.baseSeq + lenU64(len(d.ring)) + 1
	d.push(decidedEntry{id: id, deadline: deadline})
	return true
}

// push appends e, doubling the ring when it is full. Growing explicitly
// rather than leaving it to append keeps the capacity a power of two, so
// the set's memory stays a predictable function of its population.
func (d *decidedSet) push(e decidedEntry) {
	if len(d.ring) == cap(d.ring) && cap(d.ring) > 0 {
		grown := make([]decidedEntry, len(d.ring), 2*cap(d.ring))
		copy(grown, d.ring)
		d.ring = grown
	}
	d.ring = append(d.ring, e)
}

// has reports whether id is currently decided.
func (d *decidedSet) has(id [16]byte) bool {
	_, found := d.findSlot(id)
	return found
}

// evict removes every entry whose deadline has passed and returns the
// count. The ring compacts when the dead prefix dominates, so memory
// tracks the live population rather than the historical insert count.
func (d *decidedSet) evict(now int64) int {
	n := 0
	for d.head < len(d.ring) && d.ring[d.head].deadline <= now {
		if i, found := d.findSlot(d.ring[d.head].id); found {
			d.slots[i] = decidedTomb
			d.tombs++
		}
		d.head++
		n++
	}
	switch {
	case d.head == len(d.ring):
		d.baseSeq += lenU64(d.head)
		d.ring = d.ring[:0]
		d.head = 0
	case 2*d.head > len(d.ring):
		live := copy(d.ring, d.ring[d.head:])
		d.baseSeq += lenU64(d.head)
		d.ring = d.ring[:live]
		d.head = 0
	}
	return n
}

// rehash rebuilds the table at the smallest power-of-two size that keeps
// the live population under a 0.75 load factor, dropping tombstones. The
// live entries are exactly ring[head:], and their sequence numbers do not
// move, so the ring is both the source of ids and of the new references.
func (d *decidedSet) rehash() {
	size := decidedInitialSlots
	for 3*size <= 4*d.len() {
		size <<= 1
	}
	d.slots = make([]uint64, size)
	d.tombs = 0
	for i := d.head; i < len(d.ring); i++ {
		j, _ := d.findSlot(d.ring[i].id)
		d.slots[j] = d.baseSeq + lenU64(i) + 1
	}
}

// len is the live decided population: the ring above its evicted head.
func (d *decidedSet) len() int { return len(d.ring) - d.head }
