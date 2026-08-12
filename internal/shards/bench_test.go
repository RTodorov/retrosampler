// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// BenchmarkOffer measures accepted ingest through the shard layer: hash,
// free-ring handoff and fragment copy, with the shard workers appending
// behind it.
//
// The pacing is the point. A producer that never blocks outruns its
// workers, and the earlier shape did: its committed rows sat at
// 0.87-0.97 sheds/op, which made them a measurement of the queue-full
// early return — the path that copies nothing and wakes no one — rather
// than of ingest. Parking on the target shard's free ring when it is
// empty puts every iteration back on the accepted path. This is the only
// producer, so a ring seen non-empty cannot be empty by the time Offer
// reads it: sheds/op is 0 by construction, and the check below fails the
// run rather than letting a shed regime quietly pass as ingest again.
// Parking rather than spinning also leaves the cores to the workers,
// which is why the wait stays rare (1-10 per 1000 offers measured) and
// does not become the thing being timed.
//
// What the number covers is the accepted path end to end, including the
// cost of waking a parked worker — most of it, in fact, since Offer's own
// instructions are tens of nanoseconds, which is why the row is ~330ns
// where the old shed-regime row was ~40ns. Nothing got slower; the old
// number was 92% early returns. TestOfferZeroAllocs is what pins Offer's
// own instruction path.
//
// Spanning goroutines does cost precision: on an idle m-arm64 reference
// the spread is 12-13%, next to 2-4% for the single-goroutine buffer
// benchmarks, and under load it degrades faster than they do (all of them
// go past the gate's 10% band on a busy machine, which is what the
// dedicated perf runner exists for). So when this row joins ADR-004 r5's
// gated set, its time arm will be the loose one and what bites exactly
// will be allocs/op and the shed check above — the ungated-but-committed
// rows are already in benchmarks/baseline-m-arm64.txt, and
// scripts/bench_gate.sh says what the remaining move is.
func BenchmarkOffer(b *testing.B) {
	const nIDs = 1024

	clk := newFakeClock(time.Unix(1000, 0))
	opts := testOptions(b.TempDir(), clk)
	opts.SegmentSize = 1 << 30 // no roll during measurement
	s, err := New(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			b.Error(err)
		}
	})

	// 96 bytes is a small real fragment: one or two spans of one trace,
	// which is what a batch usually holds per trace.
	frag := bytes.Repeat([]byte{0xAB}, 96)
	ids := make([][16]byte, nIDs)
	owner := make([]*shard, nIDs)
	for n := range uint64(nIDs) {
		ids[n] = testID(n)
		owner[n] = s.shards[shardFor(ids[n], len(s.shards))]
	}
	now := clk.Now()
	// Grow every recycled handoff buffer past the fragment size, and the
	// workers' own high-water marks with them, so the measurement copies
	// into buffers that never need to grow again.
	for range 50 {
		for _, id := range ids {
			s.Offer(id, frag, now)
		}
	}

	shedsBefore := shedTotal(s.Stats())
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		k := i & (nIDs - 1)
		if sh := owner[k]; len(sh.free) == 0 {
			// Park until this shard's worker hands a buffer back, then put
			// it straight back on the ring for Offer to take. The ring is
			// short one buffer while it is held here, so the send has room.
			fb := <-sh.free
			sh.free <- fb
		}
		s.Offer(ids[k], frag, now)
	}
	b.StopTimer()
	sheds := shedTotal(s.Stats()) - shedsBefore
	b.ReportMetric(float64(sheds)/float64(b.N), "sheds/op")
	if sheds != 0 {
		b.Fatalf("%d sheds in the measured window: this benchmark is ingest, not the shed path", sheds)
	}
}
