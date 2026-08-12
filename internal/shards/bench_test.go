// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// BenchmarkOffer exercises Offer under full-speed load: hash, free-ring
// handoff, and fragment copy, with the shard workers appending
// concurrently. It is informational and NOT part of the ADR-004 r5 gated
// set — nothing here fails a regression, by ruling.
//
// A producer that never blocks outruns its workers eventually, so sheds
// are part of what full-speed load measures. They are reported per op
// rather than suppressed: the warm-up's sheds are excluded so the number
// describes the measured window. The committed baseline rows sit at
// 0.87-0.97 sheds/op, so by that criterion those numbers describe the
// saturated/shed regime — the queue-full early return — and not the
// routing hot path. Reshaping this benchmark around the non-shed path
// (and deciding whether it then joins the gated set) is a recorded
// stage-3 item; TestOfferZeroAllocs is what gates the routing path today.
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

	frag := bytes.Repeat([]byte{0xAB}, 512)
	ids := make([][16]byte, nIDs)
	for n := range uint64(nIDs) {
		ids[n] = testID(n)
	}
	now := clk.Now()
	// Grow every recycled handoff buffer past 512 bytes, and the workers'
	// own high-water marks with them, so the measurement copies into
	// buffers that never need to grow again.
	for range 50 {
		for _, id := range ids {
			s.Offer(id, frag, now)
		}
	}

	shedsBefore := s.Stats().ShedQueueFull
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.Offer(ids[i&(nIDs-1)], frag, now)
	}
	b.StopTimer()
	sheds := s.Stats().ShedQueueFull - shedsBefore
	b.ReportMetric(float64(sheds)/float64(b.N), "sheds/op")
}
