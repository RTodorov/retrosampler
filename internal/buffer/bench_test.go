// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rtodorov/retrosampler/internal/fragmenter"
)

// benchBuffer opens a fresh Buffer plus a Fragmenter and benchBatch, ready
// for a benchmark loop.
func benchBuffer(b *testing.B, segSize int) (*Buffer, *fragmenter.Fragmenter, ptrace.Traces) {
	b.Helper()
	buf, err := Open(b.TempDir(), Options{Window: time.Hour, SegmentSize: segSize}, time.Unix(0, 0))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = buf.Close() })
	return buf, fragmenter.New(), benchBatch()
}

// BenchmarkIngest measures the stage-1 hot path: fragment + append.
// Reported per span. ADR-004 r5 gates time/op and allocs/op regressions.
func BenchmarkIngest(b *testing.B) {
	buf, f, td := benchBuffer(b, 256<<20)
	now := time.Unix(1, 0)
	spans := td.SpanCount()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, now)
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*spans), "ns/span")
}

// BenchmarkKeepFlush measures Collect on traces spread across segments.
func BenchmarkKeepFlush(b *testing.B) {
	buf, f, td := benchBuffer(b, 4<<20)
	now := time.Unix(1, 0)
	var ids []pcommon.TraceID
	seen := map[pcommon.TraceID]bool{}
	for range 200 { // fill across many segments
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, now)
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := ids[i%len(ids)]
		_, _ = buf.Collect([16]byte(id), func([]byte) {})
	}
}

// BenchmarkExpiry measures Expire ticks over a populated buffer.
func BenchmarkExpiry(b *testing.B) {
	buf, f, td := benchBuffer(b, 1<<20)
	for i := range 500 {
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, time.Unix(int64(i), 0))
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		buf.Expire(time.Unix(int64(i%10_000), 0))
	}
}
