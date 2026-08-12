// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// idx32 converts a non-negative slice index/length to int32. A batch never
// approaches 2^31 spans in one call; the clamp exists only to satisfy
// gosec's overflow check (G115) and is unreachable in practice.
func idx32(n int) int32 {
	if n < 0 || n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// Fragmenter groups a batch's spans by trace ID and marshals each group.
// Single-threaded; all internal state is reused across calls and only
// grows to a high-water mark (zero steady-state allocations, ADR-004 r2).
type Fragmenter struct {
	groups  map[pcommon.TraceID]int32 // trace → index into heads/tails
	ids     []pcommon.TraceID         // group insertion order
	heads   []int32                   // per group: first ref (arena index)
	tails   []int32                   // per group: last ref
	keeps   []bool                    // per group: detect hit on any span
	refs    []spanRef                 // ref arena; refs[i].next chains a trace
	flat    []spanRef                 // per-group scratch, chain flattened
	scratch enc                       // marshal output, reused
}

// New returns an empty Fragmenter, ready for use.
func New() *Fragmenter {
	return &Fragmenter{groups: make(map[pcommon.TraceID]int32)}
}

// Fragment groups td's spans by trace ID and invokes fn once per trace with
// the marshaled OTLP fragment. frag is only valid during the call. keep is
// the OR of detect over the trace's spans in this batch; a nil detect makes
// every keep false.
func (f *Fragmenter) Fragment(td ptrace.Traces, detect func(ptrace.Span) bool,
	fn func(id pcommon.TraceID, frag []byte, keep bool),
) {
	clear(f.groups)
	f.ids = f.ids[:0]
	f.heads = f.heads[:0]
	f.tails = f.tails[:0]
	f.keeps = f.keeps[:0]
	f.refs = f.refs[:0]

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				sp := sps.At(k)
				id := sp.TraceID()
				hit := detect != nil && detect(sp)
				r := idx32(len(f.refs))
				f.refs = append(f.refs, spanRef{rs: idx32(i), ss: idx32(j), sp: idx32(k), next: -1})
				g, ok := f.groups[id]
				if !ok {
					g = idx32(len(f.ids))
					f.groups[id] = g
					f.ids = append(f.ids, id)
					f.heads = append(f.heads, r)
					f.tails = append(f.tails, r)
					f.keeps = append(f.keeps, hit)
					continue
				}
				if hit {
					f.keeps[g] = true
				}
				f.refs[f.tails[g]].next = r
				f.tails[g] = r
			}
		}
	}

	for g, id := range f.ids {
		f.flat = f.flat[:0]
		for r := f.heads[g]; r >= 0; r = f.refs[r].next {
			f.flat = append(f.flat, f.refs[r])
		}
		f.scratch.b = f.scratch.b[:0]
		putGroup(&f.scratch, td, f.flat)
		fn(id, f.scratch.b, f.keeps[g])
	}
}
