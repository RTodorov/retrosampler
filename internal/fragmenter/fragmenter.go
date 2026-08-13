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
	reasons []byte                    // per group: first non-zero detect result
	bases   []bool                    // per group: baseline verdict at creation
	refs    []spanRef                 // ref arena; refs[i].next chains a trace
	flat    []spanRef                 // per-group scratch, chain flattened
	scratch enc                       // marshal output, reused
}

// New returns an empty Fragmenter, ready for use.
func New() *Fragmenter {
	return &Fragmenter{groups: make(map[pcommon.TraceID]int32)}
}

// Fragment groups td's spans by trace ID and invokes fn once per trace with
// the marshaled OTLP fragment, its keep reason and its baseline verdict.
// frag is only valid during the call.
//
// reason is the FIRST non-zero detect result over the group's spans in batch
// order, and a group that has one pays no further detection: detect is not
// called again for its later spans in this batch (skip-once-kept). detect
// receives the owning ResourceSpans and ScopeSpans beside the span, so a
// policy can read resource and scope attributes.
//
// base is baseline(id), evaluated exactly once per group when the group is
// created — it is a per-trace compare, not a per-span one. A baseline-true
// group still runs detect on every span until a reason lands, so a sampled
// trace can upgrade to a stronger reason within the same batch.
//
// A nil detect makes every reason 0; a nil baseline makes every base false.
func (f *Fragmenter) Fragment(td ptrace.Traces,
	detect func(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sp ptrace.Span) byte,
	baseline func(id pcommon.TraceID) bool,
	fn func(id pcommon.TraceID, frag []byte, reason byte, base bool),
) {
	clear(f.groups)
	f.ids = f.ids[:0]
	f.heads = f.heads[:0]
	f.tails = f.tails[:0]
	f.reasons = f.reasons[:0]
	f.bases = f.bases[:0]
	f.refs = f.refs[:0]

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ssp := sss.At(j)
			sps := ssp.Spans()
			for k := 0; k < sps.Len(); k++ {
				sp := sps.At(k)
				id := sp.TraceID()
				r := idx32(len(f.refs))
				f.refs = append(f.refs, spanRef{rs: idx32(i), ss: idx32(j), sp: idx32(k), next: -1})
				g, ok := f.groups[id]
				if !ok {
					g = idx32(len(f.ids))
					f.groups[id] = g
					f.ids = append(f.ids, id)
					f.heads = append(f.heads, r)
					f.tails = append(f.tails, r)
					f.reasons = append(f.reasons, 0)
					f.bases = append(f.bases, baseline != nil && baseline(id))
				} else {
					f.refs[f.tails[g]].next = r
					f.tails[g] = r
				}
				// Skip-once-kept: a group with a verdict pays no further
				// detection — OTTL never runs on an already-decided trace.
				if f.reasons[g] == 0 && detect != nil {
					f.reasons[g] = detect(rs, ssp, sp)
				}
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
		fn(id, f.scratch.b, f.reasons[g], f.bases[g])
	}
}
