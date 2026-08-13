// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// keepIs reads a verdict byte the way the pre-verdict tests read a bool:
// any non-zero reason is a keep.
func keepIs(reason byte) bool { return reason != 0 }

// errDetect is the shipping keep-on-error predicate in seam form.
func errDetect(_ ptrace.ResourceSpans, _ ptrace.ScopeSpans, sp ptrace.Span) byte {
	if sp.Status().Code() == ptrace.StatusCodeError {
		return 1
	}
	return 0
}

// allSpans flattens all spans across resources/scopes in td, in iteration order.
func allSpans(td ptrace.Traces) []ptrace.Span {
	var out []ptrace.Span
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				out = append(out, sps.At(k))
			}
		}
	}
	return out
}

// testBatch builds a batch of nTraces traces with spansEach spans apiece,
// realistic attributes, round-robin across 4 resources.
func testBatch(nTraces, spansEach int) ptrace.Traces {
	td := ptrace.NewTraces()
	const nResources = 4
	rss := make([]ptrace.ResourceSpans, nResources)
	sss := make([]ptrace.ScopeSpans, nResources)
	for r := range nResources {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("testlib")
		ss.Scope().SetVersion("1.0.0")
		rss[r] = rs
		sss[r] = ss
	}

	for t := range nTraces {
		trID := tid(byte(t%251 + 1))
		res := t % nResources
		for s := range spansEach {
			sp := sss[res].Spans().AppendEmpty()
			sp.SetTraceID(trID)
			sp.SetSpanID(sid(byte((t*spansEach+s)%251 + 1)))
			sp.SetName(fmt.Sprintf("op-%d", s))
			sp.SetKind(ptrace.SpanKindServer)
			sp.SetStartTimestamp(pcommon.Timestamp(1_000_000_000 + lenU64(s)))
			sp.SetEndTimestamp(pcommon.Timestamp(2_000_000_000 + lenU64(s)))
			sp.Attributes().PutStr("http.method", "GET")
			sp.Attributes().PutStr("http.route", "/api/resource")
			sp.Attributes().PutInt("http.status_code", 200)
		}
	}
	return td
}

func TestFragmentGroupsByTrace(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i, id := range []byte{1, 2, 1, 3, 2, 1} {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(tid(id))
		sp.SetName(fmt.Sprintf("s%d", i))
	}
	got := map[pcommon.TraceID]int{}
	f := New()
	f.Fragment(td, nil, nil, func(id pcommon.TraceID, frag []byte, _ byte, _ bool) {
		dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
		require.NoError(t, err)
		got[id] = dec.SpanCount()
		for _, sp := range allSpans(dec) {
			assert.Equal(t, id, sp.TraceID())
		}
	})
	assert.Equal(t, map[pcommon.TraceID]int{tid(1): 3, tid(2): 2, tid(3): 1}, got)
}

func TestFragmentScratchReuseAcrossCalls(t *testing.T) {
	f := New()
	td := testBatch(50, 5)
	f.Fragment(td, nil, nil, func(pcommon.TraceID, []byte, byte, bool) {})
	f.Fragment(td, nil, nil, func(_ pcommon.TraceID, frag []byte, _ byte, _ bool) {
		dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
		require.NoError(t, err)
		assert.Positive(t, dec.SpanCount())
	})
}

// Detection folds each trace's spans in the batch into one verdict,
// delivered on the same callback as the fragment (single pass).
func TestFragmentDetectFlagsPerTrace(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	mk := func(id byte, code ptrace.StatusCode) {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{id})
		sp.Status().SetCode(code)
	}
	mk(1, ptrace.StatusCodeOk)
	mk(2, ptrace.StatusCodeError) // trace 2: one bad span among good
	mk(2, ptrace.StatusCodeOk)
	mk(1, ptrace.StatusCodeOk)

	got := map[byte]bool{}
	New().Fragment(td, errDetect, nil, func(id pcommon.TraceID, _ []byte, reason byte, _ bool) {
		got[id[0]] = keepIs(reason)
	})
	assert.Equal(t, map[byte]bool{1: false, 2: true}, got)
}

// A hit on any span folds into the group, not just the trace's first span
// in the batch — the usual shape, where an ok root precedes a failing child.
func TestFragmentDetectHitAfterGroupOpens(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for _, code := range []ptrace.StatusCode{ptrace.StatusCodeOk, ptrace.StatusCodeError} {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{1})
		sp.Status().SetCode(code)
	}
	got := map[byte]bool{}
	New().Fragment(td, errDetect, nil, func(id pcommon.TraceID, _ []byte, reason byte, _ bool) {
		got[id[0]] = keepIs(reason)
	})
	assert.Equal(t, map[byte]bool{1: true}, got)
}

func TestFragmentNilDetectNeverKeeps(t *testing.T) {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID{1})
	sp.Status().SetCode(ptrace.StatusCodeError)
	New().Fragment(td, nil, nil, func(_ pcommon.TraceID, _ []byte, reason byte, _ bool) {
		assert.False(t, keepIs(reason))
	})
}

// The reason is the FIRST non-zero detect result in batch order, so a
// later, different verdict cannot overwrite the one already recorded.
func TestFragmentFirstReasonWins(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	id := pcommon.TraceID{1}
	for _, name := range []string{"second-reason", "first-reason"} {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(id)
		sp.SetName(name)
	}
	f := New()
	det := func(_ ptrace.ResourceSpans, _ ptrace.ScopeSpans, sp ptrace.Span) byte {
		if sp.Name() == "second-reason" {
			return 2
		}
		return 3
	}
	var got byte
	f.Fragment(td, det, nil, func(_ pcommon.TraceID, _ []byte, reason byte, _ bool) {
		got = reason
	})
	assert.Equal(t, byte(2), got, "batch-order first hit wins, not the last")
}

func TestFragmentSkipsDetectOnceKept(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	id := pcommon.TraceID{1}
	for range 5 {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(id)
	}
	f := New()
	calls := 0
	det := func(ptrace.ResourceSpans, ptrace.ScopeSpans, ptrace.Span) byte {
		calls++
		return 4
	}
	f.Fragment(td, det, nil, func(pcommon.TraceID, []byte, byte, bool) {})
	assert.Equal(t, 1, calls, "a decided group's later spans must not pay detection")
}

func TestFragmentBaselineOncePerGroupAndUpgrade(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	id := pcommon.TraceID{1}
	for i := range 3 {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(id)
		if i == 2 {
			sp.SetName("err")
		}
	}
	f := New()
	baseCalls := 0
	base := func(pcommon.TraceID) bool { baseCalls++; return true }
	det := func(_ ptrace.ResourceSpans, _ ptrace.ScopeSpans, sp ptrace.Span) byte {
		if sp.Name() == "err" {
			return 1
		}
		return 0
	}
	var reason byte
	var wasBase bool
	f.Fragment(td, det, base, func(_ pcommon.TraceID, _ []byte, r byte, b bool) {
		reason, wasBase = r, b
	})
	assert.Equal(t, 1, baseCalls, "baseline is a per-group compare, not per-span")
	assert.Equal(t, byte(1), reason, "a baseline group must still upgrade on a later hit")
	assert.True(t, wasBase)
}

func TestFragmentNilHooks(t *testing.T) {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID{1})
	f := New()
	var reason byte = 99
	base := true
	f.Fragment(td, nil, nil, func(_ pcommon.TraceID, _ []byte, r byte, b bool) {
		reason, base = r, b
	})
	assert.Zero(t, reason)
	assert.False(t, base)
}

func TestFragmentZeroAllocSteadyState(t *testing.T) {
	f := New()
	td := testBatch(100, 4)
	noop := func(pcommon.TraceID, []byte, byte, bool) {}
	for range 100 { // warm every internal high-water mark
		f.Fragment(td, nil, nil, noop)
	}
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, nil, nil, noop)
	})
	assert.Zero(t, avg, "hot-path bookkeeping allocs must be 0 (ADR-004 r2)")
}

// The shipping path passes real hooks, so the allocation gate has to hold
// with detect and baseline wired in, not only for the nil case.
func TestFragmentZeroAllocWithHooks(t *testing.T) {
	f := New()
	td := testBatch(100, 4)
	noop := func(pcommon.TraceID, []byte, byte, bool) {}
	baseline := func(id pcommon.TraceID) bool { return id[0]&1 == 0 }
	for range 100 { // warm every internal high-water mark
		f.Fragment(td, errDetect, baseline, noop)
	}
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, errDetect, baseline, noop)
	})
	assert.Zero(t, avg, "the hooks must not allocate on the hot path (ADR-004 r2)")
}
