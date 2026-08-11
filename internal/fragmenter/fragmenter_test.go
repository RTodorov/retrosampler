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
	f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
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
	f.Fragment(td, func(pcommon.TraceID, []byte) {})
	f.Fragment(td, func(_ pcommon.TraceID, frag []byte) {
		dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
		require.NoError(t, err)
		assert.Positive(t, dec.SpanCount())
	})
}

func TestFragmentZeroAllocSteadyState(t *testing.T) {
	f := New()
	td := testBatch(100, 4)
	noop := func(pcommon.TraceID, []byte) {}
	for range 100 { // warm every internal high-water mark
		f.Fragment(td, noop)
	}
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, noop)
	})
	assert.Zero(t, avg, "hot-path bookkeeping allocs must be 0 (ADR-004 r2)")
}
