// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func tid(b byte) pcommon.TraceID {
	return pcommon.TraceID{b, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}
func sid(b byte) pcommon.SpanID { return pcommon.SpanID{b, 2, 3, 4, 5, 6, 7, 8} }

func fullSpan(sp ptrace.Span, id byte) {
	sp.SetTraceID(tid(1))
	sp.SetSpanID(sid(id))
	sp.SetParentSpanID(sid(id + 100))
	sp.TraceState().FromRaw("k=v")
	sp.SetName("op")
	sp.SetKind(ptrace.SpanKindServer)
	sp.SetStartTimestamp(pcommon.Timestamp(1_000_000_001))
	sp.SetEndTimestamp(pcommon.Timestamp(2_000_000_002))
	sp.SetFlags(0x101)
	sp.Attributes().PutStr("http.route", "/x")
	sp.Attributes().PutInt("code", 500)
	sp.SetDroppedAttributesCount(3)
	ev := sp.Events().AppendEmpty()
	ev.SetTimestamp(pcommon.Timestamp(1_500_000_000))
	ev.SetName("exception")
	ev.Attributes().PutStr("msg", "boom")
	sp.SetDroppedEventsCount(1)
	lk := sp.Links().AppendEmpty()
	lk.SetTraceID(tid(9))
	lk.SetSpanID(sid(9))
	lk.Attributes().PutBool("sampled", true)
	sp.SetDroppedLinksCount(2)
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.Status().SetMessage("bad")
}

func jsonOf(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	return string(b)
}

func TestGroupEncodeRoundTrip(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc-a")
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("lib")
	ss.Scope().SetVersion("1.2.3")
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	fullSpan(ss.Spans().AppendEmpty(), 1)
	fullSpan(ss.Spans().AppendEmpty(), 2)
	// same trace under a second resource → two ResourceSpans runs
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "svc-b")
	ss2 := rs2.ScopeSpans().AppendEmpty()
	fullSpan(ss2.Spans().AppendEmpty(), 3)

	refs := []spanRef{{rs: 0, ss: 0, sp: 0, next: -1}, {rs: 0, ss: 0, sp: 1, next: -1}, {rs: 1, ss: 0, sp: 0, next: -1}}
	var e enc
	putGroup(&e, td, refs)
	assert.Len(t, e.b, sizeGroup(td, refs), "size pass must match encode pass")

	got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	// expected = the original td (all spans referenced, structure preserved)
	assert.JSONEq(t, jsonOf(t, td), jsonOf(t, got))
}

// TestGroupEncodeMultiScopeRun covers the inner run-split branch of
// sizeRSRun/putGroup: two ScopeSpans runs under one ResourceSpans, for the
// same trace.
func TestGroupEncodeMultiScopeRun(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc-a")
	ss0 := rs.ScopeSpans().AppendEmpty()
	ss0.Scope().SetName("lib-a")
	fullSpan(ss0.Spans().AppendEmpty(), 1)
	ss1 := rs.ScopeSpans().AppendEmpty()
	ss1.Scope().SetName("lib-b")
	fullSpan(ss1.Spans().AppendEmpty(), 2)

	refs := []spanRef{{rs: 0, ss: 0, sp: 0, next: -1}, {rs: 0, ss: 1, sp: 0, next: -1}}
	var e enc
	putGroup(&e, td, refs)
	assert.Len(t, e.b, sizeGroup(td, refs), "size pass must match encode pass")

	got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	assert.JSONEq(t, jsonOf(t, td), jsonOf(t, got))
}

func TestGroupEncodeSubset(t *testing.T) {
	// two traces interleaved in one scope; encode only trace A's spans
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	a := ss.Spans().AppendEmpty()
	a.SetTraceID(tid(1))
	a.SetName("a")
	b := ss.Spans().AppendEmpty()
	b.SetTraceID(tid(2))
	b.SetName("b")
	a2 := ss.Spans().AppendEmpty()
	a2.SetTraceID(tid(1))
	a2.SetName("a2")

	var e enc
	putGroup(&e, td, []spanRef{{rs: 0, ss: 0, sp: 0, next: -1}, {rs: 0, ss: 0, sp: 2, next: -1}})
	got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	require.Equal(t, 2, got.SpanCount())
	sps := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	assert.Equal(t, "a", sps.At(0).Name())
	assert.Equal(t, "a2", sps.At(1).Name())
}
