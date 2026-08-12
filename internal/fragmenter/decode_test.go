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

// richTraces builds a batch exercising every encoded field shape: two
// resources, two scopes, attributes of every value type, events, links,
// status, flags, schema URLs, and two interleaved trace IDs.
func richTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	for r := range 2 {
		rs := td.ResourceSpans().AppendEmpty()
		rs.SetSchemaUrl("https://example.test/res")
		rs.Resource().Attributes().PutStr("service.name", "svc")
		rs.Resource().Attributes().PutInt("res.int", -42)
		for s := range 2 {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.SetSchemaUrl("https://example.test/scope")
			ss.Scope().SetName("scope")
			ss.Scope().SetVersion("v1")
			for k := range 2 {
				sp := ss.Spans().AppendEmpty()
				sp.SetTraceID(pcommon.TraceID{byte(k + 1), 0xEE})
				sp.SetSpanID(pcommon.SpanID{byte(r + 1), byte(s + 1), byte(k + 1)})
				sp.SetParentSpanID(pcommon.SpanID{9, 9})
				sp.SetName("op")
				sp.SetKind(ptrace.SpanKindServer)
				sp.SetStartTimestamp(1000)
				sp.SetEndTimestamp(2000)
				sp.SetFlags(257)
				sp.TraceState().FromRaw("k=v")
				a := sp.Attributes()
				a.PutStr("s", "x")
				a.PutBool("b", true)
				a.PutInt("i", -7)
				a.PutDouble("d", 3.5)
				a.PutEmptyBytes("y").FromRaw([]byte{1, 2})
				sl := a.PutEmptySlice("sl")
				sl.AppendEmpty().SetStr("in")
				a.PutEmptyMap("m").PutStr("mk", "mv")
				ev := sp.Events().AppendEmpty()
				ev.SetName("event")
				ev.SetTimestamp(1500)
				ev.Attributes().PutStr("ek", "ev")
				lk := sp.Links().AppendEmpty()
				lk.SetTraceID(pcommon.TraceID{0xAA})
				lk.SetSpanID(pcommon.SpanID{0xBB})
				lk.Attributes().PutInt("lk", 1)
				st := sp.Status()
				st.SetCode(ptrace.StatusCodeError)
				st.SetMessage("boom")
			}
		}
	}
	return td
}

// TestFragmentDecodeRoundTrip proves Decode inverts the hand-rolled
// encoder: every fragment decodes cleanly, spans land under the right
// resource/scope with fields intact, and the batch's span population is
// partitioned exactly across the per-trace fragments.
func TestFragmentDecodeRoundTrip(t *testing.T) {
	td := richTraces()
	f := New()
	total := 0
	f.Fragment(td, nil, func(id pcommon.TraceID, frag []byte, _ bool) {
		got, err := Decode(frag)
		require.NoError(t, err, "fragment must be a valid TracesData message")
		require.Positive(t, got.SpanCount())
		total += got.SpanCount()
		rss := got.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			v, ok := rs.Resource().Attributes().Get("service.name")
			require.True(t, ok)
			assert.Equal(t, "svc", v.Str())
			assert.Equal(t, "https://example.test/res", rs.SchemaUrl())
			sss := rs.ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				ss := sss.At(j)
				assert.Equal(t, "scope", ss.Scope().Name())
				for k := 0; k < ss.Spans().Len(); k++ {
					sp := ss.Spans().At(k)
					assert.Equal(t, id, sp.TraceID(), "fragment holds only its trace")
					assert.Equal(t, "op", sp.Name())
					assert.Equal(t, ptrace.StatusCodeError, sp.Status().Code())
					assert.Equal(t, "boom", sp.Status().Message())
					assert.Equal(t, 1, sp.Events().Len())
					assert.Equal(t, 1, sp.Links().Len())
					d, ok := sp.Attributes().Get("d")
					require.True(t, ok)
					assert.InEpsilon(t, 3.5, d.Double(), 1e-9)
				}
			}
		}
	})
	assert.Equal(t, td.SpanCount(), total, "fragments partition the batch")
}

func TestDecodeRejectsGarbage(t *testing.T) {
	_, err := Decode([]byte{0xFF, 0xFF, 0xFF})
	assert.Error(t, err)
}
