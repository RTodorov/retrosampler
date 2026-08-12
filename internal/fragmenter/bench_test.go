// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// benchFragmentSpans is the span count in the benchmark fragment: one
// trace's worth of one instance's spans, the unit the flusher decodes.
const benchFragmentSpans = 20

// benchTrace builds a single trace of server spans carrying the
// attributes a real pipeline puts on them, so the decode cost tracks
// something a deployment would actually flush rather than a bare shell.
func benchTrace() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	rs.Resource().Attributes().PutStr("service.version", "2.11.4")
	rs.Resource().Attributes().PutStr("k8s.pod.name", "checkout-7d4f9c8b5d-x2k9m")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
	ss.Scope().SetVersion("v0.62.0")
	for k := range benchFragmentSpans {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID{0xAB, 0xCD})
		sp.SetSpanID(pcommon.SpanID{byte(k + 1)})
		sp.SetParentSpanID(pcommon.SpanID{byte(k)})
		sp.SetName("GET /api/v1/cart")
		sp.SetKind(ptrace.SpanKindServer)
		sp.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000))
		sp.SetEndTimestamp(pcommon.Timestamp(1_700_000_000_004_000_000))
		a := sp.Attributes()
		a.PutStr("http.request.method", "GET")
		a.PutStr("url.path", "/api/v1/cart")
		a.PutInt("http.response.status_code", 500)
		a.PutStr("server.address", "checkout.svc.cluster.local")
		a.PutBool("error", true)
		ev := sp.Events().AppendEmpty()
		ev.SetName("exception")
		ev.SetTimestamp(pcommon.Timestamp(1_700_000_000_003_000_000))
		ev.Attributes().PutStr("exception.type", "context.DeadlineExceeded")
		sp.Status().SetCode(ptrace.StatusCodeError)
		sp.Status().SetMessage("upstream timeout")
	}
	return td
}

// BenchmarkDecode measures the flush path's inverse of the encoder over
// one realistic fragment. Its allocations are expected and exempt from
// the zero-alloc rule (ADR-004 scopes that to ingest); what is worth
// gating is that their COUNT does not grow, since every kept trace pays
// this per fragment. The committed baseline row is in
// benchmarks/baseline-m-arm64.txt; scripts/bench_gate.sh says what still
// has to move for ADR-004 r5 to gate it.
func BenchmarkDecode(b *testing.B) {
	var frag []byte
	New().Fragment(benchTrace(), nil, func(_ pcommon.TraceID, f []byte, _ bool) {
		frag = append(frag[:0], f...)
	})
	if len(frag) == 0 {
		b.Fatal("fragmenter produced no fragment")
	}

	var td ptrace.Traces
	b.ReportAllocs()
	for b.Loop() {
		var err error
		td, err = Decode(frag)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Reading the result back keeps the decode from being optimized away
	// and pins what the number covers: a whole fragment, every span.
	if td.SpanCount() != benchFragmentSpans {
		b.Fatalf("decoded %d spans, want %d", td.SpanCount(), benchFragmentSpans)
	}
}
