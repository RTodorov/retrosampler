// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"encoding/binary"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// benchBatch builds a batch of 25 traces with 4 spans apiece (100 spans
// total), realistic attributes, round-robin across 4 resources — the
// shared shape behind both the alloc gate (Task 13) and the benchmarks
// (Task 15). Trace and span IDs are spread across the full uint64 range
// via binary encoding rather than a small modulus, so growing the trace or
// span counts here never collides two IDs into one (Task 4's pitfall).
func benchBatch() ptrace.Traces {
	const (
		nTraces    = 25
		spansEach  = 4
		nResources = 4
	)
	td := ptrace.NewTraces()
	sss := make([]ptrace.ScopeSpans, nResources)
	for r := range nResources {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("testlib")
		ss.Scope().SetVersion("1.0.0")
		sss[r] = ss
	}

	for t := range nTraces {
		var trID pcommon.TraceID
		binary.BigEndian.PutUint64(trID[8:], lenU64(t)+1)
		res := t % nResources
		for s := range spansEach {
			sp := sss[res].Spans().AppendEmpty()
			sp.SetTraceID(trID)
			var spID pcommon.SpanID
			binary.BigEndian.PutUint64(spID[:], lenU64(t*spansEach+s)+1)
			sp.SetSpanID(spID)
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
