// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package detect

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// BenchmarkPolicyEval prices the OTTL tail (ADR-008 r2): one
// simple-compare policy over a non-matching span — the per-span cost
// every policy-configured deployment pays on every undecided span. In
// the ADR-004 r5 gated set with its own baseline rows; allocs/op is the
// number to watch (0–3 expected, expression-shape-dependent).
func BenchmarkPolicyEval(b *testing.B) {
	d, err := Build(Config{Policies: []Policy{
		{Name: "bench", Condition: `span.attributes["db.system"] == "postgres"`},
	}}, componenttest.NewNopTelemetrySettings())
	if err != nil {
		b.Fatal(err)
	}
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()
	sp.SetName("op")
	sp.Attributes().PutStr("db.system", "mysql") // walks the full condition, no match
	now := time.Unix(1_700_000_000, 0)
	b.ReportAllocs()
	for b.Loop() {
		if d.Eval(rs, ss, sp, now) != 0 {
			b.Fatal("bench span must not match")
		}
	}
}
