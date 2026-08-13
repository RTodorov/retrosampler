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
// the ADR-004 r5 gated set (ADR-010 r8) with its own baseline rows.
//
// The committed rows are 1 alloc/op at 16 B: the string value boxed by
// the compare itself, not the pooled TransformContext, which measures
// 0 allocs on its own. The shape is what decides — an int-attribute or
// status-enum compare of this same form allocates nothing, which is the
// measurement behind ADR-008 r2's "status: 28 ns/0"; a string operand
// boxes whether it comes from an attribute or from span.name.
//
// allocs/op is the number to watch, but it is a ratchet and not a
// budget: the gate compares against the committed integer, so any
// increase above 1 fails. ADR-008 r2's 0–3 range describes OTTL across
// expression shapes, not headroom this benchmark is allowed to spend.
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
