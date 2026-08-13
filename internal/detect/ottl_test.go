// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package detect

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rtodorov/retrosampler/internal/bus"
)

func TestCheckPoliciesAcceptsValid(t *testing.T) {
	require.NoError(t, CheckPolicies([]Policy{
		{Name: "db", Condition: `span.attributes["db.system"] == "postgres"`},
	}))
}

func TestCheckPoliciesRejects(t *testing.T) {
	tests := []struct {
		name     string
		policies []Policy
		want     string
	}{
		{"empty name", []Policy{{Name: "", Condition: `span.name == "x"`}}, "name"},
		{"duplicate name", []Policy{
			{Name: "a", Condition: `span.name == "x"`},
			{Name: "a", Condition: `span.name == "y"`},
		}, "duplicate"},
		{"parse failure", []Policy{{Name: "bad", Condition: `span.name ==`}}, "bad"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckPolicies(tt.policies)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestEvalPolicyMatch(t *testing.T) {
	d := build(t, Config{Policies: []Policy{
		{Name: "slow-db", Condition: `span.attributes["db.system"] == "postgres"`},
	}})
	rs, ss, sp := span()
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	sp.Attributes().PutStr("db.system", "postgres")
	assert.Equal(t, bus.ReasonPolicy, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.PolicyMatches(0))
	// A match must be routed through hit(), not returned raw: the
	// per-reason counter is what telemetry reports as a policy keep.
	assert.Equal(t, uint64(1), d.DetectedKeeps(bus.ReasonPolicy))
}

func TestEvalPolicyOrderFirstMatchWins(t *testing.T) {
	d := build(t, Config{Policies: []Policy{
		{Name: "first", Condition: `span.name == "hit"`},
		{Name: "second", Condition: `span.name != ""`},
	}})
	rs, ss, sp := span()
	sp.SetName("hit")
	assert.Equal(t, bus.ReasonPolicy, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.PolicyMatches(0))
	assert.Zero(t, d.PolicyMatches(1), "config order, first match stops the walk")
}

func TestEvalPolicyErrorIgnoredAndCounted(t *testing.T) {
	// Ignore-and-count semantics: an eval error means "no match", the
	// per-policy error counter moves, and the chain continues to the
	// NEXT policy rather than failing the span.
	// Int() over a map-valued attribute errs at EVAL time — the value is
	// an unsupported type. A non-numeric STRING would not do: that path
	// returns no value and no error, which is a silent no-match. Nor
	// would a literal bad regex in IsMatch: converters compile literal
	// args at parse time, so that failure lands in Build, not Eval.
	d := build(t, Config{Policies: []Policy{
		{Name: "errs", Condition: `Int(span.attributes["a"]) > 5`},
		{Name: "after", Condition: `span.name == "kept"`},
	}})
	rs, ss, sp := span()
	sp.Attributes().PutEmptyMap("a").PutStr("k", "v")
	sp.SetName("kept")
	assert.Equal(t, bus.ReasonPolicy, d.Eval(rs, ss, sp, t0), "chain survives the erroring policy")
	assert.Equal(t, uint64(1), d.PolicyEvalErrors(0))
	assert.Equal(t, uint64(1), d.PolicyMatches(1))
}

func TestPolicyTelemetryAccessors(t *testing.T) {
	// Task 10 reads counters by index and resolves the name only at
	// collect time, so index order must be config order and an
	// out-of-range index must read 0 rather than panic.
	d := build(t, Config{Policies: []Policy{
		{Name: "first", Condition: `span.name == "a"`},
		{Name: "second", Condition: `span.name == "b"`},
	}})
	assert.Equal(t, []string{"first", "second"}, d.PolicyNames())
	for _, i := range []int{-1, 2} {
		assert.Zero(t, d.PolicyMatches(i))
		assert.Zero(t, d.PolicyEvalErrors(i))
	}
	assert.Empty(t, build(t, Config{}).PolicyNames(), "no policies: no names")
}

func TestBuildRejectsBadPolicy(t *testing.T) {
	_, err := Build(Config{Policies: []Policy{{Name: "bad", Condition: `nonsense(((`}}},
		componenttest.NewNopTelemetrySettings())
	require.Error(t, err)
}

func TestEvalBuiltinsBeatPolicies(t *testing.T) {
	d := build(t, Config{KeepOnError: true, Policies: []Policy{
		{Name: "always", Condition: `span.name != ""`},
	}})
	rs, ss, sp := span()
	sp.SetName("x")
	sp.Status().SetCode(ptrace.StatusCodeError)
	assert.Equal(t, bus.ReasonError, d.Eval(rs, ss, sp, t0), "OTTL is last in the chain")
}

func TestEvalPolicyDuration(t *testing.T) {
	// The documented anti-example works but costs allocs — it exists to
	// prove expressiveness, and doc.go points users at the built-in.
	d := build(t, Config{Policies: []Policy{
		{Name: "slow", Condition: `span.end_time_unix_nano - span.start_time_unix_nano > 100000000`},
	}})
	rs, ss, sp := span()
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(200 * time.Millisecond)))
	assert.Equal(t, bus.ReasonPolicy, d.Eval(rs, ss, sp, t0))
}
