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

// span returns a one-span hierarchy the tests mutate before Eval.
func span() (ptrace.ResourceSpans, ptrace.ScopeSpans, ptrace.Span) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	return rs, ss, ss.Spans().AppendEmpty()
}

func build(t *testing.T, cfg Config) *Detector {
	t.Helper()
	d, err := Build(cfg, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	return d
}

var t0 = time.Unix(1_700_000_000, 0)

func TestEvalKeepOnError(t *testing.T) {
	d := build(t, Config{KeepOnError: true})
	rs, ss, sp := span()
	assert.Zero(t, d.Eval(rs, ss, sp, t0), "healthy span: no verdict")
	sp.Status().SetCode(ptrace.StatusCodeError)
	assert.Equal(t, bus.ReasonError, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.DetectedKeeps(bus.ReasonError))
}

func TestEvalSpanLatency(t *testing.T) {
	d := build(t, Config{SpanLatencyThreshold: 100 * time.Millisecond})
	rs, ss, sp := span()
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(99 * time.Millisecond)))
	assert.Zero(t, d.Eval(rs, ss, sp, t0), "under threshold")
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(101 * time.Millisecond)))
	assert.Equal(t, bus.ReasonSpanLatency, d.Eval(rs, ss, sp, t0))
}

func TestEvalSpanLatencyNegativeClampsToZero(t *testing.T) {
	// ADR-008 r7 gate: End before Start is skew; the clamped duration 0
	// must not fire any threshold, and the clamp is counted.
	d := build(t, Config{SpanLatencyThreshold: time.Nanosecond})
	rs, ss, sp := span()
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(-time.Second)))
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.SkewClamped())
}

func TestEvalTraceLatencyFromElapsed(t *testing.T) {
	d := build(t, Config{
		TraceLatencyThreshold: 2 * time.Second,
		T0Attribute:           "baggage.t0",
		ElapsedMSAttribute:    "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.elapsed_ms", 1999)
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	sp.Attributes().PutStr("baggage.elapsed_ms", "2001")
	assert.Equal(t, bus.ReasonTraceLatency, d.Eval(rs, ss, sp, t0))
}

func TestEvalTraceAge(t *testing.T) {
	d := build(t, Config{
		TraceAgeThreshold:  time.Minute,
		T0Attribute:        "baggage.t0",
		ElapsedMSAttribute: "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.t0", t0.Add(-2*time.Minute).UnixMilli())
	assert.Equal(t, bus.ReasonTraceAge, d.Eval(rs, ss, sp, t0))
}

func TestEvalTraceAgeFutureT0ClampsToZero(t *testing.T) {
	// ADR-008 r7 gate: a T0 ahead of the local clock is skew; age clamps
	// to 0, no fire, clamp counted.
	d := build(t, Config{
		TraceAgeThreshold:  time.Nanosecond,
		T0Attribute:        "baggage.t0",
		ElapsedMSAttribute: "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.t0", t0.Add(time.Hour).UnixMilli())
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.SkewClamped())
}

func TestEvalNegativeElapsedClampsToZero(t *testing.T) {
	d := build(t, Config{
		TraceLatencyThreshold: time.Nanosecond,
		T0Attribute:           "baggage.t0",
		ElapsedMSAttribute:    "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.elapsed_ms", -100)
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.SkewClamped())
}

func TestEvalDivergence(t *testing.T) {
	d := build(t, Config{
		TraceLatencyThreshold: time.Hour, // enabled but never fires here
		T0Attribute:           "baggage.t0",
		ElapsedMSAttribute:    "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.t0", t0.Add(-10*time.Second).UnixMilli())
	sp.Attributes().PutInt("baggage.elapsed_ms", 4000)
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	// age 10000ms − elapsed 4000ms: 6s of queue-wait or dropped baggage.
	assert.Equal(t, int64(6000), d.DivergenceMS())
}

func TestEvalDivergenceClampedAge(t *testing.T) {
	// A future T0 clamps age to 0, so the stored divergence must be
	// 0 − elapsed, not the raw negative age.
	d := build(t, Config{
		TraceLatencyThreshold: time.Hour,
		T0Attribute:           "baggage.t0",
		ElapsedMSAttribute:    "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutInt("baggage.t0", t0.Add(time.Hour).UnixMilli())
	sp.Attributes().PutInt("baggage.elapsed_ms", 4000)
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, int64(-4000), d.DivergenceMS())
}

func TestEvalMalformedBaggageCounted(t *testing.T) {
	d := build(t, Config{
		TraceLatencyThreshold: time.Second,
		T0Attribute:           "baggage.t0",
		ElapsedMSAttribute:    "baggage.elapsed_ms",
	})
	rs, ss, sp := span()
	sp.Attributes().PutStr("baggage.elapsed_ms", "not-a-number")
	assert.Zero(t, d.Eval(rs, ss, sp, t0))
	assert.Equal(t, uint64(1), d.BaggageMalformed())
}

func TestEvalOrderErrorBeatsLatency(t *testing.T) {
	// First hit wins in chain order: an error span over the latency
	// threshold reports ReasonError.
	d := build(t, Config{KeepOnError: true, SpanLatencyThreshold: time.Millisecond})
	rs, ss, sp := span()
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(time.Second)))
	assert.Equal(t, bus.ReasonError, d.Eval(rs, ss, sp, t0))
}

func TestEnabled(t *testing.T) {
	assert.False(t, build(t, Config{}).Enabled())
	assert.True(t, build(t, Config{KeepOnError: true}).Enabled())
	assert.True(t, build(t, Config{SpanLatencyThreshold: time.Second}).Enabled())
	assert.True(t, build(t, Config{BaselineRate: 0.01}).Enabled())
}
