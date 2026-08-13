// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package detect composes the local keep-detection chain (ADR-008): the
// guaranteed-zero-alloc built-ins evaluated per span at ingest, the
// deterministic baseline, and OTTL user policies. One Detector is built
// per processor start and shared by every pooled fragmenter closure —
// it is immutable after Build; all mutable state is atomic counters.
package detect

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// nReasons sizes the per-reason counter array: reasons are small
// contiguous bytes with 0 unused (bus.ReasonBaseline is the highest).
const nReasons = int(bus.ReasonBaseline) + 1

// Policy is one named OTTL span condition (ADR-008 r2). Name keys
// telemetry and the per-policy-chain benchmark baseline; Condition is an
// ottlspan boolean expression, OR-composed across policies.
type Policy struct {
	Name      string
	Condition string
}

// Config selects and parameterizes the detection chain. Zero values
// disable: a zero threshold compiles its condition out of the chain
// entirely, so the default config's chain is keep-on-error alone.
type Config struct {
	KeepOnError           bool
	SpanLatencyThreshold  time.Duration
	TraceLatencyThreshold time.Duration
	TraceAgeThreshold     time.Duration
	BaselineRate          float64
	T0Attribute           string
	ElapsedMSAttribute    string
	Policies              []Policy
}

// Detector evaluates the chain. Goroutine-safe: shared by every pooled
// fragmenter closure and read by telemetry callbacks concurrently.
type Detector struct {
	keepOnError   bool
	spanLatencyNS uint64 // unsigned to match pdata's timestamp arithmetic
	traceLatMS    int64
	traceAgeMS    int64
	t0Key         string
	elapsedKey    string
	readBaggage   bool

	baselineThreshold uint64
	policies          []*compiledPolicy
	logger            *zap.Logger

	detected         [nReasons]atomic.Uint64
	skewClamped      atomic.Uint64
	baggageMalformed atomic.Uint64
	divergenceMS     atomic.Int64
}

// Build compiles cfg into a Detector. The telemetry settings feed the
// OTTL parser; the built-ins need none of it.
func Build(cfg Config, set component.TelemetrySettings) (*Detector, error) {
	d := &Detector{
		keepOnError: cfg.KeepOnError,
		traceLatMS:  thresholdMillis(cfg.TraceLatencyThreshold),
		traceAgeMS:  thresholdMillis(cfg.TraceAgeThreshold),
		t0Key:       cfg.T0Attribute,
		elapsedKey:  cfg.ElapsedMSAttribute,
		logger:      set.Logger,
	}
	// A zero-valued TelemetrySettings would otherwise leave the warn call
	// in evalPolicies one nil dereference from panicking on the hot path.
	if d.logger == nil {
		d.logger = zap.NewNop()
	}
	// A negative threshold is nonsense config; leaving the field zero
	// compiles the condition out, same as the zero value.
	if ns := cfg.SpanLatencyThreshold.Nanoseconds(); ns > 0 {
		d.spanLatencyNS = uint64(ns)
	}
	if cfg.BaselineRate > 0 {
		d.baselineThreshold = uint64(math.Round(cfg.BaselineRate * float64(uint64(1)<<56)))
	}
	d.readBaggage = d.traceLatMS > 0 || d.traceAgeMS > 0
	pols, err := compilePolicies(cfg.Policies, set)
	if err != nil {
		return nil, err
	}
	d.policies = pols
	return d, nil
}

// thresholdMillis converts a baggage threshold to whole milliseconds,
// the finest granularity baggage carries. Any positive duration floors
// to 1ms rather than 0: plain truncation would turn a configured
// sub-millisecond threshold into a silently disabled condition.
func thresholdMillis(d time.Duration) int64 {
	if ms := d.Milliseconds(); ms > 0 {
		return ms
	} else if d > 0 {
		return 1
	}
	return 0
}

// Enabled reports whether any condition can ever fire; false lets the
// processor skip the per-span call entirely.
func (d *Detector) Enabled() bool {
	return d.keepOnError || d.spanLatencyNS > 0 || d.readBaggage ||
		d.baselineThreshold > 0 || len(d.policies) > 0
}

// Eval runs the chain over one span, cheapest first, first hit wins.
// Zero-alloc through the built-ins (ADR-004 r2); the OTTL tail is
// exempt and priced by its own benchmark.
func (d *Detector) Eval(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, sp ptrace.Span, now time.Time) byte {
	if d.keepOnError && sp.Status().Code() == ptrace.StatusCodeError {
		return d.hit(bus.ReasonError)
	}
	if d.spanLatencyNS > 0 {
		// Unsigned throughout: pdata timestamps are uint64 nanoseconds,
		// so the clamp is the end<start test rather than a sign test on
		// a difference that would wrap.
		start, end := sp.StartTimestamp(), sp.EndTimestamp()
		switch {
		case end < start:
			d.skewClamped.Add(1) // clamped to 0, fires nothing
		case uint64(end-start) > d.spanLatencyNS:
			return d.hit(bus.ReasonSpanLatency)
		}
	}
	if d.readBaggage {
		if r := d.evalBaggage(sp, now); r != 0 {
			return d.hit(r)
		}
	}
	return d.evalPolicies(rs, ss, sp)
}

// evalBaggage reads BOTH keys whenever either baggage condition is on:
// the divergence health signal needs the pair (ADR-003 r5), and the
// extra lookup is inside the alloc gate's budget.
func (d *Detector) evalBaggage(sp ptrace.Span, now time.Time) byte {
	attrs := sp.Attributes()
	elapsed, elOK, elBad := readMillis(attrs, d.elapsedKey)
	t0, t0OK, t0Bad := readMillis(attrs, d.t0Key)
	if elBad || t0Bad {
		d.baggageMalformed.Add(1)
	}
	if elOK && elapsed < 0 {
		d.skewClamped.Add(1)
		elapsed = 0
	}
	var age int64
	if t0OK {
		age = now.UnixMilli() - t0
		if age < 0 {
			d.skewClamped.Add(1)
			age = 0
		}
	}
	if elOK && t0OK {
		d.divergenceMS.Store(age - elapsed)
	}
	if d.traceLatMS > 0 && elOK && elapsed > d.traceLatMS {
		return bus.ReasonTraceLatency
	}
	if d.traceAgeMS > 0 && t0OK && age > d.traceAgeMS {
		return bus.ReasonTraceAge
	}
	return 0
}

func (d *Detector) hit(r byte) byte {
	d.detected[r].Add(1)
	return r
}

// Baseline is the deterministic head-sample verdict (ADR-008 r1): the
// trace id's trailing 56 bits — the OTel consistent-probability
// randomness source, random under W3C trace-context level 2 — compared
// against a threshold precomputed from BaselineRate. No hash, no float
// on the hot path, identical on every instance by construction. Never
// published; the caller routes a true through the local-only keep path.
func (d *Detector) Baseline(id pcommon.TraceID) bool {
	if d.baselineThreshold == 0 {
		return false
	}
	if binary.BigEndian.Uint64(id[8:])&(1<<56-1) < d.baselineThreshold {
		d.detected[bus.ReasonBaseline].Add(1)
		return true
	}
	return false
}

// DetectedKeeps reports detection events for one reason — raw verdict
// production, pre-decided-set dedup; kept.local counts decisions.
func (d *Detector) DetectedKeeps(reason byte) uint64 {
	if int(reason) >= nReasons {
		return 0
	}
	return d.detected[reason].Load()
}

// SkewClamped reports negative-duration clamps (ADR-008 r7).
func (d *Detector) SkewClamped() uint64 { return d.skewClamped.Load() }

// BaggageMalformed reports present-but-unusable baggage attributes.
func (d *Detector) BaggageMalformed() uint64 { return d.baggageMalformed.Load() }

// DivergenceMS reports the last observed (now−T0)−elapsed_ms sample.
func (d *Detector) DivergenceMS() int64 { return d.divergenceMS.Load() }
