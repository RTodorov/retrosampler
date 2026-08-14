// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	// elapsedMSAttribute must match the processor's elapsed_ms_attribute
	// default (factory.go), which is where the trace-latency built-in reads
	// the baggage pair from.
	elapsedMSAttribute   = "baggage.elapsed_ms"
	padAttribute         = "pad"
	serviceNameAttribute = "service.name"
	scopeName            = "loadgen"
	serviceName          = "loadgen"

	// healthySpanDuration is short enough that no span-latency threshold a
	// test would configure can mistake a healthy span for a slow one.
	healthySpanDuration = time.Millisecond

	// keptTraceIDCap bounds kept_trace_ids so a long run cannot turn the
	// summary into a multi-megabyte document. Counts stay exact past it.
	// Enforced only in summary.add: a single batch is far below the cap, so
	// a second check inside generate would be a branch no run can take.
	keptTraceIDCap = 10000
)

// genParams describes one deterministic batch. Everything that varies
// between runs is in here: given the same params and base, generate
// produces byte-identical batches.
type genParams struct {
	Endpoints          int
	Traces             int
	SpansPerTrace      int
	ErrorTraces        int
	SlowSpanTraces     int
	TraceLatencyTraces int
	SlowSpanMS         int
	ElapsedMS          int
	SpanBytes          int
	Seed               uint64
}

// summary is the JSON document the run prints on stdout. Tasks 11 and 12
// consume it, so field names and types are a contract.
type summary struct {
	Traces                       int      `json:"traces"`
	ErrorTraces                  int      `json:"error_traces"`
	SlowSpanTraces               int      `json:"slow_span_traces"`
	TraceLatencyTraces           int      `json:"trace_latency_traces"`
	HealthyTraces                int      `json:"healthy_traces"`
	SpansPerEndpoint             []int    `json:"spans_per_endpoint"`
	BytesSent                    int64    `json:"bytes_sent"`
	ElapsedSeconds               float64  `json:"elapsed_seconds"`
	ExpectedKeptSpansPerEndpoint []int    `json:"expected_kept_spans_per_endpoint"`
	KeptTraceIDs                 []string `json:"kept_trace_ids"`
}

// genResult pairs the per-endpoint batches with the summary fields the
// generator alone can know. BytesSent and ElapsedSeconds stay zero here;
// only the driver, which does the exporting, can fill those.
type genResult struct {
	perEndpoint []ptrace.Traces
	summary     summary
}

type keepClass uint8

const (
	classHealthy keepClass = iota
	classError
	classSlowSpan
	classTraceLatency
)

// classOf assigns keep classes to leading trace indices, in a fixed order
// and with no shuffle: the seed varies the ids, the class mix stays
// countable from the params alone.
func (p genParams) classOf(i int) keepClass {
	switch {
	case i < p.ErrorTraces:
		return classError
	case i < p.ErrorTraces+p.SlowSpanTraces:
		return classSlowSpan
	case i < p.ErrorTraces+p.SlowSpanTraces+p.TraceLatencyTraces:
		return classTraceLatency
	default:
		return classHealthy
	}
}

// generate builds one batch per endpoint. Span j of every trace goes to
// endpoint j%len(endpoints), and the keep marker goes on span 0 only —
// which is endpoint 0 — so every other endpoint holds a healthy-looking
// fragment of a trace that must nonetheless be kept. Flushing those
// fragments is what proves the bus.
//
// It is pure and clock-free: base is the caller's single clock read.
func generate(p genParams, base time.Time) genResult {
	r := genResult{
		perEndpoint: make([]ptrace.Traces, p.Endpoints),
		summary: summary{
			Traces:                       p.Traces,
			ErrorTraces:                  p.ErrorTraces,
			SlowSpanTraces:               p.SlowSpanTraces,
			TraceLatencyTraces:           p.TraceLatencyTraces,
			HealthyTraces:                p.Traces - p.ErrorTraces - p.SlowSpanTraces - p.TraceLatencyTraces,
			SpansPerEndpoint:             make([]int, p.Endpoints),
			ExpectedKeptSpansPerEndpoint: make([]int, p.Endpoints),
		},
	}

	spans := make([]ptrace.SpanSlice, p.Endpoints)
	for e := range r.perEndpoint {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr(serviceNameAttribute, serviceName)
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName(scopeName)
		r.perEndpoint[e] = td
		spans[e] = ss.Spans()
	}

	rng := newRNG(p.Seed)
	pad := strings.Repeat("x", max(p.SpanBytes, 0))
	start := pcommon.NewTimestampFromTime(base)
	healthyEnd := pcommon.NewTimestampFromTime(base.Add(healthySpanDuration))

	for i := range p.Traces {
		traceID := rng.traceID()
		class := p.classOf(i)

		var root pcommon.SpanID
		for j := range p.SpansPerTrace {
			e := j % p.Endpoints
			spanID := rng.spanID()
			if j == 0 {
				root = spanID
			}

			s := spans[e].AppendEmpty()
			s.SetTraceID(traceID)
			s.SetSpanID(spanID)
			if j > 0 {
				s.SetParentSpanID(root)
			}
			s.SetName("loadgen-span")
			s.SetKind(ptrace.SpanKindServer)
			s.SetStartTimestamp(start)
			s.SetEndTimestamp(healthyEnd)
			if pad != "" {
				s.Attributes().PutStr(padAttribute, pad)
			}
			if j == 0 {
				mark(s, class, base, p)
			}

			r.summary.SpansPerEndpoint[e]++
			if class != classHealthy {
				r.summary.ExpectedKeptSpansPerEndpoint[e]++
			}
		}

		if class != classHealthy {
			r.summary.KeptTraceIDs = append(r.summary.KeptTraceIDs, hex.EncodeToString(traceID[:]))
		}
	}
	return r
}

// mark arms exactly one of the processor's built-in keep conditions on the
// root span. Each class arms a different detector, so a compose assert can
// tell which one fired.
func mark(s ptrace.Span, class keepClass, base time.Time, p genParams) {
	switch class {
	case classError:
		s.Status().SetCode(ptrace.StatusCodeError)
		s.Status().SetMessage("loadgen injected error")
	case classSlowSpan:
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(p.SlowSpanMS) * time.Millisecond)))
	case classTraceLatency:
		s.Attributes().PutInt(elapsedMSAttribute, int64(p.ElapsedMS))
	case classHealthy:
		// Nothing to arm: a healthy trace is one no detector can see.
	}
}

// add folds a batch summary into the running total. Counts stay exact once
// KeptTraceIDs hits its cap.
func (s *summary) add(o summary) {
	s.Traces += o.Traces
	s.ErrorTraces += o.ErrorTraces
	s.SlowSpanTraces += o.SlowSpanTraces
	s.TraceLatencyTraces += o.TraceLatencyTraces
	s.HealthyTraces += o.HealthyTraces
	for e, n := range o.SpansPerEndpoint {
		s.SpansPerEndpoint[e] += n
	}
	for e, n := range o.ExpectedKeptSpansPerEndpoint {
		s.ExpectedKeptSpansPerEndpoint[e] += n
	}
	if room := keptTraceIDCap - len(s.KeptTraceIDs); room > 0 {
		s.KeptTraceIDs = append(s.KeptTraceIDs, o.KeptTraceIDs[:min(room, len(o.KeptTraceIDs))]...)
	}
}

// rng is splitmix64, hand-rolled. math/rand is denied here (depguard,
// ADR-001/002) and its stream carries no cross-version stability promise,
// which a --seed reproducibility contract needs: these outputs are fixed
// by the algorithm, not by the toolchain.
type rng struct{ state uint64 }

func newRNG(seed uint64) *rng { return &rng{state: seed} }

func (r *rng) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *rng) traceID() pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint64(id[0:8], r.next())
	binary.BigEndian.PutUint64(id[8:16], r.next())
	if id.IsEmpty() {
		id[15] = 1
	}
	return id
}

func (r *rng) spanID() pcommon.SpanID {
	var id pcommon.SpanID
	binary.BigEndian.PutUint64(id[0:8], r.next())
	if id.IsEmpty() {
		id[7] = 1
	}
	return id
}
