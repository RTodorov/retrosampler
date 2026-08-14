// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// tracesDigest hashes every endpoint's marshaled batch, in endpoint order.
// Two runs agreeing here agree on the wire, not merely on the counts.
func tracesDigest(t *testing.T, r genResult) string {
	t.Helper()
	h := sha256.New()
	m := &ptrace.ProtoMarshaler{}
	for _, td := range r.perEndpoint {
		b, err := m.MarshalTraces(td)
		require.NoError(t, err)
		_, err = h.Write(b)
		require.NoError(t, err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func countErrorSpans(td ptrace.Traces) int {
	n := 0
	forEachSpan(td, func(s ptrace.Span) {
		if s.Status().Code() == ptrace.StatusCodeError {
			n++
		}
	})
	return n
}

func forEachSpan(td ptrace.Traces, fn func(ptrace.Span)) {
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, s := range ss.Spans().All() {
				fn(s)
			}
		}
	}
}

func TestGenDeterministicSplitAndMix(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 2, Traces: 10, SpansPerTrace: 4,
		ErrorTraces: 3, SlowSpanTraces: 2, TraceLatencyTraces: 1,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 64, Seed: 42,
	}
	a := generate(p, base)
	b := generate(p, base)
	require.Equal(t, tracesDigest(t, a), tracesDigest(t, b), "same seed, same bytes")

	// Split: 4 spans over 2 endpoints = 2+2 per trace.
	require.Len(t, a.perEndpoint, 2)
	assert.Equal(t, 20, a.perEndpoint[0].SpanCount())
	assert.Equal(t, 20, a.perEndpoint[1].SpanCount())

	// Keep markers live on endpoint 0 only.
	assert.Equal(t, 3, countErrorSpans(a.perEndpoint[0]))
	assert.Zero(t, countErrorSpans(a.perEndpoint[1]))

	// Summary math: 6 kept traces x 2 spans on each endpoint.
	assert.Len(t, a.summary.KeptTraceIDs, 6)
	assert.Equal(t, []int{12, 12}, a.summary.ExpectedKeptSpansPerEndpoint)
	assert.Equal(t, 4, a.summary.HealthyTraces)
	assert.Equal(t, []int{20, 20}, a.summary.SpansPerEndpoint)
}

func TestGenSeedVariesIDsNotShape(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 2, Traces: 10, SpansPerTrace: 4,
		ErrorTraces: 3, SlowSpanTraces: 2, TraceLatencyTraces: 1,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 64, Seed: 42,
	}
	a := generate(p, base)
	q := p
	q.Seed = 43
	b := generate(q, base)

	assert.NotEqual(t, tracesDigest(t, a), tracesDigest(t, b), "a new seed must move the ids")
	assert.NotEqual(t, a.summary.KeptTraceIDs, b.summary.KeptTraceIDs)

	// The class mix is a contract of the params, not of the seed.
	assert.Equal(t, a.summary.ExpectedKeptSpansPerEndpoint, b.summary.ExpectedKeptSpansPerEndpoint)
	assert.Equal(t, a.summary.SpansPerEndpoint, b.summary.SpansPerEndpoint)
	assert.Equal(t, a.summary.HealthyTraces, b.summary.HealthyTraces)
}

// The three keep classes must be findable by the very conditions the
// processor's built-in detectors evaluate, on endpoint 0 and nowhere else.
func TestGenMarkersOnlyOnEndpointZero(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 3, Traces: 12, SpansPerTrace: 6,
		ErrorTraces: 2, SlowSpanTraces: 3, TraceLatencyTraces: 4,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 16, Seed: 7,
	}
	r := generate(p, base)

	slow, elapsed, errs := 0, 0, 0
	forEachSpan(r.perEndpoint[0], func(s ptrace.Span) {
		if s.Status().Code() == ptrace.StatusCodeError {
			errs++
		}
		if s.EndTimestamp().AsTime().Sub(s.StartTimestamp().AsTime()) == time.Duration(p.SlowSpanMS)*time.Millisecond {
			slow++
		}
		if v, ok := s.Attributes().Get(elapsedMSAttribute); ok {
			assert.Equal(t, pcommon.ValueTypeInt, v.Type(), "elapsed_ms must be an int")
			assert.Equal(t, int64(p.ElapsedMS), v.Int())
			elapsed++
		}
	})
	assert.Equal(t, 2, errs)
	assert.Equal(t, 3, slow)
	assert.Equal(t, 4, elapsed)

	for e := 1; e < p.Endpoints; e++ {
		forEachSpan(r.perEndpoint[e], func(s ptrace.Span) {
			assert.NotEqual(t, ptrace.StatusCodeError, s.Status().Code())
			assert.NotEqual(t, time.Duration(p.SlowSpanMS)*time.Millisecond,
				s.EndTimestamp().AsTime().Sub(s.StartTimestamp().AsTime()))
			_, ok := s.Attributes().Get(elapsedMSAttribute)
			assert.False(t, ok, "endpoint %d must carry no keep marker", e)
		})
	}
}

// An uneven split is the interesting case: spans_per_endpoint and
// expected_kept_spans_per_endpoint must both follow j%len(endpoints).
func TestGenUnevenSplit(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 3, Traces: 10, SpansPerTrace: 5,
		ErrorTraces: 2, SlowSpanTraces: 0, TraceLatencyTraces: 0,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 8, Seed: 1,
	}
	r := generate(p, base)

	// Spans 0..4 over 3 endpoints: endpoint 0 takes j=0,3; 1 takes j=1,4; 2 takes j=2.
	assert.Equal(t, []int{20, 20, 10}, r.summary.SpansPerEndpoint)
	assert.Equal(t, []int{4, 4, 2}, r.summary.ExpectedKeptSpansPerEndpoint)
	assert.Equal(t, 8, r.summary.HealthyTraces)
	for e, td := range r.perEndpoint {
		assert.Equal(t, r.summary.SpansPerEndpoint[e], td.SpanCount())
	}
}

// kept_trace_ids is an identity claim: the ids must be the marked traces,
// lowercase 32-hex, matching the file exporter's JSON traceId encoding.
func TestGenKeptTraceIDsAreTheMarkedTraces(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 2, Traces: 8, SpansPerTrace: 2,
		ErrorTraces: 1, SlowSpanTraces: 1, TraceLatencyTraces: 1,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 8, Seed: 99,
	}
	r := generate(p, base)
	require.Len(t, r.summary.KeptTraceIDs, 3)

	marked := map[string]bool{}
	forEachSpan(r.perEndpoint[0], func(s ptrace.Span) {
		_, hasElapsed := s.Attributes().Get(elapsedMSAttribute)
		slow := s.EndTimestamp().AsTime().Sub(s.StartTimestamp().AsTime()) ==
			time.Duration(p.SlowSpanMS)*time.Millisecond
		if s.Status().Code() == ptrace.StatusCodeError || slow || hasElapsed {
			id := s.TraceID()
			marked[hex.EncodeToString(id[:])] = true
		}
	})
	for _, id := range r.summary.KeptTraceIDs {
		assert.Len(t, id, 32)
		assert.Equal(t, id, strings.ToLower(id), "lowercase hex, per the file exporter")
		assert.True(t, marked[id], "%s is claimed kept but carries no marker", id)
	}
	assert.Len(t, marked, 3)
}

func TestGenIDsAreUniqueAndNonZero(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 2, Traces: 200, SpansPerTrace: 4,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 8, Seed: 5,
	}
	r := generate(p, base)

	traceIDs, spanIDs := map[pcommon.TraceID]bool{}, map[pcommon.SpanID]bool{}
	for _, td := range r.perEndpoint {
		forEachSpan(td, func(s ptrace.Span) {
			assert.False(t, s.TraceID().IsEmpty(), "a zero trace id is not a trace id")
			assert.False(t, s.SpanID().IsEmpty())
			traceIDs[s.TraceID()] = true
			spanIDs[s.SpanID()] = true
		})
	}
	assert.Len(t, traceIDs, 200)
	assert.Len(t, spanIDs, 800)
}

// Every span past the root points at the root, so the fragments a peer
// endpoint receives are a real trace and not eight orphans.
func TestGenChildrenParentTheRoot(t *testing.T) {
	base := time.Unix(3000, 0)
	p := genParams{
		Endpoints: 2, Traces: 3, SpansPerTrace: 4,
		SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: 8, Seed: 11,
	}
	r := generate(p, base)

	roots := map[pcommon.TraceID]pcommon.SpanID{}
	forEachSpan(r.perEndpoint[0], func(s ptrace.Span) {
		if s.ParentSpanID().IsEmpty() {
			roots[s.TraceID()] = s.SpanID()
		}
	})
	require.Len(t, roots, 3, "one root per trace, all on endpoint 0")

	for _, td := range r.perEndpoint {
		forEachSpan(td, func(s ptrace.Span) {
			if s.ParentSpanID().IsEmpty() {
				return
			}
			assert.Equal(t, roots[s.TraceID()], s.ParentSpanID())
		})
	}
}

func TestGenSpanBytesPads(t *testing.T) {
	base := time.Unix(3000, 0)
	for _, spanBytes := range []int{0, 1, 128, 4096} {
		p := genParams{
			Endpoints: 1, Traces: 2, SpansPerTrace: 2,
			SlowSpanMS: 2000, ElapsedMS: 10000, SpanBytes: spanBytes, Seed: 3,
		}
		r := generate(p, base)
		forEachSpan(r.perEndpoint[0], func(s ptrace.Span) {
			v, ok := s.Attributes().Get(padAttribute)
			if spanBytes == 0 {
				assert.False(t, ok, "no padding attribute when --span-bytes is 0")
				return
			}
			require.True(t, ok)
			assert.Len(t, v.Str(), spanBytes)
		})
	}
}

func TestGenZeroTracesIsEmptyButShaped(t *testing.T) {
	r := generate(genParams{Endpoints: 2, Traces: 0, SpansPerTrace: 4, Seed: 1}, time.Unix(3000, 0))
	require.Len(t, r.perEndpoint, 2)
	assert.Zero(t, r.perEndpoint[0].SpanCount())
	assert.Equal(t, []int{0, 0}, r.summary.SpansPerEndpoint)
	assert.Equal(t, []int{0, 0}, r.summary.ExpectedKeptSpansPerEndpoint)
	assert.Empty(t, r.summary.KeptTraceIDs)
}

// The digest must survive a fresh process, so the --seed contract cannot
// drift with a Go release: these are splitmix64's published outputs.
func TestRNGMatchesSplitmix64Vectors(t *testing.T) {
	r := newRNG(0)
	assert.Equal(t, uint64(0xE220A8397B1DCDAF), r.next())
	assert.Equal(t, uint64(0x6E789E6AA1B965F4), r.next())
	assert.Equal(t, uint64(0x06C45D188009454F), r.next())
}
