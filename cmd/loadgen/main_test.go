// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
)

// fakeCollector is a real OTLP/gRPC endpoint. The driver talks to it over a
// loopback socket rather than through a stubbed client, so the wire path —
// dial, marshal, Export, status codes — is the one under test.
type fakeCollector struct {
	ptraceotlp.UnimplementedGRPCServer

	mu      sync.Mutex
	got     []ptrace.Traces
	calls   int
	failN   int // reject the first failN calls
	failAll bool
}

func (f *fakeCollector) Export(_ context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failAll || f.calls <= f.failN {
		return ptraceotlp.NewExportResponse(), errors.New("fake collector refusing")
	}
	td := ptrace.NewTraces()
	req.Traces().CopyTo(td)
	f.got = append(f.got, td)
	return ptraceotlp.NewExportResponse(), nil
}

func (f *fakeCollector) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeCollector) eachSpan(fn func(ptrace.Span)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, td := range f.got {
		forEachSpan(td, fn)
	}
}

func (f *fakeCollector) spanCount() int {
	n := 0
	f.eachSpan(func(ptrace.Span) { n++ })
	return n
}

func (f *fakeCollector) traceIDs() map[pcommon.TraceID]bool {
	ids := map[pcommon.TraceID]bool{}
	f.eachSpan(func(s ptrace.Span) { ids[s.TraceID()] = true })
	return ids
}

func startCollector(t *testing.T, f *fakeCollector) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	ptraceotlp.RegisterGRPCServer(srv, f)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return lis.Addr().String()
}

// fakeClock advances on every read, so a --duration run ends after a fixed
// number of batches instead of after however many the test host manages in
// a wall-clock second.
type fakeClock struct {
	mu    sync.Mutex
	t     time.Time
	step  time.Duration
	slept time.Duration
}

func newFakeClock(step time.Duration) *fakeClock {
	return &fakeClock{t: time.Unix(1700000000, 0), step: step}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(c.step)
	return c.t
}

func (c *fakeClock) sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slept += d
	c.t = c.t.Add(d)
}

func (c *fakeClock) slptTotal() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slept
}

func (c *fakeClock) clock() clock { return clock{now: c.now, sleep: c.sleep} }

// runLoadgen drives run() and decodes the stdout summary when it succeeds.
func runLoadgen(t *testing.T, clk clock, args ...string) (int, summary, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr, clk)
	var s summary
	if stdout.Len() > 0 {
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &s))
	}
	return code, s, stderr.String()
}

func TestRunSplitsTraceAcrossEndpoints(t *testing.T) {
	a, b := &fakeCollector{}, &fakeCollector{}
	addrA, addrB := startCollector(t, a), startCollector(t, b)

	code, s, errOut := runLoadgen(t, newFakeClock(time.Millisecond).clock(),
		"--endpoints", addrA+","+addrB,
		"--traces", "10", "--spans-per-trace", "4",
		"--error-traces", "2", "--slow-span-traces", "1", "--trace-latency-traces", "1",
		"--seed", "42")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	assert.Equal(t, 10, s.Traces)
	assert.Equal(t, 6, s.HealthyTraces)
	assert.Equal(t, []int{20, 20}, s.SpansPerEndpoint)
	assert.Equal(t, []int{8, 8}, s.ExpectedKeptSpansPerEndpoint)
	assert.Len(t, s.KeptTraceIDs, 4)
	assert.Positive(t, s.BytesSent)
	assert.Positive(t, s.ElapsedSeconds)

	// What the summary claims is what the endpoints actually received.
	assert.Equal(t, s.SpansPerEndpoint[0], a.spanCount())
	assert.Equal(t, s.SpansPerEndpoint[1], b.spanCount())

	// Both endpoints see every trace; only endpoint 0 sees a keep marker.
	assert.Len(t, a.traceIDs(), 10)
	assert.Len(t, b.traceIDs(), 10)
	markers := 0
	a.eachSpan(func(s ptrace.Span) {
		if _, ok := s.Attributes().Get(elapsedMSAttribute); ok || s.Status().Code() == ptrace.StatusCodeError {
			markers++
		}
	})
	assert.Equal(t, 3, markers, "2 error + 1 elapsed_ms roots land on endpoint 0")
	b.eachSpan(func(s ptrace.Span) {
		_, ok := s.Attributes().Get(elapsedMSAttribute)
		assert.False(t, ok)
		assert.NotEqual(t, ptrace.StatusCodeError, s.Status().Code())
	})
}

// A run longer than one batch must reseed, or every batch would replay the
// same trace ids and the collector would see one batch's worth of traces.
func TestRunBatchesReseedForFreshIDs(t *testing.T) {
	c := &fakeCollector{}
	addr := startCollector(t, c)

	code, s, errOut := runLoadgen(t, newFakeClock(time.Millisecond).clock(),
		"--endpoints", addr, "--traces", "250", "--spans-per-trace", "1", "--seed", "7")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	assert.Equal(t, 250, s.Traces)
	assert.Equal(t, []int{250}, s.SpansPerEndpoint)
	assert.Equal(t, 3, c.callCount(), "250 traces at 100 per batch")
	assert.Len(t, c.traceIDs(), 250, "every batch must carry fresh ids")
}

func TestRunDurationModeAppliesErrorPct(t *testing.T) {
	c := &fakeCollector{}
	addr := startCollector(t, c)

	code, s, errOut := runLoadgen(t, newFakeClock(100*time.Millisecond).clock(),
		"--endpoints", addr, "--duration", "1s", "--spans-per-trace", "2",
		"--error-pct", "10", "--seed", "3")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	require.Positive(t, s.Traces)
	assert.Equal(t, s.Traces/10, s.ErrorTraces, "10%% of a whole number of batches")
	assert.Equal(t, s.Traces-s.ErrorTraces, s.HealthyTraces)
	assert.Len(t, s.KeptTraceIDs, s.ErrorTraces)
	assert.Zero(t, s.SlowSpanTraces)
}

func TestRunRetriesThenSucceeds(t *testing.T) {
	c := &fakeCollector{failN: 2}
	addr := startCollector(t, c)
	clk := newFakeClock(time.Millisecond)

	code, s, errOut := runLoadgen(t, clk.clock(),
		"--endpoints", addr, "--traces", "5", "--spans-per-trace", "1")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	assert.Equal(t, 3, c.callCount(), "two refusals then the delivery")
	assert.Equal(t, 5, s.Traces)
	assert.Equal(t, backoffBase+2*backoffBase, clk.slptTotal(), "100ms then 200ms")
	assert.Contains(t, errOut, "attempt 1/4")
	assert.Contains(t, errOut, "attempt 2/4")
}

func TestRunGivesUpAfterRetries(t *testing.T) {
	c := &fakeCollector{failAll: true}
	addr := startCollector(t, c)

	var stdout, stderr bytes.Buffer
	code := run([]string{"--endpoints", addr, "--traces", "5"}, &stdout, &stderr, newFakeClock(time.Millisecond).clock())

	assert.Equal(t, exitError, code)
	assert.Equal(t, 4, c.callCount(), "the attempt plus three retries")
	assert.Empty(t, stdout.String(), "a failed run prints no summary")
	assert.Contains(t, stderr.String(), "failed after 4 attempts")
}

// The smoke case Task 11 and 12 depend on: an unreachable endpoint must
// exit nonzero rather than report a successful run of zero spans.
func TestRunClosedPortExitsNonzero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--endpoints", "127.0.0.1:1", "--traces", "1"},
		&stdout, &stderr, newFakeClock(time.Millisecond).clock())

	assert.Equal(t, exitError, code)
	assert.Empty(t, stdout.String())
	assert.Contains(t, stderr.String(), "failed after 4 attempts")
}

func TestRunRejectsBadFlags(t *testing.T) {
	tests := map[string][]string{
		"no endpoints":        {"--traces", "1"},
		"no mode":             {"--endpoints", "h:1"},
		"both modes":          {"--endpoints", "h:1", "--traces", "1", "--duration", "1s"},
		"zero spans":          {"--endpoints", "h:1", "--traces", "1", "--spans-per-trace", "0"},
		"negative span bytes": {"--endpoints", "h:1", "--traces", "1", "--span-bytes", "-1"},
		"negative rate":       {"--endpoints", "h:1", "--traces", "1", "--rate", "-1"},
		"negative mbps":       {"--endpoints", "h:1", "--traces", "1", "--target-mbps", "-1"},
		"negative elapsed":    {"--endpoints", "h:1", "--traces", "1", "--elapsed-ms", "-1"},
		"negative class":      {"--endpoints", "h:1", "--traces", "1", "--error-traces", "-1"},
		"pct out of range":    {"--endpoints", "h:1", "--traces", "1", "--error-pct", "101"},
		"pct and count":       {"--endpoints", "h:1", "--traces", "10", "--error-pct", "10", "--error-traces", "1"},
		"counts with duration": {
			"--endpoints", "h:1", "--duration", "1s", "--error-traces", "1",
		},
		"classes exceed traces": {"--endpoints", "h:1", "--traces", "2", "--error-traces", "3"},
		"unknown flag":          {"--endpoints", "h:1", "--traces", "1", "--nope"},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(args, &stdout, &stderr, newFakeClock(time.Millisecond).clock())
			assert.Equal(t, exitUsage, code)
			assert.Empty(t, stdout.String())
			assert.NotEmpty(t, stderr.String(), "a rejected run must say why")
		})
	}
}

func TestRunHelpExitsZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-h"}, &stdout, &stderr, newFakeClock(time.Millisecond).clock())
	assert.Equal(t, exitOK, code)
	assert.Empty(t, stdout.String(), "usage is stderr; stdout carries only the summary")
	assert.Contains(t, stderr.String(), "-endpoints")
}

func TestRunPacesByRate(t *testing.T) {
	c := &fakeCollector{}
	addr := startCollector(t, c)
	clk := newFakeClock(time.Microsecond)

	code, _, errOut := runLoadgen(t, clk.clock(),
		"--endpoints", addr, "--traces", "300", "--spans-per-trace", "1", "--rate", "100")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	// 300 traces at 100/s is ~3s of waiting, less the microseconds the fake
	// clock charges for the reads themselves.
	assert.InDelta(t, 3*time.Second, clk.slptTotal(), float64(10*time.Millisecond))
}

func TestPaceDelay(t *testing.T) {
	tests := []struct {
		name       string
		rate       int
		targetMbps float64
		traces     int
		bytes      int64
		elapsed    time.Duration
		want       time.Duration
	}{
		{name: "unpaced", want: 0},
		{name: "rate on schedule", rate: 100, traces: 100, elapsed: time.Second, want: 0},
		{name: "rate ahead", rate: 100, traces: 100, elapsed: 400 * time.Millisecond, want: 600 * time.Millisecond},
		{name: "rate behind clamps", rate: 100, traces: 100, elapsed: 5 * time.Second, want: 0},
		// 1 Mbps = 125 000 B/s, so 125 000 bytes is one second of budget.
		{name: "mbps ahead", targetMbps: 1, bytes: 125000, elapsed: 250 * time.Millisecond, want: 750 * time.Millisecond},
		{name: "mbps on schedule", targetMbps: 1, bytes: 125000, elapsed: time.Second, want: 0},
		{name: "mbps overrides rate", targetMbps: 1, rate: 1, bytes: 125000, traces: 1000, elapsed: 0, want: time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, paceDelay(tc.rate, tc.targetMbps, tc.traces, tc.bytes, tc.elapsed))
		})
	}
}

func TestTracesPerBatchRespectsMessageLimit(t *testing.T) {
	small := options{spansPerTrace: 4, spanBytes: 128}
	assert.Equal(t, batchTraces, small.tracesPerBatch())

	// 4 spans x ~64 KiB will not fit 100 traces in one 2 MiB request.
	big := options{spansPerTrace: 4, spanBytes: 64 << 10}
	assert.Less(t, big.tracesPerBatch(), batchTraces)
	assert.GreaterOrEqual(t, big.tracesPerBatch(), 1)

	// Even a single trace over the cap must still make a batch of one.
	huge := options{spansPerTrace: 8, spanBytes: 8 << 20}
	assert.Equal(t, 1, huge.tracesPerBatch())
}

// The class mix is a run-wide contract: whatever the batch boundaries do,
// the totals must land on the requested counts.
func TestParamsForSpreadsClassesOverBatches(t *testing.T) {
	o := options{
		endpoints: []string{"a", "b"}, traces: 250, spansPerTrace: 2,
		errorTraces: 120, slowSpanTraces: 15, traceLatencyTraces: 30, seed: 5,
	}
	var errs, slow, lat, traces int
	seeds := map[uint64]bool{}
	for batch, sent := uint64(0), 0; sent < o.traces; batch++ {
		n := min(o.tracesPerBatch(), o.traces-sent)
		p := o.paramsFor(sent, n, batch)
		require.LessOrEqual(t, p.ErrorTraces+p.SlowSpanTraces+p.TraceLatencyTraces, p.Traces)
		errs += p.ErrorTraces
		slow += p.SlowSpanTraces
		lat += p.TraceLatencyTraces
		traces += p.Traces
		seeds[p.Seed] = true
		sent += n
	}
	assert.Equal(t, 250, traces)
	assert.Equal(t, 120, errs)
	assert.Equal(t, 15, slow)
	assert.Equal(t, 30, lat)
	assert.Len(t, seeds, 3, "one distinct seed per batch")
}

// Rounding the cumulative total rather than each batch is what keeps
// --error-pct from drifting a fraction of a trace per batch.
func TestErrorPctRoundsCumulatively(t *testing.T) {
	o := options{endpoints: []string{"a"}, traces: 1000, spansPerTrace: 1, errorPct: 33.3}
	total := 0
	for batch, sent := uint64(0), 0; sent < o.traces; batch++ {
		n := min(o.tracesPerBatch(), o.traces-sent)
		total += o.paramsFor(sent, n, batch).ErrorTraces
		sent += n
	}
	assert.Equal(t, 333, total)

	// A percentage below one trace per batch still accumulates.
	o.errorPct = 0.5
	total = 0
	for batch, sent := uint64(0), 0; sent < o.traces; batch++ {
		n := min(o.tracesPerBatch(), o.traces-sent)
		total += o.paramsFor(sent, n, batch).ErrorTraces
		sent += n
	}
	assert.Equal(t, 5, total)
}

func TestSummaryAddCapsKeptTraceIDs(t *testing.T) {
	total := summary{SpansPerEndpoint: make([]int, 1), ExpectedKeptSpansPerEndpoint: make([]int, 1)}
	batch := summary{
		Traces: 6000, ErrorTraces: 6000, SpansPerEndpoint: []int{6000},
		ExpectedKeptSpansPerEndpoint: []int{6000},
		KeptTraceIDs:                 make([]string, 6000),
	}
	total.add(batch)
	total.add(batch)

	assert.Equal(t, 12000, total.Traces, "counts stay exact past the cap")
	assert.Equal(t, 12000, total.ErrorTraces)
	assert.Equal(t, []int{12000}, total.SpansPerEndpoint)
	assert.Len(t, total.KeptTraceIDs, keptTraceIDCap)

	total.add(batch)
	assert.Len(t, total.KeptTraceIDs, keptTraceIDCap, "the cap holds once reached")
	assert.Equal(t, 18000, total.Traces)
}

// The summary is a contract with Tasks 11 and 12: the key names and the
// lowercase-hex ids are read by the compose asserts.
func TestSummaryJSONShape(t *testing.T) {
	c := &fakeCollector{}
	addr := startCollector(t, c)

	var stdout, stderr bytes.Buffer
	code := run([]string{"--endpoints", addr, "--traces", "4", "--error-traces", "2", "--spans-per-trace", "2"},
		&stdout, &stderr, newFakeClock(time.Millisecond).clock())
	require.Equal(t, exitOK, code, "stderr: %s", stderr.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &raw))
	for _, key := range []string{
		"traces", "error_traces", "slow_span_traces", "trace_latency_traces",
		"healthy_traces", "spans_per_endpoint", "bytes_sent", "elapsed_seconds",
		"expected_kept_spans_per_endpoint", "kept_trace_ids",
	} {
		assert.Contains(t, raw, key)
	}

	ids, ok := raw["kept_trace_ids"].([]any)
	require.True(t, ok, "kept_trace_ids must be an array, not null")
	require.Len(t, ids, 2)
	for _, id := range ids {
		s, ok := id.(string)
		require.True(t, ok)
		assert.Len(t, s, 32)
		assert.Equal(t, strings.ToLower(s), s)
	}
}

// bytes_sent is the sum of the marshaled OTLP request bodies, so the
// testbed's MB/s floor is measured against payload the collector accepted.
func TestBytesSentMatchesMarshaledPayload(t *testing.T) {
	c := &fakeCollector{}
	addr := startCollector(t, c)

	code, s, errOut := runLoadgen(t, newFakeClock(time.Millisecond).clock(),
		"--endpoints", addr, "--traces", "20", "--spans-per-trace", "2", "--span-bytes", "256", "--seed", "9")
	require.Equal(t, exitOK, code, "stderr: %s", errOut)

	sizer := &ptrace.ProtoMarshaler{}
	var got int64
	c.mu.Lock()
	for _, td := range c.got {
		got += int64(sizer.TracesSize(td))
	}
	c.mu.Unlock()
	assert.Equal(t, got, s.BytesSent)
}
