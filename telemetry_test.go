// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/metadatatest"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// metricPrefix is the emitted namespace: metadata.yaml's dotted ids
// under the collector's own otelcol. prefix.
const metricPrefix = "otelcol.processor.retrosampler."

// wantInstruments is every metric metadata.yaml declares. An async
// instrument whose callback is missing reports no data point at all, so
// comparing this list against a live collection is what proves each
// declared instrument was actually bound.
var wantInstruments = []string{
	metricPrefix + "append.errors",
	metricPrefix + "corrupt.fragments",
	metricPrefix + "decided.entries",
	metricPrefix + "duplicate.keeps",
	metricPrefix + "early_expired.segments",
	metricPrefix + "effective_window.seconds",
	metricPrefix + "expired.bytes",
	metricPrefix + "flush.errors",
	metricPrefix + "flush.retries",
	metricPrefix + "flushed.spans",
	metricPrefix + "kept.bus",
	metricPrefix + "kept.local",
	metricPrefix + "pending.flushes",
	metricPrefix + "publish.errors",
	metricPrefix + "published.keeps",
	metricPrefix + "shed.floor_protected",
	metricPrefix + "shed.nothing_reclaimable",
	metricPrefix + "shed.queue_full",
}

// metricInt64 reads the single data point of an int64 sum or gauge and
// reports false while the instrument has observed nothing. It never
// fails the test, so require.Eventually may poll it from its own
// goroutine; each call drives a fresh collection of the manual reader.
func metricInt64(tt *componenttest.Telemetry, name string) (int64, bool) {
	m, err := tt.GetMetric(name)
	if err != nil {
		return 0, false
	}
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		if len(d.DataPoints) == 1 {
			return d.DataPoints[0].Value, true
		}
	case metricdata.Gauge[int64]:
		if len(d.DataPoints) == 1 {
			return d.DataPoints[0].Value, true
		}
	}
	return 0, false
}

// requireMetricInt64 is metricInt64 for a quiesced processor, where a
// missing instrument is a failure rather than something to wait for.
func requireMetricInt64(t *testing.T, tt *componenttest.Telemetry, name string) int64 {
	t.Helper()
	v, ok := metricInt64(tt, name)
	require.True(t, ok, "%s reported no data point", name)
	return v
}

// reportedInstruments lists the retrosampler metrics that produced a
// data point in one collection, sorted.
func reportedInstruments(t *testing.T, tt *componenttest.Telemetry) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, tt.Reader.Collect(context.Background(), &rm))
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if strings.HasPrefix(m.Name, metricPrefix) {
				names = append(names, m.Name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// newTestTelemetry gives a manual-reader telemetry whose shutdown the
// goleak TestMain would otherwise catch.
func newTestTelemetry(t *testing.T) *componenttest.Telemetry {
	t.Helper()
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })
	return tt
}

// Telemetry is the factory's job: a component assembled the production
// way must report the decision plane through its own meter, with no
// test-only registration step. One error batch and one healthy batch
// exercise both verdict paths at once.
func TestTelemetryReportsTheDecisionPlane(t *testing.T) {
	tt := newTestTelemetry(t)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	sink := new(consumertest.TracesSink)
	ctx := context.Background()
	p, err := NewFactory().CreateTraces(ctx, metadatatest.NewSettings(tt), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(ctx, componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(ctx)) }()

	require.NoError(t, p.ConsumeTraces(ctx, errorSpanBatch(pcommon.TraceID{0xAE}, "boom")))
	healthy := ptrace.NewTraces()
	hs := healthy.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	hs.SetTraceID(pcommon.TraceID{0xAF})
	hs.SetName("fine")
	require.NoError(t, p.ConsumeTraces(ctx, healthy))

	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the error trace flushes")
	// The instance's own broadcast returns over the Loopback and the
	// decided set absorbs it.
	require.Eventually(t, func() bool {
		v, ok := metricInt64(tt, metricPrefix+"duplicate.keeps")
		return ok && v >= 1
	}, 5*time.Second, 10*time.Millisecond, "self-delivery lands as a duplicate keep")
	// decided.entries and pending.flushes are per-shard mirrors the
	// workers publish on their tick, so they trail the flush.
	require.Eventually(t, func() bool {
		v, ok := metricInt64(tt, metricPrefix+"decided.entries")
		return ok && v == 1
	}, 5*time.Second, 10*time.Millisecond, "the decided-set mirror is published on the tick")

	assert.Equal(t, wantInstruments, reportedInstruments(t, tt),
		"every declared instrument must be bound to a callback")

	// Exact rows go through the generated asserts, which pin unit,
	// description and monotonicity alongside the value.
	one := []metricdata.DataPoint[int64]{{Value: 1}}
	zero := []metricdata.DataPoint[int64]{{Value: 0}}
	metadatatest.AssertEqualProcessorRetrosamplerKeptLocal(t, tt, one,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerFlushedSpans(t, tt, one,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerPublishedKeeps(t, tt, one,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerKeptBus(t, tt, zero,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerPublishErrors(t, tt, zero,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerFlushErrors(t, tt, zero,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerCorruptFragments(t, tt, zero,
		metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualProcessorRetrosamplerShedQueueFull(t, tt, zero,
		metricdatatest.IgnoreTimestamp())
	assert.Positive(t, requireMetricInt64(t, tt, metricPrefix+"effective_window.seconds"),
		"an unshrunk window reports the configured retention")
	assert.Zero(t, requireMetricInt64(t, tt, metricPrefix+"pending.flushes"),
		"the flush succeeded, so nothing is parked")
}

// A refused Publish is the least observable failure in the flusher: the
// verdict is re-parked and retried silently. It must move a counter.
func TestTelemetryCountsRefusedPublishes(t *testing.T) {
	tt := newTestTelemetry(t)
	spy := newBusSpy()
	spy.failing.Store(true)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 1
	sink := new(consumertest.TracesSink)
	ctx := context.Background()
	// The bus is the factory's one hard-wired collaborator, so the spy
	// goes in through newProcessor — the same seam the other processor
	// tests use — with the factory's telemetry binding applied by hand.
	p := newTestProcessor(t, cfg, spy)
	p.next = sink
	require.NoError(t, p.bindTelemetry(tt.NewTelemetrySettings()))
	require.NoError(t, p.start(ctx, componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(ctx)) }()

	_, err := p.processTraces(ctx, errorSpanBatch(pcommon.TraceID{0xC3}, "unpublishable"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)

	// Both in one wait: publish.errors moves on the flusher goroutine and
	// flush.retries on the shard worker that receives its Retry, so the
	// second trails the first by a handoff.
	require.Eventually(t, func() bool {
		refused, ok := metricInt64(tt, metricPrefix+"publish.errors")
		if !ok || refused < 1 {
			return false
		}
		parked, ok := metricInt64(tt, metricPrefix+"flush.retries")
		return ok && parked >= 1
	}, 10*time.Second, 10*time.Millisecond,
		"the refused publish is counted and its need-bit re-parked for the tick to retry")
	assert.Zero(t, requireMetricInt64(t, tt, metricPrefix+"published.keeps"),
		"nothing reached the bus")
}

// corrupt.fragments is the one counter fed from two places: fragments
// the shard buffers skip at Collect, and fragments the flusher cannot
// decode. Dropping either term would under-report corruption silently.
// The Collect-time term is gated in internal/buffer; what is wired
// here, and only here, is the flusher's.
func TestTelemetryCorruptFragmentsCountsDecodeFailures(t *testing.T) {
	tt := newTestTelemetry(t)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 1
	ctx := context.Background()
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = new(consumertest.TracesSink)
	require.NoError(t, p.bindTelemetry(tt.NewTelemetrySettings()))
	require.NoError(t, p.start(ctx, componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(ctx)) }()

	require.Zero(t, requireMetricInt64(t, tt, metricPrefix+"corrupt.fragments"),
		"a healthy buffer reports no corruption")
	p.fl.decodeErrors.Add(2)
	assert.Equal(t, int64(2), requireMetricInt64(t, tt, metricPrefix+"corrupt.fragments"),
		"the flusher's decode failures land in the same counter")
}

// The collector builds its pipeline before it starts it, so between
// the factory call and Start the callbacks are already registered while
// the set pointer is still nil. That window must report nothing: a zero
// is a claim about a component that has not yet run, and on a monotone
// counter the first real increment after it would read as a reset.
func TestTelemetryReportsNothingBeforeStart(t *testing.T) {
	tt := newTestTelemetry(t)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	ctx := context.Background()
	p, err := NewFactory().CreateTraces(ctx, metadatatest.NewSettings(tt),
		cfg, new(consumertest.TracesSink))
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(ctx)) }()

	assert.Empty(t, reportedInstruments(t, tt),
		"a built but unstarted processor has nothing to report")
}

// A stopped component reports nothing at all, never zeros. The
// collector's meter outlives the processor, and a monotone counter that
// dropped to zero on shutdown would read downstream as a counter reset
// — a spurious spike on every restart.
//
// The drained shutdown path has its own unbind, distinct from the no-op
// path TestTelemetryUnbindsWithoutStart covers, and an empty collection
// does not prove it ran: callbacks that survived would report nothing
// against the nil set anyway. So the shutdown set is pushed back in
// afterwards, still carrying its non-zero counters. Any surviving
// registration reports them.
func TestTelemetryGoesSilentAfterShutdown(t *testing.T) {
	tt := newTestTelemetry(t)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 1
	sink := new(consumertest.TracesSink)
	ctx := context.Background()
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = sink
	require.NoError(t, p.bindTelemetry(tt.NewTelemetrySettings()))
	require.NoError(t, p.start(ctx, componenttest.NewNopHost()))

	_, err := p.processTraces(ctx, errorSpanBatch(pcommon.TraceID{0x1D}, "counted"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.Eventually(t, func() bool {
		v, ok := metricInt64(tt, metricPrefix+"kept.local")
		return ok && v == 1
	}, 5*time.Second, time.Millisecond, "the counter is live before shutdown")

	// Captured before the swap, and still readable afterwards: Stats and
	// the length mirrors are plain atomics on a stopped set.
	s := p.set.Load()
	require.NotNil(t, s)
	require.NoError(t, p.shutdown(ctx))
	assert.Empty(t, reportedInstruments(t, tt),
		"a retired processor observes nothing rather than a reset to zero")

	require.Equal(t, uint64(1), s.Stats().KeptLocal,
		"the set still holds the counters a surviving callback would find")
	p.set.Store(s)
	assert.Empty(t, reportedInstruments(t, tt),
		"the drained shutdown dropped the callbacks, so a live set goes unreported")
}

// The builder is bound at construction, so the unbinding cannot depend
// on a Start that may never have happened — a component whose Start
// failed still owes the collector's meter its callbacks back.
//
// An empty collection alone would not prove that: callbacks that
// survived would also report nothing while the set pointer is nil. So
// the shutdown is followed by a live set pushed in behind it, which any
// surviving callback would find and report.
func TestTelemetryUnbindsWithoutStart(t *testing.T) {
	tt := newTestTelemetry(t)
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	ctx := context.Background()
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = new(consumertest.TracesSink)
	require.NoError(t, p.bindTelemetry(tt.NewTelemetrySettings()))
	require.NoError(t, p.shutdown(ctx), "shutdown without start is a no-op")

	jobs := make(chan *shards.FlushJob, 1)
	set, err := shards.New(shards.Options{
		Dir: t.TempDir(), Shards: 1, Window: cfg.Window, SegmentSize: cfg.SegmentSize,
		DiskBudget: cfg.DiskBudget, WatermarkPct: cfg.WatermarkPct,
		WindowFloor: cfg.WindowFloor, Now: systemClock, Flush: jobs,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, set.Shutdown(ctx)) }()
	// A flusher too, unstarted: a surviving callback must fail the
	// assertion below rather than dereference a nil one.
	p.fl = newFlusher(jobs, set, p.next, p.b)
	p.set.Store(set)

	assert.Empty(t, reportedInstruments(t, tt),
		"the callbacks were dropped at shutdown, so a live set goes unreported")
}
