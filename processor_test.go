// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rtodorov/retrosampler/internal/buffer"
	"github.com/rtodorov/retrosampler/internal/bus"
)

// testDiskBudget clears the shards.New watermark-vs-active-segment-floor
// check (Shards x SegmentSize) on any core count these tests run on.
// Nothing preallocates it.
const testDiskBudget = 1 << 34

// newTestProcessor constructs a processor over nop telemetry. The only
// construction error is a detector that will not compile, which is a
// config error these fixtures never carry; that path is covered through
// the factory, where it surfaces in production
// (TestCreateTracesRejectsUnparsablePolicy).
func newTestProcessor(t *testing.T, cfg *Config, b bus.Bus) *retroProcessor {
	t.Helper()
	p, err := newProcessor(cfg, componenttest.NewNopTelemetrySettings(), systemClock, b)
	require.NoError(t, err)
	return p
}

// startTestProcessor builds a started processor on temp storage wired to
// sink and b, shut down via t.Cleanup. The cleanup shutdown is a second
// call for the tests that shut down themselves: it must still return nil.
func startTestProcessor(t *testing.T, sink consumer.Traces, b bus.Bus) (*retroProcessor, *Config) {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	p := newTestProcessor(t, cfg, b)
	// next is wired by the factory in production, which receives the
	// consumer separately from the config.
	p.next = sink
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, p.shutdown(context.Background())) })
	return p, cfg
}

// baseTime anchors span timestamps in the detection tests. The
// span-latency built-in reads the span's own start and end, so the
// anchor only has to be stable — it is unrelated to the processor clock.
var baseTime = time.Unix(1_700_000_000, 0)

// newStartedProcessor starts a processor on cfg over a recording bus, so
// a test can assert both what flushed and what crossed the bus. Shut
// down via t.Cleanup, like startTestProcessor.
func newStartedProcessor(t *testing.T, cfg *Config) (*retroProcessor, *consumertest.TracesSink, *busSpy) {
	t.Helper()
	sink := new(consumertest.TracesSink)
	spy := newBusSpy()
	p := newTestProcessor(t, cfg, spy)
	p.next = sink
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, p.shutdown(context.Background())) })
	return p, sink, spy
}

// singleSpanTrace is a one-span batch under id, handed to mutate for the
// attribute or timestamp the condition under test reads.
func singleSpanTrace(id pcommon.TraceID, mutate func(ptrace.Span)) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName("op")
	if mutate != nil {
		mutate(sp)
	}
	return td
}

// consumeAccepted pushes td through the processor and requires the
// accepted answer: buffered, with the flusher owning any emission.
func consumeAccepted(t *testing.T, p *retroProcessor, td ptrace.Traces) {
	t.Helper()
	_, err := p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
}

// waitOneKeptSpan waits for the single span of a one-trace batch to
// reach the sink — the whole observable effect of a keep verdict.
func waitOneKeptSpan(t *testing.T, sink *consumertest.TracesSink) {
	t.Helper()
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the kept trace flushes")
}

// A healthy-but-slow span is kept on the span-latency threshold alone,
// and the broadcast carries that reason rather than the error default.
func TestSpanLatencyThresholdKeeps(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	pinShardFixture(cfg)
	cfg.SpanLatencyThreshold = 100 * time.Millisecond
	p, sink, spy := newStartedProcessor(t, cfg)

	consumeAccepted(t, p, singleSpanTrace(pcommon.TraceID{0xA1}, func(sp ptrace.Span) {
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(baseTime))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(time.Second)))
	}))
	waitOneKeptSpan(t, sink)
	assert.Equal(t, []publishedKeep{{id: [16]byte{0xA1}, reason: bus.ReasonSpanLatency}},
		spy.publishedVerdicts())
}

// The baseline keep flushes this instance's fragments and stops there:
// it is deterministic, so every instance reaches it alone and a
// broadcast would only duplicate work (ADR-008 r1).
func TestBaselineKeepsFlushWithoutPublish(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	pinShardFixture(cfg)
	cfg.KeepOnError = false
	cfg.BaselineRate = 1 // every trace: deterministic, no id gymnastics
	p, sink, spy := newStartedProcessor(t, cfg)

	consumeAccepted(t, p, singleSpanTrace(pcommon.TraceID{0xB1}, nil))
	waitOneKeptSpan(t, sink)
	assert.Empty(t, spy.publishedVerdicts(), "baseline keeps never cross the bus (ADR-008 r1)")
}

// An OTTL policy match keeps the trace under the policy reason.
func TestPolicyKeeps(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	pinShardFixture(cfg)
	cfg.KeepOnError = false
	cfg.Policies = []PolicyConfig{{Name: "attr", Condition: `span.attributes["keep"] == "yes"`}}
	p, sink, spy := newStartedProcessor(t, cfg)

	consumeAccepted(t, p, singleSpanTrace(pcommon.TraceID{0xC1}, func(sp ptrace.Span) {
		sp.Attributes().PutStr("keep", "yes")
	}))
	waitOneKeptSpan(t, sink)
	require.Len(t, spy.publishedVerdicts(), 1)
	assert.Equal(t, bus.ReasonPolicy, spy.publishedVerdicts()[0].reason)
}

// The trace-latency threshold reads accumulated baggage, and reads it
// from the string form the W3C header actually carries.
func TestElapsedThresholdKeepsFromStringAttr(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	pinShardFixture(cfg)
	cfg.KeepOnError = false
	cfg.TraceLatencyThreshold = time.Second
	p, sink, spy := newStartedProcessor(t, cfg)

	consumeAccepted(t, p, singleSpanTrace(pcommon.TraceID{0xD1}, func(sp ptrace.Span) {
		sp.Attributes().PutStr("baggage.elapsed_ms", "5000")
	}))
	waitOneKeptSpan(t, sink)
	require.Len(t, spy.publishedVerdicts(), 1)
	assert.Equal(t, bus.ReasonTraceLatency, spy.publishedVerdicts()[0].reason)
}

// errorSpanBatch is a one-span batch carrying id and an error status.
func errorSpanBatch(id pcommon.TraceID, name string) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName(name)
	sp.Status().SetCode(ptrace.StatusCodeError)
	return td
}

// Retention: error traces are flushed exactly once via the flusher;
// healthy traces never reach the next consumer.
func TestRetentionKeepsErrorTracesOnly(t *testing.T) {
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, bus.NewLoopback())
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	bad := ss.Spans().AppendEmpty()
	bad.SetTraceID(pcommon.TraceID{1})
	bad.SetName("bad")
	bad.Status().SetCode(ptrace.StatusCodeError)
	good := ss.Spans().AppendEmpty()
	good.SetTraceID(pcommon.TraceID{2})
	good.SetName("good")

	out, err := p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData,
		"the flusher is the only emission point: the pipeline stops here")
	assert.Equal(t, 2, out.SpanCount(), "the batch itself is returned untouched")
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the error trace flushes")
	assert.Equal(t, "bad", sink.AllTraces()[0].ResourceSpans().At(0).
		ScopeSpans().At(0).Spans().At(0).Name())
	// Healthy trace: never emitted (bounded observation window).
	require.Never(t, func() bool { return sink.SpanCount() != 1 },
		250*time.Millisecond, 5*time.Millisecond,
		"the healthy trace ages out on disk instead of being emitted")
}

// With the built-in off there is no keep condition at all, so a trace
// that would otherwise be the headline case stays buffered.
func TestKeepOnErrorDisabledEmitsNothing(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	cfg.KeepOnError = false
	sink := new(consumertest.TracesSink)
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = sink
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(context.Background())) }()

	_, err := p.processTraces(context.Background(), errorSpanBatch(pcommon.TraceID{0x7E}, "boom"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.Never(t, func() bool { return sink.SpanCount() != 0 },
		250*time.Millisecond, 5*time.Millisecond,
		"no detector, no keeps: the buffer fills and nothing is emitted")
}

// Duplicate keeps across the loop — local detection plus the instance's
// own broadcast coming back — flush once and publish once (ADR-008 r5).
func TestDuplicateKeepsFlushAndPublishOnce(t *testing.T) {
	spy := newBusSpy()
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, spy)

	_, err := p.processTraces(context.Background(), errorSpanBatch(pcommon.TraceID{0x5A}, "boom"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the kept trace flushes")
	// The broadcast comes back to this very processor over the loopback;
	// the decided set absorbs it instead of deciding a second time.
	require.Eventually(t, func() bool {
		s := p.set.Load()
		return s != nil && s.Stats().DuplicateKeeps >= 1
	}, 5*time.Second, time.Millisecond, "self-delivery lands as a duplicate keep")

	assert.Equal(t, uint64(1), p.fl.publishedKeeps.Load(), "one verdict, one broadcast")
	assert.Equal(t, 1, spy.published())
	require.Never(t, func() bool { return sink.SpanCount() != 1 },
		250*time.Millisecond, 5*time.Millisecond,
		"the duplicate must not re-flush the trace")
}

// Verdict-before-spans at the pipeline level: a bus keep arrives before
// any spans; matching spans flush on arrival (ADR-008 r4 gate).
func TestVerdictBeforeSpans(t *testing.T) {
	lb := bus.NewLoopback()
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, lb)
	id := pcommon.TraceID{0x42}
	failed, err := lb.Publish(context.Background(), []bus.Keep{{ID: id, Reason: bus.ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Eventually(t, func() bool {
		s := p.set.Load()
		return s != nil && s.DecidedEntries() == 1
	}, 5*time.Second, time.Millisecond, "keep with no spans records decided")

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName("late")
	_, err = p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "late spans flush through the decided set")
	assert.Equal(t, []string{"late"}, spanNames(sink))
}

// Two full processors, one Loopback: A detects, B flushes its own
// fragment of the same trace. The cross-instance keep loop, in-process.
func TestTwoInstanceKeepLoopOverLoopback(t *testing.T) {
	lb := bus.NewLoopback()
	sinkA, sinkB := new(consumertest.TracesSink), new(consumertest.TracesSink)
	pA, _ := startTestProcessor(t, sinkA, lb)
	pB, _ := startTestProcessor(t, sinkB, lb)

	id := pcommon.TraceID{0x77}
	tdB := ptrace.NewTraces() // B holds a healthy fragment of the trace
	spB := tdB.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	spB.SetTraceID(id)
	spB.SetName("b-side")
	_, err := pB.processTraces(context.Background(), tdB)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)

	tdA := errorSpanBatch(id, "a-side") // A sees the error span
	_, err = pA.processTraces(context.Background(), tdA)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)

	require.Eventually(t, func() bool { return sinkA.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "A flushes its own fragment")
	require.Eventually(t, func() bool { return sinkB.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "B flushes its fragment on the broadcast")
	assert.Equal(t, []string{"b-side"}, spanNames(sinkB))
	assert.Equal(t, []string{"a-side"}, spanNames(sinkA))
}

// Every fragment is buffered even when nothing is emitted: retention is
// buffer-then-decide, not decide-then-buffer.
func TestHealthyTraceIsBufferedNotEmitted(t *testing.T) {
	sink := new(consumertest.TracesSink)
	p, cfg := startTestProcessor(t, sink, bus.NewLoopback())
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	id := pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sp.SetTraceID(id)
	sp.SetName("op")
	_, err := p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.NoError(t, p.shutdown(context.Background()))
	assert.Zero(t, sink.SpanCount(), "an undecided trace is never emitted")

	// The fragment landed in exactly one shard; find it by reopening the
	// shard buffers directly.
	dirs, err := filepath.Glob(filepath.Join(cfg.StorageDir, "shard-*"))
	require.NoError(t, err)
	require.NotEmpty(t, dirs)
	visits := 0
	for _, d := range dirs {
		b, err := buffer.Open(d, buffer.Options{Window: cfg.Window, SegmentSize: cfg.SegmentSize}, systemClock())
		require.NoError(t, err)
		_, cerr := b.Collect(id, func(frag []byte) {
			dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
			require.NoError(t, err)
			assert.Equal(t, "op", dec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
			visits++
		})
		require.NoError(t, cerr)
		require.NoError(t, b.Close())
	}
	assert.Equal(t, 1, visits, "span was buffered in exactly one shard")
}

// Before start and after shutdown the answer is the same retryable
// refusal: retention has no passthrough mode to fall back to.
func TestProcessTracesWithoutLiveSetRefuses(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	_, err := p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, errOverloaded, "unstarted: refuse, never pass through")
	require.NoError(t, p.shutdown(context.Background()), "shutdown without start is a no-op")
}

func TestShutdownTwiceIsSafeAndPostShutdownRefuses(t *testing.T) {
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, bus.NewLoopback())
	require.NoError(t, p.shutdown(context.Background()))
	require.NoError(t, p.shutdown(context.Background()), "second shutdown must not panic")

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	_, err := p.processTraces(context.Background(), td)
	require.ErrorIs(t, err, errOverloaded)
	assert.Zero(t, sink.SpanCount())
}

// lifecycleBus is a Loopback that records the lifecycle calls a bus with
// a connection behind it receives: the Start it is owed, the publishes
// it carries, the cancel that retires the subscription and the Close
// that ends it. The recorded ORDER is the point — every one of these
// steps is invisible in its own right, and getting them out of order
// costs keeps rather than raising an error.
type lifecycleBus struct {
	bus.Bus
	startErr error
	// closeGate, when non-nil, wedges Close until it is closed;
	// onClose runs inside Close, ahead of the gate.
	closeGate chan struct{}
	onClose   func()

	mu     sync.Mutex
	events []string
}

func newLifecycleBus() *lifecycleBus { return &lifecycleBus{Bus: bus.NewLoopback()} }

func (l *lifecycleBus) record(ev string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *lifecycleBus) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

func (l *lifecycleBus) Start(context.Context) error {
	l.record("start")
	return l.startErr
}

func (l *lifecycleBus) Close() error {
	l.record("close")
	if l.onClose != nil {
		l.onClose()
	}
	if l.closeGate != nil {
		<-l.closeGate
	}
	return nil
}

func (l *lifecycleBus) Publish(ctx context.Context, keeps []bus.Keep) ([]bus.Keep, error) {
	l.record("publish")
	return l.Bus.Publish(ctx, keeps)
}

func (l *lifecycleBus) Subscribe(fn func(id [16]byte, reason byte)) (func(), error) {
	cancel, err := l.Bus.Subscribe(fn)
	if err != nil {
		return nil, err
	}
	return func() { l.record("cancel"); cancel() }, nil
}

// A bus with a connection gets a lifecycle: dialled before anything
// subscribes to it, closed only once the drains that use it are done.
// Both halves fail quietly otherwise — an undialled client refuses every
// publish, and an unclosed one outlives the collector's shutdown.
func TestProcessorStartsAndClosesBus(t *testing.T) {
	lb := newLifecycleBus()
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, lb)
	require.Equal(t, []string{"start"}, lb.recorded(),
		"Start leads: a client that has not dialled can carry no subscription")

	consumeAccepted(t, p, errorSpanBatch(pcommon.TraceID{0x77}, "kept"))
	waitOneKeptSpan(t, sink)
	require.NoError(t, p.shutdown(context.Background()))
	assert.Equal(t, []string{"start", "publish", "cancel", "close"}, lb.recorded(),
		"the connection outlives both the broadcast it carries and the subscription it feeds")
}

// A bus that cannot start fails the processor's start, rather than
// leaving a live component whose every broadcast is refused. Nothing is
// left running behind the refusal either — the goleak census in TestMain
// is the other half of that claim.
func TestProcessorStartFailsWhenBusStartFails(t *testing.T) {
	lb := newLifecycleBus()
	lb.startErr = errors.New("unusable credentials")
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	p := newTestProcessor(t, cfg, lb)
	p.next = new(consumertest.TracesSink)

	require.ErrorContains(t, p.start(context.Background(), componenttest.NewNopHost()),
		"unusable credentials")
	assert.Nil(t, p.set.Load(), "a failed start leaves no set behind for ingest to find")
	require.NoError(t, p.shutdown(context.Background()), "shutdown after a failed start is a no-op")
}

// A bus that started, then a failure underneath it: the connection the
// start opened is the start's to close. Nothing else ever would —
// shutdown finds the nil set and returns — so it would outlive the
// component that dialled it.
func TestProcessorClosesBusWhenStartFailsUnderneathIt(t *testing.T) {
	lb := newLifecycleBus()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.Shards = 2
	// Under the shards' own active-segment floor, so New refuses: the
	// failure has to land after the bus is up, not before it.
	cfg.DiskBudget = 1 << 20
	p := newTestProcessor(t, cfg, lb)
	p.next = new(consumertest.TracesSink)

	require.Error(t, p.start(context.Background(), componenttest.NewNopHost()))
	assert.Equal(t, []string{"start", "close"}, lb.recorded(),
		"the bus does not outlive the start that opened it")
	require.NoError(t, p.shutdown(context.Background()))
	assert.Equal(t, []string{"start", "close"}, lb.recorded(),
		"and the shutdown that follows does not close it a second time")
}

// Close is bounded by the shutdown context. nats.go's drain parks up to
// its own 30s timeout on a wedged delivery callback, which a collector
// shutdown cannot afford to inherit for a connection that is about to
// die with the process. The abandoned Close must not fail an otherwise
// clean shutdown, and must not leave one owed: a shutdown that reported
// success is never retried, so a second Close would never happen anyway.
func TestShutdownAbandonsAWedgedBusClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	// Released before the goleak census, which the parked Close goroutine
	// would otherwise fail — the bound abandons the call, it cannot unwedge
	// the connection.
	defer close(release)

	lb := newLifecycleBus()
	lb.closeGate = release
	// The context expires inside Close rather than on a timer: what is
	// under test is the bound itself, not how long the drains took.
	lb.onClose = cancel

	p, _ := startTestProcessor(t, new(consumertest.TracesSink), lb)
	require.NoError(t, p.shutdown(ctx),
		"a slow drain on a dying connection is not a shutdown failure")
	assert.Nil(t, p.set.Load(),
		"the shutdown completed, so the retry path is not armed")

	require.NoError(t, p.shutdown(context.Background()))
	assert.Equal(t, []string{"start", "cancel", "close"}, lb.recorded(),
		"the abandoned Close is never retried into a second one")
}

// A refused fragment fails the whole batch: the upstream retry re-offers
// and re-detects, so a shed never silently loses a keep condition. The
// set is stopped underneath the processor to make the refusal exact —
// Offer's own overload rungs are gated in internal/shards.
func TestRefusedFragmentReportsOverload(t *testing.T) {
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, bus.NewLoopback())
	s := p.set.Load()
	require.NotNil(t, s)
	require.NoError(t, s.Shutdown(context.Background()))

	_, err := p.processTraces(context.Background(), errorSpanBatch(pcommon.TraceID{0x8C}, "refused"))
	require.ErrorIs(t, err, errOverloaded)
	assert.False(t, consumererror.IsPermanent(err),
		"overload is the receiver's to retry, not a permanent drop")
}

// A shed fragment must not suppress the keep VERDICT. The shard layer
// deliberately does not floor-gate Keep (ADR-008 r4): a verdict concerns
// data already buffered, not new volume. If an ingest refusal swallowed
// the verdict with it, an error trace whose fragments are already on
// disk would go unflushed for as long as the shed lasts — and the
// shards' zero-fragment publish-only job (TestZeroFragmentLocalKeep-
// StillPublishes) would be unreachable from production, since that path
// exists precisely for a keep whose fragments were refused.
func TestShedFragmentStillLandsTheKeepVerdict(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.Shards = 1
	cfg.SegmentSize = 1 << 20
	// The ~1.6 MiB watermark clears the single shard's 1 MiB
	// active-segment floor, and the load below goes over it. Everything
	// written here is inside window_floor, so nothing is reclaimable and
	// the shard parks at rung 2 rather than early-expiring.
	cfg.DiskBudget = 2 << 20
	sink := new(consumertest.TracesSink)
	p := newTestProcessor(t, cfg, bus.NewLoopback())
	p.next = sink
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.shutdown(context.Background())) }()

	ctx := context.Background()
	victim := pcommon.TraceID{0xE7}
	early := ptrace.NewTraces()
	sp := early.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(victim)
	sp.SetName("buffered-early")
	_, err := p.processTraces(ctx, early)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData,
		"the victim's fragment is buffered while the shard is still healthy")

	// One fragment per call, well inside the free ring, so the refusal
	// this waits for is the floor rung and not queue exhaustion.
	filler := strings.Repeat("x", 128<<10)
	n := 0
	require.Eventually(t, func() bool {
		n++
		td := ptrace.NewTraces()
		f := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		f.SetTraceID(pcommon.TraceID{0xF0, byte(n), byte(n >> 8)})
		f.SetName(filler)
		_, ferr := p.processTraces(ctx, td)
		return errors.Is(ferr, errOverloaded)
	}, 30*time.Second, 50*time.Millisecond,
		"the shard fills past its watermark and rung 2 engages")
	st := p.set.Load().Stats()
	require.Zero(t, st.ShedQueueFull, "the shed under test is the floor, not ring exhaustion")
	require.Positive(t, st.ShedFloorProtected+st.ShedNothingReclaimable)

	// The headline case, mid-shed: this batch's span is refused, but the
	// verdict it carries must still reach the shard.
	_, err = p.processTraces(ctx, errorSpanBatch(victim, "shed-error-span"))
	require.ErrorIs(t, err, errOverloaded, "a refused fragment still fails the whole batch")
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		10*time.Second, time.Millisecond,
		"the verdict lands despite the shed, so the already-buffered fragment flushes")
	assert.Equal(t, []string{"buffered-early"}, spanNames(sink),
		"the shed span never reached disk; what flushes is what was buffered before it")
	assert.Equal(t, uint64(1), p.set.Load().Stats().KeptLocal)
}

// Concurrent ingest during shutdown: no panic, no goroutine leak (the
// goleak TestMain), and post-shutdown batches get a retryable error
// rather than silent acceptance.
func TestConcurrentIngestVsShutdown(t *testing.T) {
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, bus.NewLoopback())

	const writers = 4
	var wg sync.WaitGroup
	var accepted atomic.Int64
	stop := make(chan struct{})
	bad := make(chan error, writers)
	for w := range writers {
		wg.Go(func() {
			td := ptrace.NewTraces()
			sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetTraceID(pcommon.TraceID{byte(w + 1)})
			sp.SetName("racer")
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := p.processTraces(context.Background(), td)
				switch {
				case errors.Is(err, processorhelper.ErrSkipProcessingData):
					accepted.Add(1)
				case errors.Is(err, errOverloaded):
				default:
					bad <- err
					return
				}
			}
		})
	}
	require.Eventually(t, func() bool { return accepted.Load() > 100 },
		5*time.Second, time.Millisecond, "writers are running before shutdown starts")

	// A deadline rather than Background: a quiesce that cannot observe an
	// idle moment must fail loudly here instead of hanging the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, p.shutdown(ctx))
	close(stop)
	wg.Wait()
	close(bad)
	for err := range bad {
		t.Errorf("ingest racing shutdown returned an unexpected error: %v", err)
	}

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	for range 4 {
		_, err := p.processTraces(context.Background(), td)
		require.ErrorIs(t, err, errOverloaded, "post-shutdown ingest is refused, not accepted")
	}
}

// The shards must outlive the flusher. A flusher stuck in the consumer
// still owes its shard a Retry, and that Retry aborts only on the
// flusher's own stop signal: shutting the set down first would strand it
// (and burn the whole context quiescing its in-flight token).
func TestShutdownStopsFlusherBeforeShards(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	var calls atomic.Int64
	blocking, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error {
		if calls.Add(1) > 1 {
			return nil
		}
		once.Do(func() { close(entered) })
		<-gate
		return consumererror.NewRetryableError(errors.New("downstream unavailable"))
	})
	require.NoError(t, err)
	p, _ := startTestProcessor(t, blocking, bus.NewLoopback())

	_, err = p.processTraces(context.Background(), errorSpanBatch(pcommon.TraceID{0xD0}, "wedged"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	<-entered

	s := p.set.Load()
	require.NotNil(t, s)
	errc := make(chan error, 1)
	go func() { errc <- p.shutdown(context.Background()) }()

	probe := pcommon.TraceID{0xD1}
	frag := encodeFrag(t, probe, "probe")
	require.Never(t, func() bool { return !s.Offer(probe, frag, systemClock()) },
		250*time.Millisecond, 5*time.Millisecond,
		"the set must still be live while the flusher holds unfinished work")

	close(gate)
	require.NoError(t, <-errc, "a Retry in flight at shutdown still completes cleanly")
	assert.Nil(t, p.set.Load())
}

// TestStaticCensusAlive is the root half of the positive goroutine
// census (ADR-007 r7). goleak proves every goroutine is gone after
// shutdown; nothing proved the two the processor starts are alive and
// answering WHILE it runs, and both fail silently. A flusher that never
// started leaves the shards parking jobs forever with no error anywhere;
// a subscription that never took leaves this instance deaf to every
// other instance's verdicts, which on a single-instance loopback still
// looks entirely healthy.
//
// Counting goroutines would prove neither, so each is censused by the
// effect only a live one can produce. The shard workers' own census is
// structural and lives in internal/shards, where the shard set is
// reachable.
func TestStaticCensusAlive(t *testing.T) {
	lb := bus.NewLoopback()
	sink := new(consumertest.TracesSink)
	p, _ := startTestProcessor(t, sink, lb)
	ctx := context.Background()

	_, err := p.processTraces(ctx, errorSpanBatch(pcommon.TraceID{0xC1}, "flusher-alive"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond,
		"the flusher goroutine must be draining the job channel")

	// The subscriber: a healthy trace nothing local would ever keep, then
	// a verdict arriving over the bus. Only a live subscription turns the
	// second into a flush of the first.
	deaf := pcommon.TraceID{0xC2}
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(deaf)
	sp.SetName("subscriber-alive")
	_, err = p.processTraces(ctx, td)
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	failed, err := lb.Publish(ctx, []bus.Keep{{ID: deaf, Reason: bus.ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Eventually(t, func() bool { return sink.SpanCount() == 2 },
		5*time.Second, time.Millisecond,
		"the bus subscription must be live and routing keeps into the shards")
	assert.Equal(t, []string{"flusher-alive", "subscriber-alive"}, spanNames(sink))
}

// A timed-out shutdown leaves the processor retryable: the set pointer is
// restored on error, so the retry actually shuts down instead of falsely
// succeeding against a nil set while the workers keep running.
func TestShutdownRetryAfterTimeoutIsReal(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	blocking, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	require.NoError(t, err)
	p, _ := startTestProcessor(t, blocking, bus.NewLoopback())

	_, err = p.processTraces(context.Background(), errorSpanBatch(pcommon.TraceID{0xB1}, "stuck"))
	require.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()
	require.ErrorIs(t, p.shutdown(ctx), context.DeadlineExceeded)
	require.NotNil(t, p.set.Load(),
		"a timed-out shutdown leaves the workers running: only a live set pointer can stop them")

	close(release)
	require.NoError(t, p.shutdown(context.Background()))
	assert.Nil(t, p.set.Load(), "the successful retry is what retires the set")
}
