// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"path/filepath"
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
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/buffer"
	"github.com/rtodorov/retrosampler/internal/bus"
)

// testDiskBudget clears the shards.New watermark-vs-active-segment-floor
// check (Shards x SegmentSize) on any core count these tests run on.
// Nothing preallocates it.
const testDiskBudget = 1 << 34

// startTestProcessor builds a started processor on temp storage wired to
// sink and b, shut down via t.Cleanup. The cleanup shutdown is a second
// call for the tests that shut down themselves: it must still return nil.
func startTestProcessor(t *testing.T, sink consumer.Traces, b bus.Bus) (*retroProcessor, *Config) {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	p := newProcessor(cfg, zap.NewNop(), systemClock, b)
	// next is wired by the factory in production, which receives the
	// consumer separately from the config.
	p.next = sink
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, p.shutdown(context.Background())) })
	return p, cfg
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
	p := newProcessor(cfg, zap.NewNop(), systemClock, bus.NewLoopback())
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
	require.NoError(t, lb.Publish(context.Background(), id, bus.ReasonError))
	require.Eventually(t, func() bool {
		s := p.set.Load()
		return s != nil && s.DecidedEntries() == 1
	}, 5*time.Second, time.Millisecond, "keep with no spans records decided")

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName("late")
	_, err := p.processTraces(context.Background(), td)
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
	p := newProcessor(cfg, zap.NewNop(), systemClock, bus.NewLoopback())
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
