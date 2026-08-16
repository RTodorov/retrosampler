// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/fragmenter"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// fakeProcClock is a mutable injected clock, safe for concurrent use —
// the root-package twin of the shards test clock.
type fakeProcClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeProcClock(t0 time.Time) *fakeProcClock { return &fakeProcClock{t: t0} }

func (c *fakeProcClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeProcClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newFlakyConsumer refuses every batch with a retryable error while fail
// is set, and delegates to sink once it clears.
func newFlakyConsumer(t *testing.T, fail *atomic.Bool, sink consumer.Traces) consumer.Traces {
	t.Helper()
	c, err := consumer.NewTraces(func(ctx context.Context, td ptrace.Traces) error {
		if fail.Load() {
			return consumererror.NewRetryableError(errors.New("downstream unavailable"))
		}
		return sink.ConsumeTraces(ctx, td)
	})
	require.NoError(t, err)
	return c
}

// newFlushTestSet builds a one-shard Set feeding jobs, ticking fast
// enough for a retry sweep to land inside a test.
func newFlushTestSet(t *testing.T, jobs chan *shards.FlushJob, clk *fakeProcClock) *shards.Set {
	t.Helper()
	set, err := shards.New(shards.Options{
		Dir: t.TempDir(), Shards: 1, Window: time.Hour, SegmentSize: 1 << 20,
		DiskBudget: 1 << 40, WatermarkPct: 80, WindowFloor: time.Minute,
		Now: clk.Now, Tick: 10 * time.Millisecond, Flush: jobs,
	})
	require.NoError(t, err)
	return set
}

// encodeFrag marshals a one-span trace via the production encoder,
// leaving the span's start unstamped as most of these fixtures do.
func encodeFrag(t *testing.T, id pcommon.TraceID, name string) []byte {
	t.Helper()
	return encodeFragAt(t, id, name, time.Time{})
}

// encodeFragAt is encodeFrag with the span's start stamped: that
// timestamp is the only fixture input the age ratio reads. A zero start
// is left unset rather than encoded as the epoch, so an unstamped
// fixture stays indistinguishable from a span that never carried one.
func encodeFragAt(t *testing.T, id pcommon.TraceID, name string, start time.Time) []byte {
	t.Helper()
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName(name)
	if !start.IsZero() {
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	}
	var out []byte
	fragmenter.New().Fragment(td, nil, nil, func(_ pcommon.TraceID, frag []byte, _ byte, _ bool) {
		out = append([]byte(nil), frag...)
	})
	require.NotEmpty(t, out)
	return out
}

// ageSpy collects the age ratios the flusher records. The flusher writes
// them from its own goroutine, so the slice is guarded and handed out as
// a copy — the busSpy convention.
type ageSpy struct {
	mu      sync.Mutex
	samples []float64
}

func (s *ageSpy) record(ratio float64) {
	s.mu.Lock()
	s.samples = append(s.samples, ratio)
	s.mu.Unlock()
}

func (s *ageSpy) recorded() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]float64(nil), s.samples...)
}

// spanNames flattens every span name the sink has been handed.
func spanNames(sink *consumertest.TracesSink) []string {
	var names []string
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				sps := sss.At(j).Spans()
				for k := 0; k < sps.Len(); k++ {
					names = append(names, sps.At(k).Name())
				}
			}
		}
	}
	return names
}

// publishedKeep is one broadcast verdict as the spy saw it. The reason
// is recorded alongside the id because what crosses the bus is the
// verdict, not just the trace: a keep published under the wrong reason
// misattributes it on every other instance.
type publishedKeep struct {
	id     [16]byte
	reason byte
}

// busSpy records publishes, and refuses them while failing is set.
type busSpy struct {
	bus.Bus
	failing  atomic.Bool
	mu       sync.Mutex
	verdicts []publishedKeep
}

func newBusSpy() *busSpy { return &busSpy{Bus: bus.NewLoopback()} }

func (b *busSpy) Publish(ctx context.Context, keeps []bus.Keep) ([]bus.Keep, error) {
	if len(keeps) == 0 {
		return nil, nil // nothing to fail, even while failing
	}
	if b.failing.Load() {
		return append([]bus.Keep(nil), keeps...), errors.New("bus unavailable")
	}
	b.mu.Lock()
	for _, k := range keeps {
		b.verdicts = append(b.verdicts, publishedKeep{id: k.ID, reason: k.Reason})
	}
	b.mu.Unlock()
	return b.Bus.Publish(ctx, keeps)
}

func (b *busSpy) published() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.verdicts)
}

// publishedVerdicts snapshots what has crossed the bus so far; the copy
// keeps the caller's assertions off the flusher goroutine's slice.
func (b *busSpy) publishedVerdicts() []publishedKeep {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]publishedKeep(nil), b.verdicts...)
}

func TestFlusherPublishesDecodesAndConsumes(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	spy := newBusSpy()
	fl := newFlusher(jobs, nil, sink, spy, 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0xAB}
	jobs <- &shards.FlushJob{
		ID: id, Reason: bus.ReasonError, Need: shards.NeedPublish | shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "one"), encodeFrag(t, id, "two")},
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == 2 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, 1, spy.published())
	assert.Equal(t, uint64(1), fl.publishedKeeps.Load())
	assert.Equal(t, uint64(2), fl.flushedSpans.Load())
	assert.ElementsMatch(t, []string{"one", "two"}, spanNames(sink),
		"the merge carries both fragments' spans, not just their count")

	// Flush-only job: no publish.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "three")},
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == 3 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, 1, spy.published())

	// Publish-only job: the shard emits one when a keep's fragments were
	// refused, and it must not fabricate an empty flush.
	jobs <- &shards.FlushJob{ID: id, Reason: bus.ReasonError, Need: shards.NeedPublish}
	// Wait on the flusher's own counter, not the spy: the spy records the
	// publish before delegating, so it leads publishedKeeps and waiting on
	// it would race the assertion below.
	require.Eventually(t, func() bool { return fl.publishedKeeps.Load() == 2 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, 2, spy.published())
	assert.Equal(t, 3, sink.SpanCount())
}

// consumer failure sends the intent back to the shard as a Retry; a
// later tick re-collects. Uses a real Set so Retry has a live target,
// and the spy so the broadcast is counted: only the flush bit goes back,
// so however many times the consume is retried the keep is published
// exactly once. Re-parking the job's whole Need instead would rebroadcast
// to every peer cluster on every tick.
func TestFlusherConsumerFailureRetriesViaShard(t *testing.T) {
	clk := newFakeProcClock(time.Unix(1000, 0))
	jobs := make(chan *shards.FlushJob, 4)
	set := newFlushTestSet(t, jobs, clk)
	defer func() { require.NoError(t, set.Shutdown(context.Background())) }()

	id := pcommon.TraceID{0xCD}
	frag := encodeFrag(t, id, "keepme")
	require.True(t, set.Offer(id, frag, clk.Now()))

	var fail atomic.Bool
	fail.Store(true)
	sink := new(consumertest.TracesSink)
	flaky := newFlakyConsumer(t, &fail, sink)
	spy := newBusSpy()
	fl := newFlusher(jobs, set, flaky, spy, 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	require.True(t, set.Keep(id, bus.ReasonError, clk.Now()))
	require.Eventually(t, func() bool { return set.Stats().FlushRetries >= 3 },
		5*time.Second, time.Millisecond, "failed consume returns as a shard retry")
	assert.Equal(t, 1, spy.published(), "a consume retry must not rebroadcast the keep")

	fail.Store(false)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "tick-driven retry eventually flushes")
	assert.GreaterOrEqual(t, fl.flushErrors.Load(), uint64(1))
	assert.Equal(t, 1, spy.published(), "the flush that finally lands owes no broadcast")
	assert.Equal(t, uint64(1), fl.publishedKeeps.Load())
}

// A permanent consumer error drops the job — counted, no retry loop.
func TestFlusherPermanentErrorDropsJob(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	perm := consumertest.NewErr(consumererror.NewPermanent(errors.New("bad")))
	fl := newFlusher(jobs, nil, perm, bus.NewLoopback(), 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0xEF}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "doomed")},
	}
	require.Eventually(t, func() bool { return fl.flushErrors.Load() == 1 },
		5*time.Second, time.Millisecond)
}

// A bus refusal re-parks the whole intent, publish bit included, and
// gates the flush behind it: a transient Publish failure is the flush
// machinery's to retry (ADR-008 r6), not a dropped broadcast.
func TestFlusherPublishFailureRetriesWholeIntent(t *testing.T) {
	clk := newFakeProcClock(time.Unix(1000, 0))
	jobs := make(chan *shards.FlushJob, 4)
	set := newFlushTestSet(t, jobs, clk)
	defer func() { require.NoError(t, set.Shutdown(context.Background())) }()

	id := pcommon.TraceID{0x44}
	require.True(t, set.Offer(id, encodeFrag(t, id, "keepme"), clk.Now()))

	spy := newBusSpy()
	spy.failing.Store(true)
	sink := new(consumertest.TracesSink)
	fl := newFlusher(jobs, set, sink, spy, 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	require.True(t, set.Keep(id, bus.ReasonError, clk.Now()))
	require.Eventually(t, func() bool { return set.Stats().FlushRetries >= 1 },
		5*time.Second, time.Millisecond, "a refused publish returns as a shard retry")
	assert.Zero(t, sink.SpanCount(), "the flush waits behind the unsent broadcast")

	spy.failing.Store(false)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the retried intent publishes, then flushes")
	assert.Equal(t, uint64(1), fl.publishedKeeps.Load())
}

// Permanence is what separates the two failure paths, and only a live
// Set can observe the difference: a permanent error must not re-park the
// job on the shard, or the trace loops there for the rest of its window.
func TestFlusherPermanentErrorSkipsShardRetry(t *testing.T) {
	clk := newFakeProcClock(time.Unix(1000, 0))
	jobs := make(chan *shards.FlushJob, 4)
	set := newFlushTestSet(t, jobs, clk)
	defer func() { require.NoError(t, set.Shutdown(context.Background())) }()

	id := pcommon.TraceID{0x22}
	require.True(t, set.Offer(id, encodeFrag(t, id, "doomed"), clk.Now()))

	perm := consumertest.NewErr(consumererror.NewPermanent(errors.New("bad")))
	fl := newFlusher(jobs, set, perm, bus.NewLoopback(), 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	require.True(t, set.Keep(id, bus.ReasonError, clk.Now()))
	require.Eventually(t, func() bool { return fl.flushErrors.Load() >= 1 },
		5*time.Second, time.Millisecond)
	require.Never(t, func() bool { return set.Stats().FlushRetries > 0 },
		200*time.Millisecond, 5*time.Millisecond,
		"a permanent error drops the job instead of looping on the shard")
}

// Jobs already queued when stop arrives still flush: the drain is what
// keeps a kept trace from dying on the floor at shutdown.
func TestFlusherDrainsQueuedJobsOnStop(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback(), 0, nil, nil)

	id := pcommon.TraceID{0x33}
	for _, name := range []string{"a", "b", "c"} {
		jobs <- &shards.FlushJob{
			ID: id, Need: shards.NeedFlush,
			Frags: [][]byte{encodeFrag(t, id, name)},
		}
	}
	fl.start()
	require.NoError(t, fl.stop(context.Background()))
	assert.Equal(t, 3, sink.SpanCount())
}

// A wedged consumer makes stop report its context's expiry rather than
// hang, and the retry once the blockage clears still returns cleanly —
// the reason the stop signal is closed exactly once.
func TestFlusherStopReportsTimeoutThenSucceeds(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	release := make(chan struct{})
	blocking, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error {
		<-release
		return nil
	})
	require.NoError(t, err)
	fl := newFlusher(jobs, nil, blocking, bus.NewLoopback(), 0, nil, nil)
	fl.start()

	id := pcommon.TraceID{0x55}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "stuck")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, fl.stop(ctx), context.DeadlineExceeded)

	close(release)
	require.NoError(t, fl.stop(context.Background()))
}

// stop is retried by the processor's shutdown: the second call must not
// double-close the signal, and a flusher that already exited must not be
// charged to a spent context (the shards.Shutdown convention).
func TestFlusherStopRetryAfterCleanExit(t *testing.T) {
	fl := newFlusher(make(chan *shards.FlushJob), nil, consumertest.NewNop(), bus.NewLoopback(), 0, nil, nil)
	fl.start()
	require.NoError(t, fl.stop(context.Background()))

	spent, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, fl.stop(spent))
}

// Undecodable fragments are skipped and counted; the rest still flush.
func TestFlusherSkipsUndecodableFragment(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback(), 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0x11}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{{0xFF, 0xFF}, encodeFrag(t, id, "survivor")},
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, uint64(1), fl.decodeErrors.Load())
	require.Len(t, sink.AllTraces(), 1)

	// All fragments undecodable: every bad fragment is counted on its own,
	// and the consumer is never handed the resulting empty batch.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{{0xFF, 0xFF}, {0xFE, 0xFE}},
	}
	require.Eventually(t, func() bool { return fl.decodeErrors.Load() == 3 },
		5*time.Second, time.Millisecond, "decode failures count per fragment, not per job")

	// A good job behind it: jobs are processed in order by the one flusher
	// goroutine, so once this lands the empty job is provably finished and
	// the batch count can be trusted.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "closer")},
	}
	require.Eventually(t, func() bool { return sink.SpanCount() == 2 },
		5*time.Second, time.Millisecond)
	assert.Len(t, sink.AllTraces(), 2, "an all-undecodable job sends no empty batch")
}

// Without a Set there is nowhere to re-park a retryable failure; the
// flusher counts it and carries on rather than dereferencing nil.
func TestFlusherRetryableFailureWithoutSetIsCounted(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	var fail atomic.Bool
	fail.Store(true)
	flaky := newFlakyConsumer(t, &fail, consumertest.NewNop())
	fl := newFlusher(jobs, nil, flaky, bus.NewLoopback(), 0, nil, nil)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0x66}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "orphan")},
	}
	require.Eventually(t, func() bool { return fl.flushErrors.Load() == 1 },
		5*time.Second, time.Millisecond)
}

// The W instrument (ADR-011 r9): one sample per flushed fragment batch,
// (now - the OLDEST span start across every fragment) / W. The oldest is
// what the ratio has to read — it is the span nearest expiry, and the
// question the histogram answers is whether keeps are beating W.
//
// Three phases, ordered behind one another on purpose: the single
// flusher goroutine takes jobs in order, so once a later job's spans
// reach the sink the earlier ones are provably finished and the samples
// can be counted rather than waited on.
func TestFlusherRecordsAgeRatio(t *testing.T) {
	now := time.Unix(2000, 0)
	spy := new(ageSpy)
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback(),
		time.Minute, func() time.Time { return now }, spy.record)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	// 30s of age against a 60s window, and a younger span alongside it
	// that must not win.
	id := pcommon.TraceID{0x71}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{
			encodeFragAt(t, id, "young", now.Add(-10*time.Second)),
			encodeFragAt(t, id, "oldest", now.Add(-30*time.Second)),
		},
	}
	require.Eventually(t, func() bool { return len(spy.recorded()) == 1 },
		5*time.Second, time.Millisecond)
	assert.InDelta(t, 0.5, spy.recorded()[0], 0.001,
		"the oldest span across the job sets the age, not the newest")

	// A publish-only job carries no fragments and so has no age at all;
	// recording a zero for it would pile mass at the healthy end of the
	// histogram and understate exactly what the instrument watches for.
	jobs <- &shards.FlushJob{ID: id, Reason: bus.ReasonError, Need: shards.NeedPublish}
	// An unstamped span is an age nobody knows, which is not an age of
	// zero either.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFrag(t, id, "unstamped")},
	}
	// The closer proves both jobs above are done.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFragAt(t, id, "closer", now.Add(-90*time.Second))},
	}
	// The spans are the barrier, not the sample count. The sample is taken
	// BEFORE the consume, so the closer's sample can land while its spans
	// are still in flight: waiting on the spans implies the sample, never
	// the reverse.
	require.Eventually(t, func() bool { return sink.SpanCount() == 4 },
		5*time.Second, time.Millisecond,
		"all four spans flushed: the two unrecorded jobs are unrecorded, not dropped")
	recorded := spy.recorded()
	require.Len(t, recorded, 2, "the publish-only and unstamped jobs recorded nothing")
	assert.InDelta(t, 1.5, recorded[1], 0.001,
		"age past W is recorded as it is: over 1.0 is the signal, not an error")
}

// A span stamped in the future is clock skew, not negative age. It
// clamps to 0 (ADR-008 r7) rather than putting mass below the histogram
// and dragging the distribution away from the W it is measuring.
func TestFlusherClampsFutureStampedAgeRatio(t *testing.T) {
	now := time.Unix(2000, 0)
	spy := new(ageSpy)
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback(),
		time.Minute, func() time.Time { return now }, spy.record)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0x72}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFragAt(t, id, "ahead", now.Add(time.Minute))},
	}
	require.Eventually(t, func() bool { return len(spy.recorded()) == 1 },
		5*time.Second, time.Millisecond)
	assert.Zero(t, spy.recorded()[0], "a future stamp clamps rather than going negative")
}

// The measurement needs a recorder, a clock and a positive window, and
// each missing on its own turns it off: no recorder is a processor whose
// telemetry was never bound, a zero window is a denominator that would
// divide by zero, and a nil clock is a caller that wired two of the
// three. None of them may cost the flush itself, and none may panic the
// flusher goroutine — which would take the collector with it.
func TestFlusherSkipsAgeRatioWhenUnwired(t *testing.T) {
	now := time.Unix(2000, 0)
	clock := func() time.Time { return now }
	id := pcommon.TraceID{0x73}
	// Reaching this at all means the guard let an unwired flusher through.
	refuse := func(float64) { t.Error("an unwired flusher has no ratio to record") }

	for _, tc := range []struct {
		name   string
		window time.Duration
		now    func() time.Time
		rec    func(float64)
	}{
		{"no recorder", time.Minute, clock, nil},
		{"zero window", 0, clock, refuse},
		{"no clock", time.Minute, nil, refuse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jobs := make(chan *shards.FlushJob, 1)
			sink := new(consumertest.TracesSink)
			fl := newFlusher(jobs, nil, sink, bus.NewLoopback(),
				tc.window, tc.now, tc.rec)
			fl.start()
			defer func() { require.NoError(t, fl.stop(context.Background())) }()

			jobs <- &shards.FlushJob{
				ID: id, Need: shards.NeedFlush,
				Frags: [][]byte{encodeFragAt(t, id, "quiet", now.Add(-30*time.Second))},
			}
			require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
				5*time.Second, time.Millisecond, "the flush is unaffected either way")
		})
	}
}

// The sample is taken per flush ATTEMPT, not per accepted batch: a
// refused consume records all the same, because those fragments go back
// on the shard and keep aging toward expiry there. Recording only what
// the consumer took would leave the histogram quiet through exactly the
// outage that pushes ratios past 1.0. Without a Set the refusal is
// counted and dropped, so this samples once and not in a retry loop.
func TestFlusherRecordsAgeRatioOnRefusedFlush(t *testing.T) {
	now := time.Unix(2000, 0)
	spy := new(ageSpy)
	jobs := make(chan *shards.FlushJob, 4)
	var fail atomic.Bool
	fail.Store(true)
	flaky := newFlakyConsumer(t, &fail, consumertest.NewNop())
	fl := newFlusher(jobs, nil, flaky, bus.NewLoopback(),
		time.Minute, func() time.Time { return now }, spy.record)
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	id := pcommon.TraceID{0x74}
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{encodeFragAt(t, id, "refused", now.Add(-30*time.Second))},
	}
	// The sample precedes the consume, so the error counter trailing it is
	// proof the recording had its chance.
	require.Eventually(t, func() bool { return fl.flushErrors.Load() == 1 },
		5*time.Second, time.Millisecond)
	recorded := spy.recorded()
	require.Len(t, recorded, 1, "the batch the consumer refused was sampled too")
	assert.InDelta(t, 0.5, recorded[0], 0.001)
}
