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

// encodeFrag marshals a one-span trace via the production encoder.
func encodeFrag(t *testing.T, id pcommon.TraceID, name string) []byte {
	t.Helper()
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(id)
	sp.SetName(name)
	var out []byte
	fragmenter.New().Fragment(td, nil, func(_ pcommon.TraceID, frag []byte, _ bool) {
		out = append([]byte(nil), frag...)
	})
	require.NotEmpty(t, out)
	return out
}

// busSpy records publishes, and refuses them while failing is set.
type busSpy struct {
	bus.Bus
	failing atomic.Bool
	mu      sync.Mutex
	ids     [][16]byte
}

func newBusSpy() *busSpy { return &busSpy{Bus: bus.NewLoopback()} }

func (b *busSpy) Publish(ctx context.Context, id [16]byte, reason byte) error {
	if b.failing.Load() {
		return errors.New("bus unavailable")
	}
	b.mu.Lock()
	b.ids = append(b.ids, id)
	b.mu.Unlock()
	return b.Bus.Publish(ctx, id, reason)
}

func (b *busSpy) published() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.ids)
}

func TestFlusherPublishesDecodesAndConsumes(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	sink := new(consumertest.TracesSink)
	spy := newBusSpy()
	fl := newFlusher(jobs, nil, sink, spy)
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
	require.Eventually(t, func() bool { return spy.published() == 2 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, 3, sink.SpanCount())
}

// consumer failure sends the intent back to the shard as a Retry; a
// later tick re-collects. Uses a real Set so Retry has a live target.
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
	fl := newFlusher(jobs, set, flaky, bus.NewLoopback())
	fl.start()
	defer func() { require.NoError(t, fl.stop(context.Background())) }()

	require.True(t, set.Keep(id, bus.ReasonError, clk.Now()))
	require.Eventually(t, func() bool { return set.Stats().FlushRetries >= 1 },
		5*time.Second, time.Millisecond, "failed consume returns as a shard retry")

	fail.Store(false)
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "tick-driven retry eventually flushes")
	assert.GreaterOrEqual(t, fl.flushErrors.Load(), uint64(1))
}

// A permanent consumer error drops the job — counted, no retry loop.
func TestFlusherPermanentErrorDropsJob(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	perm := consumertest.NewErr(consumererror.NewPermanent(errors.New("bad")))
	fl := newFlusher(jobs, nil, perm, bus.NewLoopback())
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
	fl := newFlusher(jobs, set, sink, spy)
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
	fl := newFlusher(jobs, set, perm, bus.NewLoopback())
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
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback())

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
	fl := newFlusher(jobs, nil, blocking, bus.NewLoopback())
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
	fl := newFlusher(make(chan *shards.FlushJob), nil, consumertest.NewNop(), bus.NewLoopback())
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
	fl := newFlusher(jobs, nil, sink, bus.NewLoopback())
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

	// All fragments undecodable: nothing reaches the pipeline at all.
	jobs <- &shards.FlushJob{
		ID: id, Need: shards.NeedFlush,
		Frags: [][]byte{{0xFF, 0xFF}},
	}
	require.Eventually(t, func() bool { return fl.decodeErrors.Load() == 2 },
		5*time.Second, time.Millisecond)
	assert.Equal(t, 1, sink.SpanCount())
}

// Without a Set there is nowhere to re-park a retryable failure; the
// flusher counts it and carries on rather than dereferencing nil.
func TestFlusherRetryableFailureWithoutSetIsCounted(t *testing.T) {
	jobs := make(chan *shards.FlushJob, 4)
	var fail atomic.Bool
	fail.Store(true)
	flaky := newFlakyConsumer(t, &fail, consumertest.NewNop())
	fl := newFlusher(jobs, nil, flaky, bus.NewLoopback())
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
