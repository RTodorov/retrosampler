// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/fragmenter"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// flusher is the processor's single emission point: it publishes
// local-origin keeps, decodes buffered fragments, and hands kept spans
// to the next consumer — every unbounded-latency step, off the shard
// workers (ADR-007: a blocked exporter never blocks ingest).
type flusher struct {
	jobs <-chan *shards.FlushJob
	set  *shards.Set // Retry target; nil in unit tests without a Set
	next consumer.Traces
	b    bus.Bus

	// window is the CONFIGURED W, the denominator of every age ratio —
	// never the effective one, which shrinks under the overload ladder.
	// A moving denominator would make the buckets incomparable across a
	// shed episode, and effective_window.seconds already reports the
	// shrink on its own.
	window    time.Duration
	now       func() time.Time
	recordAge func(float64) // nil where there is no telemetry to record into

	stopOnce sync.Once
	stopc    chan struct{}
	done     chan struct{}

	publishedKeeps atomic.Uint64
	publishErrors  atomic.Uint64
	flushedSpans   atomic.Uint64
	flushErrors    atomic.Uint64
	decodeErrors   atomic.Uint64
}

// newFlusher takes the age-ratio recorder as a plain func rather than
// the generated instrument: the flusher owes telemetry one number, and a
// nil recordAge or a non-positive window turns the measurement off
// entirely — which is how every unit test here runs without a meter.
func newFlusher(jobs <-chan *shards.FlushJob, set *shards.Set,
	next consumer.Traces, b bus.Bus,
	window time.Duration, now func() time.Time, recordAge func(float64),
) *flusher {
	return &flusher{
		jobs: jobs, set: set, next: next, b: b,
		window: window, now: now, recordAge: recordAge,
		stopc: make(chan struct{}), done: make(chan struct{}),
	}
}

// start launches the flusher goroutine (one; part of the static census).
func (fl *flusher) start() { go fl.run() }

// chanCtx adapts the flusher's stop channel into a context for the
// publish ack join: Done is the channel itself, so a batch mid-join
// aborts the moment stop is signalled. Deadline-free and value-free by
// construction.
type chanCtx <-chan struct{}

func (c chanCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c chanCtx) Done() <-chan struct{}       { return c }
func (c chanCtx) Value(any) any               { return nil }

func (c chanCtx) Err() error {
	select {
	case <-c:
		return context.Canceled
	default:
		return nil
	}
}

// drainPublishTimeout bounds each best-effort batch published after
// stop: the drain still attempts real publishes (a healthy bus acks in
// microseconds), but a wedged one costs shutdown this much per batch,
// not forever.
const drainPublishTimeout = 2 * time.Second

// run exits only via stopc. The jobs channel is never closed — by
// contract the shard workers outlive nothing here, and a closed channel
// would spin this loop on zero values — so the drain below reads it only
// until it is momentarily empty.
func (fl *flusher) run() {
	defer close(fl.done)
	var batch []*shards.FlushJob
	for {
		select {
		case <-fl.stopc:
			// Best-effort drain: flush what is already queued, then go.
			for {
				if batch = fl.gather(batch[:0]); len(batch) == 0 {
					return
				}
				fl.drainBatch(batch)
			}
		case j := <-fl.jobs:
			batch = fl.gather(append(batch[:0], j))
			// The publish context follows the stop signal's state, not the
			// branch that got here. Once stopc is closed both cases above
			// are ready and select picks uniformly, so the shutdown backlog
			// lands here about half the time — and chanCtx would cancel its
			// publish on sight, losing a whole drain's broadcasts to a coin
			// flip.
			if ctx := chanCtx(fl.stopc); ctx.Err() == nil {
				fl.processBatch(ctx, batch)
				continue
			}
			fl.drainBatch(batch)
		}
	}
}

// drainBatch processes one post-stop batch under a bounded standalone
// context. The stop signal is already spent by then, so keying the
// publish to it would refuse every keep on sight; best-effort means
// actually attempting the broadcast, and drainPublishTimeout is what
// keeps a wedged bus from owning shutdown.
func (fl *flusher) drainBatch(batch []*shards.FlushJob) {
	ctx, cancel := context.WithTimeout(context.Background(), drainPublishTimeout)
	defer cancel()
	fl.processBatch(ctx, batch)
}

// gather appends every job already queued, without blocking: the batch
// is the backlog that accumulated while the flusher was busy — never a
// wait. Its size is bounded in practice by the channel depth plus the
// handful of sends racing this loop.
func (fl *flusher) gather(batch []*shards.FlushJob) []*shards.FlushJob {
	for {
		select {
		case j := <-fl.jobs:
			batch = append(batch, j)
		default:
			return batch
		}
	}
}

// processBatch publishes the batch's owed keeps in one bus call (other
// clusters' retention windows are burning — every ack joins before any
// consume), re-parks what failed, then decodes and consumes the rest.
//
// Publish-owing keeps dedupe by id: two jobs for one trace can coexist
// in a batch only in contrived cases, but the dedupe keeps the failed-set
// lookup unambiguous either way.
func (fl *flusher) processBatch(ctx context.Context, batch []*shards.FlushJob) {
	var keeps []bus.Keep
	seen := make(map[[16]byte]struct{}, len(batch))
	for _, j := range batch {
		if j.Need&shards.NeedPublish == 0 {
			continue
		}
		if _, dup := seen[j.ID]; dup {
			continue
		}
		seen[j.ID] = struct{}{}
		keeps = append(keeps, bus.Keep{ID: j.ID, Reason: j.Reason})
	}
	failedSet := map[[16]byte]struct{}{}
	if len(keeps) > 0 {
		failed, _ := fl.b.Publish(ctx, keeps)
		for _, k := range failed {
			failedSet[k.ID] = struct{}{}
		}
		fl.publishErrors.Add(uint64(len(failed)))
		// Publish reports failed as a subset of keeps, so the difference
		// cannot go negative; the guard also keeps the conversion sign-safe.
		if acked := len(keeps) - len(failed); acked > 0 {
			fl.publishedKeeps.Add(uint64(acked))
		}
	}
	for _, j := range batch {
		if j.Need&shards.NeedPublish != 0 {
			if _, bad := failedSet[j.ID]; bad {
				fl.retry(j, j.Need)
				continue
			}
			// The publish bit is deliberately not cleared from need here:
			// it is never read again. What remains owed is exactly
			// NeedFlush, which consume re-parks by name so a retried
			// consume can never rebroadcast the keep.
		}
		fl.consume(j)
	}
}

// consume decodes one job's fragments and hands the merged batch to the
// next consumer. A retryable refusal re-parks the flush bit on the owning
// shard; permanent failures and undecodable fragments are counted and
// dropped.
func (fl *flusher) consume(j *shards.FlushJob) {
	if j.Need&shards.NeedFlush == 0 || len(j.Frags) == 0 {
		return
	}
	ctx := context.Background()
	td := ptrace.NewTraces()
	for _, frag := range j.Frags {
		ft, err := fragmenter.Decode(frag)
		if err != nil {
			fl.decodeErrors.Add(1)
			continue
		}
		ft.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
	}
	spans := td.SpanCount()
	if spans <= 0 {
		return
	}
	fl.recordAgeRatio(td)
	if err := fl.next.ConsumeTraces(ctx, td); err != nil {
		fl.flushErrors.Add(1)
		if !consumererror.IsPermanent(err) {
			fl.retry(j, shards.NeedFlush)
		}
		return
	}
	// Guard above: spans > 0 here, so the conversion cannot change sign.
	fl.flushedSpans.Add(uint64(spans))
}

// recordAgeRatio samples the W instrument (ADR-011 r9): the age of the
// batch's oldest span — the one nearest expiry — as a fraction of the
// configured window. Mass gathering near 1.0 says keeps are only just
// beating expiry and W is too tight.
//
// Sampled per flush ATTEMPT, before the consume rather than after it,
// and both halves of that are deliberate. Fragments behind a failed
// flush go back on the shard and keep aging on disk, so a retry's higher
// ratio is a real observation and the samples nearest 1.0 are exactly
// the ones a retry loop produces; recording only accepted batches would
// go quiet during the outage that puts W most at risk. Sampling before
// the consume also keeps the exporter's own latency out of the number,
// which measures retention headroom and not downstream speed. The cost
// is that one trace retried N times contributes N samples: this is a
// distribution over flush attempts, not over traces, and flush.retries
// is what counts the retries themselves.
func (fl *flusher) recordAgeRatio(td ptrace.Traces) {
	// All three are needed to compute anything; any one missing is a
	// flusher built without telemetry, which is how the unit tests run.
	if fl.recordAge == nil || fl.now == nil || fl.window <= 0 {
		return
	}
	var oldest pcommon.Timestamp
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				if st := sp.StartTimestamp(); st != 0 && (oldest == 0 || st < oldest) {
					oldest = st
				}
			}
		}
	}
	if oldest == 0 {
		// No span in the batch carries a start. An age nobody knows is
		// not an age of zero, and a zero here would pile mass at the
		// healthy end of exactly the distribution being watched.
		return
	}
	// A span stamped in the future is clock skew, not negative age
	// (ADR-008 r7 hygiene).
	age := max(fl.now().Sub(oldest.AsTime()), 0)
	fl.recordAge(age.Seconds() / fl.window.Seconds())
}

// retry re-parks need on the owning shard; the fragments are on disk,
// so the job's copies are dropped here and re-collected on the tick.
func (fl *flusher) retry(j *shards.FlushJob, need shards.Need) {
	if fl.set == nil {
		return
	}
	fl.set.Retry(j.ID, j.Reason, need, j.Deadline, fl.stopc)
}

// stop signals the flusher, waits for it under ctx, and reports whether
// it exited in time. Repeat calls are safe: a shutdown that timed out is
// retried against the same already-closed signal, and a flusher that has
// since exited is never charged to the spent context.
//
// A timed-out stop leaves the goroutine LIVE and still consuming jobs —
// it is wedged in the consumer, not abandoned — so the caller owns a
// retry. Treating the timeout as terminal would leak the goroutine and,
// with it, the shard set the processor's shutdown has yet to drain.
func (fl *flusher) stop(ctx context.Context) error {
	fl.stopOnce.Do(func() { close(fl.stopc) })
	select {
	case <-fl.done: // already exited: never charge this to ctx
		return nil
	default:
	}
	select {
	case <-fl.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
