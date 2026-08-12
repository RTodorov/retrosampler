// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
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

	stopOnce sync.Once
	stopc    chan struct{}
	done     chan struct{}

	publishedKeeps atomic.Uint64
	flushedSpans   atomic.Uint64
	flushErrors    atomic.Uint64
	decodeErrors   atomic.Uint64
}

func newFlusher(jobs <-chan *shards.FlushJob, set *shards.Set,
	next consumer.Traces, b bus.Bus,
) *flusher {
	return &flusher{
		jobs: jobs, set: set, next: next, b: b,
		stopc: make(chan struct{}), done: make(chan struct{}),
	}
}

// start launches the flusher goroutine (one; part of the static census).
func (fl *flusher) start() { go fl.run() }

func (fl *flusher) run() {
	defer close(fl.done)
	for {
		select {
		case <-fl.stopc:
			// Best-effort drain: flush what is already queued, then go.
			for {
				select {
				case j := <-fl.jobs:
					fl.process(j)
				default:
					return
				}
			}
		case j := <-fl.jobs:
			fl.process(j)
		}
	}
}

// process runs one job's remaining need-bits: publish first (other
// clusters' retention windows are burning), then decode+consume. Any
// retryable failure re-parks the un-done bits on the owning shard;
// permanent failures and undecodable fragments are counted and dropped.
func (fl *flusher) process(j *shards.FlushJob) {
	ctx := context.Background()
	need := j.Need
	if need&shards.NeedPublish != 0 {
		if err := fl.b.Publish(ctx, j.ID, j.Reason); err != nil {
			fl.retry(j, need)
			return
		}
		fl.publishedKeeps.Add(1)
		need &^= shards.NeedPublish
	}
	if need&shards.NeedFlush == 0 || len(j.Frags) == 0 {
		return
	}
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

// retry re-parks need on the owning shard; the fragments are on disk,
// so the job's copies are dropped here and re-collected on the tick.
func (fl *flusher) retry(j *shards.FlushJob, need shards.Need) {
	if fl.set == nil {
		return
	}
	fl.set.Retry(j.ID, j.Reason, need, fl.stopc)
}

// stop signals the flusher, waits for it under ctx, and reports whether
// it exited in time. Repeat calls are safe: a shutdown that timed out is
// retried against the same already-closed signal, and a flusher that has
// since exited is never charged to the spent context.
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
