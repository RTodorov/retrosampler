// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/fragmenter"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// flushQueueDepth bounds the worker->flusher handoff; a full queue parks
// intents in the shards' pending lists (fragments stay on disk), so
// depth is latency tuning, not correctness. A constant until a workload
// proves it needs to be a knob (ADR-005 r2).
const flushQueueDepth = 64

// errOverloaded is the retryable refusal: some fragment or keep verdict
// of the batch could not be enqueued (floor or queue-full). Plain — not
// consumererror.Permanent — so receivers retry (ADR-007 r5).
var errOverloaded = errors.New("retrosampler: buffer overloaded, retry")

// retroProcessor buffers every span, detects keep-on-error at ingest,
// and emits ONLY kept traces — via the flusher, never inline. Shadow
// mode is over: undecided spans age out on disk.
type retroProcessor struct {
	cfg    *Config
	logger *zap.Logger
	now    func() time.Time
	b      bus.Bus
	next   consumer.Traces

	set      atomic.Pointer[shards.Set]
	fragPool sync.Pool

	jobs      chan *shards.FlushJob
	fl        *flusher
	subCancel func()
	detect    func(sp ptrace.Span) bool
}

// pooledFrag pairs a fragmenter with a reusable routing callback,
// re-targeted per batch through the fields — the hot path stays
// allocation-free (ADR-004 r2). refused records any enqueue refusal in
// the batch; the whole batch then reports errOverloaded so the upstream
// retry re-offers and re-detects (at-least-once, duplicates possible).
type pooledFrag struct {
	f       *fragmenter.Fragmenter
	set     *shards.Set
	now     time.Time
	refused bool
	fn      func(id pcommon.TraceID, frag []byte, keep bool)
}

func (p *retroProcessor) newPooledFrag() *pooledFrag {
	pf := &pooledFrag{f: fragmenter.New()}
	pf.fn = func(id pcommon.TraceID, frag []byte, keep bool) {
		if !pf.set.Offer(id, frag, pf.now) {
			pf.refused = true
		}
		// The verdict is attempted even when the payload just shed. A
		// shed is about new volume; the verdict is about the trace, whose
		// earlier fragments are already on disk and whose peers are
		// waiting on the broadcast — which is why the shard layer does
		// not floor-gate Keep (ADR-008 r4). Dropping it here would leave
		// an error trace undecided for as long as the shed lasts.
		if keep && !pf.set.Keep(id, bus.ReasonError, pf.now) {
			// A lost verdict with accepted fragments would silently
			// un-decide an error trace: refuse the batch instead.
			pf.refused = true
		}
	}
	return pf
}

// newProcessor takes the clock and bus from the caller: the factory (the
// one production caller) passes time.Now and a fresh Loopback; tests
// inject fakes and shared instances through the same seam (ADR-002 r4).
func newProcessor(cfg *Config, logger *zap.Logger, now func() time.Time, b bus.Bus) *retroProcessor {
	p := &retroProcessor{cfg: cfg, logger: logger, now: now, b: b}
	if cfg.KeepOnError {
		p.detect = func(sp ptrace.Span) bool {
			return sp.Status().Code() == ptrace.StatusCodeError
		}
	}
	p.fragPool.New = func() any { return p.newPooledFrag() }
	return p
}

func (p *retroProcessor) start(_ context.Context, _ component.Host) error {
	n := runtime.GOMAXPROCS(0)
	if p.cfg.Shards > 0 && p.cfg.Shards < n {
		n = p.cfg.Shards
	}
	p.jobs = make(chan *shards.FlushJob, flushQueueDepth)
	set, err := shards.New(shards.Options{
		Dir:          p.cfg.StorageDir,
		Shards:       n,
		Window:       p.cfg.Window,
		SegmentSize:  p.cfg.SegmentSize,
		DiskBudget:   p.cfg.DiskBudget,
		WatermarkPct: p.cfg.WatermarkPct,
		WindowFloor:  p.cfg.WindowFloor,
		Now:          p.now,
		// Without this the workers have nowhere to hand a kept trace and
		// every keep parks forever: the flush channel is not optional.
		Flush: p.jobs,
	})
	if err != nil {
		return err
	}
	p.fl = newFlusher(p.jobs, set, p.next, p.b)
	p.fl.start()
	cancel, err := p.b.Subscribe(func(id [16]byte, reason byte) {
		// The abort must be non-nil: KeepFromBus blocks on an exhausted
		// free ring, and stopc is what releases it at shutdown. Never
		// cancel the subscription from in here — cancel waits for this
		// callback to return.
		if s := p.set.Load(); s != nil {
			s.KeepFromBus(id, reason, p.now(), p.fl.stopc)
		}
	})
	if err != nil {
		return errors.Join(err, p.fl.stop(context.Background()), set.Shutdown(context.Background()))
	}
	p.subCancel = cancel
	// Published last: a keep broadcast between Subscribe and here finds no
	// set and is dropped, which costs nothing — the pipeline is not yet
	// feeding this instance, so it has buffered no spans to flush.
	p.set.Store(set)
	return nil
}

func (p *retroProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	s := p.set.Load()
	if s == nil {
		// Not started or shutting down: refuse retryably, never pass
		// through — retention has no passthrough mode.
		return td, errOverloaded
	}
	pf := p.fragPool.Get().(*pooledFrag)
	pf.set, pf.now, pf.refused = s, p.now(), false
	pf.f.Fragment(td, p.detect, pf.fn)
	refused := pf.refused
	pf.set = nil
	p.fragPool.Put(pf)
	if refused {
		return td, errOverloaded
	}
	return td, processorhelper.ErrSkipProcessingData
}

// shutdown runs the ordered sequence — intake off (set->nil), bus
// unsubscribed, flusher drained, shards drained — and restores the set
// pointer on failure so a retried shutdown is real, not a false nil.
//
// The flusher stops BEFORE the shards, and the order is load-bearing in
// both directions. A flusher mid-job still owes its shard a Retry, and
// that Retry blocks on the shard's free ring, aborting only on stopc: with
// the shards stopped first, that parked token is exactly what the quiesce
// waits for, so Set.Shutdown burns its whole context on a sender only
// fl.stop can release. Stopping the flusher first releases it, and the
// shard workers are still running to refill the ring meanwhile. The
// unsubscribe leads for the same reason: cancel waits for an in-flight
// callback, whose KeepFromBus can only make progress while the workers
// live.
//
// After fl.stop the workers keep draining their queues into the jobs
// channel buffer, or park the intents when it fills. Either way the
// fragments are on disk and the keeps are recorded, so nothing kept is
// lost — it is replayed from the buffer on the next start.
func (p *retroProcessor) shutdown(ctx context.Context) error {
	s := p.set.Swap(nil)
	if s == nil {
		return nil
	}
	if p.subCancel != nil {
		p.subCancel()
		p.subCancel = nil
	}
	// Both restores below are plain stores: the Swap above already
	// serialises shutdown, so at most one caller is ever in this body.
	if err := p.fl.stop(ctx); err != nil {
		p.set.Store(s)
		return err
	}
	if err := s.Shutdown(ctx); err != nil {
		p.set.Store(s)
		return err
	}
	return nil
}
