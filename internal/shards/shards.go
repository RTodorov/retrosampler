// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package shards implements the ADR-007 shard layer: trace-ID-hashed
// routing onto single-writer worker goroutines, each owning one disk
// buffer, with a bounded handoff queue and the overload ladder
// (watermark early-expiry, window floor, queue-full shed).
package shards

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rtodorov/retrosampler/internal/buffer"
)

// queueDepth bounds each shard's work queue and free ring jointly: the
// pair holds exactly queueDepth fragment buffers, so a send on either
// channel can never block (ADR-007 r2). A constant until a workload
// proves it needs to be a knob (ADR-005 r2).
const queueDepth = 64

const defaultTick = time.Second

// fibSeed is the Fibonacci-hash constant, matching the buffer index's
// trace-ID hash family.
const fibSeed = 0x9E3779B97F4A7C15

// Options configures a Set. All fields are required unless noted.
type Options struct {
	// Dir is the root storage directory; shard i owns Dir/shard-000i.
	Dir string
	// Shards is the worker count, already resolved by the caller
	// (min(GOMAXPROCS, config), ADR-007 r4). Must be >= 1.
	Shards int
	// Window is the retention window W (ADR-006).
	Window time.Duration
	// SegmentSize is the per-shard segment roll threshold in bytes.
	// Must be >= 1.
	SegmentSize int
	// DiskBudget is the byte budget across all shards; the watermark
	// rung acts on it (ADR-007 r5). Its watermark must exceed the
	// unreclaimable Shards*SegmentSize active-segment floor.
	DiskBudget int64
	// WatermarkPct is the DiskBudget percentage above which shards
	// early-expire their oldest segments. Must be in (0, 100].
	WatermarkPct int
	// WindowFloor is the minimum effective window early expiry may
	// leave (ADR-007 r5). Must be in (0, Window).
	WindowFloor time.Duration
	// Now is the injected clock (ADR-002 r4).
	Now func() time.Time
	// Tick is the per-shard expiry tick interval. Defaults to 1s.
	Tick time.Duration
	// Flush receives the jobs shard workers produce for kept traces.
	// The send is non-blocking: a full (or nil) channel parks the
	// intent per shard for retry on its tick. nil is test-only.
	Flush chan<- *FlushJob

	// dequeueHook, when set, runs in each worker at the top of its
	// loop. Test-only: lets tests wedge a worker deterministically.
	dequeueHook func()
}

// eventKind tags a fragBuf's role in the shard queue: one bounded typed
// queue carries both fragments and keep verdicts, so the handoff never
// boxes an event into any (ADR-007 r2). Same-queue FIFO is also the
// ordering guarantee — a batch's fragments enqueue ahead of its keep.
type eventKind uint8

const (
	evFrag eventKind = iota
	evKeep
	evRetry
)

// Origin says which side produced a keep.
type Origin uint8

// Keep origins: local detection publishes its verdict onward to the bus,
// where a bus-received keep never re-publishes (ADR-008 r3), and a
// baseline keep is local-only by contract — every instance computes the
// same deterministic verdict, so a broadcast would only manufacture
// duplicates (ADR-008 r1).
const (
	OriginLocal Origin = iota
	OriginBus
	OriginBaseline
)

// atFloorCause values: why rung 2 is shedding this shard's ingest.
const (
	floorClear         uint32 = iota // not at the floor
	floorProtected                   // next candidate holds floor-protected data
	nothingReclaimable               // no finalized segment left to sacrifice
)

// Stats is a point-in-time snapshot of the Set's overload and decision
// counters. Every shed or refusal is counted (ADR-007 r5): these values
// back the alarms once telemetry export lands.
type Stats struct {
	ShedQueueFull          uint64
	ShedFloorProtected     uint64
	ShedNothingReclaimable uint64
	AppendErrors           uint64
	EarlyExpiredSegments   uint64
	KeptLocal              uint64
	KeptBus                uint64
	DuplicateKeeps         uint64
	CorruptFragments       uint64
	FlushRetries           uint64
	ClampedStamps          uint64
	ExpiredBytes           int64
}

// fragBuf is one recycled handoff buffer: a queue event with its routing
// metadata. A fragment's bytes are copied on Offer so the caller's memory
// is never retained past the call; a keep carries no data.
type fragBuf struct {
	id   [16]byte
	at   time.Time
	data []byte

	kind   eventKind
	origin Origin
	reason byte
	// need is the evRetry work the flusher handed back; unused by the
	// other kinds.
	need Need
}

// Set owns the shard workers and the ladder state shared across them.
type Set struct {
	opts   Options
	shards []*shard

	// diskBytes is the global on-disk total, delta-updated by each
	// shard on its expiry tick — off the per-span hot path (ADR-007 r2).
	diskBytes atomic.Int64

	shedQueueFull          atomic.Uint64
	shedFloorProtected     atomic.Uint64
	shedNothingReclaimable atomic.Uint64
	appendErrors           atomic.Uint64
	earlyExpired           atomic.Uint64
	keptLocal              atomic.Uint64
	keptBus                atomic.Uint64
	duplicateKeeps         atomic.Uint64
	corruptFragments       atomic.Uint64
	flushRetries           atomic.Uint64
	clampedStamps          atomic.Uint64
	expiredBytes           atomic.Int64

	intake atomic.Bool
	// inflight counts enqueues that are mid-send. Every entry point takes
	// its token BEFORE the intake check, so a Shutdown that has already
	// stored intake-off still sees the token of one that read intake as
	// on; the reverse order leaves a window where Shutdown reads zero and
	// stops the workers under a send that is still coming. Shutdown waits
	// the count out, so an accepted event never lands on a drained queue.
	inflight atomic.Int64
	stopOnce sync.Once
}

// shardDir returns shard i's buffer directory under root.
func shardDir(root string, i int) string {
	return fmt.Sprintf("%s/shard-%04d", root, i)
}

// shardFor routes id to one of n shards: the same both-halves-XOR
// Fibonacci hash as the buffer index (a time-prefixed trace ID must not
// hide from the hash), reduced from the well-mixed high bits.
func shardFor(id [16]byte, n int) int {
	if n <= 0 {
		return 0
	}
	h := (binary.LittleEndian.Uint64(id[:8]) ^ binary.LittleEndian.Uint64(id[8:])) * fibSeed
	// Guard above: n > 0, so uint64(n) does not lose sign information.
	result := (h >> 32) % uint64(n)
	// result < n (shard count is small), so int conversion is exact.
	if result > math.MaxInt {
		return 0
	}
	return int(result)
}

// New opens one buffer per shard under opts.Dir and starts the workers.
func New(opts Options) (*Set, error) {
	switch {
	case opts.Shards < 1:
		return nil, errors.New("shards: Options.Shards must be >= 1")
	case opts.SegmentSize < 1:
		return nil, errors.New("shards: Options.SegmentSize must be >= 1 " +
			"(buffer.Open would silently default 0, sidestepping the watermark floor check)")
	case opts.Window <= 0:
		return nil, errors.New("shards: Options.Window must be > 0")
	case opts.DiskBudget <= 0:
		return nil, errors.New("shards: Options.DiskBudget must be > 0")
	case opts.WatermarkPct <= 0 || opts.WatermarkPct > 100:
		return nil, errors.New("shards: Options.WatermarkPct must be in (0, 100]")
	case opts.WindowFloor <= 0 || opts.WindowFloor >= opts.Window:
		return nil, errors.New("shards: Options.WindowFloor must be in (0, Window)")
	case opts.Now == nil:
		return nil, errors.New("shards: Options.Now is required")
	}
	// ExpireOldest reclaims only finalized segments, so the Shards active
	// segments are permanently unreclaimable. With the watermark at or
	// under that floor every tick sets atFloor and Offer sheds 100% of
	// ingest, forever and silently: refuse at startup instead (ADR-007 r5).
	watermark := opts.DiskBudget / 100 * int64(opts.WatermarkPct) // the tick's own expression
	if floor := int64(opts.Shards) * int64(opts.SegmentSize); watermark <= floor {
		return nil, fmt.Errorf("shards: watermark (%d bytes, DiskBudget %d at %d%%) does not clear "+
			"the unreclaimable active-segment floor (Shards %d x SegmentSize %d = %d bytes): "+
			"every tick would shed all ingest; raise disk_budget or lower shards/segment_size",
			watermark, opts.DiskBudget, opts.WatermarkPct, opts.Shards, opts.SegmentSize, floor)
	}
	if opts.Tick <= 0 {
		opts.Tick = defaultTick
	}

	s := &Set{opts: opts}
	for i := range opts.Shards {
		b, err := buffer.Open(shardDir(opts.Dir, i),
			buffer.Options{Window: opts.Window, SegmentSize: opts.SegmentSize},
			opts.Now())
		if err != nil {
			for _, prev := range s.shards {
				_ = prev.buf.Close()
			}
			return nil, fmt.Errorf("shards: open shard %d: %w", i, err)
		}
		sh := &shard{
			buf:  b,
			dec:  newDecidedSet(),
			pend: make(map[[16]byte]pendReq),
			work: make(chan *fragBuf, queueDepth),
			free: make(chan *fragBuf, queueDepth),
			stop: make(chan struct{}),
			done: make(chan struct{}),
		}
		sh.effWindow.Store(int64(opts.Window))
		for range queueDepth {
			sh.free <- &fragBuf{}
		}
		s.shards = append(s.shards, sh)
	}
	s.intake.Store(true)
	for _, sh := range s.shards {
		go sh.run(s)
	}
	return s, nil
}

// Offer routes one marshaled fragment to its shard, copying it into a
// recycled buffer, and reports whether it was accepted. It never blocks:
// with no free buffer the fragment is shed and counted (ADR-007 r5 rung
// 3). After Shutdown it is a no-op returning false.
//
// Conservation — every offered fragment is buffered or counted as shed —
// holds against a concurrent Shutdown as well: the in-flight token this
// call holds keeps the workers alive until its send has landed in a
// queue (see Shutdown).
func (s *Set) Offer(id [16]byte, frag []byte, now time.Time) bool {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	if !s.intake.Load() {
		return false
	}
	sh := s.shards[shardFor(id, len(s.shards))]
	// Rung 2: shard at window floor — shed until the tick clears the cause.
	switch sh.atFloorCause.Load() {
	case floorProtected:
		s.shedFloorProtected.Add(1)
		return false
	case nothingReclaimable:
		s.shedNothingReclaimable.Add(1)
		return false
	}
	select {
	case fb := <-sh.free:
		fb.kind = evFrag // recycled buffers carry the last event's kind
		fb.id = id
		fb.at = now
		fb.data = append(fb.data[:0], frag...)
		sh.work <- fb
		return true
	default:
		s.shedQueueFull.Add(1)
		return false
	}
}

// Keep enqueues a locally detected keep verdict, non-blocking. False
// means no free buffer — the caller treats it as batch refusal, and the
// upstream retry re-detects, so no verdict is silently lost. The floor
// never refuses keeps: a keep concerns data already buffered, not new
// data volume (ADR-008 r4).
func (s *Set) Keep(id [16]byte, reason byte, now time.Time) bool {
	return s.enqueueKeep(id, OriginLocal, reason, now, nil, false)
}

// KeepLocalOnly enqueues a keep that flushes this instance's fragments
// but never broadcasts: the baseline path. Same non-blocking refusal
// contract as Keep — false refuses the batch, and the upstream retry
// re-detects the same verdict deterministically.
func (s *Set) KeepLocalOnly(id [16]byte, reason byte, now time.Time) bool {
	return s.enqueueKeep(id, OriginBaseline, reason, now, nil, false)
}

// KeepFromBus enqueues a bus-received keep, blocking on a full free ring
// until a buffer frees or abort closes: a broadcast keep must never be
// silently shed. A nil abort blocks indefinitely.
func (s *Set) KeepFromBus(id [16]byte, reason byte, now time.Time, abort <-chan struct{}) bool {
	return s.enqueueKeep(id, OriginBus, reason, now, abort, true)
}

// Retry re-queues flush work that failed downstream. It blocks on a full
// free ring (abort to bail): losing a retry loses a kept trace's
// fragments. After Shutdown it reports false and the intent dies with the
// process — the fragments are on disk for a durable-bus replay.
//
// The worker parks the need-bits and replays them from disk on its next
// tick, so a retry never carries fragments back across the queue. It also
// skips the decided check: the trace has already decided.
func (s *Set) Retry(id [16]byte, reason byte, need Need, abort <-chan struct{}) bool {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	if !s.intake.Load() {
		return false
	}
	sh := s.shards[shardFor(id, len(s.shards))]
	var fb *fragBuf
	select {
	case fb = <-sh.free:
	case <-abort:
		return false
	}
	fb.kind = evRetry
	fb.id = id
	fb.reason = reason
	fb.need = need
	fb.data = fb.data[:0]
	sh.work <- fb
	return true
}

// enqueueKeep hands a keep verdict to id's shard, waiting for a free
// buffer only when block is set.
func (s *Set) enqueueKeep(id [16]byte, origin Origin, reason byte, now time.Time,
	abort <-chan struct{}, block bool,
) bool {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	if !s.intake.Load() {
		return false
	}
	sh := s.shards[shardFor(id, len(s.shards))]
	var fb *fragBuf
	if block {
		select {
		case fb = <-sh.free:
		case <-abort:
			return false
		}
	} else {
		select {
		case fb = <-sh.free:
		default:
			s.shedQueueFull.Add(1)
			return false
		}
	}
	fb.kind = evKeep
	fb.id = id
	fb.at = now
	fb.origin = origin
	fb.reason = reason
	fb.data = fb.data[:0]
	sh.work <- fb
	return true
}

// Shutdown stops intake, waits for the enqueues already past the intake
// check to land, then signals every worker to drain and close its
// buffer, and waits for them, honouring ctx (ADR-007 r6). Safe to call
// repeatedly: a timed-out Shutdown can be retried.
//
// That quiesce is what makes the drain conserving: an accepted enqueue
// holds an in-flight token across its whole send, so nothing it accepted
// can land on a queue whose worker has already drained. The blocking
// sends (KeepFromBus, Retry) hold their token while they wait for a
// buffer, and the workers run on through the quiesce, so an exhausted
// free ring refills and the send completes.
//
// ctx bounds how long Shutdown waits, never how long a sender blocks: one
// waiting on a ring nothing will refill — its worker wedged — is released
// only by the abort channel it was given. Until then Shutdown cannot
// finish, and it deliberately leaves stop unsignalled rather than let the
// workers drain out from under an accepted send, which is the very drop
// the quiesce exists to prevent; the workers stay live with their buffers
// open. Intake is already off, so the blockage can only clear, never
// grow, and a retry gets past the quiesce once that sender lands or
// aborts. A caller needing shutdown to finish regardless must close the
// abort channel it gave the sender, then retry.
func (s *Set) Shutdown(ctx context.Context) error {
	s.intake.Store(false)
	for s.inflight.Load() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			runtime.Gosched()
		}
	}
	s.stopOnce.Do(func() {
		for _, sh := range s.shards {
			close(sh.stop)
		}
	})
	for _, sh := range s.shards {
		select {
		case <-sh.done: // already drained: never charge this to ctx
			continue
		default:
		}
		select {
		case <-sh.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var errs []error
	for _, sh := range s.shards {
		errs = append(errs, sh.closeErr)
	}
	return errors.Join(errs...)
}

// Stats snapshots the overload and decision counters.
func (s *Set) Stats() Stats {
	return Stats{
		ShedQueueFull:          s.shedQueueFull.Load(),
		ShedFloorProtected:     s.shedFloorProtected.Load(),
		ShedNothingReclaimable: s.shedNothingReclaimable.Load(),
		AppendErrors:           s.appendErrors.Load(),
		EarlyExpiredSegments:   s.earlyExpired.Load(),
		KeptLocal:              s.keptLocal.Load(),
		KeptBus:                s.keptBus.Load(),
		DuplicateKeeps:         s.duplicateKeeps.Load(),
		CorruptFragments:       s.corruptFragments.Load(),
		FlushRetries:           s.flushRetries.Load(),
		ClampedStamps:          s.clampedStamps.Load(),
		ExpiredBytes:           s.expiredBytes.Load(),
	}
}

// PendingFlushes reports the parked flush intents across shards as of the
// last ticks. Each worker owns its pending map outright, so the value is a
// sum of per-shard mirrors published on tick, not a live count.
func (s *Set) PendingFlushes() int64 {
	var n int64
	for _, sh := range s.shards {
		n += sh.pendLen.Load()
	}
	return n
}

// DecidedEntries reports the pending-keeps population across shards as of
// the last ticks. Each worker owns its decided set outright, so the value
// is a sum of per-shard mirrors published on tick, not a live count.
func (s *Set) DecidedEntries() int64 {
	var n int64
	for _, sh := range s.shards {
		n += sh.decidedLen.Load()
	}
	return n
}

// DiskBytesTotal returns the global on-disk total as of the last ticks.
func (s *Set) DiskBytesTotal() int64 {
	return s.diskBytes.Load()
}

// EffectiveWindow reports the smallest effective retention window across
// shards — W unless the watermark rung has been sacrificing segments.
func (s *Set) EffectiveWindow() time.Duration {
	m := int64(s.opts.Window)
	for _, sh := range s.shards {
		if w := sh.effWindow.Load(); w < m {
			m = w
		}
	}
	return time.Duration(m)
}
