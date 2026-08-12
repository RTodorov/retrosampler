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

	// dequeueHook, when set, runs in each worker at the top of its
	// loop. Test-only: lets tests wedge a worker deterministically.
	dequeueHook func()
}

// Stats is a point-in-time snapshot of the Set's overload counters.
// Every shed or refusal is counted (ADR-007 r5): these values back the
// alarms once telemetry export lands.
type Stats struct {
	ShedQueueFull        uint64
	ShedFloor            uint64
	AppendErrors         uint64
	EarlyExpiredSegments uint64
}

// fragBuf is one recycled handoff buffer: a marshaled fragment and its
// routing metadata, copied on Offer so the caller's memory is never
// retained past the call.
type fragBuf struct {
	id   [16]byte
	at   time.Time
	data []byte
}

// Set owns the shard workers and the ladder state shared across them.
type Set struct {
	opts   Options
	shards []*shard

	// diskBytes is the global on-disk total, delta-updated by each
	// shard on its expiry tick — off the per-span hot path (ADR-007 r2).
	diskBytes atomic.Int64

	shedQueueFull atomic.Uint64
	shedFloor     atomic.Uint64
	appendErrors  atomic.Uint64
	earlyExpired  atomic.Uint64

	intake   atomic.Bool
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
// recycled buffer. It never blocks: with no free buffer the fragment is
// shed and counted (ADR-007 r5 rung 3). After Shutdown it is a no-op.
//
// Conservation — every offered fragment is buffered or counted as shed —
// is guaranteed only for Offers that complete before Shutdown begins. An
// Offer racing Shutdown may enqueue onto a worker that has already
// drained and exited, and that fragment is then dropped uncounted; in
// shadow mode the span still passes through the pipeline. Closing the
// race is stage-3 work, landing with retention.
func (s *Set) Offer(id [16]byte, frag []byte, now time.Time) {
	if !s.intake.Load() {
		return
	}
	sh := s.shards[shardFor(id, len(s.shards))]
	// Rung 2: shard at window floor — shed until the tick clears the flag.
	if sh.atFloor.Load() {
		s.shedFloor.Add(1)
		return
	}
	select {
	case fb := <-sh.free:
		fb.id = id
		fb.at = now
		fb.data = append(fb.data[:0], frag...)
		sh.work <- fb
	default:
		s.shedQueueFull.Add(1)
	}
}

// Shutdown stops intake, signals every worker to drain and close its
// buffer, and waits for them, honouring ctx (ADR-007 r6). Safe to call
// repeatedly: a timed-out Shutdown can be retried.
//
// It drains what the queues hold when the workers observe the stop, so
// it conserves every Offer that completed before this call. A fragment
// accepted concurrently with Shutdown may be dropped uncounted instead
// (see Offer).
func (s *Set) Shutdown(ctx context.Context) error {
	s.intake.Store(false)
	s.stopOnce.Do(func() {
		for _, sh := range s.shards {
			close(sh.stop)
		}
	})
	for _, sh := range s.shards {
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

// Stats snapshots the overload counters.
func (s *Set) Stats() Stats {
	return Stats{
		ShedQueueFull:        s.shedQueueFull.Load(),
		ShedFloor:            s.shedFloor.Load(),
		AppendErrors:         s.appendErrors.Load(),
		EarlyExpiredSegments: s.earlyExpired.Load(),
	}
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
