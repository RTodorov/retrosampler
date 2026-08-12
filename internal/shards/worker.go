// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"sync/atomic"
	"time"

	"github.com/rtodorov/retrosampler/internal/buffer"
)

// shard is one single-writer worker: its goroutine is the only toucher
// of buf after New returns (ADR-007 r1).
type shard struct {
	buf  *buffer.Buffer
	work chan *fragBuf
	free chan *fragBuf
	stop chan struct{}
	done chan struct{}

	// dec is the shard's decided/pending-keeps set (ADR-008 r3). Owned
	// by the worker goroutine, like buf.
	dec *decidedSet

	// atFloorCause, when not floorClear, makes Offer shed this shard's
	// fragments and says which rung-2 cause to charge (ADR-007 r5).
	// Read-mostly; not part of the per-span append path.
	atFloorCause atomic.Uint32
	// effWindow is the shard's effective retention window in
	// nanoseconds, shrunk by watermark early-expiry.
	effWindow atomic.Int64
	// decidedLen mirrors dec's population for DecidedEntries, published
	// once per tick so readers never touch the worker-owned set.
	decidedLen atomic.Int64

	// lastDiskBytes is the shard's last DiskBytes report, for
	// delta-updating Set.diskBytes. Worker-local.
	lastDiskBytes int64

	// closeErr is the worker's buffer Close result, readable after
	// done is closed.
	closeErr error
}

// run is the shard worker loop: appends handed-off fragments, ticks
// expiry and the overload ladder, and on stop drains the queue and
// closes the buffer.
func (sh *shard) run(s *Set) {
	defer close(sh.done)
	t := time.NewTicker(s.opts.Tick)
	defer t.Stop()
	for {
		if s.opts.dequeueHook != nil {
			s.opts.dequeueHook()
		}
		select {
		case <-sh.stop:
			sh.drain(s)
			sh.closeErr = sh.buf.Close()
			return
		case fb := <-sh.work:
			sh.handle(s, fb)
		case <-t.C:
			sh.tick(s)
		}
	}
}

// tick runs the shard's periodic maintenance: window expiry, the disk
// delta report, then the watermark rung — sacrifice this shard's oldest
// segments, oldest-first, while the global total sits above the
// watermark, but never past the window floor (ADR-007 r5).
func (sh *shard) tick(s *Set) {
	now := s.opts.Now()
	sh.buf.Expire(now)
	sh.dec.evict(now.UnixNano())
	sh.decidedLen.Store(int64(sh.dec.len()))
	sh.reportDisk(s)

	limit := s.opts.DiskBudget / 100 * int64(s.opts.WatermarkPct)
	floorCutoff := now.Add(-s.opts.WindowFloor).UnixNano()
	cause := floorClear
	for s.diskBytes.Load() > limit {
		// Nothing left this shard may sacrifice: either no finalized
		// segment, or the next candidate still holds floor-protected
		// data. The two causes shed under separate counters.
		tMax, ok := sh.buf.OldestFinalizedTMax()
		if !ok {
			cause = nothingReclaimable
			break
		}
		if tMax >= floorCutoff {
			cause = floorProtected
			break
		}
		freed, removed := sh.buf.ExpireOldest()
		if !removed {
			cause = nothingReclaimable
			break
		}
		s.diskBytes.Add(-freed)
		sh.lastDiskBytes -= freed
		s.earlyExpired.Add(1)
	}
	sh.atFloorCause.Store(cause)

	if tMax, ok := sh.buf.OldestFinalizedTMax(); ok {
		// max: future-stamped data (producer clock ahead of ours) would
		// otherwise report a negative window.
		w := max(now.UnixNano()-tMax, 0)
		if capN := int64(s.opts.Window); w > capN {
			w = capN
		}
		sh.effWindow.Store(w)
	} else {
		sh.effWindow.Store(int64(s.opts.Window))
	}
}

// reportDisk publishes the shard's disk-usage delta since its last
// report into the Set-wide total.
func (sh *shard) reportDisk(s *Set) {
	cur := sh.buf.DiskBytes()
	s.diskBytes.Add(cur - sh.lastDiskBytes)
	sh.lastDiskBytes = cur
}

// handle processes one queue event and recycles its buffer: fragments
// append, keep verdicts mark the decided set exactly once (ADR-008 r5).
// Append errors cannot reach the pipeline from here; they are counted
// (ADR-007 r5: never a silent drop).
func (sh *shard) handle(s *Set, fb *fragBuf) {
	switch fb.kind {
	case evFrag:
		if err := sh.buf.Append(fb.id, fb.data, fb.at); err != nil {
			s.appendErrors.Add(1)
		}
	case evKeep:
		sh.keep(s, fb)
	}
	sh.free <- fb
}

// keep records one keep verdict as decided until its deadline, decide
// time + W. A repeat delivery — local re-detection, a bus echo, or the
// instance's own broadcast coming back — finds the id already marked and
// counts as a duplicate, with no second decision side effect.
func (sh *shard) keep(s *Set, fb *fragBuf) {
	if !sh.dec.mark(fb.id, fb.at.Add(s.opts.Window).UnixNano()) {
		s.duplicateKeeps.Add(1)
		return
	}
	if fb.origin == OriginLocal {
		s.keptLocal.Add(1)
		return
	}
	s.keptBus.Add(1)
}

// drain empties whatever the queue holds at shutdown so accepted
// fragments reach disk, and accepted keeps are decided, before Close.
func (sh *shard) drain(s *Set) {
	for {
		select {
		case fb := <-sh.work:
			sh.handle(s, fb)
		default:
			return
		}
	}
}
