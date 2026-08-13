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

	// pend holds flush work the flusher could not take, deduped by trace
	// id, and pendScratch snapshots its keys for the tick's retry pass.
	// Both worker-owned.
	pend        map[[16]byte]pendReq
	pendScratch [][16]byte

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
	// pendLen mirrors pend's population for PendingFlushes, published on
	// the same terms as decidedLen.
	pendLen atomic.Int64

	// lastDiskBytes is the shard's last DiskBytes report, for
	// delta-updating Set.diskBytes. Worker-local.
	lastDiskBytes int64

	// closeErr is the worker's buffer Close result, readable after
	// done is closed.
	closeErr error
}

// pendReq is parked flush work: need-bits to run when the flush channel
// has room again, deduped by trace id.
type pendReq struct {
	reason byte
	need   Need
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
	s.expiredBytes.Add(sh.buf.Expire(now))
	sh.dec.evict(now.UnixNano())
	sh.decidedLen.Store(int64(sh.dec.len()))
	sh.retryPending(s)
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
// append, keep verdicts mark the decided set exactly once (ADR-008 r5),
// and a flusher retry re-parks its need-bits. Append errors cannot reach
// the pipeline from here; they are counted (ADR-007 r5: never a silent
// drop).
func (sh *shard) handle(s *Set, fb *fragBuf) {
	switch fb.kind {
	case evFrag:
		sh.appendFragment(s, fb)
	case evKeep:
		sh.keep(s, fb)
	case evRetry:
		s.flushRetries.Add(1)
		sh.park(fb.id, fb.reason, fb.need)
	}
	sh.free <- fb
}

// clampAt pulls an event's stamp back to this worker's own clock (the
// ingest side of ADR-008 r7 time hygiene). A producer clock running ahead
// of ours used to be unreclaimable: a future tMax outlived BOTH Expire and
// the watermark rung's floor check, so enough skewed data could pin a
// shard atFloor and shed all its ingest until real time passed the skew,
// and a future keep deadline held its decided entry — and, the ring
// evicting in insertion order, every entry behind it — past W. One clock
// read per event, on the worker goroutine and so off the ADR-004 producer
// path; a no-op whenever producers stamp honestly, and counted when it is
// not, so upstream skew reads as a number instead of vanishing silently.
//
// This closes the INGEST side only. A tMax already on disk can still sit
// ahead of now — written by a pre-clamp build and recovered on restart
// replay, or left behind by a local clock stepping backwards — so the
// buffer-side residual remains a recorded carry-over, and the tick's
// max(now-tMax, 0) guard stays load-bearing for it.
func (sh *shard) clampAt(s *Set, fb *fragBuf) {
	if now := s.opts.Now(); fb.at.After(now) {
		fb.at = now
		s.clampedStamps.Add(1)
	}
}

// appendFragment writes one fragment to disk, and for an already-decided
// trace forwards it straight through as well (ADR-008 r3). The append
// keeps disk the source of truth for restart replay; the forward spares
// the flusher a whole-trace re-Collect per late span. A fragment carries
// no verdict, so the job's Reason stays zero — the decided set does not
// retain the deciding one.
func (sh *shard) appendFragment(s *Set, fb *fragBuf) {
	sh.clampAt(s, fb)
	if err := sh.buf.Append(fb.id, fb.data, fb.at); err != nil {
		s.appendErrors.Add(1)
		return
	}
	if !sh.dec.has(fb.id) {
		return
	}
	sh.sendJob(s, &FlushJob{
		ID:    fb.id,
		Need:  NeedFlush,
		Frags: [][]byte{append([]byte(nil), fb.data...)},
	})
}

// keep records one keep verdict as decided until its deadline, decide
// time + W, then hands the trace's buffered fragments to the flusher. A
// repeat delivery — local re-detection, a bus echo, or the instance's own
// broadcast coming back — finds the id already marked and counts as a
// duplicate, with no second decision side effect.
func (sh *shard) keep(s *Set, fb *fragBuf) {
	sh.clampAt(s, fb)
	if !sh.dec.mark(fb.id, fb.at.Add(s.opts.Window).UnixNano()) {
		s.duplicateKeeps.Add(1)
		return
	}
	need := NeedFlush
	switch fb.origin {
	case OriginLocal:
		s.keptLocal.Add(1)
		need |= NeedPublish // the one origin that owes a broadcast
	case OriginBaseline:
		s.keptLocal.Add(1)
	case OriginBus:
		s.keptBus.Add(1)
	}
	sh.collectAndSend(s, fb.id, fb.reason, need)
}

// collectAndSend builds id's job from the buffer and hands it to the
// flusher, parking the intent on a full channel. A zero-fragment job
// still goes out when it owes a publish — the verdict must broadcast even
// when this batch's fragments were refused.
func (sh *shard) collectAndSend(s *Set, id [16]byte, reason byte, need Need) {
	j := &FlushJob{ID: id, Reason: reason, Need: need}
	skipped, err := sh.buf.Collect(id, func(frag []byte) {
		j.Frags = append(j.Frags, append([]byte(nil), frag...))
	})
	s.corruptFragments.Add(lenU64(skipped))
	if err != nil {
		// Read failure: the fragments stay on disk, so retry on the tick.
		sh.park(id, reason, need)
		return
	}
	if len(j.Frags) == 0 && need&NeedPublish == 0 {
		return // pending keep: later arrivals forward themselves
	}
	sh.sendJob(s, j)
}

// sendJob is the non-blocking flusher handoff; a full channel parks the
// job's need-bits for the tick to retry via a fresh Collect. That retry
// can re-forward fragments the flusher already took, which is why
// delivery is at-least-once.
func (sh *shard) sendJob(s *Set, j *FlushJob) {
	select {
	case s.opts.Flush <- j:
	default:
		sh.park(j.ID, j.Reason, j.Need)
	}
}

// park merges flush work into the pending map: need-bits OR, first
// nonzero reason wins.
func (sh *shard) park(id [16]byte, reason byte, need Need) {
	req := sh.pend[id]
	if req.reason == 0 {
		req.reason = reason
	}
	req.need |= need
	sh.pend[id] = req
}

// retryPending replays every parked intent once against the buffer.
// The keys are snapshotted first because collectAndSend re-parks on a
// still-full channel, and Go leaves it undefined whether a range revisits
// a key re-added during iteration — one retry per intent per tick has to
// be exact. The scratch slice is worker-local and reused; the tick is off
// the hot path either way.
func (sh *shard) retryPending(s *Set) {
	sh.pendScratch = sh.pendScratch[:0]
	for id := range sh.pend {
		sh.pendScratch = append(sh.pendScratch, id)
	}
	for _, id := range sh.pendScratch {
		req := sh.pend[id]
		delete(sh.pend, id)
		sh.collectAndSend(s, id, req.reason, req.need)
	}
	sh.pendLen.Store(int64(len(sh.pend)))
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
