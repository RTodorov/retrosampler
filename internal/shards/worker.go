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

	// atFloor, when set by the shard's tick, makes Offer shed this
	// shard's fragments (ADR-007 r5 rung 2). Read-mostly; not part of
	// the per-span append path.
	atFloor atomic.Bool
	// effWindow is the shard's effective retention window in
	// nanoseconds, shrunk by watermark early-expiry.
	effWindow atomic.Int64

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
			sh.append(s, fb)
		case <-t.C:
			sh.tick(s)
		}
	}
}

// tick runs the shard's periodic maintenance: window expiry, then the
// delta report of this shard's disk usage into the global total
// (ADR-007 r5 rung 1's input, kept off the per-span hot path).
func (sh *shard) tick(s *Set) {
	now := s.opts.Now()
	sh.buf.Expire(now)
	sh.reportDisk(s)
}

// reportDisk publishes the shard's disk-usage delta since its last
// report into the Set-wide total.
func (sh *shard) reportDisk(s *Set) {
	cur := sh.buf.DiskBytes()
	s.diskBytes.Add(cur - sh.lastDiskBytes)
	sh.lastDiskBytes = cur
}

// append writes one handed-off fragment and recycles its buffer. Append
// errors cannot reach the pipeline from here; they are counted
// (ADR-007 r5: never a silent drop).
func (sh *shard) append(s *Set, fb *fragBuf) {
	if err := sh.buf.Append(fb.id, fb.data, fb.at); err != nil {
		s.appendErrors.Add(1)
	}
	sh.free <- fb
}

// drain empties whatever the queue holds at shutdown so accepted
// fragments reach disk before Close.
func (sh *shard) drain(s *Set) {
	for {
		select {
		case fb := <-sh.work:
			sh.append(s, fb)
		default:
			return
		}
	}
}
