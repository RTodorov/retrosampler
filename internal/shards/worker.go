// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

import (
	"sync/atomic"

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

	// closeErr is the worker's buffer Close result, readable after
	// done is closed.
	closeErr error
}

// run is the shard worker loop: appends handed-off fragments and, on
// stop, drains the queue and closes the buffer.
func (sh *shard) run(s *Set) {
	defer close(sh.done)
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
		}
	}
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
