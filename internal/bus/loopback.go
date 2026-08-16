// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package bus

import (
	"context"
	"sync"
)

// loopbackDepth bounds each subscriber's delivery buffer. A full buffer
// blocks Publish — safe backpressure in-process, since every consumer
// (the shard keep path) always makes progress.
const loopbackDepth = 1024

// keepMsg is one broadcast keep in flight to a single subscriber.
type keepMsg struct {
	id     [16]byte
	reason byte
}

// subscriber is one Subscribe registration: a buffered delivery channel
// drained by a dedicated goroutine.
type subscriber struct {
	ch   chan keepMsg
	quit chan struct{}
	done chan struct{}
}

// Loopback is the in-process Bus: broadcast to every subscriber in the
// same process. It is the production default (single-instance mode —
// local keeps drive the full decide→flush loop with no infrastructure)
// and the test fake; a shared instance wired into several processors
// carries keeps between them.
type Loopback struct {
	mu   sync.Mutex
	subs map[int]*subscriber
	next int
}

// Loopback is the contract's in-process implementation; a stage-6 real
// client implements the same interface behind the same call sites.
var _ Bus = (*Loopback)(nil)

// NewLoopback returns an empty Loopback.
func NewLoopback() *Loopback {
	return &Loopback{subs: make(map[int]*subscriber)}
}

// Publish delivers every keep to every current subscriber, blocking on
// a full buffer until the subscriber drains or cancels. In-process
// delivery cannot fail: failed is always nil.
func (l *Loopback) Publish(_ context.Context, keeps []Keep) ([]Keep, error) {
	l.mu.Lock()
	targets := make([]*subscriber, 0, len(l.subs))
	for _, s := range l.subs {
		targets = append(targets, s)
	}
	l.mu.Unlock()
	for _, k := range keeps {
		for _, s := range targets {
			select {
			case s.ch <- keepMsg{id: k.ID, reason: k.Reason}:
			case <-s.quit:
			}
		}
	}
	return nil, nil
}

// Subscribe registers fn and starts its delivery goroutine. Loopback's
// cancel is stronger than the Bus contract requires: it waits for that
// goroutine to stop, so no delivery is in flight once it returns. The
// cost is that a cancel called from inside fn deadlocks — in-process,
// the only caller is the processor's shutdown path, which does not.
func (l *Loopback) Subscribe(fn func(id [16]byte, reason byte)) (func(), error) {
	s := &subscriber{
		ch:   make(chan keepMsg, loopbackDepth),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	l.mu.Lock()
	key := l.next
	l.next++
	l.subs[key] = s
	l.mu.Unlock()

	go func() {
		defer close(s.done)
		for {
			select {
			case m := <-s.ch:
				fn(m.id, m.reason)
			case <-s.quit:
				return
			}
		}
	}()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.subs, key)
			l.mu.Unlock()
			close(s.quit)
			<-s.done
		})
	}
	return cancel, nil
}
