// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package bustest is the ADR-011 r5 conformance suite. RunContract is
// the tier every bus.Bus implementation passes; RunHardening is the
// real-client tier (the documented Loopback constraints, which exempt
// Loopback); RunDurable is the replay-promise tier. Implementations
// compose the tiers that state their contract — composition, not
// skipping (ADR-002 r2).
package bustest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// Harness gives the hardening and durable tiers control beyond the
// interface: building a bus against a server they can stop and restart.
type Harness struct {
	// Mk builds a connected Bus, registering all cleanup on t.
	Mk func(t *testing.T) bus.Bus
	// StopServer halts the backing server; StartServer restarts it on
	// the same address with the same on-disk state.
	StopServer  func(t *testing.T)
	StartServer func(t *testing.T)
}

type recorded struct {
	mu    sync.Mutex
	keeps []struct {
		ID     [16]byte
		Reason byte
	}
}

func (r *recorded) add(id [16]byte, reason byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keeps = append(r.keeps, struct {
		ID     [16]byte
		Reason byte
	}{id, reason})
}

func (r *recorded) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keeps)
}

func (r *recorded) last() (id [16]byte, reason byte, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.keeps) == 0 {
		return id, 0, false
	}
	k := r.keeps[len(r.keeps)-1]
	return k.ID, k.Reason, true
}

// has answers whether one specific keep arrived. Counting cannot say
// that: wherever keeps are republished until one lands, or replayed out
// of a backlog, only identity distinguishes the keep under test from the
// others in flight.
func (r *recorded) has(id [16]byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.keeps {
		if k.ID == id {
			return true
		}
	}
	return false
}

func tid(n byte) (id [16]byte) { id[0] = n; id[15] = 1; return id }

// RunContract runs the tier every implementation must pass. mk is
// called once per subtest and must register cleanup on t.
func RunContract(t *testing.T, mk func(t *testing.T) bus.Bus) {
	t.Helper()

	t.Run("fanout_reaches_every_subscriber_and_echoes", func(t *testing.T) {
		b := mk(t)
		var r1, r2 recorded
		c1, err := b.Subscribe(r1.add)
		require.NoError(t, err)
		defer c1()
		c2, err := b.Subscribe(r2.add)
		require.NoError(t, err)
		defer c2()
		require.NoError(t, b.Publish(context.Background(), tid(1), bus.ReasonError))
		require.Eventually(t, func() bool { return r1.count() == 1 && r2.count() == 1 },
			5*time.Second, time.Millisecond, "every subscriber, publisher's own included")
		id, reason, ok := r1.last()
		require.True(t, ok)
		assert.Equal(t, tid(1), id)
		assert.Equal(t, bus.ReasonError, reason)
	})

	t.Run("duplicates_are_delivered_not_deduped", func(t *testing.T) {
		// The bus is dumb: dedup belongs to the decided set (ADR-008 r5).
		b := mk(t)
		var r recorded
		c, err := b.Subscribe(r.add)
		require.NoError(t, err)
		defer c()
		require.NoError(t, b.Publish(context.Background(), tid(2), bus.ReasonPolicy))
		require.NoError(t, b.Publish(context.Background(), tid(2), bus.ReasonPolicy))
		require.Eventually(t, func() bool { return r.count() == 2 },
			5*time.Second, time.Millisecond)
	})

	t.Run("cancel_is_idempotent_and_deregisters", func(t *testing.T) {
		b := mk(t)
		var r recorded
		c, err := b.Subscribe(r.add)
		require.NoError(t, err)
		c()
		c() // idempotent
		require.NoError(t, b.Publish(context.Background(), tid(3), bus.ReasonError))
		time.Sleep(50 * time.Millisecond) // absence needs a bounded wait
		assert.Zero(t, r.count(), "a cancelled subscriber receives nothing")
	})

	t.Run("publish_after_subscriber_churn_still_fans_out", func(t *testing.T) {
		b := mk(t)
		var r1, r2 recorded
		c1, err := b.Subscribe(r1.add)
		require.NoError(t, err)
		c1()
		c2, err := b.Subscribe(r2.add)
		require.NoError(t, err)
		defer c2()
		require.NoError(t, b.Publish(context.Background(), tid(4), bus.ReasonSpanLatency))
		require.Eventually(t, func() bool { return r2.count() == 1 },
			5*time.Second, time.Millisecond)
		assert.Zero(t, r1.count())
	})
}

// RunHardening is the real-client tier: the three documented Loopback
// constraints, discharged (ADR-011 r5). It is mode-agnostic — at-most-once
// and durable delivery both owe every property here.
func RunHardening(t *testing.T, h Harness) {
	t.Helper()

	t.Run("cancel_from_inside_fn_returns", func(t *testing.T) {
		b := h.Mk(t)
		// The cancel reaches fn down a channel rather than through a
		// shared variable: fn runs on a delivery goroutine, so a plain
		// assignment would be a data race whatever the ordering.
		cancelc := make(chan func(), 1)
		done := make(chan struct{})
		var once sync.Once
		c, err := b.Subscribe(func([16]byte, byte) {
			once.Do(func() {
				cancel := <-cancelc
				cancel()
				close(done)
			})
		})
		require.NoError(t, err)
		cancelc <- c
		require.NoError(t, b.Publish(context.Background(), tid(41), bus.ReasonError))
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("cancel called from inside fn deadlocked (Loopback constraint #1, discharged)")
		}
		c() // and stays idempotent afterwards
	})

	t.Run("blocked_subscriber_does_not_stall_another", func(t *testing.T) {
		// Every Mk'd bus dials its own connection, so a subscriber wedged
		// inside fn can only ever stall itself. The property is the
		// architecture's, and this is where it is pinned.
		bBlocked, bLive := h.Mk(t), h.Mk(t)
		release := make(chan struct{})
		entered := make(chan struct{}, 1)
		cb, err := bBlocked.Subscribe(func([16]byte, byte) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		})
		require.NoError(t, err)
		// Release before cancelling: a cancel that waits on a delivery
		// still wedged inside fn would be waiting on this test.
		defer func() { close(release); cb() }()
		var live recorded
		cl, err := bLive.Subscribe(live.add)
		require.NoError(t, err)
		defer cl()
		pub := h.Mk(t)
		require.NoError(t, pub.Publish(context.Background(), tid(42), bus.ReasonError))
		require.Eventually(t, func() bool { return live.count() == 1 },
			5*time.Second, time.Millisecond,
			"one wedged subscriber must not stall delivery to another (Loopback constraint #3, discharged)")
		// The wedge has to be real, or the assertion above says nothing:
		// a subscriber that never received anything blocks nothing.
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the blocked subscriber never entered fn, so nothing was ever wedged")
		}
	})

	t.Run("reconnect_resumes_delivery", func(t *testing.T) {
		b := h.Mk(t)
		var r recorded
		c, err := b.Subscribe(r.add)
		require.NoError(t, err)
		defer c()
		require.NoError(t, b.Publish(context.Background(), tid(43), bus.ReasonError))
		require.Eventually(t, func() bool { return r.has(tid(43)) },
			10*time.Second, 10*time.Millisecond, "delivery works before the bounce")
		h.StopServer(t)
		h.StartServer(t)
		pub := h.Mk(t)
		// The keep has to be one published AFTER the bounce, and it has to
		// be republished until it lands: at-most-once drops by contract
		// whatever is published before the old subscription is
		// re-registered, and a single publish here loses the race every
		// time — the reconnect waits a second while a fresh client
		// connects immediately. The claim is that the subscription
		// resumes, not that the bounce conserved a keep.
		require.Eventually(t, func() bool {
			if err := pub.Publish(context.Background(), tid(44), bus.ReasonError); err != nil {
				return false
			}
			return r.has(tid(44))
		}, 30*time.Second, 100*time.Millisecond,
			"the subscription that lived through the bounce delivers again")
	})
}

// RunDurable is the replay-promise tier (ADR-011 r2, r5): durable
// implementations only — core mode simply does not compose it.
func RunDurable(t *testing.T, h Harness) {
	t.Helper()

	t.Run("publish_honors_ctx_while_unreachable", func(t *testing.T) {
		b := h.Mk(t)
		h.StopServer(t)
		// Restart on the way out whatever happens: a failed assertion
		// would otherwise leave every later subtest, and every client
		// teardown, facing a server that is gone.
		defer h.StartServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		// The publish runs on its own goroutine so the deadline being
		// tested is the one under test: it must come back on ctx, well
		// inside any transport timeout it might otherwise wait out.
		errc := make(chan error, 1)
		go func() { errc <- b.Publish(ctx, tid(40), bus.ReasonError) }()
		select {
		case err := <-errc:
			require.Error(t, err, "an unreachable durable bus must refuse, not swallow")
		case <-time.After(5 * time.Second):
			t.Fatal("Publish ignored ctx and parked on a transport timeout instead")
		}
	})

	t.Run("fresh_subscribe_replays_backlog", func(t *testing.T) {
		pub := h.Mk(t)
		require.NoError(t, pub.Publish(context.Background(), tid(45), bus.ReasonPolicy))
		late := h.Mk(t) // built, and subscribed, only after the publish
		var r recorded
		c, err := late.Subscribe(r.add)
		require.NoError(t, err)
		defer c()
		require.Eventually(t, func() bool { return r.has(tid(45)) },
			10*time.Second, 10*time.Millisecond, "a fresh subscriber replays the backlog")
		id, reason, ok := r.last()
		require.True(t, ok)
		assert.Equal(t, tid(45), id)
		assert.Equal(t, bus.ReasonPolicy, reason, "replay carries the whole keep, reason included")
	})
}
