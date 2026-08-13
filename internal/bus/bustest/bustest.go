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

// RunHardening is implemented in the hardening task; declaring it here
// fixes the seam the natsbus tests compose.
func RunHardening(t *testing.T, h Harness) {
	t.Helper()
	_ = h // subtests land with the hardening task
}

// RunDurable is implemented in the hardening task; declaring it here
// fixes the seam the natsbus tests compose.
func RunDurable(t *testing.T, h Harness) {
	t.Helper()
	_ = h // subtests land with the hardening task
}
