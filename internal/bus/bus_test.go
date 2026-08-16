// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package bus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recorder collects deliveries for one subscriber.
type recorder struct {
	mu  sync.Mutex
	got [][16]byte
}

func (r *recorder) fn(id [16]byte, _ byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, id)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func TestLoopbackFansOutToEverySubscriber(t *testing.T) {
	lb := NewLoopback()
	a, b := &recorder{}, &recorder{}
	cancelA, err := lb.Subscribe(a.fn)
	require.NoError(t, err)
	defer cancelA()
	cancelB, err := lb.Subscribe(b.fn)
	require.NoError(t, err)
	defer cancelB()

	failed, err := lb.Publish(context.Background(), []Keep{{ID: [16]byte{1}, Reason: ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Eventually(t, func() bool { return a.count() == 1 && b.count() == 1 },
		time.Second, time.Millisecond, "broadcast reaches every subscriber, publisher included")
}

func TestLoopbackCancelStopsDeliveryAndGoroutine(t *testing.T) {
	lb := NewLoopback()
	r := &recorder{}
	cancel, err := lb.Subscribe(r.fn)
	require.NoError(t, err)
	cancel()
	cancel() // idempotent
	failed, err := lb.Publish(context.Background(), []Keep{{ID: [16]byte{2}, Reason: ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
	time.Sleep(10 * time.Millisecond)
	assert.Zero(t, r.count(), "cancelled subscriber receives nothing")
	// goleak's TestMain proves the delivery goroutine exited.
}

func TestLoopbackCancelDeregistersSubscriber(t *testing.T) {
	lb := NewLoopback()
	cancel, err := lb.Subscribe((&recorder{}).fn)
	require.NoError(t, err)
	lb.mu.Lock()
	live := len(lb.subs)
	lb.mu.Unlock()
	require.Equal(t, 1, live)

	cancel()
	lb.mu.Lock()
	defer lb.mu.Unlock()
	assert.Empty(t, lb.subs, "cancel deregisters, so a long-lived bus does not grow by one entry per subscribe/cancel cycle")
}

func TestLoopbackPublishWithNoSubscribersIsANoOp(t *testing.T) {
	failed, err := NewLoopback().Publish(context.Background(), []Keep{{ID: [16]byte{3}, Reason: ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
}
