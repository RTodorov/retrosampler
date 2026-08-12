// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package bus defines the keep-notification bus contract (ADR-008 r6):
// dumb broadcast fan-out of kept trace IDs. Implementations must be
// duplicate-tolerant and late-tolerant up to the retention window W —
// the subscriber's decided set absorbs duplicates and self-delivery. A
// durable implementation's replay horizon must not exceed W. Transient
// Publish failures are the caller's retry problem (the flush pending
// machinery); Publish must not block unboundedly on a healthy bus.
package bus

import "context"

// ReasonError marks a keep triggered by the keep-on-error built-in.
// Further reasons (latency, age, baseline) join with their conditions.
const ReasonError byte = 1

// Bus is the keep-notification transport. Subscribe's fn is invoked on
// a bus-owned goroutine; cancel is idempotent and waits for that
// goroutine to stop.
type Bus interface {
	// Publish broadcasts a keep for id to every subscriber, including
	// the publisher's own. A non-nil error is the caller's to retry.
	Publish(ctx context.Context, id [16]byte, reason byte) error
	// Subscribe registers fn for every subsequent keep and returns the
	// cancel that deregisters it.
	Subscribe(fn func(id [16]byte, reason byte)) (cancel func(), err error)
}
