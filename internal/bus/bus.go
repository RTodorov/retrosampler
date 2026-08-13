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

// Keep reasons carried in the bus contract's optional reason byte
// (ADR-008 r6). 0 always means "no verdict". ReasonBaseline never
// crosses the bus — baseline keeps are local-only by contract
// (ADR-008 r1) — but it shares this space so one byte names every
// decision source in telemetry and FlushJobs alike.
const (
	ReasonError        byte = 1
	ReasonSpanLatency  byte = 2
	ReasonTraceLatency byte = 3
	ReasonTraceAge     byte = 4
	ReasonPolicy       byte = 5
	ReasonBaseline     byte = 6
)

// Bus is the keep-notification transport. Subscribe's fn is invoked on
// a bus-owned goroutine; cancel is idempotent and stops delivery, but an
// implementation may return from cancel while a delivery is still in
// flight. That is harmless by contract — keeps are duplicate- and
// late-tolerant up to W, and the processor hands KeepFromBus an abort
// channel — so only Loopback's stronger wait-for-the-goroutine behavior
// is its own, documented there.
type Bus interface {
	// Publish broadcasts a keep for id to every subscriber, including
	// the publisher's own. A non-nil error is the caller's to retry.
	Publish(ctx context.Context, id [16]byte, reason byte) error
	// Subscribe registers fn for every subsequent keep and returns the
	// cancel that deregisters it.
	Subscribe(fn func(id [16]byte, reason byte)) (cancel func(), err error)
}
