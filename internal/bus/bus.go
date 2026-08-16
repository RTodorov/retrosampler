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

// Keep is one broadcastable verdict: the id and the reason byte that
// cross the bus together (ADR-011 r6 wire format, unchanged — a batch
// exists only at this API; the wire still carries one keep per message).
type Keep struct {
	ID     [16]byte
	Reason byte
}

// Bus is the keep-notification transport. Subscribe's fn is invoked on
// a bus-owned goroutine; cancel is idempotent and stops delivery, but an
// implementation may return from cancel while a delivery is still in
// flight. That is harmless by contract — keeps are duplicate- and
// late-tolerant up to W, and the processor hands KeepFromBus an abort
// channel — so only Loopback's stronger wait-for-the-goroutine behavior
// is its own, documented there.
type Bus interface {
	// Publish broadcasts a batch of keeps. It returns once every keep
	// is as durable as this bus's delivery guarantee makes it — or ctx
	// is done, with every still-unresolved keep reported failed.
	// failed ⊆ keeps; err describes why, nil iff failed is empty.
	// Order within the batch carries no meaning. The batch is the
	// caller's natural backlog, never a latency trade: an adapter
	// whose guarantee needs no round trip treats a batch of N exactly
	// as N single publishes (ADR-012).
	Publish(ctx context.Context, keeps []Keep) (failed []Keep, err error)
	// Subscribe registers fn for every subsequent keep and returns the
	// cancel that deregisters it.
	Subscribe(fn func(id [16]byte, reason byte)) (cancel func(), err error)
}
