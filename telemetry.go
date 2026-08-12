// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"math"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"

	"github.com/rtodorov/retrosampler/internal/metadata"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// asInt64 narrows a counter to the width the OTel instruments take. The
// clamp is unreachable at any ingest rate this processor can survive; it
// is here so the conversion is total instead of sign-flipping.
func asInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// bindTelemetry builds the generated telemetry and binds every async
// instrument to a counter the hot path already maintains. Nothing here
// runs on ingest: the meter pulls, the hot path only ever increments an
// atomic (ADR-004 r2).
//
// Each callback observes nothing while the set pointer is nil — before
// start, and from the moment shutdown retires it. On a monotone counter
// a zero there would read as a reset, whereas an absent data point says
// exactly what is true: this component is not running. That same load
// is also what makes p.fl safe to read without a second atomic, since
// start assigns the flusher before it publishes the set.
func (p *retroProcessor) bindTelemetry(ts component.TelemetrySettings) error {
	tb, err := metadata.NewTelemetryBuilder(ts)
	if err != nil {
		return err
	}
	live := func(read func(*shards.Set) int64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error {
			if s := p.set.Load(); s != nil {
				o.Observe(read(s))
			}
			return nil
		}
	}
	stat := func(read func(shards.Stats) int64) metric.Int64Callback {
		return live(func(s *shards.Set) int64 { return read(s.Stats()) })
	}
	fl := func(read func(*flusher) int64) metric.Int64Callback {
		return live(func(*shards.Set) int64 { return read(p.fl) })
	}
	if err := errors.Join(
		tb.RegisterProcessorRetrosamplerAppendErrorsCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.AppendErrors) }),
		),
		tb.RegisterProcessorRetrosamplerCorruptFragmentsCallback(
			// Two sources, each monotone on its own terms: fragments the
			// shard buffers skip at Collect, and fragments the flusher
			// cannot decode.
			live(func(s *shards.Set) int64 {
				return asInt64(s.Stats().CorruptFragments + p.fl.decodeErrors.Load())
			}),
		),
		tb.RegisterProcessorRetrosamplerDecidedEntriesCallback(
			live((*shards.Set).DecidedEntries),
		),
		tb.RegisterProcessorRetrosamplerDuplicateKeepsCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.DuplicateKeeps) }),
		),
		tb.RegisterProcessorRetrosamplerEarlyExpiredSegmentsCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.EarlyExpiredSegments) }),
		),
		tb.RegisterProcessorRetrosamplerEffectiveWindowSecondsCallback(
			live(func(s *shards.Set) int64 {
				return int64(s.EffectiveWindow() / time.Second)
			}),
		),
		tb.RegisterProcessorRetrosamplerExpiredBytesCallback(
			stat(func(s shards.Stats) int64 { return s.ExpiredBytes }),
		),
		tb.RegisterProcessorRetrosamplerFlushErrorsCallback(
			fl(func(f *flusher) int64 { return asInt64(f.flushErrors.Load()) }),
		),
		tb.RegisterProcessorRetrosamplerFlushRetriesCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.FlushRetries) }),
		),
		tb.RegisterProcessorRetrosamplerFlushedSpansCallback(
			fl(func(f *flusher) int64 { return asInt64(f.flushedSpans.Load()) }),
		),
		tb.RegisterProcessorRetrosamplerKeptBusCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.KeptBus) }),
		),
		tb.RegisterProcessorRetrosamplerKeptLocalCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.KeptLocal) }),
		),
		tb.RegisterProcessorRetrosamplerPendingFlushesCallback(
			live((*shards.Set).PendingFlushes),
		),
		tb.RegisterProcessorRetrosamplerPublishErrorsCallback(
			fl(func(f *flusher) int64 { return asInt64(f.publishErrors.Load()) }),
		),
		tb.RegisterProcessorRetrosamplerPublishedKeepsCallback(
			fl(func(f *flusher) int64 { return asInt64(f.publishedKeeps.Load()) }),
		),
		tb.RegisterProcessorRetrosamplerShedFloorProtectedCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.ShedFloorProtected) }),
		),
		tb.RegisterProcessorRetrosamplerShedNothingReclaimableCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.ShedNothingReclaimable) }),
		),
		tb.RegisterProcessorRetrosamplerShedQueueFullCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.ShedQueueFull) }),
		),
	); err != nil {
		// A partial binding would report a subset of the ladder with no
		// sign that the rest is missing: unregister what took.
		tb.Shutdown()
		return err
	}
	p.tb = tb
	return nil
}

// unbindTelemetry drops the callbacks. Repeat calls are safe — the SDK
// unregistration is idempotent — which matters because shutdown reaches
// here on both its no-op and its drained path.
func (p *retroProcessor) unbindTelemetry() {
	if p.tb != nil {
		p.tb.Shutdown()
	}
}
