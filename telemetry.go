// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"math"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/detect"
	"github.com/rtodorov/retrosampler/internal/metadata"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// detReasons pairs every keep reason with the attribute set its counter
// reports under, built once rather than per collect.
var detReasons = []struct {
	reason byte
	attrs  metric.MeasurementOption
}{
	{bus.ReasonError, metric.WithAttributes(attribute.String("reason", "error"))},
	{bus.ReasonSpanLatency, metric.WithAttributes(attribute.String("reason", "span_latency"))},
	{bus.ReasonTraceLatency, metric.WithAttributes(attribute.String("reason", "trace_latency"))},
	{bus.ReasonTraceAge, metric.WithAttributes(attribute.String("reason", "trace_age"))},
	{bus.ReasonPolicy, metric.WithAttributes(attribute.String("reason", "policy"))},
	{bus.ReasonBaseline, metric.WithAttributes(attribute.String("reason", "baseline"))},
}

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
// start assigns the flusher before it publishes the set; p.det is safer
// still, assigned at construction, before any callback can exist.
//
// The attributed instruments extend that silence per series: a reason
// or a policy whose counter is still zero is not observed at all, so a
// dashboard carries only the series that ever fired.
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
	det := func(read func(*detect.Detector) int64) metric.Int64Callback {
		return live(func(*shards.Set) int64 { return read(p.det) })
	}
	// The policy names are immutable after Build and the counters are
	// addressed by index, so the attribute sets resolve once here.
	polAttrs := make([]metric.MeasurementOption, 0, len(p.det.PolicyNames()))
	for _, name := range p.det.PolicyNames() {
		polAttrs = append(polAttrs, metric.WithAttributes(attribute.String("policy", name)))
	}
	perPolicy := func(read func(int) uint64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error {
			if p.set.Load() == nil {
				return nil
			}
			for i, attrs := range polAttrs {
				if v := read(i); v > 0 {
					o.Observe(asInt64(v), attrs)
				}
			}
			return nil
		}
	}
	if err := errors.Join(
		tb.RegisterProcessorRetrosamplerAppendErrorsCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.AppendErrors) }),
		),
		tb.RegisterProcessorRetrosamplerBaggageDivergenceMsCallback(
			det((*detect.Detector).DivergenceMS),
		),
		tb.RegisterProcessorRetrosamplerBaggageMalformedCallback(
			det(func(d *detect.Detector) int64 { return asInt64(d.BaggageMalformed()) }),
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
		tb.RegisterProcessorRetrosamplerDetectedKeepsCallback(
			func(_ context.Context, o metric.Int64Observer) error {
				if p.set.Load() == nil {
					return nil
				}
				for _, r := range detReasons {
					if v := p.det.DetectedKeeps(r.reason); v > 0 {
						o.Observe(asInt64(v), r.attrs)
					}
				}
				return nil
			},
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
		tb.RegisterProcessorRetrosamplerPolicyEvalErrorsCallback(
			perPolicy(p.det.PolicyEvalErrors),
		),
		tb.RegisterProcessorRetrosamplerPolicyMatchesCallback(
			perPolicy(p.det.PolicyMatches),
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
		tb.RegisterProcessorRetrosamplerSkewClampedCallback(
			det(func(d *detect.Detector) int64 { return asInt64(d.SkewClamped()) }),
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
