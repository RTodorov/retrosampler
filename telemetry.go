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

// detReasons names every keep reason for the reason attribute: one row
// per byte in 1..bus.ReasonBaseline, pinned by TestDetReasonsPinsEveryLabel.
var detReasons = []struct {
	reason byte
	name   string
}{
	{bus.ReasonError, "error"},
	{bus.ReasonSpanLatency, "span_latency"},
	{bus.ReasonTraceLatency, "trace_latency"},
	{bus.ReasonTraceAge, "trace_age"},
	{bus.ReasonPolicy, "policy"},
	{bus.ReasonBaseline, "baseline"},
}

// busMetrics is the optional counter half of the bus contract: an
// implementation with a connection behind it can report on it, the
// in-process Loopback has nothing to report. SlowConsumerEpisodes is
// deliberately left unbound — it counts entries into the slow state, not
// keeps, and bus.dropped already carries the loss an operator acts on.
type busMetrics interface {
	Reconnects() uint64
	Malformed() uint64
	Dropped() uint64
	Errors() uint64
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
// start assigns the flusher before it publishes the set. The detector
// is read behind detectorLive, which repeats the nil-detector test the
// ingest path makes before it calls Eval.
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
	// detectorLive is the det-family guard: the same nil-set test as live,
	// plus the nil-detector test the hot path makes before it calls Eval.
	// Build never returns a nil detector today, so this agrees with
	// pooledFrag rather than disputing it.
	detectorLive := func() bool { return p.set.Load() != nil && p.det != nil }
	det := func(read func(*detect.Detector) int64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error {
			if detectorLive() {
				o.Observe(read(p.det))
			}
			return nil
		}
	}
	// bm is the bus family's guard: the same nil-set test as live, plus
	// the interface test. A bus that cannot count observes nothing at all
	// rather than zeros — under the Loopback these four would describe a
	// transport that does not exist, and a permanent zero there is
	// indistinguishable from a healthy external bus. A bus that CAN count
	// reports its zeros, which is what makes bus.dropped's guaranteed zero
	// in durable mode readable as a fact instead of a silence.
	bm := func(read func(busMetrics) uint64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error {
			if m, ok := p.b.(busMetrics); ok && p.set.Load() != nil {
				o.Observe(asInt64(read(m)))
			}
			return nil
		}
	}
	// Both attribute tables resolve once here: the reason set is fixed and
	// the policy names are immutable after Build, so no collect allocates.
	reasonAttrs := make([]metric.MeasurementOption, len(detReasons))
	for i, r := range detReasons {
		reasonAttrs[i] = metric.WithAttributes(attribute.String("reason", r.name))
	}
	var polNames []string
	if p.det != nil {
		polNames = p.det.PolicyNames()
	}
	polAttrs := make([]metric.MeasurementOption, len(polNames))
	for i, name := range polNames {
		polAttrs[i] = metric.WithAttributes(attribute.String("policy", name))
	}
	perPolicy := func(read func(int) uint64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error {
			if !detectorLive() {
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
		tb.RegisterProcessorRetrosamplerBusDroppedCallback(
			bm(busMetrics.Dropped),
		),
		tb.RegisterProcessorRetrosamplerBusErrorsCallback(
			bm(busMetrics.Errors),
		),
		tb.RegisterProcessorRetrosamplerBusMalformedCallback(
			bm(busMetrics.Malformed),
		),
		tb.RegisterProcessorRetrosamplerBusReconnectsCallback(
			bm(busMetrics.Reconnects),
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
				if !detectorLive() {
					return nil
				}
				for i, r := range detReasons {
					if v := p.det.DetectedKeeps(r.reason); v > 0 {
						o.Observe(asInt64(v), reasonAttrs[i])
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
		tb.RegisterProcessorRetrosamplerPendingPublishesAbandonedCallback(
			stat(func(s shards.Stats) int64 { return asInt64(s.PublishesAbandoned) }),
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

// ageRecorder hands the flusher the one synchronous instrument in the
// plane, or nil where the builder was never bound — every processor test
// that skips bindTelemetry, and any future construction path that does
// the same. Resolved once at start rather than read per flush, so the
// flusher never touches the builder itself.
//
// Unlike the async callbacks there is no set-pointer guard: nothing can
// reach this before start (the flusher is built there) and the drain at
// shutdown is exactly when a last flush still deserves recording.
func (p *retroProcessor) ageRecorder() func(float64) {
	if p.tb == nil {
		return nil
	}
	return func(ratio float64) {
		p.tb.ProcessorRetrosamplerFlushAgeRatio.Record(context.Background(), ratio)
	}
}

// unbindTelemetry drops the callbacks. Repeat calls are safe — the SDK
// unregistration is idempotent — which matters because shutdown reaches
// here on both its no-op and its drained path.
func (p *retroProcessor) unbindTelemetry() {
	if p.tb != nil {
		p.tb.Shutdown()
	}
}
