// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/metadata"
)

// systemClock is the wall clock, injected into the processor from here — the
// one place forbidigo permits a bare time.Now (ADR-002 r4).
var systemClock = time.Now

// NewFactory creates a factory for the retrosampler processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Window:             5 * time.Minute,
		SegmentSize:        32 << 20,
		WatermarkPct:       80,
		WindowFloor:        time.Minute,
		KeepOnError:        true,
		T0Attribute:        "baggage.t0",
		ElapsedMSAttribute: "baggage.elapsed_ms",
	}
}

func createTraces(ctx context.Context, set processor.Settings,
	cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	// The Loopback is the single-instance default: local keeps drive the
	// whole decide->flush loop with no infrastructure (ADR-008 r6).
	p := newProcessor(cfg.(*Config), set.Logger, systemClock, bus.NewLoopback())
	// next reaches the processor here rather than through newProcessor:
	// the factory receives the consumer separately from the config.
	p.next = next
	if err := p.bindTelemetry(set.TelemetrySettings); err != nil {
		return nil, err
	}
	tp, err := processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces,
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown))
	if err != nil {
		// No component was returned, so nothing will ever call shutdown
		// to release the callbacks bound just above.
		p.unbindTelemetry()
		return nil, err
	}
	return tp, nil
}
