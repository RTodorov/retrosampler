// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/natsbus"
	"github.com/rtodorov/retrosampler/internal/metadata"
)

// The natsbus client reaches the processor as a bus.Bus, so everything
// past that interface is optional and structural: a signature that
// drifted out of one of these seams would not fail to compile, it would
// stop being called. Start would never dial and Close would never
// drain — two failures with no error anywhere.
var (
	_ busStarter = (*natsbus.Client)(nil)
	_ busCloser  = (*natsbus.Client)(nil)
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

// busFor builds the configured bus: the discriminated block selects the
// backend, absence selects the in-process Loopback (ADR-009 r4). No I/O
// happens here — an unreachable server is an outage, and outages belong
// to Start (ADR-008), so a collector whose bus is down still assembles
// its pipeline.
func busFor(cfg *Config) (bus.Bus, error) {
	if cfg.Bus == nil {
		// The Loopback is the single-instance default: local keeps drive
		// the whole decide->flush loop with no infrastructure (ADR-008 r6).
		return bus.NewLoopback(), nil
	}
	if cfg.Bus.NATS == nil {
		// Validate refuses this first in production. Here it is a refusal
		// rather than a nil dereference, since CreateTraces takes whatever
		// config it is handed (ADR-001 r12).
		return nil, errors.New("bus.nats is required when bus.type is nats")
	}
	// Validate leaves the optional fields empty, so the defaults are
	// applied here or not at all — and natsbus refuses every one of them.
	n := cfg.Bus.NATS.withDefaults()
	return natsbus.New(natsbus.Config{
		URL: n.URL, Mode: n.Mode, Subject: n.Subject,
		Stream: n.Stream, CredsFile: n.CredsFile,
		Window: cfg.Window,
	})
}

func createTraces(ctx context.Context, set processor.Settings,
	cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	b, err := busFor(cfg.(*Config))
	if err != nil {
		return nil, err
	}
	p, err := newProcessor(cfg.(*Config), set.TelemetrySettings, systemClock, b)
	if err != nil {
		return nil, err
	}
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
