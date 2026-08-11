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
	return &Config{Window: 5 * time.Minute, SegmentSize: 32 << 20}
}

func createTraces(ctx context.Context, set processor.Settings,
	cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	p := newShadowProcessor(cfg.(*Config), set.Logger, systemClock)
	return processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces,
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown))
}
