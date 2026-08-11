// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/rtodorov/retrosampler/internal/metadata"
)

func TestNewFactory_TypeAndDefaultConfig(t *testing.T) {
	t.Parallel()
	f := NewFactory()
	require.Equal(t, metadata.Type, f.Type())
	cfg, ok := f.CreateDefaultConfig().(*Config)
	require.True(t, ok)
	require.NoError(t, cfg.Validate())
}

func TestProcessor_PassesSpansThroughUnchanged(t *testing.T) {
	t.Parallel()
	sink := new(consumertest.TracesSink)
	f := NewFactory()
	p, err := f.CreateTraces(context.Background(),
		processortest.NewNopSettings(metadata.Type), f.CreateDefaultConfig(), sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))

	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("probe")

	require.NoError(t, p.ConsumeTraces(context.Background(), td))
	require.Equal(t, 1, sink.SpanCount())
	require.NoError(t, p.Shutdown(context.Background()))
}
