// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/metadata"
)

func TestShadowDisabledWithoutStorageDir(t *testing.T) {
	p := newShadowProcessor(&Config{Window: time.Minute, SegmentSize: 32 << 20}, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount())
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowBuffersAndPassesThrough(t *testing.T) {
	cfg := &Config{StorageDir: t.TempDir(), Window: time.Minute, SegmentSize: 32 << 20}
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetName("op")
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount(), "shadow mode: everything still passes through")

	visits := 0
	p.mu.Lock()
	require.NoError(t, p.buf.Collect([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		func(frag []byte) {
			dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
			require.NoError(t, err)
			assert.Equal(t, "op", dec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
			visits++
		}))
	p.mu.Unlock()
	assert.Equal(t, 1, visits, "span was buffered")
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowShutdownStopsTicker(t *testing.T) {
	// goleak (TestMain) is the real assertion; this exercises the path.
	cfg := &Config{StorageDir: t.TempDir(), Window: time.Minute, SegmentSize: 32 << 20}
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowShutdownTwiceIsSafeAndPostShutdownPassesThrough(t *testing.T) {
	cfg := &Config{StorageDir: t.TempDir(), Window: time.Minute, SegmentSize: 32 << 20}
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))

	require.NoError(t, p.shutdown(context.Background()))
	require.NoError(t, p.shutdown(context.Background()), "second shutdown must not panic")

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount(), "post-shutdown: still passes through cleanly")
}

func TestFactoryLifecycleWithStorage(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	sink := new(consumertest.TracesSink)
	proc, err := f.CreateTraces(context.Background(),
		processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	require.NoError(t, proc.ConsumeTraces(context.Background(), td))
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.SpanCount())
}
