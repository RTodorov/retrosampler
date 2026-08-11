// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/buffer"
	"github.com/rtodorov/retrosampler/internal/metadata"
)

func TestShadowDisabledWithoutStorageDir(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount())
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowBuffersAndPassesThrough(t *testing.T) {
	dir := t.TempDir()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = dir
	cfg.DiskBudget = 1 << 30
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sp.SetTraceID(id)
	sp.SetName("op")
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount(), "shadow mode: everything still passes through")
	require.NoError(t, p.shutdown(context.Background()))

	// The fragment landed in exactly one shard; find it by reopening
	// the shard buffers directly.
	dirs, err := filepath.Glob(filepath.Join(dir, "shard-*"))
	require.NoError(t, err)
	require.NotEmpty(t, dirs)
	visits := 0
	for _, d := range dirs {
		b, err := buffer.Open(d, buffer.Options{Window: cfg.Window, SegmentSize: cfg.SegmentSize}, systemClock())
		require.NoError(t, err)
		require.NoError(t, b.Collect(id, func(frag []byte) {
			dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
			require.NoError(t, err)
			assert.Equal(t, "op", dec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
			visits++
		}))
		require.NoError(t, b.Close())
	}
	assert.Equal(t, 1, visits, "span was buffered in exactly one shard")
}

func TestShadowShutdownStopsWorkers(t *testing.T) {
	// goleak (TestMain) is the real assertion; this exercises the path.
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = 1 << 30
	p := newShadowProcessor(cfg, zap.NewNop(), systemClock)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowShutdownTwiceIsSafeAndPostShutdownPassesThrough(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = 1 << 30
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
	cfg.DiskBudget = 1 << 30
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
