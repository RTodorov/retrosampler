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
	"go.opentelemetry.io/collector/pdata/pcommon"
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
	assert.True(t, cfg.KeepOnError)
	require.Error(t, cfg.Validate(),
		"storage_dir and disk_budget have no safe default: the operator must choose")
}

// A policy that will not parse is a config error, and the factory is
// where it must surface: the collector reports it while assembling the
// pipeline, not after the component is live. Validate would reject this
// config first in production, so it is deliberately not called here —
// what is under test is the constructor's own refusal.
func TestCreateTracesRejectsUnparsablePolicy(t *testing.T) {
	t.Parallel()
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Policies = []PolicyConfig{{Name: "bad", Condition: "span.name =="}}

	_, err := f.CreateTraces(context.Background(),
		processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.ErrorContains(t, err, `policy "bad"`,
		"the detector build error names the offending policy")
}

// The factory is the one production wiring of processor, flusher and bus:
// a fully assembled component must emit the kept trace and nothing else.
func TestFactoryEmitsOnlyKeptTraces(t *testing.T) {
	t.Parallel()
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.TracesSink)
	p, err := f.CreateTraces(context.Background(),
		processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	bad := ss.Spans().AppendEmpty()
	bad.SetTraceID(pcommon.TraceID{0x91})
	bad.SetName("bad")
	bad.Status().SetCode(ptrace.StatusCodeError)
	good := ss.Spans().AppendEmpty()
	good.SetTraceID(pcommon.TraceID{0x92})
	good.SetName("good")

	// ErrSkipProcessingData never surfaces: processorhelper turns it into a
	// clean nil, so the receiver sees an accepted batch, not a failure.
	require.NoError(t, p.ConsumeTraces(context.Background(), td))
	require.Eventually(t, func() bool { return sink.SpanCount() == 1 },
		5*time.Second, time.Millisecond, "the kept trace flushes")
	assert.Equal(t, []string{"bad"}, spanNames(sink),
		"only the error trace is emitted; the healthy one stays buffered")
}
