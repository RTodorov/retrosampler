// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/natsbus"
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

// freePort returns a port with nothing listening on it: the address a
// bus client must tolerate rather than fail to start against.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// The bus block selects the backend and its absence selects the
// in-process Loopback (ADR-009 r4). The block carrying nothing but a URL
// is the load-bearing case: Validate leaves mode, subject and stream
// empty, and natsbus refuses all three, so this passes only if busFor
// applies the config defaults on the way through.
func TestFactoryBuildsBusFromConfig(t *testing.T) {
	t.Parallel()
	cfg := createDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	cfg.Bus = &BusConfig{Type: "nats", NATS: &NATSConfig{URL: "nats://127.0.0.1:59999"}}
	require.NoError(t, cfg.Validate())

	b, err := busFor(cfg)
	require.NoError(t, err)
	assert.IsType(t, &natsbus.Client{}, b, "the bus block must build the natsbus client")

	cfg.Bus = nil
	b, err = busFor(cfg)
	require.NoError(t, err)
	assert.IsType(t, &bus.Loopback{}, b, "no bus block keeps the Loopback default (ADR-009 r4)")
}

// A bus block whose sub-block is missing is a config error, and Validate
// is where it is caught. busFor is reachable without it — CreateTraces
// takes the config the caller hands it — so the answer there is a
// refusal rather than a nil dereference (ADR-001 r12).
func TestFactoryRefusesABusBlockWithNoBackend(t *testing.T) {
	t.Parallel()
	cfg := createDefaultConfig().(*Config)
	cfg.Bus = &BusConfig{Type: "nats"}
	require.Error(t, cfg.Validate(), "Validate is the primary guard")

	_, err := busFor(cfg)
	require.ErrorContains(t, err, "bus.nats")
}

// An unreachable bus is an outage, not a config error (ADR-008): the
// pipeline must assemble, start and stop cleanly against a server that
// is not there, with the dial retrying in the background. Assembly is
// the half that would fail loudest — a collector that cannot build its
// pipeline while its bus is down never comes up at all.
func TestFactoryLifecycleAgainstAnUnreachableBus(t *testing.T) {
	t.Parallel()
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	cfg.DiskBudget = testDiskBudget
	cfg.Shards = 2
	// Core mode: durable would spend its ensure timeout here proving
	// nothing this test is about, and the seam under test is the same.
	cfg.Bus = &BusConfig{Type: "nats", NATS: &NATSConfig{
		URL:  fmt.Sprintf("nats://127.0.0.1:%d", freePort(t)),
		Mode: "at_most_once",
	}}
	require.NoError(t, cfg.Validate())

	ctx := context.Background()
	p, err := f.CreateTraces(ctx, processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err, "the dial belongs to Start; building the pipeline does no I/O")
	require.NoError(t, p.Start(ctx, componenttest.NewNopHost()),
		"a bus that is down must not fail startup")
	require.NoError(t, p.Shutdown(ctx))
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
