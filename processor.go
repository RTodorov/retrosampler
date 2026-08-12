// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/fragmenter"
	"github.com/rtodorov/retrosampler/internal/shards"
)

// shadowProcessor buffers every span into the shard set and passes every
// span through (shadow mode; retention semantics land with the decision
// plane). ConsumeTraces may run on many goroutines: shared state is the
// atomic set pointer and a pool of single-threaded fragmenters.
type shadowProcessor struct {
	cfg    *Config
	logger *zap.Logger
	now    func() time.Time

	set      atomic.Pointer[shards.Set]
	fragPool sync.Pool
}

// pooledFrag pairs a fragmenter with a reusable routing callback: the
// closure is allocated once per pooled entry and re-targeted per batch
// through the struct fields, keeping the hot path allocation-free
// (ADR-004 r2).
type pooledFrag struct {
	f   *fragmenter.Fragmenter
	set *shards.Set
	now time.Time
	fn  func(id pcommon.TraceID, frag []byte, keep bool)
}

func newPooledFrag() *pooledFrag {
	pf := &pooledFrag{f: fragmenter.New()}
	pf.fn = func(id pcommon.TraceID, frag []byte, _ bool) { pf.set.Offer(id, frag, pf.now) }
	return pf
}

// newShadowProcessor takes the clock from the caller (the factory in
// production), defaulting to time.Now — bare time.Now() calls are blocked
// by forbidigo outside factory.go (ADR-002 r4).
func newShadowProcessor(cfg *Config, logger *zap.Logger, now func() time.Time) *shadowProcessor {
	p := &shadowProcessor{cfg: cfg, logger: logger, now: now}
	p.fragPool.New = func() any { return newPooledFrag() }
	return p
}

func (p *shadowProcessor) start(_ context.Context, _ component.Host) error {
	if p.cfg.StorageDir == "" {
		p.logger.Warn("retrosampler: storage_dir empty, shadow buffering disabled")
		return nil
	}
	n := runtime.GOMAXPROCS(0)
	if p.cfg.Shards > 0 && p.cfg.Shards < n {
		n = p.cfg.Shards
	}
	set, err := shards.New(shards.Options{
		Dir:          p.cfg.StorageDir,
		Shards:       n,
		Window:       p.cfg.Window,
		SegmentSize:  p.cfg.SegmentSize,
		DiskBudget:   p.cfg.DiskBudget,
		WatermarkPct: p.cfg.WatermarkPct,
		WindowFloor:  p.cfg.WindowFloor,
		Now:          p.now,
	})
	if err != nil {
		return err
	}
	p.set.Store(set)
	return nil
}

func (p *shadowProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	s := p.set.Load()
	if s == nil {
		return td, nil
	}
	pf := p.fragPool.Get().(*pooledFrag)
	pf.set, pf.now = s, p.now()
	pf.f.Fragment(td, nil, pf.fn)
	pf.set = nil
	p.fragPool.Put(pf)
	return td, nil
}

// shutdown swaps the set out atomically, so it runs the real shutdown at
// most once and later ConsumeTraces calls fall back to pure passthrough.
func (p *shadowProcessor) shutdown(ctx context.Context) error {
	s := p.set.Swap(nil)
	if s == nil {
		return nil
	}
	return s.Shutdown(ctx)
}
