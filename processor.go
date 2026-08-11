// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package retrosampler

import (
	"context"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/buffer"
	"github.com/rtodorov/retrosampler/internal/fragmenter"
)

const expireInterval = time.Second

// shadowProcessor buffers every span and passes every span through
// (stage-1 shadow mode; retention semantics land with the decision plane).
// The coarse mutex is temporary scaffolding — stage 2 replaces it with
// shard-owned single-writer buffers (ADR-007).
type shadowProcessor struct {
	cfg    *Config
	logger *zap.Logger
	now    func() time.Time

	mu   sync.Mutex
	frag *fragmenter.Fragmenter
	buf  *buffer.Buffer

	stop chan struct{}
	done chan struct{}
}

// newShadowProcessor takes the clock from the caller (the factory in
// production), defaulting to time.Now — bare time.Now() calls are blocked by
// forbidigo outside factory.go (ADR-002 r4).
func newShadowProcessor(cfg *Config, logger *zap.Logger, now func() time.Time) *shadowProcessor {
	return &shadowProcessor{cfg: cfg, logger: logger, now: now}
}

func (p *shadowProcessor) start(_ context.Context, _ component.Host) error {
	if p.cfg.StorageDir == "" {
		p.logger.Warn("retrosampler: storage_dir empty, shadow buffering disabled")
		return nil
	}
	if err := os.MkdirAll(p.cfg.StorageDir, 0o750); err != nil {
		return err
	}
	buf, err := buffer.Open(p.cfg.StorageDir, buffer.Options{
		Window:      p.cfg.Window,
		SegmentSize: p.cfg.SegmentSize,
	}, p.now())
	if err != nil {
		return err
	}
	p.buf = buf
	p.frag = fragmenter.New()
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	go p.expireLoop()
	return nil
}

func (p *shadowProcessor) expireLoop() {
	defer close(p.done)
	t := time.NewTicker(expireInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-t.C:
			p.mu.Lock()
			p.buf.Expire(now)
			p.mu.Unlock()
		}
	}
}

func (p *shadowProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if p.buf == nil {
		return td, nil
	}
	now := p.now()
	p.mu.Lock()
	p.frag.Fragment(td, func(id pcommon.TraceID, frag []byte) {
		if err := p.buf.Append(id, frag, now); err != nil {
			// Shadow mode: buffering failure must never fail the pipeline.
			p.logger.Debug("retrosampler: shadow append failed", zap.Error(err))
		}
	})
	p.mu.Unlock()
	return td, nil
}

func (p *shadowProcessor) shutdown(context.Context) error {
	if p.buf == nil {
		return nil
	}
	close(p.stop)
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buf.Close()
}
