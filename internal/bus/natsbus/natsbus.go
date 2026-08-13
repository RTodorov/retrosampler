// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package natsbus is the ADR-011 NATS implementation of bus.Bus: core
// pub/sub (at-most-once) or JetStream (durable), one client, mode fixed
// at construction. An unreachable server never fails Start — outage
// degrades gracefully (ADR-008), so the dial retries in the background
// and Publish errors surface to the flusher's retry machinery.
package natsbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// The delivery modes an operator chooses between (ADR-008 r6), fixed at
// construction.
const (
	ModeDurable    = "durable"
	ModeAtMostOnce = "at_most_once"
)

// Config is natsbus's own plain config: the root package maps its
// NATSConfig (pre-defaulted) onto this, so internal/ never imports root.
type Config struct {
	URL       string
	Mode      string
	Subject   string
	Stream    string
	CredsFile string
	// Window is W: durable-mode stream MaxAge and the replay-horizon
	// bound validated at ensure time (ADR-011 r2).
	Window time.Duration
}

// Client implements bus.Bus over one NATS connection.
type Client struct {
	cfg Config

	nc *nats.Conn
	js jetstream.JetStream

	// inFn counts in-flight fn deliveries; cancel skips its wait while
	// one is running (a cancel from inside fn cannot wait for fn's own
	// return — the relaxation the hardening tier pins).
	inFn atomic.Int32

	reconnects atomic.Uint64
	malformed  atomic.Uint64
	dropped    atomic.Uint64
}

var _ bus.Bus = (*Client)(nil)

// New validates cfg and builds an unconnected client (no I/O).
func New(cfg Config) (*Client, error) {
	switch {
	case cfg.URL == "":
		return nil, errors.New("natsbus: URL is required")
	case cfg.Mode != ModeDurable && cfg.Mode != ModeAtMostOnce:
		return nil, fmt.Errorf("natsbus: mode %q must be %q or %q", cfg.Mode, ModeDurable, ModeAtMostOnce)
	case cfg.Subject == "":
		return nil, errors.New("natsbus: Subject is required")
	case cfg.Mode == ModeDurable && cfg.Stream == "":
		return nil, errors.New("natsbus: Stream is required in durable mode")
	case cfg.Mode == ModeDurable && cfg.Window <= 0:
		return nil, errors.New("natsbus: Window must be positive in durable mode")
	}
	return &Client{cfg: cfg}, nil
}

// Start dials. RetryOnFailedConnect keeps an unreachable server from
// failing collector startup: the connection object is live immediately
// and redials in the background. Malformed options still fail here.
func (c *Client) Start(_ context.Context) error {
	opts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectHandler(func(*nats.Conn) { c.reconnects.Add(1) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				c.dropped.Add(1) // the at-most-once trade, made visible
			}
		}),
	}
	if c.cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(c.cfg.CredsFile))
	}
	nc, err := nats.Connect(c.cfg.URL, opts...)
	if err != nil {
		return fmt.Errorf("natsbus: connect: %w", err)
	}
	c.nc = nc
	if c.cfg.Mode == ModeDurable {
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return fmt.Errorf("natsbus: jetstream: %w", err)
		}
		c.js = js
	}
	return nil
}

// Close drains the connection: in-flight callbacks finish, then the
// connection and its goroutines stop. A connection that never reached a
// server has nothing to drain and Drain has already closed it — an
// outage at shutdown is not a shutdown failure (ADR-008).
func (c *Client) Close() error {
	if c.nc == nil {
		return nil
	}
	err := c.nc.Drain()
	for !c.nc.IsClosed() {
		time.Sleep(10 * time.Millisecond)
	}
	if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionReconnecting) {
		return nil
	}
	return err
}

// Publish broadcasts one keep. Core mode is fire-and-forget into the
// connection buffer (at-most-once by contract); durable mode is a
// synchronous acked JetStream publish honoring ctx — a non-nil error is
// the flusher's to retry, bounded by the ADR-011 r3 intent deadline.
func (c *Client) Publish(ctx context.Context, id [16]byte, reason byte) error {
	payload := encodeKeep(id, reason)
	if c.cfg.Mode == ModeAtMostOnce {
		return c.nc.Publish(c.cfg.Subject, payload)
	}
	_, err := c.js.Publish(ctx, c.cfg.Subject, payload)
	return err
}

// Subscribe registers fn. Core mode subscribes directly; durable mode
// (the durable task) runs the ensure→ordered-consumer loop. cancel is
// idempotent; it stops delivery and, when it can do so without
// deadlocking, waits for in-flight delivery to finish.
func (c *Client) Subscribe(fn func(id [16]byte, reason byte)) (func(), error) {
	if c.nc == nil {
		return nil, errors.New("natsbus: Subscribe before Start")
	}
	if c.cfg.Mode == ModeAtMostOnce {
		return c.subscribeCore(fn)
	}
	return c.subscribeDurable(fn)
}

// deliver invokes fn under the inFn count cancel consults.
func (c *Client) deliver(fn func(id [16]byte, reason byte), data []byte) {
	id, reason, ok := decodeKeep(data)
	if !ok {
		c.malformed.Add(1)
		return
	}
	c.inFn.Add(1)
	fn(id, reason)
	c.inFn.Add(-1)
}

func (c *Client) subscribeCore(fn func(id [16]byte, reason byte)) (func(), error) {
	sub, err := c.nc.Subscribe(c.cfg.Subject, func(m *nats.Msg) {
		c.deliver(fn, m.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("natsbus: subscribe: %w", err)
	}
	var once sync.Once
	cancel := func() {
		once.Do(func() { _ = sub.Unsubscribe() })
	}
	return cancel, nil
}

func (c *Client) subscribeDurable(_ func(id [16]byte, reason byte)) (func(), error) {
	return nil, errors.New("natsbus: durable subscribe lands with the durable-mode task")
}

// Reconnects counts redials the connection completed after losing the
// server; it backs the bus.* telemetry, as Malformed and Dropped do.
func (c *Client) Reconnects() uint64 { return c.reconnects.Load() }

// Malformed counts received payloads the wire codec refused.
func (c *Client) Malformed() uint64 { return c.malformed.Load() }

// Dropped counts keeps the server discarded for a slow consumer — the
// at-most-once trade, and always zero in durable mode.
func (c *Client) Dropped() uint64 { return c.dropped.Load() }
