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

	// inFn counts in-flight fn deliveries. The durable cancel consults it
	// to skip its wait when the cancel comes from inside fn; see that
	// cancel for why the guard is belt-and-braces rather than
	// load-bearing.
	inFn atomic.Int32

	// subsMu guards subs, the live core subscriptions Dropped sums over.
	subsMu sync.Mutex
	subs   map[*nats.Subscription]struct{}

	reconnects atomic.Uint64
	malformed  atomic.Uint64
	// errs counts the failures nobody is waiting on: the async errors
	// nats.go reports out of band, and every failed stream ensure.
	errs atomic.Uint64
	// droppedGone carries the drop counts of cancelled subscriptions,
	// which stop answering once unsubscribed.
	droppedGone  atomic.Uint64
	slowEpisodes atomic.Uint64
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
	return &Client{cfg: cfg, subs: make(map[*nats.Subscription]struct{})}, nil
}

// Start dials. RetryOnFailedConnect keeps an unreachable server from
// failing collector startup: the connection object is live immediately
// and redials in the background. Malformed options still fail here.
func (c *Client) Start(ctx context.Context) error {
	opts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectHandler(func(*nats.Conn) { c.reconnects.Add(1) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if errors.Is(err, nats.ErrSlowConsumer) {
				// One episode, not one keep: nats.go raises this on the
				// transition into the slow state, then stays quiet while
				// it keeps discarding. Dropped counts the keeps.
				c.slowEpisodes.Add(1)
				return
			}
			// Everything else arrives with no caller to return it to — an
			// auth or permissions refusal above all, which costs this
			// instance every keep it broadcasts while the connection
			// itself stays healthy. Unreported, that loss is total and
			// invisible.
			c.errs.Add(1)
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
		// Ensure here so a publisher-only collector still gets its stream,
		// but best-effort and bounded: every subscribe ensures again, and a
		// bus that is down at boot costs startup the timeout at worst
		// rather than failing it (ADR-008).
		ectx, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		_ = c.ensureStream(ectx)
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
	if c.nc == nil {
		return errors.New("natsbus: Publish before Start")
	}
	payload := encodeKeep(id, reason)
	if c.cfg.Mode == ModeAtMostOnce {
		return c.nc.Publish(c.cfg.Subject, payload)
	}
	_, err := c.js.Publish(ctx, c.cfg.Subject, payload)
	return err
}

// Subscribe registers fn. Core mode subscribes directly; durable mode
// runs the ensure→ordered-consumer loop. cancel is idempotent and stops
// new deliveries, but may return while a delivery is still in flight —
// it does not wait for fn. The hardening tier pins the property that
// matters, that cancel never deadlocks, including a cancel called from
// inside fn (ADR-011 r5).
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
	c.subsMu.Lock()
	c.subs[sub] = struct{}{}
	c.subsMu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			// Latch under the lock Dropped reads, so the count neither
			// double-counts nor gaps across the handover: sub.Dropped
			// stops answering once Unsubscribe closes the subscription.
			c.subsMu.Lock()
			if n, derr := sub.Dropped(); derr == nil && n > 0 {
				c.droppedGone.Add(uint64(n))
			}
			delete(c.subs, sub)
			c.subsMu.Unlock()
			_ = sub.Unsubscribe()
		})
	}
	return cancel, nil
}

// ensureStream idempotently creates-or-updates the stream with
// MaxAge = W, which both bounds the replay horizon and repairs a live
// horizon above W (ADR-011 r2). The stream belongs to the processor
// fleet, so a foreign MaxAge above W is repaired, not tolerated.
//
// Every failed attempt counts: one caller discards the error entirely
// (Start's best-effort ensure) and the other retries it on a loop, so
// counting here rather than at either call site is what keeps a
// permanently unusable stream — durable mode delivering nothing at all
// — from being a purely silent condition.
func (c *Client) ensureStream(ctx context.Context) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     c.cfg.Stream,
		Subjects: []string{c.cfg.Subject},
		MaxAge:   c.cfg.Window,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		c.errs.Add(1)
		return fmt.Errorf("natsbus: ensure stream %s: %w", c.cfg.Stream, err)
	}
	return nil
}

// subscribeDurable runs the ensure→ordered-consumer loop on its own
// goroutine: with the bus unreachable or its JetStream not yet
// answering, it retries every second until cancel, so a collector
// deployed before its bus still subscribes eventually (ADR-008: outage
// degrades, never fails startup). The
// ordered consumer is deliver-all (a fresh subscribe replays the ≤W
// backlog) and self-recreates across reconnects and server restarts;
// the loop rebuilds it only once it has stopped for good.
func (c *Client) subscribeDurable(fn func(id [16]byte, reason byte)) (func(), error) {
	quit := make(chan struct{})
	done := make(chan struct{})
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		for {
			cc, err := c.consumeOnce(ctx, fn)
			if err == nil {
				select {
				case <-quit:
					cc.Stop()
					return
				case <-cc.Closed():
					// Not a bounce — those the ordered consumer heals
					// itself. This is a consume that ended for good.
				}
			}
			if c.nc.IsClosed() {
				// A closed connection never redials, so no rebuild can
				// succeed: exit rather than retry until cancel.
				return
			}
			select {
			case <-quit:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			stop()
			close(quit)
			// Skip the wait when the cancel came from inside fn, which
			// cannot wait for fn's own return. The guard is
			// belt-and-braces and NOT load-bearing against nats.go as it
			// stands: fn runs on the consumer's dispatcher goroutine, and
			// ConsumeContext.Stop does not join that goroutine, so <-done
			// completes with fn still on the stack either way. Removing
			// the guard leaves the hardening tier green — it is kept for
			// a future Stop that does join its dispatcher. What pins the
			// property itself, that a cancel from inside fn returns
			// rather than deadlocking, is the tier and not this line
			// (ADR-011 r5).
			if c.inFn.Load() == 0 {
				<-done
			}
		})
	}
	return cancel, nil
}

// consumeOnce ensures the stream and starts one ordered consume.
func (c *Client) consumeOnce(ctx context.Context, fn func(id [16]byte, reason byte)) (jetstream.ConsumeContext, error) {
	ectx, ecancel := context.WithTimeout(ctx, 10*time.Second)
	defer ecancel()
	if err := c.ensureStream(ectx); err != nil {
		return nil, err
	}
	cons, err := c.js.OrderedConsumer(ectx, c.cfg.Stream, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, err
	}
	return cons.Consume(func(m jetstream.Msg) {
		c.deliver(fn, m.Data())
	})
}

// Reconnects counts redials the connection completed after losing the
// server; it backs the bus.* telemetry, as Malformed and Dropped do.
func (c *Client) Reconnects() uint64 { return c.reconnects.Load() }

// Malformed counts received payloads the wire codec refused.
func (c *Client) Malformed() uint64 { return c.malformed.Load() }

// Errors counts bus failures that reach no caller: the asynchronous
// errors nats.go reports out of band (auth and permissions refusals
// among them, each a total and otherwise invisible keep loss) and every
// failed stream ensure. A sustained non-zero count means this instance's
// keeps are not crossing the bus, however healthy the rest looks.
func (c *Client) Errors() uint64 { return c.errs.Load() }

// Dropped counts keeps discarded client-side, out of a subscription's
// pending queue, when a subscriber cannot keep up with delivery — the
// at_most_once trade, and always zero in durable mode. Live
// subscriptions are summed with the drops latched from cancelled ones.
func (c *Client) Dropped() uint64 {
	total := c.droppedGone.Load()
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for sub := range c.subs {
		if n, err := sub.Dropped(); err == nil && n > 0 {
			total += uint64(n)
		}
	}
	return total
}

// SlowConsumerEpisodes counts the times a subscription entered the slow
// state. It is not a keep count — one episode discards as many keeps as
// it takes the subscriber to catch up, which Dropped reports.
func (c *Client) SlowConsumerEpisodes() uint64 { return c.slowEpisodes.Load() }
