// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/bustest"
	"github.com/rtodorov/retrosampler/internal/bus/natsbus"
)

func tid(n byte) (id [16]byte) { id[0] = n; id[15] = 1; return id }

func newDurableClientOn(t *testing.T, url, subject, stream string) *natsbus.Client {
	t.Helper()
	c, err := natsbus.New(natsbus.Config{
		URL: url, Mode: natsbus.ModeDurable,
		Subject: subject, Stream: stream, Window: time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func newDurableClient(t *testing.T, url string) *natsbus.Client {
	t.Helper()
	return newDurableClientOn(t, url, "test.keeps", "test-keeps")
}

func TestDurableContract(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	url := ns.ClientURL()
	var seq atomic.Int64
	bustest.RunContract(t, func(t *testing.T) bus.Bus {
		t.Helper()
		// A stream per subtest: replay is the whole point of durable mode,
		// so a shared stream would hand each subtest the previous one's
		// keeps before its own.
		n := seq.Add(1)
		return newDurableClientOn(t, url,
			fmt.Sprintf("test.keeps.%d", n), fmt.Sprintf("test-keeps-%d", n))
	})
}

// Deliver-all: a FRESH subscriber replays the whole <=W backlog — the
// restarted-pod recovery behavior (ADR-011 r2).
func TestDurableFreshSubscribeReplaysBacklog(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	pub := newDurableClient(t, ns.ClientURL())
	for i := byte(1); i <= 5; i++ {
		require.NoError(t, pub.Publish(context.Background(), tid(i), bus.ReasonError))
	}
	sub := newDurableClient(t, ns.ClientURL()) // subscribes AFTER the publishes
	var got atomic.Int64
	cancel, err := sub.Subscribe(func([16]byte, byte) { got.Add(1) })
	require.NoError(t, err)
	defer cancel()
	require.Eventually(t, func() bool { return got.Load() == 5 },
		10*time.Second, 10*time.Millisecond, "deliver-all replays the backlog")
}

// startPlainServer runs an embedded nats-server with JetStream OFF: the
// bus is reachable, so nothing buffers the ensure until better times —
// it fails fast against a server with no JetStream to answer it.
func startPlainServer(t *testing.T, port int) *server.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: port,
		NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(10*time.Second), "embedded nats-server never came up")
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	return ns
}

// A collector deployed before its bus is usable still subscribes: the
// loop retries ensure→consume until it succeeds (ADR-008). The failed
// attempt is what a single-shot consume cannot survive — it would give
// the subscription up for good, since a consumer that was never created
// has nothing to self-heal from.
func TestDurableSubscribeRetriesUntilEnsureSucceeds(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	plain := startPlainServer(t, port)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)

	sub := newDurableClient(t, url)
	defer func() { require.NoError(t, sub.Close()) }()
	var got atomic.Int64
	cancel, err := sub.Subscribe(func([16]byte, byte) { got.Add(1) })
	require.NoError(t, err, "a subscribe against a bus that cannot ensure must not fail")
	defer cancel()

	plain.Shutdown()
	plain.WaitForShutdown()
	startServer(t, dir, port) // same port, JetStream this time

	pub := newDurableClient(t, url)
	defer func() { require.NoError(t, pub.Close()) }()
	require.Eventually(t, func() bool {
		return pub.Publish(context.Background(), tid(9), bus.ReasonError) == nil
	}, 30*time.Second, 100*time.Millisecond, "publish reaches the JetStream server")
	require.Eventually(t, func() bool { return got.Load() >= 1 },
		30*time.Second, 100*time.Millisecond, "the retrying subscribe caught the usable bus")
}

// A failed ensure is durable mode's silent failure: with no stream the
// ordered consumer has nothing to read from, so this instance delivers
// no keeps whatsoever while every health signal it has stays green.
// Start's best-effort attempt and the subscribe loop's retries must all
// count — a single latched flag would go stale the moment the operator
// looked at it.
func TestErrorsCountsFailedEnsureAttempts(t *testing.T) {
	port := freePort(t)
	startPlainServer(t, port) // reachable, with no JetStream to answer
	c, err := natsbus.New(natsbus.Config{
		URL: fmt.Sprintf("nats://127.0.0.1:%d", port), Mode: natsbus.ModeDurable,
		Subject: "test.keeps", Stream: "test-keeps", Window: time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()),
		"a bus that cannot ensure must still not fail startup (ADR-008)")
	defer func() { require.NoError(t, c.Close()) }()
	require.Equal(t, uint64(1), c.Errors(),
		"Start's best-effort ensure failed, and best-effort is not the same as unreported")

	cancel, err := c.Subscribe(func([16]byte, byte) {})
	require.NoError(t, err)
	defer cancel()
	require.Eventually(t, func() bool { return c.Errors() >= 3 },
		30*time.Second, 50*time.Millisecond,
		"every retry of the subscribe loop counts its own failed ensure")
}

// A pre-existing stream with MaxAge > W violates the <=W replay-horizon
// contract; ensure must repair it to W (the stream belongs to the
// processor fleet) — and the repaired MaxAge must be observable.
func TestDurableEnsureRepairsHorizon(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name: "test-keeps", Subjects: []string{"test.keeps"},
		MaxAge: time.Hour, Storage: jetstream.FileStorage,
	})
	require.NoError(t, err)

	newDurableClient(t, ns.ClientURL()) // Start ensures with Window=1m
	s, err := js.Stream(context.Background(), "test-keeps")
	require.NoError(t, err)
	assert.Equal(t, time.Minute, s.CachedInfo().Config.MaxAge,
		"ensure repairs a horizon above W (ADR-011 r2)")
}

// Close without a preceding cancel must still stop the subscribe loop:
// a live consume has to notice the connection it rides on is closed for
// good and give up, rather than park on a cancel that never comes or
// retry a rebuild that can never succeed. goleak (TestMain) is the
// assertion — the loop goroutine outliving the binary is the failure.
func TestDurableCloseWithoutCancelStopsTheLoop(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c := newDurableClient(t, ns.ClientURL())
	got := make(chan struct{}, 1)
	// The cancel is dropped on purpose: Close is the only teardown here.
	_, err := c.Subscribe(func([16]byte, byte) {
		select {
		case got <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)
	require.NoError(t, c.Publish(context.Background(), tid(1), bus.ReasonError))
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the consume never went live, so Close would prove nothing")
	}
	require.NoError(t, c.Close())
}

// Replay survives a server restart: file-store state persists, the
// ordered consumer resumes, and a publish after recovery reaches the
// subscriber that lived through the bounce.
func TestDurableReplaySurvivesServerRestart(t *testing.T) {
	dir := t.TempDir()
	ns := startServer(t, dir, 0)
	port := serverPort(ns)
	url := ns.ClientURL()

	sub := newDurableClient(t, url)
	// Close both clients here rather than leaving it to the cleanup the
	// constructor registers: that one runs after the restarted server's
	// own cleanup has shut it down, and draining a connection whose
	// server is already gone parks nats.go's drain goroutine on its 5s
	// flush timeout, which goleak reads as a leak. Close is idempotent,
	// so the registered cleanup stays harmless.
	defer func() { require.NoError(t, sub.Close()) }()
	var got atomic.Int64
	cancel, err := sub.Subscribe(func([16]byte, byte) { got.Add(1) })
	require.NoError(t, err)
	defer cancel()
	pub := newDurableClient(t, url)
	defer func() { require.NoError(t, pub.Close()) }()
	require.NoError(t, pub.Publish(context.Background(), tid(1), bus.ReasonError))
	require.Eventually(t, func() bool { return got.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	ns.Shutdown()
	ns.WaitForShutdown()
	startServer(t, dir, port) // same port, same store

	require.Eventually(t, func() bool {
		return pub.Publish(context.Background(), tid(2), bus.ReasonError) == nil
	}, 30*time.Second, 100*time.Millisecond, "publish recovers after server restart")
	require.Eventually(t, func() bool { return got.Load() == 2 },
		30*time.Second, 100*time.Millisecond, "the ordered consumer resumed across the restart")
}
