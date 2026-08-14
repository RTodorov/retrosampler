// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/bustest"
	"github.com/rtodorov/retrosampler/internal/bus/natsbus"
)

func coreConfig(url string) natsbus.Config {
	return natsbus.Config{
		URL: url, Mode: natsbus.ModeAtMostOnce,
		Subject: "test.keeps", Stream: "test-keeps", Window: time.Minute,
	}
}

func newCoreClientOn(t *testing.T, url, subject string) *natsbus.Client {
	t.Helper()
	cfg := coreConfig(url)
	cfg.Subject = subject
	c, err := natsbus.New(cfg)
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func newCoreClient(t *testing.T, url string) *natsbus.Client {
	t.Helper()
	return newCoreClientOn(t, url, "test.keeps")
}

func TestCoreContract(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	url := ns.ClientURL()
	bustest.RunContract(t, func(t *testing.T) bus.Bus {
		t.Helper()
		return newCoreClient(t, url)
	})
}

func TestMalformedCountedAndDropped(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c := newCoreClient(t, ns.ClientURL())
	got := make(chan struct{}, 4)
	cancel, err := c.Subscribe(func([16]byte, byte) { got <- struct{}{} })
	require.NoError(t, err)
	defer cancel()
	// A raw wrong-length message straight through a second connection.
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	require.NoError(t, nc.Publish("test.keeps", []byte("junk")))
	require.NoError(t, nc.Flush())
	require.Eventually(t, func() bool { return c.Malformed() == 1 },
		5*time.Second, time.Millisecond)
	select {
	case <-got:
		t.Fatal("malformed message must not invoke fn")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Zero(t, c.Dropped(), "an undecodable payload is malformed, not a slow-consumer drop")
}

func TestDroppedCountsKeepsNotEpisodesAndSurvivesCancel(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c := newCoreClient(t, ns.ClientURL())

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseFn := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseFn)
	cancel, err := c.Subscribe(func([16]byte, byte) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	require.NoError(t, err)

	pub, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	defer pub.Close()

	// Wedge the subscriber inside fn so nothing drains its pending queue.
	require.NoError(t, pub.Publish("test.keeps", make([]byte, 17)))
	require.NoError(t, pub.Flush())
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("subscriber never entered fn")
	}

	// Overrun the pending queue's 64MB byte budget with big payloads,
	// which costs far less than the 500k messages its message budget
	// would need. These decode as malformed and never reach fn; the drop
	// happens upstream of the codec, in nats.go's pending queue.
	big := make([]byte, 512*1024)
	for range 200 {
		require.NoError(t, pub.Publish("test.keeps", big))
	}
	require.NoError(t, pub.Flush())

	require.Eventually(t,
		func() bool { return c.Dropped() > 0 && c.SlowConsumerEpisodes() > 0 },
		30*time.Second, 10*time.Millisecond, "the pending queue must overflow")

	dropped, episodes := c.Dropped(), c.SlowConsumerEpisodes()
	require.Greater(t, dropped, episodes,
		"Dropped must count discarded keeps, not slow-consumer episodes")
	assert.Zero(t, c.Errors(),
		"a slow consumer is the documented at_most_once trade, not a bus failure: "+
			"counting it under Errors would raise a false alarm on every burst")

	releaseFn()
	cancel()
	assert.GreaterOrEqual(t, c.Dropped(), dropped,
		"a cancelled subscriber's drops must not vanish from the total")
}

// startRestrictedServer runs an embedded nats-server that authenticates
// its one user and denies it the keep subject: the shape of a
// misconfigured deployment, where the connection is healthy and every
// publish is refused.
func startRestrictedServer(t *testing.T) string {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1,
		NoLog: true, NoSigs: true,
		Users: []*server.User{{
			Username: "u", Password: "p",
			Permissions: &server.Permissions{
				Publish: &server.SubjectPermission{Deny: []string{"test.keeps"}},
			},
		}},
	})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(10*time.Second), "embedded nats-server never came up")
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	return fmt.Sprintf("nats://u:p@127.0.0.1:%d", serverPort(ns))
}

// The async error handler is the only place an auth or permissions
// failure ever surfaces: nats.go reports it out of band, so the publish
// returns nil to the caller and this instance loses every keep it ever
// broadcasts while looking entirely healthy. Counting it is what makes
// the loss visible.
func TestErrorsCountsAsyncPermissionFailures(t *testing.T) {
	c := newCoreClient(t, startRestrictedServer(t))
	require.Zero(t, c.Errors(), "a healthy connection reports no async failure")

	require.NoError(t, c.Publish(context.Background(), tid(1), bus.ReasonError),
		"core publish is fire-and-forget: the server's refusal never reaches the caller")
	require.Eventually(t, func() bool { return c.Errors() >= 1 },
		10*time.Second, 10*time.Millisecond,
		"the permissions violation must be counted, not swallowed")
	assert.Zero(t, c.SlowConsumerEpisodes(),
		"a refused publish is not a slow consumer; the two failures must not share a counter")
}

func TestNewValidatesConfig(t *testing.T) {
	valid := coreConfig("nats://127.0.0.1:4222")
	durable := valid
	durable.Mode = natsbus.ModeDurable

	tests := map[string]struct {
		mutate  func(*natsbus.Config)
		wantErr string
	}{
		"missing url":            {func(c *natsbus.Config) { c.URL = "" }, "URL is required"},
		"empty mode":             {func(c *natsbus.Config) { c.Mode = "" }, `must be "durable" or "at_most_once"`},
		"unknown mode":           {func(c *natsbus.Config) { c.Mode = "exactly_once" }, `must be "durable" or "at_most_once"`},
		"missing subject":        {func(c *natsbus.Config) { c.Subject = "" }, "Subject is required"},
		"core needs no stream":   {func(c *natsbus.Config) { c.Stream = "" }, ""},
		"core needs no window":   {func(c *natsbus.Config) { c.Window = 0 }, ""},
		"durable needs stream":   {func(c *natsbus.Config) { c.Mode, c.Stream = natsbus.ModeDurable, "" }, "Stream is required"},
		"durable needs window":   {func(c *natsbus.Config) { c.Mode, c.Window = natsbus.ModeDurable, 0 }, "Window must be positive"},
		"durable window is sign": {func(c *natsbus.Config) { c.Mode, c.Window = natsbus.ModeDurable, -time.Second }, "Window must be positive"},
		"valid core":             {func(*natsbus.Config) {}, ""},
		"valid durable":          {func(c *natsbus.Config) { c.Mode = natsbus.ModeDurable }, ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			c, err := natsbus.New(cfg)
			if tc.wantErr == "" {
				require.NoError(t, err)
				assert.NotNil(t, c)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, c)
		})
	}
}

func TestStartAgainstUnreachableServerSucceeds(t *testing.T) {
	// ADR-008: an outage degrades gracefully. A bus that is down when the
	// collector boots must not fail startup — the dial retries behind the
	// live connection object.
	c, err := natsbus.New(coreConfig(fmt.Sprintf("nats://127.0.0.1:%d", freePort(t))))
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()), "an unreachable bus must not fail Start")

	cancel, err := c.Subscribe(func([16]byte, byte) {})
	require.NoError(t, err, "subscriptions are registered locally and replayed on connect")
	cancel()
	// Core mode is at-most-once: a publish while disconnected lands in the
	// reconnect buffer rather than erroring.
	assert.NoError(t, c.Publish(context.Background(), [16]byte{1}, bus.ReasonError))
	assert.NoError(t, c.Close(), "closing a never-connected client is not a failure")
}

func TestStartRejectsMalformedURL(t *testing.T) {
	// The retry posture covers unreachable servers, not unusable options.
	c, err := natsbus.New(coreConfig("nats://127.0.0.1:not-a-port"))
	require.NoError(t, err)
	err = c.Start(context.Background())
	require.ErrorContains(t, err, "natsbus: connect:")
	require.ErrorContains(t, err, "invalid port")
	assert.NoError(t, c.Close())
}

func TestStartRejectsUnreadableCredentialsFile(t *testing.T) {
	// Same unreachable URL the test above starts cleanly against, so the
	// only difference is the credentials path: nats.go smoke-tests the
	// file while processing options, ahead of any dial, and a typo fails
	// startup loudly instead of silently never authenticating.
	cfg := coreConfig(fmt.Sprintf("nats://127.0.0.1:%d", freePort(t)))
	cfg.CredsFile = filepath.Join(t.TempDir(), "user.creds")
	c, err := natsbus.New(cfg)
	require.NoError(t, err)
	require.ErrorContains(t, c.Start(context.Background()), "user.creds")
	assert.NoError(t, c.Close())
}

func TestSubscribeBeforeStartFails(t *testing.T) {
	c, err := natsbus.New(coreConfig("nats://127.0.0.1:4222"))
	require.NoError(t, err)
	cancel, err := c.Subscribe(func([16]byte, byte) {})
	require.ErrorContains(t, err, "Subscribe before Start")
	assert.Nil(t, cancel)
}

func TestSubscribeAfterCloseFails(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c, err := natsbus.New(coreConfig(ns.ClientURL()))
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()))
	require.NoError(t, c.Close())
	cancel, err := c.Subscribe(func([16]byte, byte) {})
	require.ErrorContains(t, err, "natsbus: subscribe:")
	require.ErrorIs(t, err, nats.ErrConnectionClosed)
	assert.Nil(t, cancel)
}

func TestPublishBeforeStartFails(t *testing.T) {
	// Durable mode would otherwise call a method on a nil jetstream
	// interface and panic, which ADR-001 r12 bans outside main.
	for _, mode := range []string{natsbus.ModeAtMostOnce, natsbus.ModeDurable} {
		t.Run(mode, func(t *testing.T) {
			cfg := coreConfig("nats://127.0.0.1:4222")
			cfg.Mode = mode
			c, err := natsbus.New(cfg)
			require.NoError(t, err)
			require.ErrorContains(t,
				c.Publish(context.Background(), [16]byte{1}, bus.ReasonError),
				"Publish before Start")
		})
	}
}

func TestCloseBeforeStartIsNil(t *testing.T) {
	c, err := natsbus.New(coreConfig("nats://127.0.0.1:4222"))
	require.NoError(t, err)
	assert.NoError(t, c.Close())
}

func TestCloseIsIdempotent(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c, err := natsbus.New(coreConfig(ns.ClientURL()))
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()))
	require.NoError(t, c.Close())
	assert.NoError(t, c.Close())
}

func TestReconnectsCounted(t *testing.T) {
	dir := t.TempDir()
	ns := startServer(t, dir, 0)
	port := serverPort(ns)
	c := newCoreClient(t, ns.ClientURL())
	require.Zero(t, c.Reconnects())

	ns.Shutdown()
	ns.WaitForShutdown()
	startServer(t, dir, port)

	require.Eventually(t, func() bool { return c.Reconnects() == 1 },
		30*time.Second, 10*time.Millisecond, "the client must redial the restarted server")
	// Delivery resumes over the reconnected connection.
	got := make(chan struct{}, 1)
	cancel, err := c.Subscribe(func([16]byte, byte) { got <- struct{}{} })
	require.NoError(t, err)
	defer cancel()
	require.NoError(t, c.Publish(context.Background(), [16]byte{7}, bus.ReasonError))
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery after reconnect")
	}
}
