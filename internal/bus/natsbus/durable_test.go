// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus_test

import (
	"context"
	"fmt"
	"sync"
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

// batchOf builds n distinct keeps. Two bytes of counter carry the
// identity, so a batch stays distinguishable well past the chunk size
// the async window splits it at.
func batchOf(n int) []bus.Keep {
	keeps := make([]bus.Keep, n)
	for i := range keeps {
		keeps[i] = bus.Keep{ID: [16]byte{0: byte(i), 1: byte(i >> 8), 15: 1}, Reason: bus.ReasonError}
	}
	return keeps
}

// distinctKeeps records delivered ids by identity. Counting alone cannot
// carry the claim: a window that delivered one keep a hundred times
// would satisfy a counter and still have lost ninety-nine keeps.
type distinctKeeps struct {
	mu  sync.Mutex
	ids map[[16]byte]struct{}
}

func (d *distinctKeeps) add(id [16]byte, _ byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ids == nil {
		d.ids = make(map[[16]byte]struct{})
	}
	d.ids[id] = struct{}{}
}

func (d *distinctKeeps) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.ids)
}

// batchResult carries a Publish return off the goroutine it was run on.
type batchResult struct {
	failed []bus.Keep
	err    error
}

// publishBounded runs pub on its own goroutine and fails the test if it
// has not returned within the bound. The publish is passed as a closure
// so ctx stays the first argument of the call it belongs to. The
// timeout path marks the failure and then still waits for pub: a
// t.Fatalf here would leave the publish running against a client the
// registered cleanup is about to close, turning a timing failure into a
// race report or a nil dereference instead of the message below.
func publishBounded(t *testing.T, within time.Duration, why string, pub func() ([]bus.Keep, error)) batchResult {
	t.Helper()
	done := make(chan batchResult, 1)
	go func() {
		failed, err := pub()
		done <- batchResult{failed, err}
	}()
	select {
	case r := <-done:
		return r
	case <-time.After(within):
		t.Errorf("the batch did not return within %v: %s", within, why)
		return <-done
	}
}

// A durable batch lands every keep through the async window: N distinct
// keeps in, none failed, N distinct keeps durable. Publish returns only
// once the acks are joined, so a subscriber attached afterwards replays
// exactly what the batch made durable — the count is the proof the
// pipelining is not dropping acks on the floor.
func TestDurableBatchPublishLandsEveryKeep(t *testing.T) {
	// Sixteen chunks of the async window, so the per-chunk join is
	// exercised repeatedly rather than once.
	const batchN = 1000
	ns := startServer(t, t.TempDir(), 0)
	c := newDurableClient(t, ns.ClientURL())

	failed, err := c.Publish(context.Background(), batchOf(batchN))
	require.NoError(t, err)
	require.Empty(t, failed, "a healthy durable bus fails nothing")

	var d distinctKeeps
	cancel, err := c.Subscribe(d.add)
	require.NoError(t, err)
	defer cancel()
	require.Eventually(t, func() bool { return d.count() >= batchN },
		10*time.Second, 10*time.Millisecond, "every keep in the batch is durable")
	assert.Equal(t, batchN, d.count(), "the batch lands its keeps, not one keep repeatedly")
}

// The pipelining claim, measured against a yardstick that scales with
// the machine instead of a wall-clock constant. A batch costs one ack
// round trip per chunk; a sequential publisher costs one per keep. So a
// batchN batch is sixteen round trips wide and must finish ahead of seqN
// single-keep publishes running beside it — the margin is architectural
// (16 against seqN), which is what makes it survive a slow CI box, where
// a hard millisecond bound calibrated on a dev machine would flake. The
// pre-window implementation spent batchN round trips on the same batch
// and loses this race decisively.
//
// The yardstick publishes on its own subject and stream so it cannot
// contribute keeps to anything the batch client is asserted on.
func TestDurableBatchOutrunsSequentialPublisher(t *testing.T) {
	const batchN = 1000
	const seqN = 250
	ns := startServer(t, t.TempDir(), 0)
	batchC := newDurableClient(t, ns.ClientURL())
	seqC := newDurableClientOn(t, ns.ClientURL(), "test.yardstick", "test-yardstick")

	yard := batchOf(seqN)
	seqDone := make(chan struct{})
	go func() {
		defer close(seqDone)
		for i := range yard {
			if _, err := seqC.Publish(context.Background(), yard[i:i+1]); err != nil {
				return
			}
		}
	}()
	batchDone := make(chan batchResult, 1)
	go func() {
		failed, err := batchC.Publish(context.Background(), batchOf(batchN))
		batchDone <- batchResult{failed, err}
	}()

	select {
	case r := <-batchDone:
		<-seqDone // no publish still in flight when the clients close
		require.NoError(t, r.err)
		require.Empty(t, r.failed, "a healthy durable bus fails nothing")
	case <-seqDone:
		r := <-batchDone
		require.NoError(t, r.err)
		t.Fatalf("a %d-keep batch lost to %d sequential publishes: "+
			"the window is paying an ack round trip per keep, not per chunk", batchN, seqN)
	}
}

// Every keep of a batch wider than one chunk comes back failed when the
// bus is unreachable and ctx expires — the exhaustive half of failed ⊆
// keeps, which a batch narrower than the chunk size cannot ask. Chunk 1
// is still waiting on acks when ctx dies, so chunk 2 is never attempted:
// reporting only what was attempted would hand the flusher 64 failures
// and silently drop the other 36 keeps on the floor.
func TestDurableBatchFailsEveryKeepAcrossChunksWhenCtxDies(t *testing.T) {
	// Two chunks: one attempted and left waiting, one never reached.
	const batchN = 100
	dir := t.TempDir()
	ns := newRunningServer(t, dir, 0)
	port := serverPort(ns)
	c := newDurableClient(t, ns.ClientURL())
	// Close before the restarted server's own teardown, and restart before
	// that Close: draining a connection whose server is gone parks nats.go
	// on its flush timeout, which goleak reads as a leak. Deferred LIFO, so
	// the server is back up by the time the client drains.
	defer func() { require.NoError(t, c.Close()) }()
	defer startServer(t, dir, port)

	ns.Shutdown()
	ns.WaitForShutdown()

	// Establish the precondition before asserting on it: until the client
	// has noticed the server is gone, a publish can still resolve its acks
	// against the connection it thinks it has, and the batch under test
	// would never reach the ctx deadline it exists to exercise. Each probe
	// gets its own budget — a ctx built once outside the poll would expire
	// on its own and satisfy the gate without the bus having anything to
	// do with it.
	require.Eventually(t, func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pcancel()
		failed, err := c.Publish(pctx, batchOf(1))
		return err != nil && len(failed) == 1
	}, 30*time.Second, 100*time.Millisecond, "the client must notice the bus is unreachable")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	r := publishBounded(t, 5*time.Second,
		"Publish ignored ctx and parked on a transport timeout instead",
		func() ([]bus.Keep, error) { return c.Publish(ctx, batchOf(batchN)) })
	require.Error(t, r.err, "an unreachable durable bus must refuse, not swallow")
	assert.Len(t, r.failed, batchN, "every keep is reported failed, attempted or not")
	var d distinctKeeps
	for _, k := range r.failed {
		d.add(k.ID, k.Reason)
	}
	assert.Equal(t, batchN, d.count(), "the failed set names every keep, not one keep batchN times")
}

// The exhaustive tail again, this time pinned deterministically and
// against a healthy bus. A ctx that is already dead breaks the publish
// loop before the first keep, so nothing is attempted and no future can
// resolve: the tail append is the only path by which any keep can be
// reported, and it must report all of them. The unreachable-bus test
// above asserts the same outcome but reaches it through whichever arm
// wins a race with nats.go's disconnect handling, so it cannot pin this
// branch on its own.
func TestDurableBatchFailsEveryKeepWhenCtxIsAlreadyDead(t *testing.T) {
	const batchN = 100
	ns := startServer(t, t.TempDir(), 0)
	c := newDurableClient(t, ns.ClientURL())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failed, err := c.Publish(ctx, batchOf(batchN))
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, failed, batchN, "a dead ctx fails every keep it was handed, attempted or not")
	var d distinctKeeps
	for _, k := range failed {
		d.add(k.ID, k.Reason)
	}
	assert.Equal(t, batchN, d.count(), "the failed set names every keep")
}

// The bus.Bus invariant "err nil iff failed empty", swept across the
// deadline region where a durable batch finishes. The interesting
// deadline is the one expiring just as the last ack lands: the batch is
// then fully durable, failed must be empty, and an error beside an empty
// failed set is a lie the flusher acts on by requeueing keeps that are
// already on the bus. That crossing point moves with the machine, so the
// sweep walks a range wide enough to contain it instead of targeting a
// constant — and on a box slow enough that the range falls short, every
// sample simply lands on the failing side and the run still holds.
//
// The assertion cannot fail on correct code, since the invariant holds
// at every deadline. So this costs nothing when the code is right and
// catches a real violation on most runs when it is not.
func TestDurablePublishHoldsErrorInvariantAcrossDeadlines(t *testing.T) {
	ns := startServer(t, t.TempDir(), 0)
	c := newDurableClient(t, ns.ClientURL())
	// Warm the async reply plumbing so the first sample is not charged
	// for creating it.
	_, err := c.Publish(context.Background(), batchOf(1))
	require.NoError(t, err)

	for step := 1; step <= 400; step++ {
		deadline := time.Duration(step) * 50 * time.Microsecond
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		// One full window per sample: the wider the chunk, the wider the
		// join it has to cross to reach the deadline.
		failed, perr := c.Publish(ctx, batchOf(64))
		cancel()
		if len(failed) == 0 {
			require.NoErrorf(t, perr,
				"deadline %v: a batch that failed nothing must report no error", deadline)
			continue
		}
		require.Errorf(t, perr,
			"deadline %v: %d keeps came back failed with no error to explain them",
			deadline, len(failed))
	}
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
		failed, err := pub.Publish(context.Background(), []bus.Keep{{ID: tid(i), Reason: bus.ReasonError}})
		require.NoError(t, err)
		require.Empty(t, failed)
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
		failed, err := pub.Publish(context.Background(), []bus.Keep{{ID: tid(9), Reason: bus.ReasonError}})
		return err == nil && len(failed) == 0
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
	failed, err := c.Publish(context.Background(), []bus.Keep{{ID: tid(1), Reason: bus.ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
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
	failed, err := pub.Publish(context.Background(), []bus.Keep{{ID: tid(1), Reason: bus.ReasonError}})
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Eventually(t, func() bool { return got.Load() == 1 }, 10*time.Second, 10*time.Millisecond)

	ns.Shutdown()
	ns.WaitForShutdown()
	startServer(t, dir, port) // same port, same store

	require.Eventually(t, func() bool {
		failed, err := pub.Publish(context.Background(), []bus.Keep{{ID: tid(2), Reason: bus.ReasonError}})
		return err == nil && len(failed) == 0
	}, 30*time.Second, 100*time.Millisecond, "publish recovers after server restart")
	require.Eventually(t, func() bool { return got.Load() == 2 },
		30*time.Second, 100*time.Millisecond, "the ordered consumer resumed across the restart")
}
