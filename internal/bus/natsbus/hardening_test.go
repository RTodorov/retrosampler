// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/bustest"
)

// subtestNames derives a subject and stream from the running subtest's
// name. Durable delivery is deliver-all, so subtests sharing a stream
// would each replay the previous ones' keeps before their own; the core
// harness names the same way, so the two harnesses differ only in the
// client they build.
func subtestNames(t *testing.T) (subject, stream string) {
	t.Helper()
	key := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		}
		return '-' // '/' and '.' are neither stream-name nor single-token material
	}, t.Name())
	return "keeps." + key, "keeps-" + key
}

// newHarness runs one embedded server for a whole tier and hands the tier
// the stop/restart control it needs: same address, same store, so clients
// built before a bounce reconnect to what they were talking to.
func newHarness(t *testing.T, mk func(t *testing.T, url string) bus.Bus) bustest.Harness {
	t.Helper()
	dir := t.TempDir()
	live := newRunningServer(t, dir, 0)
	port := serverPort(live)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	// The server's teardown belongs to the tier rather than to whichever
	// subtest restarted it: clients register their Close on the subtest's
	// t, and draining a connection whose server is already gone parks
	// nats.go's drain on its flush timeout, which goleak reads as a leak.
	// Registered here, this runs only once every subtest has closed its
	// clients.
	t.Cleanup(func() {
		if live != nil {
			live.Shutdown()
			live.WaitForShutdown()
		}
	})
	return bustest.Harness{
		Mk: func(t *testing.T) bus.Bus {
			t.Helper()
			return mk(t, url)
		},
		StopServer: func(t *testing.T) {
			t.Helper()
			require.NotNil(t, live, "StopServer with no server running")
			live.Shutdown()
			live.WaitForShutdown()
			live = nil
		},
		StartServer: func(t *testing.T) {
			t.Helper()
			require.Nil(t, live, "StartServer with the server already running")
			live = newRunningServer(t, dir, port)
		},
	}
}

func coreHarness(t *testing.T) bustest.Harness {
	t.Helper()
	return newHarness(t, func(t *testing.T, url string) bus.Bus {
		t.Helper()
		subject, _ := subtestNames(t)
		return newCoreClientOn(t, url, subject)
	})
}

func durableHarness(t *testing.T) bustest.Harness {
	t.Helper()
	return newHarness(t, func(t *testing.T, url string) bus.Bus {
		t.Helper()
		subject, stream := subtestNames(t)
		return newDurableClientOn(t, url, subject, stream)
	})
}

// The hardening tier is mode-agnostic: at-most-once owes the discharged
// Loopback constraints exactly as durable mode does (ADR-011 r5).
func TestHardeningAtMostOnce(t *testing.T) {
	bustest.RunHardening(t, coreHarness(t))
}

// Durable mode owes the hardening tier and, on top of it, the replay
// promises core mode never made.
func TestHardeningDurable(t *testing.T) {
	h := durableHarness(t)
	bustest.RunHardening(t, h)
	bustest.RunDurable(t, h)
}
