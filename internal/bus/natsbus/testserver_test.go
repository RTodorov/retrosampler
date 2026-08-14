// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus_test

import (
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

// newRunningServer runs an embedded nats-server (ADR-011 r4, test-only)
// with JetStream file storage in dir, on an ephemeral port unless
// port > 0, and registers NO teardown: a harness that stops and restarts
// one address owns that lifecycle itself, because a teardown registered
// per restart would run in the wrong order against the clients built
// before it.
func newRunningServer(t *testing.T, dir string, port int) *server.Server {
	t.Helper()
	if port == 0 {
		port = -1 // ephemeral
	}
	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: port,
		JetStream: true, StoreDir: dir,
		NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(10*time.Second), "embedded nats-server never came up")
	return ns
}

// startServer is newRunningServer with teardown on t, which is what every
// test that never restarts its server wants.
func startServer(t *testing.T, dir string, port int) *server.Server {
	t.Helper()
	ns := newRunningServer(t, dir, port)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	return ns
}

func serverPort(ns *server.Server) int { return ns.Addr().(*net.TCPAddr).Port }

// freePort returns a port with nothing listening on it: the address a
// client must tolerate rather than fail to start against.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
