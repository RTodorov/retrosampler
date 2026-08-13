// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package bus_test

import (
	"testing"

	"github.com/rtodorov/retrosampler/internal/bus"
	"github.com/rtodorov/retrosampler/internal/bus/bustest"
)

// Loopback passes the contract tier. The hardening tier is exactly the
// three documented Loopback constraints, so Loopback composes only this.
func TestLoopbackContract(t *testing.T) {
	bustest.RunContract(t, func(t *testing.T) bus.Bus {
		t.Helper()
		return bus.NewLoopback()
	})
}
