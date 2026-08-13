// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package bustest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// The suite is infrastructure the natsbus tests compose, so its own
// shape is pinned here: mk is called once per contract subtest and each
// call yields a fresh bus, so no subtest can observe a sibling's
// deliveries. t.Run's completion orders the calls; no lock is needed.
func TestRunContractBuildsOneBusPerSubtest(t *testing.T) {
	built := make(map[*bus.Loopback]struct{})
	RunContract(t, func(t *testing.T) bus.Bus {
		t.Helper()
		b := bus.NewLoopback()
		built[b] = struct{}{}
		return b
	})
	assert.Len(t, built, 4, "one fresh bus per contract subtest")
}
