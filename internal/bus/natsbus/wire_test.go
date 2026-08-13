// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rtodorov/retrosampler/internal/bus"
)

func TestWireRoundTrip(t *testing.T) {
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	b := encodeKeep(id, bus.ReasonTraceAge)
	require.Len(t, b, 17)
	gotID, gotReason, ok := decodeKeep(b)
	require.True(t, ok)
	assert.Equal(t, id, gotID)
	assert.Equal(t, bus.ReasonTraceAge, gotReason)
}

func TestWireBareIDReadsReasonZero(t *testing.T) {
	// The contract's "optional reason byte", literally (ADR-011 r6).
	id := [16]byte{9}
	gotID, gotReason, ok := decodeKeep(id[:])
	require.True(t, ok)
	assert.Equal(t, id, gotID)
	assert.Equal(t, byte(0), gotReason)
}

func TestWireWrongLengthIsMalformed(t *testing.T) {
	for _, n := range []int{0, 1, 15, 18, 64} {
		_, _, ok := decodeKeep(make([]byte, n))
		assert.False(t, ok, "length %d must be malformed", n)
	}
}
