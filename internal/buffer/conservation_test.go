// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deterministicRNG provides deterministic pseudo-random values with a fixed seed.
type deterministicRNG struct {
	state uint64
}

func newDeterministicRNG(seed int64) *deterministicRNG {
	s := uint64(1)
	if seed >= 0 {
		s = uint64(seed)
	}
	return &deterministicRNG{state: s ^ 0x9E3779B97F4A7C15}
}

func (r *deterministicRNG) Intn(n int) int {
	// xorshift64* algorithm. Guard for gosec: n is always > 0 in this test.
	if n <= 0 {
		return 0
	}
	x := r.state
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.state = x
	// Result is always < n (which fits in int), so conversion is safe.
	result := x % uint64(n)
	if result > 0x7FFFFFFFFFFFFFFF {
		return 0
	}
	return int(result)
}

func (r *deterministicRNG) Int63n(n int64) int64 {
	// xorshift64* algorithm. Guard for gosec: n is always > 0 in this test.
	if n <= 0 {
		return 0
	}
	x := r.state
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.state = x
	// Result is always < n (which fits in int64), so conversion is safe.
	result := x % uint64(n)
	if result > 0x7FFFFFFFFFFFFFFF {
		return 0
	}
	return int64(result)
}

// TestConservationProperty verifies that every appended fragment is either
// collectable or was in a whole expired segment — no third fate. Deterministic
// seed; model tracks the gen each fragment landed in.
func TestConservationProperty(t *testing.T) {
	rng := newDeterministicRNG(1)
	b, err := Open(t.TempDir(),
		Options{Window: 50 * time.Second, SegmentSize: 1 << 20, SweepChunk: 1 << 20},
		time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	type frag struct {
		payload []byte
		gen     uint32
	}
	model := map[[16]byte][]frag{}
	now := int64(0)
	for op := range 20_000 {
		_ = op
		switch rng.Intn(10) {
		case 9:
			now += rng.Int63n(5)
			b.Expire(time.Unix(now, 0))
		default:
			var id [16]byte
			traceIdx := rng.Intn(500)
			if traceIdx >= 0 {
				binary.LittleEndian.PutUint64(id[:8], uint64(traceIdx))
			}
			p := bytes.Repeat([]byte{byte(rng.Intn(256))}, 100+rng.Intn(2000))
			require.NoError(t, b.Append(id, p, time.Unix(now, 0)))
			model[id] = append(model[id], frag{payload: p, gen: b.ActiveGen()})
		}
	}

	minGen := b.MinLiveGen()
	for id, want := range model {
		var expected [][]byte
		for _, f := range want {
			if f.gen >= minGen {
				expected = append(expected, f.payload)
			}
		}
		var got [][]byte
		require.NoError(t, b.Collect(id, func(p []byte) {
			got = append(got, append([]byte(nil), p...))
		}))
		require.Len(t, got, len(expected), "trace %x", id[:8])
		for i := range expected {
			assert.True(t, bytes.Equal(expected[i], got[i]), "trace %x frag %d", id[:8], i)
		}
	}
}
