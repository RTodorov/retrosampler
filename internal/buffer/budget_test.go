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

// ADR-006 r5: index ≤150 B per live trace at the pinned workload
// (5 fragments/trace — matches the ADR's ~4.2 KB/trace, ~424 B/span,
// multi-span batches). Budget holds across expiry windows (steady state,
// not first-window luck).
//
// ADR-001 r3 bans t.Skip as an escape hatch, so unlike the brief's
// testing.Short()-gated form this always runs; it's a few seconds of
// ~1.5M appends, which make test already pays for.
func TestIndexBudgetPerLiveTrace(t *testing.T) {
	const (
		tracesPerWindow = 100_000
		fragsPerTrace   = 5
		budget          = 150
	)
	b, err := Open(t.TempDir(),
		Options{Window: 100 * time.Second, SegmentSize: 8 << 20, SweepChunk: 1 << 20},
		time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	frag := bytes.Repeat([]byte("f"), 64) // payload size irrelevant to index budget
	now := int64(0)
	for window := range 3 {
		for i := range tracesPerWindow {
			var id [16]byte
			binary.LittleEndian.PutUint64(id[:8], lenU64(window)*lenU64(tracesPerWindow)+lenU64(i))
			binary.LittleEndian.PutUint64(id[8:], lenU64(i)*0x9E37)
			ts := time.Unix(now+int64(i*100/tracesPerWindow), 0)
			for range fragsPerTrace {
				require.NoError(t, b.Append(id, frag, ts))
			}
			if i%1000 == 0 {
				b.Expire(ts)
			}
		}
		now += 100
		b.Expire(time.Unix(now, 0))
	}
	// settle: sweep the whole table
	for range 64 {
		b.Expire(time.Unix(now, 0))
	}
	live := b.LiveTraces()
	require.Positive(t, live)
	perTrace := float64(b.IndexMemoryBytes()) / float64(live)
	t.Logf("index: %d B / %d live traces = %.1f B/trace", b.IndexMemoryBytes(), live, perTrace)
	assert.LessOrEqual(t, perTrace, float64(budget), "ADR-006 r5 index budget")
}
