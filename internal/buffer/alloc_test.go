// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/rtodorov/retrosampler/internal/fragmenter"
)

// TestHotPathZeroAllocs gates the stage-1 hot path (ADR-004 r2): fragmenting
// a batch and appending every resulting fragment to the buffer must cost 0
// bookkeeping allocations/span once every internal high-water mark (index
// growth, segmentWriter dir growth) is warm. Window and SegmentSize are set
// so no roll occurs during measurement — a roll is not part of this budget.
func TestHotPathZeroAllocs(t *testing.T) {
	b, err := Open(t.TempDir(),
		Options{Window: time.Hour, SegmentSize: 1 << 30},
		time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	f := fragmenter.New()
	td := benchBatch()
	now := time.Unix(1, 0)

	for range 200 { // warm all high-water marks incl. index growth
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			require.NoError(t, b.Append(id, frag, now))
		})
	}

	// Declared once, outside AllocsPerRun's measured closure: a closure
	// literal there would allocate on capture and produce a false positive.
	sink := func(id pcommon.TraceID, frag []byte) { _ = b.Append(id, frag, now) }
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, sink)
	})
	assert.Zero(t, avg, "ADR-004 r2: 0 bookkeeping allocs/span on the hot path")
}
