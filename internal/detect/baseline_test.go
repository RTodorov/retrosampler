// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package detect

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/rtodorov/retrosampler/internal/bus"
)

// idWithTail builds a trace id whose trailing 56 bits are exactly tail.
func idWithTail(tail uint64) pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint64(id[8:], tail&(1<<56-1))
	return id
}

func TestBaselineDisabledAtZeroRate(t *testing.T) {
	d := build(t, Config{})
	assert.False(t, d.Baseline(idWithTail(0)), "rate 0 keeps nothing, even tail 0")
}

func TestBaselineDisabledAtInvalidRate(t *testing.T) {
	// Build's `> 0` guard is what keeps a nonsense rate away from the
	// out-of-range float→uint64 conversion, whose result is
	// implementation-defined.
	for _, rate := range []float64{-1, math.NaN()} {
		d := build(t, Config{BaselineRate: rate})
		assert.False(t, d.Baseline(idWithTail(0)), "rate %v keeps nothing, even tail 0", rate)
	}
}

func TestBaselineMasksLeadingBits(t *testing.T) {
	d := build(t, Config{BaselineRate: 0.5})
	id := idWithTail(0)
	id[8] = 0xFF
	assert.True(t, d.Baseline(id), "byte 8's leading bits never reach the compare")
}

func TestBaselineBoundaryExact(t *testing.T) {
	// rate 0.5 → threshold 2^55: tail 2^55−1 keeps, tail 2^55 does not.
	d := build(t, Config{BaselineRate: 0.5})
	assert.True(t, d.Baseline(idWithTail(1<<55-1)))
	assert.False(t, d.Baseline(idWithTail(1<<55)))
	assert.Equal(t, uint64(1), d.DetectedKeeps(bus.ReasonBaseline))
}

func TestBaselineRateOneKeepsAll(t *testing.T) {
	d := build(t, Config{BaselineRate: 1})
	assert.True(t, d.Baseline(idWithTail(1<<56-1)), "max tail still under 2^56")
}

// TestBaselineIdenticalAcrossInstances is the ADR-003 r6 / ADR-008 r1
// gate: two independently built detectors agree on every id.
func TestBaselineIdenticalAcrossInstances(t *testing.T) {
	a := build(t, Config{BaselineRate: 0.01})
	b := build(t, Config{BaselineRate: 0.01})
	for tail := range uint64(100_000) {
		id := idWithTail(tail * 0x9E3779B97F4A7C15 >> 8) // spread the sample
		assert.Equal(t, a.Baseline(id), b.Baseline(id), "tail %d", tail)
	}
}

func TestBaselineStatisticalRate(t *testing.T) {
	// Uniform tails at rate 1% over 1e6 ids: binomial σ≈99.5, so ±5σ
	// (9500..10500) cannot flake in any plausible run count.
	d := build(t, Config{BaselineRate: 0.01})
	kept := 0
	rng := uint64(1)
	for range 1_000_000 {
		rng = rng*6364136223846793005 + 1442695040888963407
		if d.Baseline(idWithTail(rng >> 8)) {
			kept++
		}
	}
	assert.Greater(t, kept, 9500)
	assert.Less(t, kept, 10500)
}
