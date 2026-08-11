// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/rtodorov/retrosampler/internal/fragmenter"
)

// allocTestBatch builds a batch of 25 traces with 4 spans apiece (100 spans
// total), realistic attributes, round-robin across 4 resources — mirrors
// fragmenter's testBatch shape so this gate exercises the real stage-1 hot
// path (fragment → append) at a representative fan-out.
func allocTestBatch() ptrace.Traces {
	const (
		nTraces    = 25
		spansEach  = 4
		nResources = 4
	)
	td := ptrace.NewTraces()
	sss := make([]ptrace.ScopeSpans, nResources)
	for r := range nResources {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("testlib")
		ss.Scope().SetVersion("1.0.0")
		sss[r] = ss
	}

	for t := range nTraces {
		var trID pcommon.TraceID
		trID[0] = byte(t%251 + 1)
		res := t % nResources
		for s := range spansEach {
			sp := sss[res].Spans().AppendEmpty()
			sp.SetTraceID(trID)
			var spID pcommon.SpanID
			spID[0] = byte((t*spansEach+s)%251 + 1)
			sp.SetSpanID(spID)
			sp.SetName(fmt.Sprintf("op-%d", s))
			sp.SetKind(ptrace.SpanKindServer)
			sp.SetStartTimestamp(pcommon.Timestamp(1_000_000_000 + lenU64(s)))
			sp.SetEndTimestamp(pcommon.Timestamp(2_000_000_000 + lenU64(s)))
			sp.Attributes().PutStr("http.method", "GET")
			sp.Attributes().PutStr("http.route", "/api/resource")
			sp.Attributes().PutInt("http.status_code", 200)
		}
	}
	return td
}

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
	td := allocTestBatch()
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
