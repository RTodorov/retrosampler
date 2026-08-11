// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestEncPrimitives(t *testing.T) {
	var e enc
	e.uvarint(300)
	assert.Equal(t, []byte{0xAC, 0x02}, e.b)

	e.b = e.b[:0]
	e.str(5, "ab") // key 5<<3|2 = 0x2A, len 2
	assert.Equal(t, []byte{0x2A, 0x02, 'a', 'b'}, e.b)

	e.b = e.b[:0]
	e.str(5, "") // proto3 default: skipped
	assert.Empty(t, e.b)

	e.b = e.b[:0]
	e.fixed32F(16, 1) // key 16<<3|5 = 133 → varint {0x85, 0x01}
	assert.Equal(t, []byte{0x85, 0x01, 1, 0, 0, 0}, e.b)

	assert.Equal(t, 2, sizeUvarint(300))
	assert.Equal(t, 1, sizeKey(15))
	assert.Equal(t, 2, sizeKey(16))
	assert.Equal(t, 1+1+3, sizeLen(1, 3))
}

// encodes attrs as the Resource of a one-span TracesData and round-trips.
func attrsRoundTrip(t *testing.T, fill func(pcommon.Map)) pcommon.Map {
	t.Helper()
	m := pcommon.NewMap()
	fill(m)
	var e enc
	body := sizeAttrs(1, m) // Resource.attributes = field 1
	// ResourceSpans{ resource=1{ attrs } }
	e.msg(1, sizeLen(1, body)) // TracesData.resource_spans
	e.msg(1, body)             // ResourceSpans.resource
	putAttrs(&e, 1, m)

	td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	require.Equal(t, 1, td.ResourceSpans().Len())
	return td.ResourceSpans().At(0).Resource().Attributes()
}

func TestAttrEncodingRoundTrip(t *testing.T) {
	got := attrsRoundTrip(t, func(m pcommon.Map) {
		m.PutStr("s", "v")
		m.PutStr("empty", "") // oneof: empty string still round-trips as Str
		m.PutInt("i", -3)
		m.PutInt("zero", 0)
		m.PutBool("b", false)
		m.PutDouble("d", 1.5)
		m.PutEmptyBytes("raw").FromRaw([]byte{1, 2})
		sl := m.PutEmptySlice("arr")
		sl.AppendEmpty().SetStr("x")
		sl.AppendEmpty().SetInt(7)
		mm := m.PutEmptyMap("nested")
		mm.PutStr("k", "v")
	})
	got2 := got.AsRaw()
	assert.Equal(t, "v", got2["s"])
	assert.Empty(t, got2["empty"])
	assert.Equal(t, int64(-3), got2["i"])
	assert.Equal(t, int64(0), got2["zero"])
	assert.Equal(t, false, got2["b"])
	assert.InEpsilon(t, 1.5, got2["d"], 1e-9)
	assert.Equal(t, []byte{1, 2}, got2["raw"])
	assert.Equal(t, []any{"x", int64(7)}, got2["arr"])
	assert.Equal(t, map[string]any{"k": "v"}, got2["nested"])
}

// TestInterfaceFunctions validates that Task 3's encoder interface functions are available.
func TestInterfaceFunctions(t *testing.T) {
	var e enc

	// These functions are part of the interface for Task 3 span/group encoder.
	e.varintF(1, 100)
	assert.NotEmpty(t, e.b)

	e.b = e.b[:0]
	e.fixed64F(1, 100)
	assert.NotEmpty(t, e.b)

	e.b = e.b[:0]
	e.fixed32F(1, 100)
	assert.NotEmpty(t, e.b)

	assert.Equal(t, 2, sizeVarintF(1, 300))
	assert.Equal(t, 0, sizeVarintF(1, 0))
	assert.Equal(t, 9, sizeFixed64F(1, 100))
	assert.Equal(t, 0, sizeFixed64F(1, 0))
	assert.Equal(t, 5, sizeFixed32F(1, 100))
	assert.Equal(t, 0, sizeFixed32F(1, 0))
}
