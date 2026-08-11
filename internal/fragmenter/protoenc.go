// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package fragmenter encodes OTLP spans into protobuf wire format.
package fragmenter

import (
	"encoding/binary"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Minimal protobuf wire-format writer. Two-pass: size*, then put* into a
// caller-owned enc. All encoders append; nothing allocates once e.b has
// reached its high-water capacity.

const (
	wVarint  = 0
	wFixed64 = 1
	wBytes   = 2
	wFixed32 = 5
)

// i64wire returns v's protobuf varint bit pattern (two's complement).
// Both conversions are safe for overflow—this is how Go handles signed-to-unsigned.
func i64wire(v int64) uint64 {
	if v >= 0 {
		return uint64(v)
	}
	n := -(v + 1) // for MinInt64 this is MaxInt64; always >= 0
	// Guard for gosec: after subtraction, n is guaranteed non-negative.
	if n >= 0 {
		return ^uint64(n)
	}
	return 0 // unreachable; satisfies type checker
}

// lenU64 converts a non-negative int length to uint64, with guard for negative input.
// Callers must ensure n >= 0.
func lenU64(n int) uint64 {
	if n < 0 {
		return 0
	}
	// Safe: n is guaranteed non-negative at this point.
	return uint64(n)
}

type enc struct{ b []byte }

func (e *enc) uvarint(v uint64) {
	for v >= 0x80 {
		e.b = append(e.b, byte(v)|0x80)
		v >>= 7
	}
	e.b = append(e.b, byte(v))
}

func (e *enc) key(field, wire uint64) { e.uvarint(field<<3 | wire) }

func (e *enc) str(field uint64, s string) {
	if s == "" {
		return
	}
	e.key(field, wBytes)
	e.uvarint(uint64(len(s)))
	e.b = append(e.b, s...)
}

func (e *enc) bytesF(field uint64, p []byte) {
	if len(p) == 0 {
		return
	}
	e.key(field, wBytes)
	e.uvarint(uint64(len(p)))
	e.b = append(e.b, p...)
}

func (e *enc) varintF(field, v uint64) {
	if v == 0 {
		return
	}
	e.key(field, wVarint)
	e.uvarint(v)
}

func (e *enc) fixed64F(field, v uint64) {
	if v == 0 {
		return
	}
	e.key(field, wFixed64)
	e.b = binary.LittleEndian.AppendUint64(e.b, v)
}

func (e *enc) fixed32F(field uint64, v uint32) {
	if v == 0 {
		return
	}
	e.key(field, wFixed32)
	e.b = binary.LittleEndian.AppendUint32(e.b, v)
}

// msg writes the key and length prefix for a nested message of known size.
func (e *enc) msg(field uint64, size int) {
	e.key(field, wBytes)
	e.uvarint(lenU64(size))
}

func sizeUvarint(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func sizeKey(field uint64) int { return sizeUvarint(field << 3) }

// sizeLen is key + length prefix + n payload bytes.
func sizeLen(field uint64, n int) int {
	return sizeKey(field) + sizeUvarint(lenU64(n)) + n
}

func sizeVarintF(field, v uint64) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + sizeUvarint(v)
}

func sizeFixed64F(field, v uint64) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + 8
}

func sizeFixed32F(field uint64, v uint32) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + 4
}

func sizeStr(field uint64, s string) int {
	if s == "" {
		return 0
	}
	return sizeLen(field, len(s))
}

// AnyValue. Oneof: the set variant is encoded even at its zero value.
func sizeValue(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return sizeLen(1, len(v.Str()))
	case pcommon.ValueTypeBool:
		return sizeKey(2) + 1
	case pcommon.ValueTypeInt:
		return sizeKey(3) + sizeUvarint(i64wire(v.Int()))
	case pcommon.ValueTypeDouble:
		return sizeKey(4) + 8
	case pcommon.ValueTypeSlice:
		n := 0
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			n += sizeLen(1, sizeValue(sl.At(i)))
		}
		return sizeLen(5, n)
	case pcommon.ValueTypeMap:
		n := 0
		v.Map().Range(func(k string, mv pcommon.Value) bool {
			n += sizeLen(1, sizeKeyValue(k, mv))
			return true
		})
		return sizeLen(6, n)
	case pcommon.ValueTypeBytes:
		return sizeLen(7, v.Bytes().Len())
	case pcommon.ValueTypeEmpty:
		return 0
	}
	return 0
}

func putValue(e *enc, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		s := v.Str()
		e.key(1, wBytes)
		e.uvarint(uint64(len(s)))
		e.b = append(e.b, s...)
	case pcommon.ValueTypeBool:
		e.key(2, wVarint)
		if v.Bool() {
			e.b = append(e.b, 1)
		} else {
			e.b = append(e.b, 0)
		}
	case pcommon.ValueTypeInt:
		e.key(3, wVarint)
		e.uvarint(i64wire(v.Int()))
	case pcommon.ValueTypeDouble:
		e.key(4, wFixed64)
		e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(v.Double()))
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		n := 0
		for i := 0; i < sl.Len(); i++ {
			n += sizeLen(1, sizeValue(sl.At(i)))
		}
		e.msg(5, n)
		for i := 0; i < sl.Len(); i++ {
			e.msg(1, sizeValue(sl.At(i)))
			putValue(e, sl.At(i))
		}
	case pcommon.ValueTypeMap:
		m := v.Map()
		n := 0
		m.Range(func(k string, mv pcommon.Value) bool {
			n += sizeLen(1, sizeKeyValue(k, mv))
			return true
		})
		e.msg(6, n)
		m.Range(func(k string, mv pcommon.Value) bool {
			e.msg(1, sizeKeyValue(k, mv))
			putKeyValue(e, k, mv)
			return true
		})
	case pcommon.ValueTypeBytes:
		e.bytesF(7, v.Bytes().AsRaw())
	case pcommon.ValueTypeEmpty:
		// Empty value, nothing to encode
	}
}

func sizeKeyValue(k string, v pcommon.Value) int {
	return sizeStr(1, k) + sizeLen(2, sizeValue(v))
}

func putKeyValue(e *enc, k string, v pcommon.Value) {
	e.str(1, k)
	e.msg(2, sizeValue(v))
	putValue(e, v)
}

func sizeAttrs(field uint64, m pcommon.Map) int {
	n := 0
	m.Range(func(k string, v pcommon.Value) bool {
		n += sizeLen(field, sizeKeyValue(k, v))
		return true
	})
	return n
}

func putAttrs(e *enc, field uint64, m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		e.msg(field, sizeKeyValue(k, v))
		putKeyValue(e, k, v)
		return true
	})
}
