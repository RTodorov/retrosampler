// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package detect

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestReadMillis(t *testing.T) {
	tests := []struct {
		name      string
		set       func(m pcommon.Map)
		v         int64
		ok        bool
		malformed bool
	}{
		{"int value", func(m pcommon.Map) { m.PutInt("k", 1234) }, 1234, true, false},
		{"string value", func(m pcommon.Map) { m.PutStr("k", "56789") }, 56789, true, false},
		{"missing key", func(pcommon.Map) {}, 0, false, false},
		{
			"negative int passes through for the caller's skew clamp",
			func(m pcommon.Map) { m.PutInt("k", -5) }, -5, true, false,
		},
		{"empty string", func(m pcommon.Map) { m.PutStr("k", "") }, 0, false, true},
		{"non-digit string", func(m pcommon.Map) { m.PutStr("k", "12x4") }, 0, false, true},
		{
			"signed string is malformed (grammar is digits only)",
			func(m pcommon.Map) { m.PutStr("k", "-5") }, 0, false, true,
		},
		{"overflowing string", func(m pcommon.Map) { m.PutStr("k", "99999999999999999999") }, 0, false, true},
		{
			"max int64 is the last accepted string",
			func(m pcommon.Map) { m.PutStr("k", "9223372036854775807") }, math.MaxInt64, true, false,
		},
		{
			"one past max int64 is malformed",
			func(m pcommon.Map) { m.PutStr("k", "9223372036854775808") }, 0, false, true,
		},
		{"wrong type", func(m pcommon.Map) { m.PutBool("k", true) }, 0, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := pcommon.NewMap()
			tt.set(m)
			v, ok, malformed := readMillis(m, "k")
			assert.Equal(t, tt.v, v)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.malformed, malformed)
		})
	}
}
