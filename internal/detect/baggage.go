// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package detect implements ADR-008 keep detection: the local, per-span
// decision of whether a trace must be kept.
package detect

import "go.opentelemetry.io/collector/pdata/pcommon"

// readMillis reads a millisecond quantity stamped onto span attributes by
// the platform's baggage propagation. Baggage values are strings on the
// wire, so a generic baggage→attribute copier yields string attributes;
// custom stampers may write ints — both are accepted. The string grammar
// is decimal digits only: strconv is avoided because its error path
// allocates and anything outside that grammar is misconfiguration, not
// data. A negative int is returned ok — negativity is clock skew, and the
// clamp (with its counter) belongs to the caller.
func readMillis(attrs pcommon.Map, key string) (v int64, ok, malformed bool) {
	val, found := attrs.Get(key)
	if !found {
		return 0, false, false
	}
	switch val.Type() {
	case pcommon.ValueTypeInt:
		return val.Int(), true, false
	case pcommon.ValueTypeStr:
		s := val.Str()
		if len(s) == 0 {
			return 0, false, true
		}
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, false, true
			}
			d := int64(c - '0')
			if v > (1<<63-1-d)/10 {
				return 0, false, true // would overflow int64
			}
			v = v*10 + d
		}
		return v, true, false
	case pcommon.ValueTypeEmpty, pcommon.ValueTypeBool, pcommon.ValueTypeDouble,
		pcommon.ValueTypeMap, pcommon.ValueTypeSlice, pcommon.ValueTypeBytes:
		return 0, false, true
	}
	return 0, false, true
}
