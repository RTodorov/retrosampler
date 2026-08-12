// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package fragmenter

import "go.opentelemetry.io/collector/pdata/ptrace"

// Decode parses one buffered fragment back into pdata. Fragments are
// exact OTLP TracesData messages, so the stock unmarshaler is the
// inverse of this package's encoder. The flush path owns the resulting
// allocations; it is deliberately outside the ADR-004 zero-alloc gate.
func Decode(frag []byte) (ptrace.Traces, error) {
	return (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
}
