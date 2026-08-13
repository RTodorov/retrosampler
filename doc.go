// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package retrosampler buffers marshaled spans on local disk and
// retroactively samples whole traces on locally detected keep conditions.
//
// # Keep conditions and their cost
//
// Keep conditions come in two forms, and they cost differently (ADR-008,
// ADR-010). The built-ins — keep_on_error, span_latency_threshold,
// trace_latency_threshold, trace_age_threshold and baseline_rate — are
// plain scalar settings evaluated natively per span at ingest, and they
// are guaranteed alloc-free under the ADR-004 gate. The policies list is
// OTTL, the one place user rules are written, and it costs roughly
// 30–110 ns and 0–3 allocations per span per condition depending on the
// shape of the expression: enum and simple attribute compares evaluate
// without allocating, while regex and arithmetic do not. Zero-alloc is
// expression-shape-dependent and is never guaranteed. A deployment with
// no policies configured pays nothing for the OTTL tail at all.
//
// Span latency is the case to watch: express it with the
// span_latency_threshold setting, never as an OTTL arithmetic expression
// over the span's timestamps. The two mean the same thing, and only the
// setting is alloc-free.
package retrosampler // import "github.com/rtodorov/retrosampler"
