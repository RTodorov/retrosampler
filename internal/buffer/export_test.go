// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

// ActiveGen returns the active segment's generation. Test-only.
func (b *Buffer) ActiveGen() uint32 { return b.w.gen }

// MinLiveGen returns the oldest generation still considered live. Test-only.
func (b *Buffer) MinLiveGen() uint32 { return b.minGen }

// IndexMemoryBytes returns the trace index's estimated resident memory, in
// bytes. Test-only.
func (b *Buffer) IndexMemoryBytes() int64 { return b.idx.memoryBytes() }

// LiveTraces returns the number of live traces in the index. Test-only.
func (b *Buffer) LiveTraces() int { return b.idx.live() }
