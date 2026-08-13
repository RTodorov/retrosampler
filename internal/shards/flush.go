// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package shards

// Need is the bit set of work a FlushJob still owes.
type Need uint8

const (
	// NeedPublish broadcasts the keep on the bus (OriginLocal only).
	NeedPublish Need = 1 << iota
	// NeedFlush decodes the fragments and hands them to the pipeline.
	NeedFlush
)

// FlushJob is one unit of flush work handed from a shard worker to the
// flusher. Frags are job-owned copies; jobs are plain allocations — the
// flush path runs at the kept-trace rate, outside the ADR-004 gate.
type FlushJob struct {
	ID     [16]byte
	Reason byte
	Need   Need
	Frags  [][]byte
}
