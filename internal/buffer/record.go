// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

// Package buffer implements the retrosampler disk-segment span buffer
// (ADR-006): append-only CRC'd records, footer directories, whole-segment
// expiry, and a compact trace index. Single-writer; no locks (ADR-007).
package buffer

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// Record layout: u32 fragLen | 16B traceID | u32 CRC32C(frag) | frag.
const recHeaderLen = 4 + 16 + 4

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// lenU32 converts a non-negative int length to uint32, with guard for overflow.
// Returns 0 for negative or out-of-range input (> MaxUint32).
// Callers must ensure 0 <= n <= math.MaxUint32.
func lenU32(n int) uint32 {
	if n < 0 || n > math.MaxUint32 {
		return 0
	}
	// Guard for gosec: after guard check, n is guaranteed safe for uint32 conversion.
	return uint32(n)
}

func putRecordHeader(dst *[recHeaderLen]byte, id [16]byte, frag []byte) {
	binary.LittleEndian.PutUint32(dst[0:4], lenU32(len(frag)))
	copy(dst[4:20], id[:])
	binary.LittleEndian.PutUint32(dst[20:24], crc32.Checksum(frag, castagnoli))
}

func parseRecordHeader(h [recHeaderLen]byte) (length uint32, id [16]byte, crc uint32) {
	length = binary.LittleEndian.Uint32(h[0:4])
	copy(id[:], h[4:20])
	crc = binary.LittleEndian.Uint32(h[20:24])
	return length, id, crc
}
