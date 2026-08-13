// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package natsbus

// encodeKeep is the ADR-011 r6 wire format: 16-byte id + reason byte.
func encodeKeep(id [16]byte, reason byte) []byte {
	b := make([]byte, 17)
	copy(b, id[:])
	b[16] = reason
	return b
}

// decodeKeep accepts 17 bytes (id+reason) or 16 (id, reason 0 —
// "unspecified"); anything else is malformed.
func decodeKeep(b []byte) (id [16]byte, reason byte, ok bool) {
	switch len(b) {
	case 17:
		copy(id[:], b[:16])
		return id, b[16], true
	case 16:
		copy(id[:], b)
		return id, 0, true
	default:
		return id, 0, false
	}
}
