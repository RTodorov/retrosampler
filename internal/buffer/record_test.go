// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordHeaderRoundTrip(t *testing.T) {
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	frag := []byte("payload")
	var h [recHeaderLen]byte
	putRecordHeader(&h, id, frag)
	length, gotID, crc := parseRecordHeader(h)
	assert.Equal(t, lenU32(len(frag)), length)
	assert.Equal(t, id, gotID)
	assert.Equal(t, crc32.Checksum(frag, castagnoli), crc)
}
