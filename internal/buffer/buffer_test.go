// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendCollectRoundTrip(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	id := [16]byte{1}
	require.NoError(t, b.Append(id, []byte("frag-1"), time.Unix(1, 0)))
	require.NoError(t, b.Append([16]byte{2}, []byte("other"), time.Unix(1, 0)))
	require.NoError(t, b.Append(id, []byte("frag-2"), time.Unix(2, 0)))

	var got []string
	require.NoError(t, b.Collect(id, func(f []byte) { got = append(got, string(f)) }))
	assert.Equal(t, []string{"frag-1", "frag-2"}, got, "active-segment reads must flush the write buffer")
}

func TestOpenRejectsOversizedSegmentSize(t *testing.T) {
	_, err := Open(t.TempDir(), Options{Window: time.Minute, SegmentSize: 1<<30 + 1}, time.Unix(0, 0))
	assert.Error(t, err, "segment_size above 1 GiB would let record offsets overflow their u32 field")
}

func TestOpenAcceptsSegmentSizeAtBound(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute, SegmentSize: 1 << 30}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
}

func TestCollectUnknownTrace(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	require.NoError(t, b.Collect([16]byte{9}, func([]byte) { t.Fatal("no visits expected") }))
}

func TestAppendRollsSegments(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	frag := bytes.Repeat([]byte("x"), 64<<10)
	id := [16]byte{1}
	for i := range 40 { // ~2.5 MiB -> at least 2 rolls
		require.NoError(t, b.Append(id, frag, time.Unix(int64(i), 0)))
	}
	assert.GreaterOrEqual(t, b.ActiveGen(), uint32(2))
	n := 0
	require.NoError(t, b.Collect(id, func(f []byte) { require.Len(t, f, 64<<10); n++ }))
	assert.Equal(t, 40, n, "fragments must survive rolls, across finalized and active segments")
}

func TestCollectVerifiesCRC(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	id := [16]byte{1}
	require.NoError(t, b.Append(id, []byte("frag-1"), time.Unix(1, 0)))
	require.NoError(t, b.Close())
	// flip a payload byte on disk
	raw, err := os.ReadFile(segPath(dir, 1))
	require.NoError(t, err)
	raw[recHeaderLen] ^= 0xFF
	require.NoError(t, os.WriteFile(segPath(dir, 1), raw, 0o600))

	b2, err := Open(dir, Options{Window: time.Minute}, time.Unix(2, 0))
	require.NoError(t, err)
	defer func() { _ = b2.Close() }()
	require.NoError(t, b2.Collect(id, func([]byte) { t.Fatal("corrupt fragment must not be visited") }))
}
