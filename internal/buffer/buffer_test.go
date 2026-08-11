// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bytes"
	"io"
	"math"
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

func TestAppendRejectsOversizedFragment(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	oversized := make([]byte, maxRecordLen+1)
	err = b.Append([16]byte{1}, oversized, time.Unix(1, 0))
	require.Error(t, err, "a fragment past maxRecordLen would be unreadable by scanRecords on recovery, truncating every later record too")

	// A rejected append must leave the buffer usable.
	id := [16]byte{2}
	require.NoError(t, b.Append(id, []byte("ok"), time.Unix(1, 0)))
	var got []string
	require.NoError(t, b.Collect(id, func(f []byte) { got = append(got, string(f)) }))
	assert.Equal(t, []string{"ok"}, got)
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

func TestCloseIsBestEffort(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)

	frag := bytes.Repeat([]byte("x"), 512<<10)
	require.NoError(t, b.Append([16]byte{1}, frag, time.Unix(1, 0))) // gen1
	require.NoError(t, b.Append([16]byte{2}, frag, time.Unix(2, 0))) // rolls: gen1 finalized, gen2 active
	require.NoError(t, b.Append([16]byte{3}, frag, time.Unix(3, 0))) // rolls: gen2 finalized, gen3 active
	require.NoError(t, b.Append([16]byte{4}, frag, time.Unix(4, 0))) // rolls: gen3 finalized, gen4 active

	// Pre-close two finalized readers (gen1 and gen3, with a good one, gen2,
	// in between) so Close's own Close calls on both error. A first-error
	// return would only ever report whichever of the two the (unordered)
	// map iteration reaches first; every handle must be attempted and every
	// error reported.
	require.NoError(t, b.readers[1].Close())
	require.NoError(t, b.readers[3].Close())
	activeGen := b.ActiveGen()

	err = b.Close()
	require.Error(t, err)
	require.ErrorContains(t, err, "segment 1")
	require.ErrorContains(t, err, "segment 3")

	// The active segment's writer must still have been flushed and closed.
	f, ferr := os.Open(segPath(dir, activeGen))
	require.NoError(t, ferr)
	defer func() { _ = f.Close() }()
	buf, rerr := io.ReadAll(f)
	require.NoError(t, rerr)
	assert.NotEmpty(t, buf, "active segment must have been flushed even though reader closes failed")
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

func TestCollectSkipsCorruptLengthLoc(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	id := [16]byte{1}
	require.NoError(t, b.Append(id, []byte("real"), time.Unix(1, 0)))
	// A loc with an impossible length models index corruption from a
	// hostile or torn recovery source: Collect must skip it, not size a
	// 2 GiB read buffer from it.
	b.idx.put(id, loc{gen: b.w.gen, off: 0, length: math.MaxUint32})

	visits := 0
	require.NoError(t, b.Collect(id, func([]byte) { visits++ }))
	assert.Equal(t, 1, visits, "the real fragment is visited, the corrupt loc skipped")
}
