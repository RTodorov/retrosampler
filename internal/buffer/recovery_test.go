// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReopenCollectsEverything(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	id := [16]byte{1}
	frag := bytes.Repeat([]byte("y"), 400<<10)
	for i := range 5 { // spans finalized + torn-active segments
		require.NoError(t, b.Append(id, frag, time.Unix(int64(i), 0)))
	}
	require.NoError(t, b.Close())

	b2, err := Open(dir, Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(10, 0))
	require.NoError(t, err)
	defer func() { _ = b2.Close() }()
	n := 0
	require.NoError(t, b2.Collect(id, func(f []byte) { require.Len(t, f, 400<<10); n++ }))
	assert.Equal(t, 5, n)

	// and the recovered active segment accepts appends
	require.NoError(t, b2.Append(id, []byte("post"), time.Unix(11, 0)))
	n = 0
	require.NoError(t, b2.Collect(id, func([]byte) { n++ }))
	assert.Equal(t, 6, n)
}

func TestTornTailTruncatesAtEveryByteBoundary(t *testing.T) {
	// Build a reference torn segment: 3 records, no footer.
	ref := t.TempDir()
	b, err := Open(ref, Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	id := [16]byte{1}
	for i := range 3 {
		require.NoError(t, b.Append(id, fmt.Appendf(nil, "frag-%d-padding", i), time.Unix(int64(i), 0)))
	}
	require.NoError(t, b.Close())
	raw, err := os.ReadFile(segPath(ref, 1))
	require.NoError(t, err)
	recLen := recHeaderLen + len("frag-0-padding")

	for cut := len(raw); cut >= 0; cut-- {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(segPath(dir, 1), raw[:cut], 0o600))
		b2, err := Open(dir, Options{Window: time.Minute}, time.Unix(10, 0))
		require.NoError(t, err, "cut=%d", cut)
		n := 0
		require.NoError(t, b2.Collect(id, func([]byte) { n++ }))
		assert.Equal(t, cut/recLen, n, "cut=%d: whole valid records survive, partial tail dropped", cut)
		require.NoError(t, b2.Close())
	}
}

func TestFooterlessNonLastSegmentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	frag := bytes.Repeat([]byte("z"), 600<<10)
	require.NoError(t, b.Append([16]byte{1}, frag, time.Unix(1, 0)))
	require.NoError(t, b.Append([16]byte{1}, frag, time.Unix(2, 0))) // rolls gen1
	require.NoError(t, b.Close())
	// vandalize gen1's trailer (finalized, non-last)
	path := segPath(dir, 1)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, fi.Size()-4))

	_, err = Open(dir, Options{Window: time.Minute}, time.Unix(10, 0))
	assert.Error(t, err)
}

// TestCollectSkipsCorruptFragmentInFinalizedSegment corrupts a payload byte
// in an already-finalized (footer-valid) segment. The footer's directory
// and trailer describe offsets/lengths only, not payload CRCs, so the
// footer stays valid, Open trusts it and folds the corrupt entry into the
// index, and it's Collect's runtime CRC check — not recovery-time
// truncation — that must skip the corrupt fragment silently.
func TestCollectSkipsCorruptFragmentInFinalizedSegment(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	id := [16]byte{1}
	frag := bytes.Repeat([]byte("c"), 600<<10)
	require.NoError(t, b.Append(id, frag, time.Unix(1, 0)))
	require.NoError(t, b.Append(id, frag, time.Unix(2, 0))) // rolls gen1, appends into gen2
	require.NoError(t, b.Close())

	// corrupt a payload byte in gen1: finalized, footer stays valid.
	raw, err := os.ReadFile(segPath(dir, 1))
	require.NoError(t, err)
	raw[recHeaderLen] ^= 0xFF
	require.NoError(t, os.WriteFile(segPath(dir, 1), raw, 0o600))

	b2, err := Open(dir, Options{Window: time.Minute}, time.Unix(10, 0))
	require.NoError(t, err)
	defer func() { _ = b2.Close() }()

	n := 0
	require.NoError(t, b2.Collect(id, func([]byte) { n++ }))
	assert.Equal(t, 1, n, "gen1's corrupt fragment must be skipped at read time; gen2's intact fragment must survive")
}
