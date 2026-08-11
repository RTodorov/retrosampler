// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentAppendFinalizeReadFooter(t *testing.T) {
	dir := t.TempDir()
	w, err := newSegmentWriter(dir, 7, nil)
	require.NoError(t, err)
	id1, id2 := [16]byte{1}, [16]byte{2}
	off1, err := w.append(id1, []byte("aaaa"), 100)
	require.NoError(t, err)
	off2, err := w.append(id2, []byte("bbbbbb"), 200)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), off1)
	assert.Equal(t, uint32(recHeaderLen+4), off2)
	_, err = w.finalize()
	require.NoError(t, err)

	f, err := os.Open(segPath(dir, 7))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	meta, err := readFooter(f)
	require.NoError(t, err)
	assert.Equal(t, int64(100), meta.tMin)
	assert.Equal(t, int64(200), meta.tMax)
	require.Len(t, meta.entries, 2)
	assert.Equal(t, dirEntry{id: id1, off: 0, length: 4}, meta.entries[0])
	assert.Equal(t, dirEntry{id: id2, off: off2, length: 6}, meta.entries[1])
}

func TestReadFooterRejectsTornSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := newSegmentWriter(dir, 1, nil)
	require.NoError(t, err)
	_, err = w.append([16]byte{1}, []byte("x"), 1)
	require.NoError(t, err)
	require.NoError(t, w.flush())
	require.NoError(t, w.f.Close()) // no finalize: torn

	f, err := os.Open(segPath(dir, 1))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = readFooter(f)
	assert.ErrorIs(t, err, errNoFooter)
}

func TestScanRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := newSegmentWriter(dir, 1, nil)
	require.NoError(t, err)
	_, _ = w.append([16]byte{1}, []byte("aaaa"), 1)
	_, _ = w.append([16]byte{2}, []byte("bb"), 2)
	require.NoError(t, w.flush())
	require.NoError(t, w.f.Close())

	f, err := os.Open(segPath(dir, 1))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	entries, validEnd, err := scanRecords(f)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint32(4), entries[0].length)
	fi, _ := f.Stat()
	assert.Equal(t, fi.Size(), validEnd)
}
