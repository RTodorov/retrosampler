// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpireDeletesWholeSegments(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: 10 * time.Second, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	old, fresh := [16]byte{1}, [16]byte{2}
	frag := bytes.Repeat([]byte("x"), 512<<10)
	// each 512 KiB append rolls the previous one out at 1 MiB segments:
	// gen1 seals [1,1], gen2 seals [2,2], fresh lands in active gen3.
	require.NoError(t, b.Append(old, frag, time.Unix(1, 0)))
	require.NoError(t, b.Append(old, frag, time.Unix(2, 0)))
	require.NoError(t, b.Append(fresh, frag, time.Unix(8, 0)))

	b.Expire(time.Unix(13, 0)) // threshold 3: gen1+gen2 deleted; gen3 (tMax=8) not stale
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	assert.Len(t, files, 1, "expired segment files removed, active remains")

	require.NoError(t, b.Collect(old, func([]byte) { t.Fatal("all of old's locs expired") }))
	got := 0
	require.NoError(t, b.Collect(fresh, func([]byte) { got++ }))
	assert.Equal(t, 1, got, "unexpired fragments still collectable")
}

func TestExpireRollsStaleActiveSegment(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: 10 * time.Second}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	require.NoError(t, b.Append([16]byte{1}, []byte("old"), time.Unix(1, 0)))
	b.Expire(time.Unix(30, 0)) // rolls stale active
	b.Expire(time.Unix(30, 0)) // deletes it
	require.NoError(t, b.Collect([16]byte{1}, func([]byte) { t.Fatal("expired") }))
	assert.Greater(t, b.MinLiveGen(), uint32(1))
}

func TestExpireBestEffortClosesWedgedReader(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: 8 * time.Second, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	frag := bytes.Repeat([]byte("x"), 512<<10)
	require.NoError(t, b.Append([16]byte{1}, frag, time.Unix(1, 0)))
	require.NoError(t, b.Append([16]byte{2}, frag, time.Unix(2, 0))) // rolls: gen1 finalized (tMax=1), gen2 active (tMax=2)

	// Pre-close gen1's finalized reader so deleteExpired's own Close call on
	// it errors, simulating whatever made the handle unusable.
	require.NoError(t, b.readers[1].Close())

	// cutoff=2: gen1 (tMax=1) is stale, gen2 (tMax=2) is not, so only gen1
	// is a deleteExpired candidate here — isolates the close-error path.
	b.Expire(time.Unix(10, 0))

	_, stillTracked := b.readers[1]
	assert.False(t, stillTracked, "a wedged reader must still be dropped from the map (best-effort close)")
	assert.Equal(t, uint32(2), b.MinLiveGen(), "minGen must advance past the deleted segment despite the close error")
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	assert.Len(t, files, 1, "expired segment file must still be removed despite the close error")

	// A retry (the old wedge point) must be a no-op, not an error loop.
	b.Expire(time.Unix(10, 0))
}

func TestExpireSweepsIndex(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: 10 * time.Second, SweepChunk: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	for i := range 100 {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[:8], lenU64(i))
		require.NoError(t, b.Append(id, []byte("x"), time.Unix(1, 0)))
	}
	require.Equal(t, 100, b.LiveTraces())
	b.Expire(time.Unix(30, 0)) // roll stale active
	b.Expire(time.Unix(30, 0)) // delete + sweep (chunk covers whole table)
	assert.Zero(t, b.LiveTraces(), "dead traces reclaimed from index")
}
