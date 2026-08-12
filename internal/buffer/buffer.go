// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSegmentSize = 32 << 20
	// maxSegmentSize bounds Options.SegmentSize: record offsets are u32
	// (lenU32), so a segment materially larger than this would let offsets
	// wrap and silently corrupt later records on recovery.
	maxSegmentSize    = 1 << 30
	defaultSweepChunk = 4096
)

// Options configures a Buffer.
type Options struct {
	// Window is the retention window W: a segment's fragments become
	// eligible for expiry once its tMax is older than now-W. Required,
	// must be > 0.
	Window time.Duration
	// SegmentSize is the roll threshold in bytes. Defaults to 32 MiB.
	SegmentSize int
	// SweepChunk is the number of index slots Expire examines per call.
	// Defaults to 4096.
	SweepChunk int
}

// Buffer is the retrosampler disk-segment span buffer (ADR-006):
// append-only CRC'd records rolled into whole segments, expired whole
// segment at a time, indexed by a compact in-memory trace index.
// Single-goroutine use only (ADR-007).
type Buffer struct {
	dir  string
	opts Options

	idx *index
	w   *segmentWriter

	// readers holds finalized segments, open read-only for ReadAt, keyed
	// by generation.
	readers map[uint32]*os.File
	// activeReader is a lazily opened read-only handle onto the active
	// segment's file (the writer's own handle is write-only). Reopened at
	// most once per roll.
	activeReader *os.File

	// metas holds finalized segments' [tMin, tMax] only; their footer
	// entries are folded into idx on load/roll and not retained here.
	metas map[uint32]segMeta

	// finalizedBytes sums metas' file sizes; DiskBytes adds the active segment.
	finalizedBytes int64

	// minGen is the oldest generation still considered live; Collect
	// skips locs with gen < minGen. Task 9's Expire advances it.
	minGen uint32

	// readBuf is reused across Collect calls, grown to the high-water
	// record size (header + fragment) seen so far.
	readBuf []byte

	// dirScratch is the footer directory slice recycled across
	// segmentWriters on roll: segmentWriter.finalize's return value, fed
	// into the next newSegmentWriter's reuse parameter.
	dirScratch []dirEntry
}

// parseSegGen extracts the generation encoded in a segPath-formatted file
// name ("%09d.seg"). ok is false if name doesn't match.
func parseSegGen(name string) (gen uint32, ok bool) {
	base, hasSuffix := strings.CutSuffix(name, ".seg")
	if !hasSuffix {
		return 0, false
	}
	n, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, false
	}
	if n > math.MaxUint32 {
		return 0, false
	}
	// Guard above: n <= MaxUint32, so the conversion is exact.
	return uint32(n), true
}

// fragBufLen converts a fragment length from the index to the int size of
// its on-disk record (header + payload). ok is false for lengths that
// cannot be a real record (> MaxInt32): index lengths come from disk
// recovery, so they are not trusted.
func fragBufLen(length uint32) (n int, ok bool) {
	if length > math.MaxInt32 {
		return 0, false
	}
	// length <= MaxInt32 here, so header+payload fits in an int on every
	// platform Go supports (int is at least 32 bits).
	return recHeaderLen + int(length), true
}

// Open creates or recovers a buffer in dir. now anchors recovered
// segments' retention clock. Single-goroutine use only (ADR-007).
func Open(dir string, opts Options, now time.Time) (*Buffer, error) {
	if opts.Window <= 0 {
		return nil, errors.New("buffer: Options.Window must be > 0")
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	if opts.SegmentSize > maxSegmentSize {
		return nil, errors.New("buffer: Options.SegmentSize must be at most 1 GiB")
	}
	if opts.SweepChunk <= 0 {
		opts.SweepChunk = defaultSweepChunk
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("buffer: mkdir %s: %w", dir, err)
	}

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("buffer: readdir %s: %w", dir, err)
	}
	var gens []uint32
	for _, e := range dirEntries {
		if e.IsDir() {
			continue
		}
		gen, ok := parseSegGen(e.Name())
		if !ok {
			continue
		}
		gens = append(gens, gen)
	}
	slices.Sort(gens)

	b := &Buffer{
		dir:     dir,
		opts:    opts,
		idx:     newIndex(),
		readers: make(map[uint32]*os.File),
		metas:   make(map[uint32]segMeta),
		minGen:  1,
	}

	if len(gens) == 0 {
		w, werr := newSegmentWriter(dir, 1, nil)
		if werr != nil {
			return nil, werr
		}
		b.w = w
		return b, nil
	}

	if err := b.recover(gens, now); err != nil {
		return nil, err
	}
	return b, nil
}

// recover loads an existing (nonempty) buffer directory: footer-valid
// segments fold their directory into idx, in ascending gen order; the last
// segment, if footerless, becomes the active writer, continuing its gen
// (first cut — Task 10 hardens torn-tail and multi-footerless handling).
func (b *Buffer) recover(gens []uint32, now time.Time) error {
	for i, gen := range gens {
		f, err := os.Open(segPath(b.dir, gen))
		if err != nil {
			return fmt.Errorf("buffer: open segment %d: %w", gen, err)
		}

		meta, ferr := readFooter(f)
		if ferr == nil {
			for _, e := range meta.entries {
				b.idx.put(e.id, loc{gen: gen, off: e.off, length: e.length})
			}
			fi, statErr := f.Stat()
			if statErr != nil {
				_ = f.Close()
				return fmt.Errorf("buffer: stat segment %d: %w", gen, statErr)
			}
			b.metas[gen] = segMeta{gen: gen, tMin: meta.tMin, tMax: meta.tMax, size: fi.Size()}
			b.finalizedBytes += fi.Size()
			b.readers[gen] = f
			continue
		}
		if !errors.Is(ferr, errNoFooter) {
			_ = f.Close()
			return fmt.Errorf("buffer: read footer segment %d: %w", gen, ferr)
		}
		if i != len(gens)-1 {
			_ = f.Close()
			return fmt.Errorf("buffer: segment %d has no footer but is not the newest segment", gen)
		}
		if err := b.recoverActive(f, gen, now); err != nil {
			return err
		}
	}

	if b.w == nil {
		// Every existing segment had a valid footer: continue at the next
		// generation.
		w, err := newSegmentWriter(b.dir, gens[len(gens)-1]+1, nil)
		if err != nil {
			return err
		}
		b.w = w
	}
	return nil
}

// recoverActive rebuilds the footerless segment gen by scanning its
// CRC-valid record prefix, truncating away any torn tail, and reopening it
// as the active writer, continuing gen.
func (b *Buffer) recoverActive(f *os.File, gen uint32, now time.Time) error {
	entries, validEnd, err := scanRecords(f)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("buffer: scan segment %d: %w", gen, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("buffer: close segment %d: %w", gen, err)
	}
	if err := os.Truncate(segPath(b.dir, gen), validEnd); err != nil {
		return fmt.Errorf("buffer: truncate segment %d: %w", gen, err)
	}
	wf, err := os.OpenFile(segPath(b.dir, gen), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("buffer: reopen segment %d: %w", gen, err)
	}
	nowNanos := now.UnixNano()
	b.w = &segmentWriter{
		f:    wf,
		w:    bufio.NewWriterSize(wf, writeBufSize),
		gen:  gen,
		size: validEnd,
		tMin: nowNanos,
		tMax: nowNanos,
		dir:  entries,
	}
	for _, e := range entries {
		b.idx.put(e.id, loc{gen: gen, off: e.off, length: e.length})
	}
	return nil
}

// Append appends frag for id to the active segment, rolling to a new
// generation first if frag would not fit within Options.SegmentSize.
func (b *Buffer) Append(id [16]byte, frag []byte, now time.Time) error {
	if len(frag) > maxRecordLen {
		return fmt.Errorf("buffer: fragment length %d exceeds max record length %d", len(frag), maxRecordLen)
	}
	if b.w.size > 0 && b.w.size+int64(recHeaderLen)+int64(len(frag)) > int64(b.opts.SegmentSize) {
		if err := b.roll(); err != nil {
			return err
		}
	}
	off, err := b.w.append(id, frag, now.UnixNano())
	if err != nil {
		return fmt.Errorf("buffer: append: %w", err)
	}
	b.idx.put(id, loc{gen: b.w.gen, off: off, length: lenU32(len(frag))})
	return nil
}

// roll finalizes the active segment, opens it read-only for Collect, and
// starts a new segmentWriter at the next generation.
func (b *Buffer) roll() error {
	gen := b.w.gen
	tMin, tMax := b.w.tMin, b.w.tMax
	entries, err := b.w.finalize()
	if err != nil {
		return fmt.Errorf("buffer: finalize segment %d: %w", gen, err)
	}
	b.dirScratch = entries

	f, err := os.Open(segPath(b.dir, gen))
	if err != nil {
		return fmt.Errorf("buffer: open finalized segment %d: %w", gen, err)
	}
	fi, err := f.Stat()
	if err != nil {
		// This return leaves the buffer half-rolled — segment finalized,
		// no reader registered — which Collect surfaces as "no reader for
		// segment %d" on a later read. Restructuring that is stage-3
		// work; leaking the handle here is not (mirrors recover()).
		_ = f.Close()
		return fmt.Errorf("buffer: stat finalized segment %d: %w", gen, err)
	}
	b.readers[gen] = f
	b.metas[gen] = segMeta{gen: gen, tMin: tMin, tMax: tMax, size: fi.Size()}
	b.finalizedBytes += fi.Size()

	if b.activeReader != nil {
		if cerr := b.activeReader.Close(); cerr != nil {
			return fmt.Errorf("buffer: close active reader for segment %d: %w", gen, cerr)
		}
		b.activeReader = nil
	}

	w, err := newSegmentWriter(b.dir, gen+1, b.dirScratch)
	if err != nil {
		return fmt.Errorf("buffer: new segment %d: %w", gen+1, err)
	}
	b.w = w
	return nil
}

// Collect visits every live fragment of id in append order. frag is only
// valid during the call. Unknown id: no visits, nil error.
func (b *Buffer) Collect(id [16]byte, visit func(frag []byte)) error {
	flushed := false
	for i := b.idx.head(id); i >= 0; {
		l := b.idx.at(i)
		if l.gen < b.minGen {
			i = l.next
			continue
		}

		r, err := b.readerFor(l.gen, &flushed)
		if err != nil {
			return err
		}

		need, lenOK := fragBufLen(l.length)
		if !lenOK {
			i = l.next
			continue
		}
		if cap(b.readBuf) < need {
			b.readBuf = make([]byte, need)
		}
		buf := b.readBuf[:need]
		if _, err := r.ReadAt(buf, int64(l.off)); err != nil {
			return fmt.Errorf("buffer: read segment %d offset %d: %w", l.gen, l.off, err)
		}
		wantCRC := binary.LittleEndian.Uint32(buf[20:24])
		payload := buf[recHeaderLen:need]
		if crc32.Checksum(payload, castagnoli) == wantCRC {
			visit(payload)
		}
		i = l.next
	}
	return nil
}

// readerFor returns the read handle for generation gen: the active
// segment's lazily opened read-only handle (flushing the write buffer
// first, once) or a finalized segment's reader.
func (b *Buffer) readerFor(gen uint32, flushed *bool) (*os.File, error) {
	if gen != b.w.gen {
		r, ok := b.readers[gen]
		if !ok {
			return nil, fmt.Errorf("buffer: no reader for segment %d", gen)
		}
		return r, nil
	}
	if !*flushed {
		if err := b.w.flush(); err != nil {
			return nil, fmt.Errorf("buffer: flush active segment: %w", err)
		}
		*flushed = true
	}
	if b.activeReader == nil {
		f, err := os.Open(segPath(b.dir, gen))
		if err != nil {
			return nil, fmt.Errorf("buffer: open active segment %d for read: %w", gen, err)
		}
		b.activeReader = f
	}
	return b.activeReader, nil
}

// Expire advances the buffer's retention boundary and reclaims expired
// segments and index entries, in order:
//  1. delete every finalized segment whose tMax is older than now-Window,
//     ascending gen, advancing minGen past each;
//  2. if the active segment is itself stale (nonempty, tMax older than
//     now-Window), roll it so rule 1 deletes it on the next call;
//  3. sweep up to SweepChunk index slots to reclaim dead traces' locs now
//     that minGen has advanced.
func (b *Buffer) Expire(now time.Time) {
	cutoff := now.Add(-b.opts.Window).UnixNano()
	b.deleteExpired(cutoff)

	if b.w.size > 0 && b.w.tMax < cutoff {
		// Expire has no error return (brief's signature); a roll failure here
		// leaves the active segment as-is and is retried on the next call.
		_ = b.roll()
	}

	b.idx.sweep(b.opts.SweepChunk, b.minGen)
}

// deleteExpired removes every finalized segment whose tMax < cutoff, in
// ascending gen order, advancing minGen past each deleted segment. Gens are
// contiguous by construction (roll always allocates gen+1), so finding and
// removing the minimum repeatedly correctly walks the expired prefix.
func (b *Buffer) deleteExpired(cutoff int64) {
	for {
		gen, meta, ok := b.oldestFinalized()
		if !ok || meta.tMax >= cutoff {
			return
		}
		if !b.removeSegment(gen) {
			return
		}
	}
}

// removeSegment unlinks finalized segment gen and drops its bookkeeping.
// The file is removed first: if Remove fails, the reader, meta entry, and
// minGen are left untouched, so Collect keeps working and the next Expire
// retries. Nothing is torn down until the disk space is actually gone.
func (b *Buffer) removeSegment(gen uint32) bool {
	if err := os.Remove(segPath(b.dir, gen)); err != nil {
		return false
	}
	if r, open := b.readers[gen]; open {
		// Best-effort: a close failure must not wedge the map entry in
		// place, or every later Expire would retry a Close that can no
		// longer matter — the file is already gone.
		_ = r.Close()
		delete(b.readers, gen)
	}
	b.finalizedBytes -= b.metas[gen].size
	delete(b.metas, gen)
	b.minGen = gen + 1
	return true
}

// oldestFinalized returns the finalized segment with the lowest gen still
// tracked in metas, or ok=false if none remain.
func (b *Buffer) oldestFinalized() (gen uint32, meta segMeta, ok bool) {
	first := true
	for g, m := range b.metas {
		if first || g < gen {
			gen, meta, first = g, m, false
		}
	}
	return gen, meta, !first
}

// DiskBytes returns the buffer's on-disk footprint: finalized segment
// files (including footers) plus the active segment's logical size.
func (b *Buffer) DiskBytes() int64 {
	return b.finalizedBytes + b.w.size
}

// OldestFinalizedTMax returns the newest-record timestamp of the oldest
// finalized segment — the data the watermark rung would sacrifice next —
// or ok=false when no finalized segment exists.
func (b *Buffer) OldestFinalizedTMax() (tMax int64, ok bool) {
	_, meta, ok := b.oldestFinalized()
	return meta.tMax, ok
}

// ExpireOldest force-expires the oldest finalized segment regardless of
// the retention window (the ADR-007 watermark rung) and returns the disk
// bytes freed. ok is false when no finalized segment exists or the file
// removal failed (the next call retries).
func (b *Buffer) ExpireOldest() (freed int64, ok bool) {
	gen, meta, ok := b.oldestFinalized()
	if !ok {
		return 0, false
	}
	if !b.removeSegment(gen) {
		return 0, false
	}
	return meta.size, true
}

// Close flushes and fsyncs the active segment and closes every open file
// handle. No footer is written for the active segment — recovery rescans
// it on the next Open (ADR-006 r6). Best-effort: every handle is attempted
// regardless of an earlier failure, and every error is reported.
func (b *Buffer) Close() error {
	var errs []error
	if err := b.w.flush(); err != nil {
		errs = append(errs, fmt.Errorf("buffer: flush active segment: %w", err))
	}
	if err := b.w.f.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("buffer: sync active segment: %w", err))
	}
	if err := b.w.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("buffer: close active segment: %w", err))
	}
	if b.activeReader != nil {
		if err := b.activeReader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("buffer: close active reader: %w", err))
		}
	}
	for gen, f := range b.readers {
		if err := f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("buffer: close segment %d: %w", gen, err))
		}
	}
	return errors.Join(errs...)
}
