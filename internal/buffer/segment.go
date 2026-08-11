// Copyright The retrosampler Authors
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
)

const (
	segMagic     = 0x52535347
	dirEntryLen  = 16 + 4 + 4
	trailerLen   = 8 + 4 + 8 + 8 + 4 + 4
	writeBufSize = 256 << 10
	maxRecordLen = 64 << 20
)

var errNoFooter = errors.New("segment has no valid footer")

// i64FromU64 converts a uint64 to int64, guarding against values that would
// overflow the signed range. ok is false for out-of-range input.
func i64FromU64(u uint64) (v int64, ok bool) {
	if u > math.MaxInt64 {
		return 0, false
	}
	// u <= MaxInt64 here, so the conversion cannot overflow or change sign.
	return int64(u), true
}

// u64FromI64 converts an int64 to uint64, guarding against negative input.
// ok is false for negative input.
func u64FromI64(v int64) (u uint64, ok bool) {
	if v < 0 {
		return 0, false
	}
	// v >= 0 here, so the conversion preserves its value exactly.
	return uint64(v), true
}

// dirEntry locates one record within a segment: its trace ID, byte offset
// of the record header, and fragment length (excluding the header).
type dirEntry struct {
	id     [16]byte
	off    uint32
	length uint32
}

// segmentWriter appends CRC'd records to a single append-only segment file
// and, on finalize, writes a footer directory and trailer.
type segmentWriter struct {
	f    *os.File
	w    *bufio.Writer
	gen  uint32
	size int64
	tMin int64
	tMax int64
	dir  []dirEntry
	hdr  [recHeaderLen]byte
}

// segMeta is the parsed footer of a finalized segment. gen is not parsed
// from the file — readFooter always returns it zero. The caller already
// knows the generation (it opened the file at a segPath-formatted path, or
// created the segmentWriter with that gen) and must set segMeta.gen itself.
type segMeta struct {
	gen     uint32
	tMin    int64
	tMax    int64
	entries []dirEntry
}

// segPath returns the on-disk path for segment gen within dir.
func segPath(dir string, gen uint32) string {
	return fmt.Sprintf("%s/%09d.seg", dir, gen)
}

// newSegmentWriter creates a new segment file for gen and opens it for
// append. reuse, if non-nil, is a footer directory slice from a prior
// finalized segment whose backing array is reused (truncated to len 0)
// to avoid reallocating across segment rolls.
func newSegmentWriter(dir string, gen uint32, reuse []dirEntry) (*segmentWriter, error) {
	f, err := os.OpenFile(segPath(dir, gen), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("segment %d: create: %w", gen, err)
	}
	return &segmentWriter{
		f:    f,
		w:    bufio.NewWriterSize(f, writeBufSize),
		gen:  gen,
		tMin: math.MaxInt64,
		dir:  reuse[:0],
	}, nil
}

// append writes one record (header + fragment) and returns the byte offset
// of the record header within the segment.
func (s *segmentWriter) append(id [16]byte, frag []byte, now int64) (off uint32, err error) {
	putRecordHeader(&s.hdr, id, frag)
	off = lenU32(int(s.size))
	if _, err = s.w.Write(s.hdr[:]); err != nil {
		return 0, err
	}
	if _, err = s.w.Write(frag); err != nil {
		return 0, err
	}
	s.dir = append(s.dir, dirEntry{id: id, off: off, length: lenU32(len(frag))})
	s.size += int64(recHeaderLen) + int64(len(frag))
	if now < s.tMin {
		s.tMin = now
	}
	if now > s.tMax {
		s.tMax = now
	}
	return off, nil
}

// flush pushes buffered writes to the underlying file. It does not fsync.
func (s *segmentWriter) flush() error {
	return s.w.Flush()
}

// finalize writes the footer directory and trailer, fsyncs, and closes the
// segment file. It returns the footer directory slice so the caller can
// reuse its backing array for the next segment (via newSegmentWriter's
// reuse parameter).
func (s *segmentWriter) finalize() (dir []dirEntry, err error) {
	if err = s.flush(); err != nil {
		return nil, err
	}
	footerOff, ok := u64FromI64(s.size)
	if !ok {
		return nil, fmt.Errorf("segment %d: footer offset %d out of range", s.gen, s.size)
	}
	tMin, ok := u64FromI64(s.tMin)
	if !ok {
		return nil, fmt.Errorf("segment %d: tMin %d out of range", s.gen, s.tMin)
	}
	tMax, ok := u64FromI64(s.tMax)
	if !ok {
		return nil, fmt.Errorf("segment %d: tMax %d out of range", s.gen, s.tMax)
	}

	sum := crc32.New(castagnoli)
	var entryBuf [dirEntryLen]byte
	for _, e := range s.dir {
		copy(entryBuf[0:16], e.id[:])
		binary.LittleEndian.PutUint32(entryBuf[16:20], e.off)
		binary.LittleEndian.PutUint32(entryBuf[20:24], e.length)
		if _, err = s.w.Write(entryBuf[:]); err != nil {
			return nil, err
		}
		if _, err = sum.Write(entryBuf[:]); err != nil {
			return nil, err
		}
	}

	var trailer [trailerLen]byte
	binary.LittleEndian.PutUint64(trailer[0:8], footerOff)
	binary.LittleEndian.PutUint32(trailer[8:12], lenU32(len(s.dir)))
	binary.LittleEndian.PutUint64(trailer[12:20], tMin)
	binary.LittleEndian.PutUint64(trailer[20:28], tMax)
	binary.LittleEndian.PutUint32(trailer[28:32], sum.Sum32())
	binary.LittleEndian.PutUint32(trailer[32:36], segMagic)
	if _, err = s.w.Write(trailer[:]); err != nil {
		return nil, err
	}
	if err = s.flush(); err != nil {
		return nil, err
	}
	if err = s.f.Sync(); err != nil {
		return nil, err
	}
	if err = s.f.Close(); err != nil {
		return nil, err
	}
	return s.dir, nil
}

// readFooter parses a finalized segment's footer and trailer. It returns
// errNoFooter if the trailer is absent, its magic/CRC is invalid, or the
// footer offset/entry count is inconsistent with the file size (torn
// segment: process crashed mid-write, before finalize completed).
//
// The returned segMeta.gen is always zero: readFooter has no reliable way
// to learn the generation from f alone. The caller must set it.
func readFooter(f *os.File) (segMeta, error) {
	fi, err := f.Stat()
	if err != nil {
		return segMeta{}, err
	}
	size := fi.Size()
	if size < trailerLen {
		return segMeta{}, errNoFooter
	}

	var trailer [trailerLen]byte
	if _, err = f.ReadAt(trailer[:], size-trailerLen); err != nil {
		return segMeta{}, errNoFooter
	}
	footerOff := binary.LittleEndian.Uint64(trailer[0:8])
	entryCount := binary.LittleEndian.Uint32(trailer[8:12])
	footerCRC := binary.LittleEndian.Uint32(trailer[28:32])
	magic := binary.LittleEndian.Uint32(trailer[32:36])
	if magic != segMagic {
		return segMeta{}, errNoFooter
	}

	tMin, ok := i64FromU64(binary.LittleEndian.Uint64(trailer[12:20]))
	if !ok {
		return segMeta{}, errNoFooter
	}
	tMax, ok := i64FromU64(binary.LittleEndian.Uint64(trailer[20:28]))
	if !ok {
		return segMeta{}, errNoFooter
	}
	footerOffI, ok := i64FromU64(footerOff)
	if !ok {
		return segMeta{}, errNoFooter
	}
	footerLen := int64(entryCount) * int64(dirEntryLen)
	if footerOffI+footerLen+trailerLen != size {
		return segMeta{}, errNoFooter
	}

	footer := make([]byte, footerLen)
	if _, err = f.ReadAt(footer, footerOffI); err != nil {
		return segMeta{}, errNoFooter
	}
	if crc32.Checksum(footer, castagnoli) != footerCRC {
		return segMeta{}, errNoFooter
	}

	entries := make([]dirEntry, entryCount)
	for i := range entries {
		b := footer[i*dirEntryLen : (i+1)*dirEntryLen]
		copy(entries[i].id[:], b[0:16])
		entries[i].off = binary.LittleEndian.Uint32(b[16:20])
		entries[i].length = binary.LittleEndian.Uint32(b[20:24])
	}
	// gen is intentionally left zero: readFooter has no reliable source for
	// it from f alone (see segMeta's doc comment); the caller sets it.
	return segMeta{gen: 0, tMin: tMin, tMax: tMax, entries: entries}, nil
}

// scanRecords walks records from offset 0, validating each record's CRC,
// and returns an entry for every CRC-valid prefix record plus the offset
// at which validity ends (the start of the first invalid, truncated, or
// absent record). It is used to rebuild a directory for a segment whose
// footer is missing or invalid (torn segment).
func scanRecords(f *os.File) (entries []dirEntry, validEnd int64, err error) {
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	r := bufio.NewReader(f)
	var off int64
	var hdr [recHeaderLen]byte
	for {
		if _, readErr := io.ReadFull(r, hdr[:]); readErr != nil {
			break
		}
		length, id, wantCRC := parseRecordHeader(hdr)
		if length > maxRecordLen {
			break
		}
		frag := make([]byte, length)
		if _, readErr := io.ReadFull(r, frag); readErr != nil {
			break
		}
		if crc32.Checksum(frag, castagnoli) != wantCRC {
			break
		}
		entries = append(entries, dirEntry{id: id, off: lenU32(int(off)), length: length})
		off += int64(recHeaderLen) + int64(length)
		validEnd = off
	}
	return entries, validEnd, nil
}
