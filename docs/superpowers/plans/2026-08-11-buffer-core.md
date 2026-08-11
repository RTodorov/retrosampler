# Buffer Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The retrosampler buffer core — a zero-alloc fragmenter (pdata batch → per-trace OTLP proto fragments) and a disk-segment store with compact index, expiry, and restart recovery, wired into the processor in shadow mode (buffer everything, still pass everything through).

**Architecture:** Two single-threaded units per spec `docs/superpowers/specs/2026-08-11-buffer-core-design.md`: `internal/fragmenter` hand-encodes OTLP proto directly from pdata accessors into a reused scratch buffer (this is the only way to hit 0 allocs/span — `CopyTo`-based subsetting allocates per span); `internal/buffer` stores fragments in append-only segment files with CRC records, a footer directory per rolled segment, whole-segment expiry, and an open-addressing index with a flat loc arena under a 150 B/live-trace budget. The processor wires them behind a temporary coarse mutex (deleted in stage 2, ADR-007).

**Tech Stack:** Go 1.25, stdlib only for the new packages (`hash/crc32` Castagnoli, `bufio`, `encoding/binary`) per ADR-005; `go.opentelemetry.io/collector/pdata` v1.64.0 at the fragmenter input edge only.

## Global Constraints

- ADRs are authoritative: 002 (TDD: failing test first, always), 004 (budgets are tests), 006 (buffer format), 007 (single-writer; no locks inside the units).
- Every new `.go` file starts with the two-line license header (code blocks below omit it for brevity — the file must have it):
  ```go
  // Copyright The retrosampler Authors
  // SPDX-License-Identifier: Apache-2.0
  ```
- Commit messages are conventional (`feat:`, `test:`, `docs:` …) — a commit-msg hook rejects anything else.
- A PostToolUse hook auto-runs gofumpt/gci after edits; pre-commit runs lint + build. Don't fight them (ADR-001).
- No new module dependencies. `internal/fragmenter` may import pdata; `internal/buffer` must not import pdata (takes `[16]byte` + `[]byte`).
- Neither unit calls `time.Now()`; time is always a parameter. No goroutines inside the units.
- Benchmark names must be exactly `BenchmarkIngest`, `BenchmarkKeepFlush`, `BenchmarkExpiry` (the Makefile bench regex anchors on these).
- Proto oneof caveat: inside `AnyValue`, the set variant is encoded **even at its zero value** (an empty string, `false`, `0`) — that's how oneof discriminates. Top-level proto3 scalar fields skip zero values.
- Test helpers that need buffer internals go in `internal/buffer/export_test.go`, never by exporting production API.
- Run `make test` (race) before every commit; `make generate` must be diff-clean (stop gate checks).

---

### Task 1: Config fields

**Files:**
- Modify: `config.go`
- Modify: `config_test.go`

**Interfaces:**
- Produces: `Config{StorageDir string, Window time.Duration, SegmentSize int}` with mapstructure tags `storage_dir`, `window`, `segment_size`. Defaults: `""`, `5m`, `32<<20`. Empty `StorageDir` means shadow buffering disabled (processor stays pure passthrough) — this keeps the mdatagen-generated default-config lifecycle test valid without inventing a default path.

- [ ] **Step 1: Write the failing tests**

Append to `config_test.go`:

```go
func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, "", cfg.StorageDir)
	assert.Equal(t, 5*time.Minute, cfg.Window)
	assert.Equal(t, 32<<20, cfg.SegmentSize)
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		ok   bool
	}{
		{"defaults", func(*Config) {}, true},
		{"with dir", func(c *Config) { c.StorageDir = t.TempDir() }, true},
		{"zero window", func(c *Config) { c.Window = 0 }, false},
		{"negative window", func(c *Config) { c.Window = -time.Second }, false},
		{"tiny segment", func(c *Config) { c.SegmentSize = 1 << 10 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			tc.mut(cfg)
			if tc.ok {
				assert.NoError(t, cfg.Validate())
			} else {
				assert.Error(t, cfg.Validate())
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test . -run TestConfig -v` — expected: compile error (fields don't exist).

- [ ] **Step 3: Implement**

Replace `Config` in `config.go`:

```go
// Config defines configuration for the retrosampler processor.
type Config struct {
	// StorageDir is the buffer segment directory. Empty disables
	// buffering (passthrough only) — stage-1 shadow mode is opt-in.
	StorageDir string `mapstructure:"storage_dir"`
	// Window is the retention window W (ADR-006).
	Window time.Duration `mapstructure:"window"`
	// SegmentSize is the segment roll threshold in bytes.
	SegmentSize int `mapstructure:"segment_size"`
}

// Validate checks that the configuration is usable.
func (cfg *Config) Validate() error {
	if cfg.Window <= 0 {
		return errors.New("window must be positive")
	}
	if cfg.SegmentSize < 1<<20 {
		return errors.New("segment_size must be at least 1 MiB")
	}
	return nil
}
```

Update `createDefaultConfig` in `factory.go`:

```go
func createDefaultConfig() component.Config {
	return &Config{Window: 5 * time.Minute, SegmentSize: 32 << 20}
}
```

- [ ] **Step 4: Verify green + generated-sync**

Run: `go test . -v && make generate && git diff --exit-code`
Expected: PASS, no generated diff.

- [ ] **Step 5: Commit**

```bash
git add config.go config_test.go factory.go
git commit -m "feat: buffer config fields storage_dir, window, segment_size"
```

---

### Task 2: Proto writer primitives and attribute encoding

**Files:**
- Create: `internal/fragmenter/protoenc.go`
- Create: `internal/fragmenter/protoenc_test.go`

**Interfaces:**
- Produces (package-private, used by Task 3):
  - `type enc struct{ b []byte }` with methods `uvarint(uint64)`, `key(field, wire uint64)`, `str(field uint64, s string)`, `bytesF(field uint64, p []byte)`, `varintF(field, v uint64)`, `fixed64F(field, v uint64)`, `fixed32F(field uint64, v uint32)`, `msg(field uint64, size int)`
  - `sizeUvarint(v uint64) int`, `sizeKey(field uint64) int`, `sizeLen(field uint64, n int) int`
  - `sizeValue(v pcommon.Value) int`, `putValue(e *enc, v pcommon.Value)`
  - `sizeAttrs(field uint64, m pcommon.Map) int`, `putAttrs(e *enc, field uint64, m pcommon.Map)`
- Wire types: varint=0, fixed64=1, len-delimited=2, fixed32=5. AnyValue fields: string=1, bool=2, int=3, double=4, array=5, kvlist=6, bytes=7; KeyValue: key=1, value=2.

- [ ] **Step 1: Write the failing tests**

`protoenc_test.go` — hand-computed wire bytes for primitives, and a pdata-round-trip for attributes. For the round-trip, wrap the encoded attributes in a minimal valid `TracesData` (one ResourceSpans whose Resource carries the attrs) and decode with `ptrace.ProtoUnmarshaler`:

```go
package fragmenter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestEncPrimitives(t *testing.T) {
	var e enc
	e.uvarint(300)
	assert.Equal(t, []byte{0xAC, 0x02}, e.b)

	e.b = e.b[:0]
	e.str(5, "ab") // key 5<<3|2 = 0x2A, len 2
	assert.Equal(t, []byte{0x2A, 0x02, 'a', 'b'}, e.b)

	e.b = e.b[:0]
	e.str(5, "") // proto3 default: skipped
	assert.Empty(t, e.b)

	e.b = e.b[:0]
	e.fixed32F(16, 1) // key 16<<3|5 = 133 → varint {0x85, 0x01}
	assert.Equal(t, []byte{0x85, 0x01, 1, 0, 0, 0}, e.b)

	assert.Equal(t, 2, sizeUvarint(300))
	assert.Equal(t, 1, sizeKey(15))
	assert.Equal(t, 2, sizeKey(16))
	assert.Equal(t, 1+1+3, sizeLen(1, 3))
}

// encodes attrs as the Resource of a one-span TracesData and round-trips.
func attrsRoundTrip(t *testing.T, fill func(pcommon.Map)) pcommon.Map {
	t.Helper()
	m := pcommon.NewMap()
	fill(m)
	var e enc
	body := sizeAttrs(1, m) // Resource.attributes = field 1
	// ResourceSpans{ resource=1{ attrs } }
	e.msg(1, sizeLen(1, body)) // TracesData.resource_spans
	e.msg(1, body)             // ResourceSpans.resource
	putAttrs(&e, 1, m)

	td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	require.Equal(t, 1, td.ResourceSpans().Len())
	return td.ResourceSpans().At(0).Resource().Attributes()
}

func TestAttrEncodingRoundTrip(t *testing.T) {
	got := attrsRoundTrip(t, func(m pcommon.Map) {
		m.PutStr("s", "v")
		m.PutStr("empty", "") // oneof: empty string still round-trips as Str
		m.PutInt("i", -3)
		m.PutInt("zero", 0)
		m.PutBool("b", false)
		m.PutDouble("d", 1.5)
		m.PutEmptyBytes("raw").FromRaw([]byte{1, 2})
		sl := m.PutEmptySlice("arr")
		sl.AppendEmpty().SetStr("x")
		sl.AppendEmpty().SetInt(7)
		mm := m.PutEmptyMap("nested")
		mm.PutStr("k", "v")
	})
	got2 := got.AsRaw()
	assert.Equal(t, "v", got2["s"])
	assert.Equal(t, "", got2["empty"])
	assert.Equal(t, int64(-3), got2["i"])
	assert.Equal(t, int64(0), got2["zero"])
	assert.Equal(t, false, got2["b"])
	assert.Equal(t, 1.5, got2["d"])
	assert.Equal(t, []byte{1, 2}, got2["raw"])
	assert.Equal(t, []any{"x", int64(7)}, got2["arr"])
	assert.Equal(t, map[string]any{"k": "v"}, got2["nested"])
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/fragmenter -v` — expected: compile error (package doesn't exist).

- [ ] **Step 3: Implement `protoenc.go`**

```go
package fragmenter

import (
	"encoding/binary"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Minimal protobuf wire-format writer. Two-pass: size*, then put* into a
// caller-owned enc. All encoders append; nothing allocates once e.b has
// reached its high-water capacity.

const (
	wVarint  = 0
	wFixed64 = 1
	wBytes   = 2
	wFixed32 = 5
)

type enc struct{ b []byte }

func (e *enc) uvarint(v uint64) {
	for v >= 0x80 {
		e.b = append(e.b, byte(v)|0x80)
		v >>= 7
	}
	e.b = append(e.b, byte(v))
}

func (e *enc) key(field, wire uint64) { e.uvarint(field<<3 | wire) }

func (e *enc) str(field uint64, s string) {
	if s == "" {
		return
	}
	e.key(field, wBytes)
	e.uvarint(uint64(len(s)))
	e.b = append(e.b, s...)
}

func (e *enc) bytesF(field uint64, p []byte) {
	if len(p) == 0 {
		return
	}
	e.key(field, wBytes)
	e.uvarint(uint64(len(p)))
	e.b = append(e.b, p...)
}

func (e *enc) varintF(field, v uint64) {
	if v == 0 {
		return
	}
	e.key(field, wVarint)
	e.uvarint(v)
}

func (e *enc) fixed64F(field, v uint64) {
	if v == 0 {
		return
	}
	e.key(field, wFixed64)
	e.b = binary.LittleEndian.AppendUint64(e.b, v)
}

func (e *enc) fixed32F(field uint64, v uint32) {
	if v == 0 {
		return
	}
	e.key(field, wFixed32)
	e.b = binary.LittleEndian.AppendUint32(e.b, v)
}

// msg writes the key and length prefix for a nested message of known size.
func (e *enc) msg(field uint64, size int) {
	e.key(field, wBytes)
	e.uvarint(uint64(size))
}

func sizeUvarint(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func sizeKey(field uint64) int { return sizeUvarint(field << 3) }

// sizeLen is key + length prefix + n payload bytes.
func sizeLen(field uint64, n int) int {
	return sizeKey(field) + sizeUvarint(uint64(n)) + n
}

func sizeVarintF(field, v uint64) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + sizeUvarint(v)
}

func sizeFixed64F(field, v uint64) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + 8
}

func sizeFixed32F(field uint64, v uint32) int {
	if v == 0 {
		return 0
	}
	return sizeKey(field) + 4
}

func sizeStr(field uint64, s string) int {
	if s == "" {
		return 0
	}
	return sizeLen(field, len(s))
}

// AnyValue. Oneof: the set variant is encoded even at its zero value.
func sizeValue(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return sizeLen(1, len(v.Str()))
	case pcommon.ValueTypeBool:
		return sizeKey(2) + 1
	case pcommon.ValueTypeInt:
		return sizeKey(3) + sizeUvarint(uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		return sizeKey(4) + 8
	case pcommon.ValueTypeSlice:
		n := 0
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			n += sizeLen(1, sizeValue(sl.At(i)))
		}
		return sizeLen(5, n)
	case pcommon.ValueTypeMap:
		n := 0
		v.Map().Range(func(k string, mv pcommon.Value) bool {
			n += sizeLen(1, sizeKeyValue(k, mv))
			return true
		})
		return sizeLen(6, n)
	case pcommon.ValueTypeBytes:
		return sizeLen(7, v.Bytes().Len())
	default: // Empty
		return 0
	}
}

func putValue(e *enc, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		s := v.Str()
		e.key(1, wBytes)
		e.uvarint(uint64(len(s)))
		e.b = append(e.b, s...)
	case pcommon.ValueTypeBool:
		e.key(2, wVarint)
		if v.Bool() {
			e.b = append(e.b, 1)
		} else {
			e.b = append(e.b, 0)
		}
	case pcommon.ValueTypeInt:
		e.key(3, wVarint)
		e.uvarint(uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		e.key(4, wFixed64)
		e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(v.Double()))
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		n := 0
		for i := 0; i < sl.Len(); i++ {
			n += sizeLen(1, sizeValue(sl.At(i)))
		}
		e.msg(5, n)
		for i := 0; i < sl.Len(); i++ {
			e.msg(1, sizeValue(sl.At(i)))
			putValue(e, sl.At(i))
		}
	case pcommon.ValueTypeMap:
		m := v.Map()
		n := 0
		m.Range(func(k string, mv pcommon.Value) bool {
			n += sizeLen(1, sizeKeyValue(k, mv))
			return true
		})
		e.msg(6, n)
		m.Range(func(k string, mv pcommon.Value) bool {
			e.msg(1, sizeKeyValue(k, mv))
			putKeyValue(e, k, mv)
			return true
		})
	case pcommon.ValueTypeBytes:
		e.bytesF(7, v.Bytes().AsRaw())
	}
}

func sizeKeyValue(k string, v pcommon.Value) int {
	return sizeStr(1, k) + sizeLen(2, sizeValue(v))
}

func putKeyValue(e *enc, k string, v pcommon.Value) {
	e.str(1, k)
	e.msg(2, sizeValue(v))
	putValue(e, v)
}

func sizeAttrs(field uint64, m pcommon.Map) int {
	n := 0
	m.Range(func(k string, v pcommon.Value) bool {
		n += sizeLen(field, sizeKeyValue(k, v))
		return true
	})
	return n
}

func putAttrs(e *enc, field uint64, m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		e.msg(field, sizeKeyValue(k, v))
		putKeyValue(e, k, v)
		return true
	})
}

```

(Imports: `encoding/binary`, `math`, pdata `pcommon`.) Note `v.Bytes().AsRaw()` allocates a copy in some pdata versions — if it does (check `AllocsPerRun` in Task 4), iterate `v.Bytes().At(i)` appending bytes instead.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/fragmenter -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/fragmenter
git commit -m "feat: proto writer primitives and attribute encoding"
```

---

### Task 3: OTLP span and group encoder

**Files:**
- Create: `internal/fragmenter/spanenc.go`
- Create: `internal/fragmenter/spanenc_test.go`

**Interfaces:**
- Consumes: Task 2's `enc`, `size*`/`put*` helpers.
- Produces (package-private, used by Task 4):
  - `type spanRef struct{ rs, ss, sp, next int32 }`
  - `sizeGroup(td ptrace.Traces, refs []spanRef) int`
  - `putGroup(e *enc, td ptrace.Traces, refs []spanRef)` — encodes a valid `TracesData` (`resource_spans = 1`) containing exactly the referenced spans, with one `ResourceSpans` per contiguous `rs` run and one `ScopeSpans` per contiguous `ss` run inside it.
- OTLP field numbers — ResourceSpans: resource=1, scope_spans=2, schema_url=3. Resource: attributes=1, dropped=2. ScopeSpans: scope=1, spans=2, schema_url=3. Scope: name=1, version=2, attributes=3, dropped=4. Span: trace_id=1, span_id=2, trace_state=3, parent_span_id=4, name=5, kind=6(varint), start=7(fixed64), end=8(fixed64), attributes=9, dropped_attrs=10, events=11, dropped_events=12, links=13, dropped_links=14, status=15, flags=16(fixed32). Event: time=1(fixed64), name=2, attributes=3, dropped=4. Link: trace_id=1, span_id=2, trace_state=3, attributes=4, dropped=5, flags=6(fixed32). Status: message=2, code=3.

- [ ] **Step 1: Write the failing test**

The oracle: build a batch with pdata, take all spans of one trace as refs, encode, unmarshal with `ptrace.ProtoUnmarshaler`, and compare against a pdata-built expectation via JSON (deterministic):

```go
package fragmenter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func tid(b byte) pcommon.TraceID  { return pcommon.TraceID{b, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} }
func sid(b byte) pcommon.SpanID   { return pcommon.SpanID{b, 2, 3, 4, 5, 6, 7, 8} }

func fullSpan(sp ptrace.Span, id byte) {
	sp.SetTraceID(tid(1))
	sp.SetSpanID(sid(id))
	sp.SetParentSpanID(sid(id + 100))
	sp.TraceState().FromRaw("k=v")
	sp.SetName("op")
	sp.SetKind(ptrace.SpanKindServer)
	sp.SetStartTimestamp(pcommon.Timestamp(1_000_000_001))
	sp.SetEndTimestamp(pcommon.Timestamp(2_000_000_002))
	sp.SetFlags(0x101)
	sp.Attributes().PutStr("http.route", "/x")
	sp.Attributes().PutInt("code", 500)
	sp.SetDroppedAttributesCount(3)
	ev := sp.Events().AppendEmpty()
	ev.SetTimestamp(pcommon.Timestamp(1_500_000_000))
	ev.SetName("exception")
	ev.Attributes().PutStr("msg", "boom")
	sp.SetDroppedEventsCount(1)
	lk := sp.Links().AppendEmpty()
	lk.SetTraceID(tid(9))
	lk.SetSpanID(sid(9))
	lk.Attributes().PutBool("sampled", true)
	sp.SetDroppedLinksCount(2)
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.Status().SetMessage("bad")
}

func jsonOf(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	return string(b)
}

func TestGroupEncodeRoundTrip(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc-a")
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("lib")
	ss.Scope().SetVersion("1.2.3")
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.34.0")
	fullSpan(ss.Spans().AppendEmpty(), 1)
	fullSpan(ss.Spans().AppendEmpty(), 2)
	// same trace under a second resource → two ResourceSpans runs
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "svc-b")
	ss2 := rs2.ScopeSpans().AppendEmpty()
	fullSpan(ss2.Spans().AppendEmpty(), 3)

	refs := []spanRef{{rs: 0, ss: 0, sp: 0}, {rs: 0, ss: 0, sp: 1}, {rs: 1, ss: 0, sp: 0}}
	var e enc
	putGroup(&e, td, refs)
	assert.Equal(t, sizeGroup(td, refs), len(e.b), "size pass must match encode pass")

	got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	// expected = the original td (all spans referenced, structure preserved)
	assert.JSONEq(t, jsonOf(t, td), jsonOf(t, got))
}

func TestGroupEncodeSubset(t *testing.T) {
	// two traces interleaved in one scope; encode only trace A's spans
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	a := ss.Spans().AppendEmpty()
	a.SetTraceID(tid(1))
	a.SetName("a")
	b := ss.Spans().AppendEmpty()
	b.SetTraceID(tid(2))
	b.SetName("b")
	a2 := ss.Spans().AppendEmpty()
	a2.SetTraceID(tid(1))
	a2.SetName("a2")

	var e enc
	putGroup(&e, td, []spanRef{{rs: 0, ss: 0, sp: 0}, {rs: 0, ss: 0, sp: 2}})
	got, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(e.b)
	require.NoError(t, err)
	require.Equal(t, 2, got.SpanCount())
	sps := got.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	assert.Equal(t, "a", sps.At(0).Name())
	assert.Equal(t, "a2", sps.At(1).Name())
}
```

Remove the stray `e.msgTop` line when writing the real test — `putGroup` writes top-level `resource_spans` fields directly; the assertion pair is `putGroup` output length vs `sizeGroup`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/fragmenter -run TestGroup -v` — expected: compile error (`spanRef` undefined).

- [ ] **Step 3: Implement `spanenc.go`**

```go
package fragmenter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// spanRef addresses one span in a batch; next chains refs of one trace
// (arena index in Fragmenter, unused by the encoder which gets a flat slice).
type spanRef struct{ rs, ss, sp, next int32 }

func sizeStatus(st ptrace.Status) int {
	return sizeStr(2, st.Message()) + sizeVarintF(3, uint64(st.Code()))
}

func putStatus(e *enc, st ptrace.Status) {
	e.str(2, st.Message())
	e.varintF(3, uint64(st.Code()))
}

func sizeEvent(ev ptrace.SpanEvent) int {
	return sizeFixed64F(1, uint64(ev.Timestamp())) +
		sizeStr(2, ev.Name()) +
		sizeAttrs(3, ev.Attributes()) +
		sizeVarintF(4, uint64(ev.DroppedAttributesCount()))
}

func putEvent(e *enc, ev ptrace.SpanEvent) {
	e.fixed64F(1, uint64(ev.Timestamp()))
	e.str(2, ev.Name())
	putAttrs(e, 3, ev.Attributes())
	e.varintF(4, uint64(ev.DroppedAttributesCount()))
}

func sizeLink(lk ptrace.SpanLink) int {
	ltid, lsid := lk.TraceID(), lk.SpanID()
	n := 0
	if !ltid.IsEmpty() {
		n += sizeLen(1, 16)
	}
	if !lsid.IsEmpty() {
		n += sizeLen(2, 8)
	}
	return n + sizeStr(3, lk.TraceState().AsRaw()) +
		sizeAttrs(4, lk.Attributes()) +
		sizeVarintF(5, uint64(lk.DroppedAttributesCount())) +
		sizeFixed32F(6, lk.Flags())
}

func putLink(e *enc, lk ptrace.SpanLink) {
	if ltid := lk.TraceID(); !ltid.IsEmpty() {
		e.bytesF(1, ltid[:])
	}
	if lsid := lk.SpanID(); !lsid.IsEmpty() {
		e.bytesF(2, lsid[:])
	}
	e.str(3, lk.TraceState().AsRaw())
	putAttrs(e, 4, lk.Attributes())
	e.varintF(5, uint64(lk.DroppedAttributesCount()))
	e.fixed32F(6, lk.Flags())
}

func sizeSpan(sp ptrace.Span) int {
	stid, ssid, spid := sp.TraceID(), sp.SpanID(), sp.ParentSpanID()
	n := 0
	if !stid.IsEmpty() {
		n += sizeLen(1, 16)
	}
	if !ssid.IsEmpty() {
		n += sizeLen(2, 8)
	}
	n += sizeStr(3, sp.TraceState().AsRaw())
	if !spid.IsEmpty() {
		n += sizeLen(4, 8)
	}
	n += sizeStr(5, sp.Name()) +
		sizeVarintF(6, uint64(sp.Kind())) +
		sizeFixed64F(7, uint64(sp.StartTimestamp())) +
		sizeFixed64F(8, uint64(sp.EndTimestamp())) +
		sizeAttrs(9, sp.Attributes()) +
		sizeVarintF(10, uint64(sp.DroppedAttributesCount()))
	for i := 0; i < sp.Events().Len(); i++ {
		n += sizeLen(11, sizeEvent(sp.Events().At(i)))
	}
	n += sizeVarintF(12, uint64(sp.DroppedEventsCount()))
	for i := 0; i < sp.Links().Len(); i++ {
		n += sizeLen(13, sizeLink(sp.Links().At(i)))
	}
	n += sizeVarintF(14, uint64(sp.DroppedLinksCount()))
	if s := sizeStatus(sp.Status()); s > 0 {
		n += sizeLen(15, s)
	}
	n += sizeFixed32F(16, sp.Flags())
	return n
}

func putSpan(e *enc, sp ptrace.Span) {
	if stid := sp.TraceID(); !stid.IsEmpty() {
		e.bytesF(1, stid[:])
	}
	if ssid := sp.SpanID(); !ssid.IsEmpty() {
		e.bytesF(2, ssid[:])
	}
	e.str(3, sp.TraceState().AsRaw())
	if spid := sp.ParentSpanID(); !spid.IsEmpty() {
		e.bytesF(4, spid[:])
	}
	e.str(5, sp.Name())
	e.varintF(6, uint64(sp.Kind()))
	e.fixed64F(7, uint64(sp.StartTimestamp()))
	e.fixed64F(8, uint64(sp.EndTimestamp()))
	putAttrs(e, 9, sp.Attributes())
	e.varintF(10, uint64(sp.DroppedAttributesCount()))
	for i := 0; i < sp.Events().Len(); i++ {
		e.msg(11, sizeEvent(sp.Events().At(i)))
		putEvent(e, sp.Events().At(i))
	}
	e.varintF(12, uint64(sp.DroppedEventsCount()))
	for i := 0; i < sp.Links().Len(); i++ {
		e.msg(13, sizeLink(sp.Links().At(i)))
		putLink(e, sp.Links().At(i))
	}
	e.varintF(14, uint64(sp.DroppedLinksCount()))
	if s := sizeStatus(sp.Status()); s > 0 {
		e.msg(15, s)
		putStatus(e, sp.Status())
	}
	e.fixed32F(16, sp.Flags())
}

func sizeResource(res pcommon.Resource) int {
	return sizeAttrs(1, res.Attributes()) +
		sizeVarintF(2, uint64(res.DroppedAttributesCount()))
}

func putResource(e *enc, res pcommon.Resource) {
	putAttrs(e, 1, res.Attributes())
	e.varintF(2, uint64(res.DroppedAttributesCount()))
}

func sizeScope(sc pcommon.InstrumentationScope) int {
	return sizeStr(1, sc.Name()) + sizeStr(2, sc.Version()) +
		sizeAttrs(3, sc.Attributes()) +
		sizeVarintF(4, uint64(sc.DroppedAttributesCount()))
}

func putScope(e *enc, sc pcommon.InstrumentationScope) {
	e.str(1, sc.Name())
	e.str(2, sc.Version())
	putAttrs(e, 3, sc.Attributes())
	e.varintF(4, uint64(sc.DroppedAttributesCount()))
}

// ssRun is a contiguous run of refs sharing (rs, ss).
func sizeSSRun(ss ptrace.ScopeSpans, refs []spanRef) int {
	n := sizeLen(1, sizeScope(ss.Scope()))
	for _, r := range refs {
		n += sizeLen(2, sizeSpan(ss.Spans().At(int(r.sp))))
	}
	return n + sizeStr(3, ss.SchemaUrl())
}

func sizeRSRun(rs ptrace.ResourceSpans, refs []spanRef) int {
	n := sizeLen(1, sizeResource(rs.Resource()))
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].ss == refs[i].ss {
			j++
		}
		ss := rs.ScopeSpans().At(int(refs[i].ss))
		n += sizeLen(2, sizeSSRun(ss, refs[i:j]))
		i = j
	}
	return n + sizeStr(3, rs.SchemaUrl())
}

// sizeGroup/putGroup encode a TracesData (resource_spans = 1) holding
// exactly the referenced spans. refs must be in batch iteration order.
func sizeGroup(td ptrace.Traces, refs []spanRef) int {
	n := 0
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].rs == refs[i].rs {
			j++
		}
		rs := td.ResourceSpans().At(int(refs[i].rs))
		n += sizeLen(1, sizeRSRun(rs, refs[i:j]))
		i = j
	}
	return n
}

func putGroup(e *enc, td ptrace.Traces, refs []spanRef) {
	for i := 0; i < len(refs); {
		j := i
		for j < len(refs) && refs[j].rs == refs[i].rs {
			j++
		}
		rs := td.ResourceSpans().At(int(refs[i].rs))
		e.msg(1, sizeRSRun(rs, refs[i:j]))
		e.msg(1, sizeResource(rs.Resource()))
		putResource(e, rs.Resource())
		for k := i; k < j; {
			l := k
			for l < j && refs[l].ss == refs[k].ss {
				l++
			}
			ss := rs.ScopeSpans().At(int(refs[k].ss))
			e.msg(2, sizeSSRun(ss, refs[k:l]))
			e.msg(1, sizeScope(ss.Scope()))
			putScope(e, ss.Scope())
			for _, r := range refs[k:l] {
				sp := ss.Spans().At(int(r.sp))
				e.msg(2, sizeSpan(sp))
				putSpan(e, sp)
			}
			e.str(3, ss.SchemaUrl())
			k = l
		}
		e.str(3, rs.SchemaUrl())
		i = j
	}
}
```

API caveats to verify while implementing (fix locally, don't fight): `pcommon.TraceID`/`SpanID` are `[16]byte`/`[8]byte` arrays with `IsEmpty()`; `TraceState().AsRaw()` returns `string`; `lk.Flags()`/`sp.Flags()` return `uint32`. If `AsRaw`/attribute iteration differ in v1.64, adapt — the tests are the contract, not these snippets.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/fragmenter -v` — expected: PASS, including the size==len assertion.

- [ ] **Step 5: Commit**

```bash
git add internal/fragmenter
git commit -m "feat: OTLP span and group proto encoder"
```

---

### Task 4: Fragmenter — grouping and zero-alloc reuse

**Files:**
- Create: `internal/fragmenter/fragmenter.go`
- Create: `internal/fragmenter/fragmenter_test.go`

**Interfaces:**
- Consumes: `sizeGroup`, `putGroup`, `spanRef` from Task 3.
- Produces (the package's public API, consumed by the processor in Task 14 and benchmarks in Task 15):

```go
func New() *Fragmenter
// Fragment groups td's spans by trace ID and invokes fn once per trace
// with the marshaled OTLP fragment. frag is only valid during the call.
func (f *Fragmenter) Fragment(td ptrace.Traces, fn func(id pcommon.TraceID, frag []byte))
```

- [ ] **Step 1: Write the failing tests**

```go
func TestFragmentGroupsByTrace(t *testing.T) {
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for i, id := range []byte{1, 2, 1, 3, 2, 1} {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(tid(id))
		sp.SetName(fmt.Sprintf("s%d", i))
	}
	got := map[pcommon.TraceID]int{}
	f := New()
	f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
		dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
		require.NoError(t, err)
		got[id] = dec.SpanCount()
		for _, sp := range allSpans(dec) { // helper: flatten
			assert.Equal(t, id, sp.TraceID())
		}
	})
	assert.Equal(t, map[pcommon.TraceID]int{tid(1): 3, tid(2): 2, tid(3): 1}, got)
}

func TestFragmentScratchReuseAcrossCalls(t *testing.T) {
	f := New()
	td := testBatch(50, 5) // helper below
	f.Fragment(td, func(pcommon.TraceID, []byte) {})
	f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
		dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
		require.NoError(t, err)
		assert.Positive(t, dec.SpanCount())
	})
}

func TestFragmentZeroAllocSteadyState(t *testing.T) {
	f := New()
	td := testBatch(100, 4)
	for range 100 { // warm every internal high-water mark
		f.Fragment(td, func(pcommon.TraceID, []byte) {})
	}
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, func(pcommon.TraceID, []byte) {})
	})
	assert.Zero(t, avg, "hot-path bookkeeping allocs must be 0 (ADR-004 r2)")
}
```

`testBatch(nTraces, spansEach int) ptrace.Traces` builds a batch with realistic attributes (a few string/int attrs per span, one resource attr) round-robin across 4 resources. Write it in this test file; Task 15's benchmarks reuse it (move to a shared `testutil_test.go` then).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/fragmenter -run TestFragment -v` — expected: compile error (`New` undefined).

- [ ] **Step 3: Implement `fragmenter.go`**

```go
package fragmenter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Fragmenter groups a batch's spans by trace ID and marshals each group.
// Single-threaded; all internal state is reused across calls and only
// grows to a high-water mark (zero steady-state allocations, ADR-004 r2).
type Fragmenter struct {
	groups  map[pcommon.TraceID]int32 // trace → index into heads/tails
	ids     []pcommon.TraceID         // group insertion order
	heads   []int32                   // per group: first ref (arena index)
	tails   []int32                   // per group: last ref
	refs    []spanRef                 // ref arena; refs[i].next chains a trace
	flat    []spanRef                 // per-group scratch, chain flattened
	scratch enc                       // marshal output, reused
}

func New() *Fragmenter {
	return &Fragmenter{groups: make(map[pcommon.TraceID]int32)}
}

func (f *Fragmenter) Fragment(td ptrace.Traces, fn func(id pcommon.TraceID, frag []byte)) {
	clear(f.groups)
	f.ids = f.ids[:0]
	f.heads = f.heads[:0]
	f.tails = f.tails[:0]
	f.refs = f.refs[:0]

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				id := sps.At(k).TraceID()
				r := int32(len(f.refs))
				f.refs = append(f.refs, spanRef{rs: int32(i), ss: int32(j), sp: int32(k), next: -1})
				g, ok := f.groups[id]
				if !ok {
					g = int32(len(f.ids))
					f.groups[id] = g
					f.ids = append(f.ids, id)
					f.heads = append(f.heads, r)
					f.tails = append(f.tails, r)
					continue
				}
				f.refs[f.tails[g]].next = r
				f.tails[g] = r
			}
		}
	}

	for g, id := range f.ids {
		f.flat = f.flat[:0]
		for r := f.heads[g]; r >= 0; r = f.refs[r].next {
			f.flat = append(f.flat, f.refs[r])
		}
		f.scratch.b = f.scratch.b[:0]
		putGroup(&f.scratch, td, f.flat)
		fn(id, f.scratch.b)
	}
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/fragmenter -v` — expected: PASS including the zero-alloc assertion. If `AllocsPerRun` is nonzero, chase it now (`-benchmem` on a temp benchmark, or `go test -run TestFragmentZeroAlloc -memprofile`): usual suspects are `pcommon.Map.Range` closures capturing loop state (hoist them), `Bytes().AsRaw()` (iterate instead), and map growth (warm-up insufficient).

- [ ] **Step 5: Commit**

```bash
git add internal/fragmenter
git commit -m "feat: fragmenter grouping with zero-alloc reuse"
```

---

### Task 5: Segment record format with CRC32C

**Files:**
- Create: `internal/buffer/record.go`
- Create: `internal/buffer/record_test.go`

**Interfaces:**
- Produces (package-private):
  - `const recHeaderLen = 24` — layout `u32 len(frag) | 16B traceID | u32 CRC32C(frag)`, little-endian.
  - `putRecordHeader(dst *[recHeaderLen]byte, id [16]byte, frag []byte)`
  - `parseRecordHeader(h [recHeaderLen]byte) (length uint32, id [16]byte, crc uint32)`
  - `var castagnoli = crc32.MakeTable(crc32.Castagnoli)` — every CRC in the package uses this table.

- [ ] **Step 1: Write the failing test**

```go
func TestRecordHeaderRoundTrip(t *testing.T) {
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	frag := []byte("payload")
	var h [recHeaderLen]byte
	putRecordHeader(&h, id, frag)
	length, gotID, crc := parseRecordHeader(h)
	assert.Equal(t, uint32(len(frag)), length)
	assert.Equal(t, id, gotID)
	assert.Equal(t, crc32.Checksum(frag, castagnoli), crc)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -v` — expected: compile error (package doesn't exist).

- [ ] **Step 3: Implement `record.go`**

```go
// Package buffer implements the retrosampler disk-segment span buffer
// (ADR-006): append-only CRC'd records, footer directories, whole-segment
// expiry, and a compact trace index. Single-writer; no locks (ADR-007).
package buffer

import (
	"encoding/binary"
	"hash/crc32"
)

// Record layout: u32 fragLen | 16B traceID | u32 CRC32C(frag) | frag.
const recHeaderLen = 4 + 16 + 4

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func putRecordHeader(dst *[recHeaderLen]byte, id [16]byte, frag []byte) {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(len(frag)))
	copy(dst[4:20], id[:])
	binary.LittleEndian.PutUint32(dst[20:24], crc32.Checksum(frag, castagnoli))
}

func parseRecordHeader(h [recHeaderLen]byte) (length uint32, id [16]byte, crc uint32) {
	length = binary.LittleEndian.Uint32(h[0:4])
	copy(id[:], h[4:20])
	crc = binary.LittleEndian.Uint32(h[20:24])
	return length, id, crc
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: segment record format with CRC32C"
```

---

### Task 6: Segment writer — roll, footer, trailer, fsync

**Files:**
- Create: `internal/buffer/segment.go`
- Create: `internal/buffer/segment_test.go`

**Interfaces:**
- Consumes: Task 5's record helpers.
- Produces (package-private):

```go
type dirEntry struct { id [16]byte; off, length uint32 }
type segmentWriter struct { /* f *os.File; w *bufio.Writer; gen uint32;
	size int64; tMin, tMax int64; dir []dirEntry; hdr [recHeaderLen]byte */ }

func segPath(dir string, gen uint32) string            // "%s/%09d.seg"
func newSegmentWriter(dir string, gen uint32, reuse []dirEntry) (*segmentWriter, error)
func (s *segmentWriter) append(id [16]byte, frag []byte, now int64) (off uint32, err error)
func (s *segmentWriter) flush() error                  // bufio flush only
// finalize writes the footer directory + 36-byte trailer, fsyncs, closes,
// and returns the still-open-for-read *os.File via reopen plus times.
func (s *segmentWriter) finalize() (dir []dirEntry, err error)

type segMeta struct { gen uint32; tMin, tMax int64; entries []dirEntry }
// readFooter parses a finalized segment. Returns errNoFooter if the
// trailer magic/CRC is absent or invalid (torn segment).
func readFooter(f *os.File) (segMeta, error)
// scanRecords walks records from offset 0, returning entries for every
// CRC-valid prefix record and the offset where validity ends.
func scanRecords(f *os.File) (entries []dirEntry, validEnd int64, err error)
```

- Footer format, after the last record: repeated `dirEntry` (16+4+4 bytes each, LE), then a 36-byte trailer: `u64 footerOff | u32 entryCount | i64 tMin | i64 tMax | u32 footerCRC | u32 magic`. `footerCRC` is CRC32C of the footer entry bytes. `magic = 0x52535347`. `tMin`/`tMax` are the min/max `now` (unix nanos) passed to `append`.
- fsync policy: `finalize` fsyncs; nothing else does (ADR-006 r6 — loss bound is the active segment).

- [ ] **Step 1: Write the failing tests**

```go
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
	defer f.Close()
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
	defer f.Close()
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
	defer f.Close()
	entries, validEnd, err := scanRecords(f)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint32(4), entries[0].length)
	fi, _ := f.Stat()
	assert.Equal(t, fi.Size(), validEnd)
}
```

(Testing `w.f` directly is fine — same package.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -run TestSegment -v` — expected: compile errors.

- [ ] **Step 3: Implement `segment.go`**

Key points the implementation must honor (write the obvious Go for the rest):

```go
const (
	segMagic     = 0x52535347
	trailerLen   = 8 + 4 + 8 + 8 + 4 + 4
	writeBufSize = 256 << 10
)

var errNoFooter = errors.New("segment has no valid footer")
```

- `newSegmentWriter`: `os.OpenFile(segPath(dir,gen), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)`, wrap in `bufio.NewWriterSize(f, writeBufSize)`, `dir: reuse[:0]` (footer directory slice is passed back in by the buffer to reuse capacity across segments).
- `append`: fill `s.hdr` via `putRecordHeader`, write header then frag through `s.w`, push `dirEntry{id, uint32(s.size), uint32(len(frag))}`, advance `s.size`, update `tMin`/`tMax` (tMin initialized to `math.MaxInt64`).
- `finalize`: flush; write each `dirEntry` (reuse `s.hdr` scratch for LE encoding; compute running CRC32C with `crc32.Update`); write trailer; flush; `f.Sync()`; `f.Close()`; return `s.dir` for reuse.
- `readFooter`: stat; if size < trailerLen → `errNoFooter`; `ReadAt` trailer; check magic, `footerOff+entryCount*24+trailerLen == size`, read footer bytes, verify CRC → else `errNoFooter`; parse entries.
- `scanRecords`: `bufio.Reader` from offset 0; loop: read header; if `io.EOF`/short → stop; if `length > 64<<20` (sanity cap) → stop; read frag; verify CRC → append entry else stop. `validEnd` = offset after last valid record. A file that happens to end with a valid-looking trailer is fine — `scanRecords` is only called when `readFooter` already said `errNoFooter`.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: segment writer with roll footer and trailer"
```

---

### Task 7: Compact index — open addressing + loc arena

**Files:**
- Create: `internal/buffer/index.go`
- Create: `internal/buffer/index_test.go`

**Interfaces:**
- Produces (package-private):

```go
type loc struct { gen, off, length uint32; next int32 } // next: arena index, -1 end
type index struct { /* slots []slot; arena []loc; free int32; live, tombs, cursor int */ }
// slot: id [16]byte; head, tail int32. head 0 = empty, -1 = tombstone,
// else arena index+1 (offset by 1 so the zero value means empty).

func newIndex() *index
func (x *index) put(id [16]byte, l loc)              // appends to the trace's chain
func (x *index) head(id [16]byte) int32              // arena index or -1
func (x *index) at(i int32) loc                      // arena accessor
func (x *index) live() int                           // live trace count
// sweep examines up to n slots from the rotating cursor; a trace whose
// newest loc (tail) has gen < minGen is dead: tombstone it, free its chain.
func (x *index) sweep(n int, minGen uint32)
func (x *index) memoryBytes() int64                  // cap(slots)*sizeofSlot + cap(arena)*sizeofLoc
```

- Probing: linear, power-of-two capacity, full 16-byte key compare (false trace merge = correctness bug, ADR-006 r5). Grow/rehash when `(live+tombs)*4 >= cap*3`; rehash drops tombstones. Freed locs chain onto `free` via their `next` field.
- Budget context: slot is 24 B → at 0.75 max load and power-of-2 rounding, worst case ≈64 B/trace amortized table + 16 B/fragment. 150 B budget holds at the pinned 5-fragment workload (Task 11).

- [ ] **Step 1: Write the failing tests**

```go
func TestIndexPutGetChains(t *testing.T) {
	x := newIndex()
	a, b := [16]byte{1}, [16]byte{2}
	x.put(a, loc{gen: 1, off: 0, length: 10})
	x.put(b, loc{gen: 1, off: 34, length: 5})
	x.put(a, loc{gen: 2, off: 0, length: 7})

	var got []loc
	for i := x.head(a); i >= 0; i = x.at(i).next {
		l := x.at(i)
		l.next = 0
		got = append(got, l)
	}
	assert.Equal(t, []loc{{gen: 1, off: 0, length: 10}, {gen: 2, off: 0, length: 7}}, got)
	assert.Equal(t, int32(-1), x.head([16]byte{9}))
	assert.Equal(t, 2, x.live())
}

func TestIndexGrowKeepsAllEntries(t *testing.T) {
	x := newIndex()
	for i := range 10_000 {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[:8], uint64(i))
		x.put(id, loc{gen: 1, off: uint32(i), length: 1})
	}
	assert.Equal(t, 10_000, x.live())
	for i := range 10_000 {
		var id [16]byte
		binary.LittleEndian.PutUint64(id[:8], uint64(i))
		h := x.head(id)
		require.GreaterOrEqual(t, h, int32(0))
		assert.Equal(t, uint32(i), x.at(h).off)
	}
}

func TestIndexSweepFreesDeadTraces(t *testing.T) {
	x := newIndex()
	old, live := [16]byte{1}, [16]byte{2}
	x.put(old, loc{gen: 1, off: 0, length: 1})
	x.put(live, loc{gen: 1, off: 24, length: 1})
	x.put(live, loc{gen: 5, off: 0, length: 1})

	for range 64 { // cursor is rotating; sweep everything
		x.sweep(1024, 3)
	}
	assert.Equal(t, int32(-1), x.head(old), "all locs gen<3: dead")
	require.GreaterOrEqual(t, x.head(live), int32(0), "tail gen 5: alive")
	assert.Equal(t, 1, x.live())

	// freed arena slots are reused, not appended
	before := cap(x.arena)
	x.put([16]byte{7}, loc{gen: 6, off: 0, length: 1})
	assert.Equal(t, before, cap(x.arena))
}

func TestIndexReuseAfterSweepThenReinsert(t *testing.T) {
	x := newIndex()
	id := [16]byte{1}
	x.put(id, loc{gen: 1, off: 0, length: 1})
	for range 64 {
		x.sweep(1024, 2)
	}
	require.Equal(t, int32(-1), x.head(id))
	x.put(id, loc{gen: 3, off: 9, length: 1}) // tombstone slot must be reusable
	h := x.head(id)
	require.GreaterOrEqual(t, h, int32(0))
	assert.Equal(t, uint32(9), x.at(h).off)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -run TestIndex -v` — expected: compile errors.

- [ ] **Step 3: Implement `index.go`**

Implementation notes beyond the interface block (write the standard open-addressing code around them):

- `slots` starts at 1024. Hash: `binary.LittleEndian.Uint64(id[:8]) * 0x9E3779B97F4A7C15`, masked to capacity (trace IDs are generated random; if a test needs adversarial distribution it can construct colliding low bytes — linear probing handles it).
- Probe loop: empty slot ends the search; tombstone remembers first reusable slot; key match appends to chain (`arena[tail].next = new; tail = new`).
- Arena alloc: pop `free` list else `append`. Arena index stored in slot as `i+1` (0 = empty sentinel); `head()` translates back. `at(i)` indexes `arena[i]` directly with real indices; only slot head/tail carry the +1 offset — keep the translation in one place.
- `sweep(n, minGen)`: examine `n` slots starting at `cursor`, wrapping; `cursor` advances by `n` each call. Dead check is **tail only** — chains are appended in gen order, so the tail is the newest loc.
- Rehash when `(live+tombs)*4 >= 3*len(slots)`: new size = smallest power of two with `live*4 < 3*size`, re-insert live slots only (chains keep their arena indices — only the table is rebuilt).
- `memoryBytes()`: `int64(cap(x.slots))*24 + int64(cap(x.arena))*16`. Use `unsafe.Sizeof` in a compile-time assertion test if you want to pin struct sizes; the constants 24/16 are the budget-relevant truth (fields are ordered to pack: `[16]byte` + two `int32`; three `uint32` + `int32`).

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: compact trace index with loc arena and sweep"
```

---

### Task 8: Buffer — Open (fresh dir), Append, Collect

**Files:**
- Create: `internal/buffer/buffer.go`
- Create: `internal/buffer/buffer_test.go`
- Create: `internal/buffer/export_test.go`

**Interfaces:**
- Consumes: Tasks 5–7.
- Produces (public API of the package — the processor and later stages code against exactly this):

```go
type Options struct {
	Window      time.Duration // W; required > 0
	SegmentSize int           // roll threshold; default 32<<20 if 0
	SweepChunk  int           // index slots swept per Expire; default 4096 if 0
}

// Open creates or recovers a buffer in dir. now anchors recovered
// segments' retention clock. Single-goroutine use only (ADR-007).
func Open(dir string, opts Options, now time.Time) (*Buffer, error)
func (b *Buffer) Append(id [16]byte, frag []byte, now time.Time) error
// Collect visits every live fragment of id in append order. frag is only
// valid during the call. Unknown id: no visits, nil error.
func (b *Buffer) Collect(id [16]byte, visit func(frag []byte)) error
func (b *Buffer) Expire(now time.Time)   // Task 9
func (b *Buffer) Close() error           // flush + fsync, no footer
```

- `export_test.go` exposes for tests only: `func (b *Buffer) ActiveGen() uint32`, `func (b *Buffer) MinLiveGen() uint32`, `func (b *Buffer) IndexMemoryBytes() int64`, `func (b *Buffer) LiveTraces() int`.
- Internal state: `idx *index`, `w *segmentWriter`, `readers map[uint32]*os.File` (finalized segments, open for ReadAt), `metas map[uint32]segMeta` times only (entries are folded into idx then dropped), `minGen uint32`, `readBuf []byte` (reused, grows to high-water), `dirScratch []dirEntry` (recycled through segmentWriters).

- [ ] **Step 1: Write the failing tests**

```go
func TestAppendCollectRoundTrip(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
	id := [16]byte{1}
	require.NoError(t, b.Append(id, []byte("frag-1"), time.Unix(1, 0)))
	require.NoError(t, b.Append([16]byte{2}, []byte("other"), time.Unix(1, 0)))
	require.NoError(t, b.Append(id, []byte("frag-2"), time.Unix(2, 0)))

	var got []string
	require.NoError(t, b.Collect(id, func(f []byte) { got = append(got, string(f)) }))
	assert.Equal(t, []string{"frag-1", "frag-2"}, got, "active-segment reads must flush the write buffer")
}

func TestCollectUnknownTrace(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute}, time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
	require.NoError(t, b.Collect([16]byte{9}, func([]byte) { t.Fatal("no visits expected") }))
}

func TestAppendRollsSegments(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: time.Minute, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
	frag := bytes.Repeat([]byte("x"), 64<<10)
	id := [16]byte{1}
	for i := range 40 { // ~2.5 MiB → at least 2 rolls
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
	path := segPath(dir, 1)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[recHeaderLen] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	b2, err := Open(dir, Options{Window: time.Minute}, time.Unix(2, 0))
	require.NoError(t, err)
	defer b2.Close()
	require.NoError(t, b2.Collect(id, func([]byte) { t.Fatal("corrupt fragment must not be visited") }))
}
```

(The last test also pins recovery behavior loosely — full recovery lands in Task 10; for now `Open` on a nonempty dir may take the simplest correct path: scan-truncate the footerless tail, Task 10 hardens it. If sequencing bites, mark it `t.Skip("until recovery task")` and unskip in Task 10 — but only if it genuinely can't pass yet.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -run 'TestAppend|TestCollect' -v` — expected: compile errors.

- [ ] **Step 3: Implement `buffer.go`**

Structure (fill in with straightforward Go):

- `Open`: `os.MkdirAll(dir, 0o755)`; list `*.seg`; **fresh-dir path now** (empty listing): `minGen = 1`, start `segmentWriter` at gen 1. Nonempty dir → recovery, Task 10 (a first cut: footer-valid segments → readers + fold entries into idx in gen order; last footerless → `scanRecords`, truncate to `validEnd` via `os.Truncate`, reopen `O_WRONLY|O_APPEND` as active writer continuing that gen, entries into idx and `w.dir`, `tMin=tMax=now`).
- `Append`: if `w.size + recHeaderLen + len(frag) > SegmentSize` and `w.size > 0` → roll: `entries, err := w.finalize()` (returns dir slice for reuse), open the finalized file read-only into `readers[gen]`, record `metas[gen]` times, `newSegmentWriter(dir, gen+1, entries)`. Then `off, err := w.append(...)`; `idx.put(id, loc{gen: w.gen, off: off, length: uint32(len(frag))})`.
- `Collect`: `flushed := false`; walk chain from `idx.head(id)`: skip `l.gen < b.minGen`; if `l.gen == w.gen` and `!flushed` → `w.flush()`, `flushed = true`; pick reader (`readers[l.gen]` or `w.f` — active writer file needs a separate read handle: open the active segment path `O_RDONLY` lazily once per roll and keep as `activeReader`); grow `readBuf` to `recHeaderLen + length`, `ReadAt`, verify header CRC against payload — mismatch: skip the fragment (corruption is a skip, not an error: partial trace, counted later in stage 2 telemetry), else `visit(readBuf[recHeaderLen : recHeaderLen+length])`.
- `Close`: `w.flush()`, `w.f.Sync()`, close all files. No footer — recovery rescans (spec).

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: buffer open append collect with active-segment reads"
```

---

### Task 9: Expire — whole-segment deletes, stale-active roll, sweep

**Files:**
- Modify: `internal/buffer/buffer.go`
- Create: `internal/buffer/expire_test.go`

**Interfaces:**
- Consumes: everything so far.
- Produces: working `func (b *Buffer) Expire(now time.Time)`:
  1. finalized segments with `tMax < now − Window`, ascending gen: close reader, `os.Remove`, delete meta, `minGen = gen+1`;
  2. stale active segment (`w.size > 0 && w.tMax < (now − Window).UnixNano()`): roll it (finalize → readers) so rule 1 catches it next call;
  3. `idx.sweep(SweepChunk, minGen)`.

- [ ] **Step 1: Write the failing tests**

```go
func TestExpireDeletesWholeSegments(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, Options{Window: 10 * time.Second, SegmentSize: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
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
	defer b.Close()
	require.NoError(t, b.Append([16]byte{1}, []byte("old"), time.Unix(1, 0)))
	b.Expire(time.Unix(30, 0)) // rolls stale active
	b.Expire(time.Unix(30, 0)) // deletes it
	require.NoError(t, b.Collect([16]byte{1}, func([]byte) { t.Fatal("expired") }))
	assert.Greater(t, b.MinLiveGen(), uint32(1))
}

func TestExpireSweepsIndex(t *testing.T) {
	b, err := Open(t.TempDir(), Options{Window: 10 * time.Second, SweepChunk: 1 << 20}, time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
	for i := range 100 {
		var id [16]byte
		id[0] = byte(i)
		id[1] = 1
		require.NoError(t, b.Append(id, []byte("x"), time.Unix(1, 0)))
	}
	require.Equal(t, 100, b.LiveTraces())
	b.Expire(time.Unix(30, 0)) // roll stale active
	b.Expire(time.Unix(30, 0)) // delete + sweep (chunk covers whole table)
	assert.Zero(t, b.LiveTraces(), "dead traces reclaimed from index")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -run TestExpire -v` — expected: FAIL (Expire is a stub or missing).

- [ ] **Step 3: Implement** per the interface block. Note `metas` must be iterated in ascending gen order — keep a sorted `finalized []uint32` slice alongside, or find min repeatedly (segment count is small: window/segment-fill-time).

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: whole-segment expiry with stale-active roll and index sweep"
```

---

### Task 10: Recovery — footer rebuild + torn-tail truncation

**Files:**
- Modify: `internal/buffer/buffer.go` (harden `Open`'s nonempty-dir path)
- Create: `internal/buffer/recovery_test.go`

**Interfaces:**
- Consumes: `readFooter`, `scanRecords` (Task 6).
- Produces: `Open` on a nonempty dir: footer-valid segments become readers with entries folded into the index in ascending-gen order; a footerless **last** segment is scan-truncated to the last valid CRC and reopened as the active writer (same gen, `tMin=tMax=now.UnixNano()`, footer directory refilled from scanned entries); a footerless **non-last** segment is a corruption error (`Open` fails — operator intervention beats silent data invention).

- [ ] **Step 1: Write the failing tests**

```go
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
	defer b2.Close()
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
		require.NoError(t, b.Append(id, []byte(fmt.Sprintf("frag-%d-padding", i)), time.Unix(int64(i), 0)))
	}
	require.NoError(t, b.Close())
	raw, err := os.ReadFile(segPath(ref, 1))
	require.NoError(t, err)
	recLen := recHeaderLen + len("frag-0-padding")

	for cut := len(raw); cut >= 0; cut-- {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(segPath(dir, 1), raw[:cut], 0o644))
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
```

The `cut/recLen` arithmetic works because all 3 fragments have equal length — keep them equal.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/buffer -run 'TestReopen|TestTorn|TestFooterless' -v` — expected: FAIL (some paths may pass from Task 8's first cut; the boundary sweep and non-last check will not).

- [ ] **Step 3: Implement** the hardened `Open`. Also unskip `TestCollectVerifiesCRC` if it was skipped in Task 8.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/buffer -v` — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/buffer
git commit -m "feat: restart recovery with torn-tail truncation"
```

---

### Task 11: Gate — index memory budget (≤150 B/live-trace)

**Files:**
- Create: `internal/buffer/budget_test.go`

**Interfaces:** consumes `IndexMemoryBytes`/`LiveTraces` from `export_test.go`.

- [ ] **Step 1: Write the test (it IS the deliverable — if it fails, fix the index, not the test)**

```go
// ADR-006 r5: index ≤150 B per live trace at the pinned workload
// (5 fragments/trace — matches the ADR's ~4.2 KB/trace, ~424 B/span,
// multi-span batches). Budget holds across expiry windows (steady state,
// not first-window luck).
func TestIndexBudgetPerLiveTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("workload test")
	}
	const (
		tracesPerWindow = 100_000
		fragsPerTrace   = 5
		budget          = 150
	)
	b, err := Open(t.TempDir(),
		Options{Window: 100 * time.Second, SegmentSize: 8 << 20, SweepChunk: 1 << 20},
		time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()

	frag := bytes.Repeat([]byte("f"), 64) // payload size irrelevant to index budget
	now := int64(0)
	for window := range 3 {
		for i := range tracesPerWindow {
			var id [16]byte
			binary.LittleEndian.PutUint64(id[:8], uint64(window*tracesPerWindow+i))
			binary.LittleEndian.PutUint64(id[8:], uint64(i)*0x9E37)
			ts := time.Unix(now+int64(i*100/tracesPerWindow), 0)
			for range fragsPerTrace {
				require.NoError(t, b.Append(id, frag, ts))
			}
			if i%1000 == 0 {
				b.Expire(ts)
			}
		}
		now += 100
		b.Expire(time.Unix(now, 0))
	}
	// settle: sweep the whole table
	for range 64 {
		b.Expire(time.Unix(now, 0))
	}
	live := b.LiveTraces()
	require.Positive(t, live)
	perTrace := float64(b.IndexMemoryBytes()) / float64(live)
	t.Logf("index: %d B / %d live traces = %.1f B/trace", b.IndexMemoryBytes(), live, perTrace)
	assert.LessOrEqual(t, perTrace, float64(budget), "ADR-006 r5 index budget")
}
```

- [ ] **Step 2: Run it**

Run: `go test ./internal/buffer -run TestIndexBudget -v`
Expected: PASS if Task 7's layout holds; if it fails, the failure log shows the real B/trace — fix the index layout (slot packing, growth policy, arena free-list leaks) until green. Do not touch the budget constant (ADR-004: budgets loosen only by superseding the ADR).

- [ ] **Step 3: Commit**

```bash
git add internal/buffer/budget_test.go
git commit -m "test: index memory budget gate at 150B per live trace"
```

---

### Task 12: Gate — span conservation property

**Files:**
- Create: `internal/buffer/conservation_test.go`

**Interfaces:** consumes `ActiveGen`/`MinLiveGen` from `export_test.go`.

- [ ] **Step 1: Write the property test**

```go
// Spec: every appended fragment is either collectable or was in a whole
// expired segment — no third fate. Deterministic seed; model tracks the
// gen each fragment landed in.
func TestConservationProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	b, err := Open(t.TempDir(),
		Options{Window: 50 * time.Second, SegmentSize: 1 << 20, SweepChunk: 1 << 20},
		time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()

	type frag struct {
		payload []byte
		gen     uint32
	}
	model := map[[16]byte][]frag{}
	now := int64(0)
	for op := range 20_000 {
		_ = op
		switch rng.Intn(10) {
		case 9:
			now += rng.Int63n(5)
			b.Expire(time.Unix(now, 0))
		default:
			var id [16]byte
			binary.LittleEndian.PutUint64(id[:8], uint64(rng.Intn(500)))
			p := bytes.Repeat([]byte{byte(rng.Intn(256))}, 100+rng.Intn(2000))
			require.NoError(t, b.Append(id, p, time.Unix(now, 0)))
			model[id] = append(model[id], frag{payload: p, gen: b.ActiveGen()})
		}
	}

	minGen := b.MinLiveGen()
	for id, want := range model {
		var expected [][]byte
		for _, f := range want {
			if f.gen >= minGen {
				expected = append(expected, f.payload)
			}
		}
		var got [][]byte
		require.NoError(t, b.Collect(id, func(p []byte) {
			got = append(got, append([]byte(nil), p...))
		}))
		require.Equal(t, len(expected), len(got), "trace %x", id[:8])
		for i := range expected {
			assert.True(t, bytes.Equal(expected[i], got[i]), "trace %x frag %d", id[:8], i)
		}
	}
}
```

Subtlety: `ActiveGen()` must be read **after** `Append` (the append itself may roll). That's what the model records.

- [ ] **Step 2: Run it**

Run: `go test ./internal/buffer -run TestConservation -v` — expected: PASS; any failure is a real buffer bug (most likely: roll bookkeeping attributing a fragment to the pre-roll gen, or sweep killing a live chain).

- [ ] **Step 3: Commit**

```bash
git add internal/buffer/conservation_test.go
git commit -m "test: span conservation property for buffer"
```

---

### Task 13: Gate — hot-path zero allocations (fragment → append)

**Files:**
- Create: `internal/buffer/alloc_test.go`

**Interfaces:** consumes `fragmenter.New`/`Fragment` and `Buffer.Append` — the exact stage-1 hot path (ADR-004 r1: shard routing joins in stage 2).

- [ ] **Step 1: Write the gate**

```go
func TestHotPathZeroAllocs(t *testing.T) {
	b, err := Open(t.TempDir(),
		Options{Window: time.Hour, SegmentSize: 1 << 30}, // no rolls during measurement
		time.Unix(0, 0))
	require.NoError(t, err)
	defer b.Close()
	f := fragmenter.New()
	td := allocTestBatch() // 100 spans, 25 traces, realistic attrs — mirror fragmenter.testBatch
	now := time.Unix(1, 0)

	for range 200 { // warm all high-water marks incl. index growth
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			require.NoError(t, b.Append(id, frag, now))
		})
	}
	avg := testing.AllocsPerRun(100, func() {
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = b.Append(id, frag, now)
		})
	})
	assert.Zero(t, avg, "ADR-004 r2: 0 bookkeeping allocs/span on the hot path")
}
```

`allocTestBatch` is written here (buffer package can import fragmenter; the reverse would cycle). Note the callback closes over `b` and `now` — declare it once outside `AllocsPerRun`'s closure if closure allocation shows up:

```go
	sink := func(id pcommon.TraceID, frag []byte) { _ = b.Append(id, frag, now) }
	avg := testing.AllocsPerRun(100, func() { f.Fragment(td, sink) })
```

- [ ] **Step 2: Run it**

Run: `go test ./internal/buffer -run TestHotPathZeroAllocs -v`
Expected: PASS. If nonzero: `go test -run TestHotPathZeroAllocs -memprofile mem.out ./internal/buffer && go tool pprof -top -sample_index=alloc_objects mem.out`. Known offenders and fixes: index growth mid-measure (warm longer), `w.dir` growth (prewarm covers), fmt/error paths on append (must be alloc-free on success), map iteration order churn (irrelevant to allocs).

- [ ] **Step 3: Commit**

```bash
git add internal/buffer/alloc_test.go
git commit -m "test: hot-path zero-alloc gate for fragment and append"
```

---

### Task 14: Shadow processor wiring

**Files:**
- Create: `processor.go`
- Create: `processor_test.go`
- Modify: `factory.go`
- Modify: `e2e/config.yaml`, `e2e/compose/config.yaml` (add `storage_dir` under the `retrosampler:` processor entry)

**Interfaces:**
- Consumes: `Config` (Task 1), `fragmenter.Fragmenter`, `buffer.Buffer`.
- Produces: `newShadowProcessor(cfg *Config, logger *zap.Logger) *shadowProcessor` with methods `start(context.Context, component.Host) error`, `shutdown(context.Context) error`, `processTraces(context.Context, ptrace.Traces) (ptrace.Traces, error)`; factory wires them via `processorhelper.WithStart`/`WithShutdown`.

- [ ] **Step 1: Write the failing tests**

```go
func TestShadowDisabledWithoutStorageDir(t *testing.T) {
	p := newShadowProcessor(&Config{Window: time.Minute, SegmentSize: 32 << 20}, zap.NewNop())
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount())
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowBuffersAndPassesThrough(t *testing.T) {
	cfg := &Config{StorageDir: t.TempDir(), Window: time.Minute, SegmentSize: 32 << 20}
	p := newShadowProcessor(cfg, zap.NewNop())
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	sp.SetName("op")
	out, err := p.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 1, out.SpanCount(), "shadow mode: everything still passes through")

	visits := 0
	p.mu.Lock()
	require.NoError(t, p.buf.Collect([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		func(frag []byte) {
			dec, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(frag)
			require.NoError(t, err)
			assert.Equal(t, "op", dec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
			visits++
		}))
	p.mu.Unlock()
	assert.Equal(t, 1, visits, "span was buffered")
	require.NoError(t, p.shutdown(context.Background()))
}

func TestShadowShutdownStopsTicker(t *testing.T) {
	// goleak (TestMain) is the real assertion; this exercises the path.
	cfg := &Config{StorageDir: t.TempDir(), Window: time.Minute, SegmentSize: 32 << 20}
	p := newShadowProcessor(cfg, zap.NewNop())
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, p.shutdown(context.Background()))
}

func TestFactoryLifecycleWithStorage(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.StorageDir = t.TempDir()
	sink := new(consumertest.TracesSink)
	proc, err := f.CreateTraces(context.Background(),
		processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	require.NoError(t, proc.ConsumeTraces(context.Background(), td))
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.SpanCount())
}
```

(Match `processortest.NewNopSettings` signature to what `factory_test.go` already uses.)

- [ ] **Step 2: Run to verify failure**

Run: `go test . -v` — expected: compile errors.

- [ ] **Step 3: Implement `processor.go` and wire the factory**

```go
package retrosampler

import (
	"context"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/rtodorov/retrosampler/internal/buffer"
	"github.com/rtodorov/retrosampler/internal/fragmenter"
)

const expireInterval = time.Second

// shadowProcessor buffers every span and passes every span through
// (stage-1 shadow mode; retention semantics land with the decision plane).
// The coarse mutex is temporary scaffolding — stage 2 replaces it with
// shard-owned single-writer buffers (ADR-007).
type shadowProcessor struct {
	cfg    *Config
	logger *zap.Logger

	mu   sync.Mutex
	frag *fragmenter.Fragmenter
	buf  *buffer.Buffer

	stop chan struct{}
	done chan struct{}
}

func newShadowProcessor(cfg *Config, logger *zap.Logger) *shadowProcessor {
	return &shadowProcessor{cfg: cfg, logger: logger}
}

func (p *shadowProcessor) start(_ context.Context, _ component.Host) error {
	if p.cfg.StorageDir == "" {
		p.logger.Warn("retrosampler: storage_dir empty, shadow buffering disabled")
		return nil
	}
	if err := os.MkdirAll(p.cfg.StorageDir, 0o755); err != nil {
		return err
	}
	buf, err := buffer.Open(p.cfg.StorageDir, buffer.Options{
		Window:      p.cfg.Window,
		SegmentSize: p.cfg.SegmentSize,
	}, time.Now())
	if err != nil {
		return err
	}
	p.buf = buf
	p.frag = fragmenter.New()
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	go p.expireLoop()
	return nil
}

func (p *shadowProcessor) expireLoop() {
	defer close(p.done)
	t := time.NewTicker(expireInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-t.C:
			p.mu.Lock()
			p.buf.Expire(now)
			p.mu.Unlock()
		}
	}
}

func (p *shadowProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if p.buf == nil {
		return td, nil
	}
	now := time.Now()
	p.mu.Lock()
	p.frag.Fragment(td, func(id pcommon.TraceID, frag []byte) {
		if err := p.buf.Append(id, frag, now); err != nil {
			// Shadow mode: buffering failure must never fail the pipeline.
			p.logger.Debug("retrosampler: shadow append failed", zap.Error(err))
		}
	})
	p.mu.Unlock()
	return td, nil
}

func (p *shadowProcessor) shutdown(context.Context) error {
	if p.buf == nil {
		return nil
	}
	close(p.stop)
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buf.Close()
}
```

`factory.go` `createTraces` becomes:

```go
func createTraces(ctx context.Context, set processor.Settings,
	cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	p := newShadowProcessor(cfg.(*Config), set.Logger)
	return processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces,
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown))
}
```

Delete the old package-level `processTraces` function; update any `factory_test.go` assertions that referenced it (the lifecycle tests above supersede them).

e2e configs: in both `e2e/config.yaml` and `e2e/compose/config.yaml`, the `processors:` section's `retrosampler:` entry (currently likely `retrosampler:` with no body) gains:

```yaml
  retrosampler:
    storage_dir: /tmp/retrosampler-buffer
```

(Stale data from a previous run just ages out — recovery handles a nonempty dir; no cleanup hooks needed. The distroless image has `/tmp`.)

- [ ] **Step 4: Verify green, including lifecycle + goleak + e2e**

Run: `make test && make generate && git diff --exit-code && make e2e`
Expected: all green — the generated lifecycle test plus goleak proves the ticker stops; e2e proves the ocb-built collector runs shadow buffering with span conservation intact. If docker is available, also run `make e2e-compose`.

- [ ] **Step 5: Commit**

```bash
git add processor.go processor_test.go factory.go e2e/config.yaml e2e/compose/config.yaml
git commit -m "feat: shadow buffering wiring in processor"
```

---

### Task 15: Benchmarks and first perf baseline

**Files:**
- Create: `internal/buffer/bench_test.go`
- Create: `benchmarks/baseline-m-arm64.txt` (generated by `make bench-baseline`)

**Interfaces:** benchmark names are load-bearing — the Makefile runs exactly `^(BenchmarkIngest|BenchmarkKeepFlush|BenchmarkExpiry)$`.

- [ ] **Step 1: Write the benchmarks**

```go
func benchBuffer(b *testing.B, segSize int) (*Buffer, *fragmenter.Fragmenter, ptrace.Traces) {
	b.Helper()
	buf, err := Open(b.TempDir(), Options{Window: time.Hour, SegmentSize: segSize}, time.Unix(0, 0))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { buf.Close() })
	return buf, fragmenter.New(), benchBatch() // ~100 spans, 25 traces, realistic attrs
}

// BenchmarkIngest measures the stage-1 hot path: fragment + append.
// Reported per span. ADR-004 r5 gates time/op and allocs/op regressions.
func BenchmarkIngest(b *testing.B) {
	buf, f, td := benchBuffer(b, 256<<20)
	now := time.Unix(1, 0)
	spans := td.SpanCount()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, now)
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*spans), "ns/span")
}

// BenchmarkKeepFlush measures Collect on traces spread across segments.
func BenchmarkKeepFlush(b *testing.B) {
	buf, f, td := benchBuffer(b, 4<<20)
	now := time.Unix(1, 0)
	var ids []pcommon.TraceID
	seen := map[pcommon.TraceID]bool{}
	for range 200 { // fill across many segments
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, now)
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := ids[i%len(ids)]
		_ = buf.Collect([16]byte(id), func([]byte) {})
	}
}

// BenchmarkExpiry measures Expire ticks over a populated buffer.
func BenchmarkExpiry(b *testing.B) {
	buf, f, td := benchBuffer(b, 1<<20)
	for i := range 500 {
		f.Fragment(td, func(id pcommon.TraceID, frag []byte) {
			_ = buf.Append(id, frag, time.Unix(int64(i), 0))
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		buf.Expire(time.Unix(int64(i%10_000), 0))
	}
}
```

`benchBatch()` mirrors `allocTestBatch` (Task 13) — extract the shared builder into `internal/buffer/testutil_test.go` now rather than duplicating.

Note `BenchmarkKeepFlush` with `Window: time.Hour` never expires — that's intended: it measures pure read cost. `BenchmarkExpiry`'s advancing clock drives real deletes for early iterations then no-op ticks — both are the production mix.

- [ ] **Step 2: Run them**

Run: `make bench`
Expected: all three run with plausible numbers (Ingest should be well under ~2.4 µs/span ≈ the 433 MB/s spike at ~424 B/span — this machine class differs from the target, so just sanity-check the order of magnitude and 0 allocs/op on Ingest).

- [ ] **Step 3: Cut the baseline (must run on the m-arm64 class machine — this one)**

Run: `make bench-baseline`
Then: `make bench-gate` — expected: gate passes against the fresh baseline.

- [ ] **Step 4: Commit (message must state why the baseline changes — ADR-004 r5)**

```bash
git add internal/buffer/bench_test.go internal/buffer/testutil_test.go benchmarks/baseline-m-arm64.txt
git commit -m "test: buffer benchmarks and first m-arm64 baseline

First committed baseline for BenchmarkIngest/KeepFlush/Expiry: none
existed before this commit; the bench gate was failing loudly by design
(ADR-004 r5). Numbers from the buffer-core implementation this baseline
gates."
```

---

### Final verification (whole sub-project)

- [ ] Run the full acceptance from the spec:

```bash
make lint && make test && make cover && make generate && git diff --exit-code
make bench-gate
make e2e
make e2e-compose   # requires docker
```

Expected: everything green. Update `docs/progress.json`: move stage-1 items from `next` into `done` (with date), remove the three closed gates from `pending_gates` (allocs=0 hot path 004 r2 — note it's stage-1-scoped, shard routing joins in stage 2; index budget 006 r5; torn-tail 006 r6), set `next` to stage 2 (shard layer + overload, ADR-007). Commit:

```bash
git add docs/progress.json
git commit -m "chore: record buffer-core completion in progress.json"
```
