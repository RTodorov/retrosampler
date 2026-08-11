# Buffer core — design spec (sub-project 1)

- **Date:** 2026-08-11
- **Status:** Approved for planning
- **Authority:** ADR-006 (buffer), ADR-004 (budgets), ADR-007 (single-writer),
  ADR-002 (TDD). Where this spec and an ADR disagree, the ADR wins.

## Build roadmap (context)

The processor builds in six sub-projects, each its own spec → plan →
implement cycle:

1. **Buffer core** (this spec) — fragmenter + disk-segment store + index.
2. **Shard layer + overload** (ADR-007) — traceID-sharded single-writer
   workers, Shutdown deadline+flush, goleak, overload ladder.
3. **Decision plane + flush** (ADR-008) — decided set, pending keeps, bus
   client interface + in-process fake, keep→collect→emit path, minimal
   keep-on-error built-in. End-to-end keep loop works after this stage.
4. **Keep detection** (ADR-008) — baggage T0/elapsed_ms, zero-alloc
   built-ins, OTTL user rules, deterministic baseline, skew clamp.
5. **Config surface** — every field wired, reflection test (ADR-005 gate).
6. **Integration hardening** — real bus in compose, cross-instance keep
   assertions, fragment-splitting loadgen, testbed, W validation.

Bridge rule: stages 1–2 run in **shadow mode** — buffer everything AND pass
everything through, so compose-e2e span-conservation assertions stay valid
until stage 3 flips to retention semantics.

## Scope

Two single-threaded units plus shadow wiring. Concurrency is stage-2's
problem (ADR-007 single-writer); neither unit contains locks. Time is always
a parameter — no `time.Now` inside either unit.

### Unit 1: `internal/fragmenter`

`Fragment(td ptrace.Traces, fn func(traceID [16]byte, frag []byte))`

- Groups spans by traceID preserving resource/scope context; marshals each
  group (OTLP proto) into a reusable scratch buffer; invokes `fn` per trace.
- `frag` is only valid during the callback (scratch reuse).
- pdata never escapes; zero allocations per span on the steady path
  (ADR-004 r2 gate). Growth of internal scratch/maps to a new high-water
  mark is the only permitted allocation source.

### Unit 2: `internal/buffer`

```
Open(dir, opts) (*Buffer, error)   // restart rebuild included
Append(traceID, frag, now) error
Collect(traceID, visit func(frag []byte)) error
Expire(now)
Close() error
```

**Segments.** One dir per buffer instance. Files named by monotonic
generation (`%09d.seg`), never reused; restart resumes at max+1. Appends via
buffered writer; roll at `segment_size` (default 32 MiB): write footer,
fsync, open next gen. fsync on roll only — loss bound is the active segment
(ADR-006 r6).

**Record:** `u32 len | 16B traceID | u32 CRC32C(frag) | frag`.

**Footer:** directory of `(traceID, offset, len)`, `[t_min, t_max]`, entry
count, magic, footer CRC.

**Restart.** Footer-valid segments are trusted (no record scan). The last,
footer-less segment is scanned forward validating CRCs and truncated at the
first invalid record.

**Index.** Open-addressing table keyed by full 16-byte traceID (false trace
merge = correctness bug) → head/tail into a flat loc arena. Loc entry 16 B
packed `{gen u32, offset u32, len u32, next u32}`; per-trace singly-linked
chain, O(1) append. Budget: **≤150 B per live trace** (ADR-006 r5), enforced
by test, not review. Exact probing/packing is a plan/TDD detail.

**Reclamation.** `Expire(now)` deletes whole segments oldest-first once
`t_max < now − W`, and drives an incremental table sweep (bounded slice per
call) dropping entries whose every loc is in an expired gen; freed loc slots
go to a free list. Steady state proven by the budget test, not assumed.

**Collect.** Walks the loc chain via `ReadAt`; locs in expired gens are
skipped (partial trace, ADR-003 rule 4). A loc in the active segment forces
a write-buffer flush first — keep path only, never on ingest.

### Shadow wiring

- metadata.yaml first: add `storage_dir`, `window` (default 5m),
  `segment_size` (default 32 MiB); `make generate`; then implement.
- `ConsumeTraces`: fragmenter → `Append`, then pass spans through
  unchanged. All buffer access (`Append`, `Expire`, `Close`) goes through a
  **temporary coarse mutex**, deleted in stage 2 when shard workers own the
  buffers.
- Periodic `Expire` driven by a ticker goroutine owned by the processor
  (goleak-clean, stops on Shutdown).

## Out of scope

Sharding, overload/backpressure, keep detection, bus, flush-to-consumer,
retention mode, reflection config test (stage 5), unmarshal-on-flush.

## TDD sequence

1. Fragmenter correctness (grouping, context preservation, proto
   round-trip), then `testing.AllocsPerRun` zero-alloc.
2. Record + segment writer: round-trip, CRC, roll, footer, fsync-on-roll.
3. Restart: footer rebuild; torn-tail table test truncating the last
   segment at every byte boundary → recovery at last valid CRC (006 r6).
4. Collect: multi-fragment chains, active-segment flush, expired-loc skip.
5. Expiry + reclamation: whole-segment delete, incremental sweep, free
   list.
6. Index budget: ≤150 B/live-trace over multiple expiry windows at a pinned
   workload (006 r5).
7. Conservation property: every appended span is collectable or was in a
   whole expired segment — no third fate.

## Benchmarks and gates

- `BenchmarkIngest` (fragmenter+Append, realistic pdata batches),
  `BenchmarkKeepFlush` (Collect), `BenchmarkExpiry`.
- First `make bench-baseline` on m-arm64 commits
  `benchmarks/baseline-m-arm64.txt`; perf gate live from then on.
- Gates closed here: hot-path allocs/span = 0 (004 r2), index ≤150 B
  (006 r5), torn-tail restart (006 r6).

## Acceptance

lint + race + coverage floor green; the three gates above green;
benchmarks + committed baseline; compose e2e conservation-green with shadow
buffering enabled in the ocb-built collector.
