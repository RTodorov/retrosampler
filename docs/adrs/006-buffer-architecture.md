# ADR-006: Buffer architecture — marshal-on-ingest disk segments

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-003, ADR-004

## Context

Measured 2026-08-10 (single core, arm64, 200k-live-trace window ≈ 860 MB
payload, ~424 B/span marshaled):

| representation        | ingest   | GC CPU | live heap | Get p99 |
| --------------------- | -------- | ------ | --------- | ------- |
| retained pdata        | 232 MB/s | 38.9%  | 2.5 GB    | 166 µs  |
| retained pdata GOGC=400 | 404 MB/s | 10.7% | 4.8 GB    | 24 µs   |
| slab arena (memory)   | 650 MB/s | 0.2%   | 1.7 GB    | 18.5 µs |
| disk segments         | 433 MB/s | 1.4%   | 179 MB    | 21 µs   |

Retained pdata fails at any tuning: the GC tax is the pointer-dense
representation (5.6× memory inflation to buy it down). GC cost tracks live
pointer density, not alloc rate — marshal-on-ingest to noscan bytes removes
it. 433 MB/s/core is 3.5× the ADR-003 per-instance target before sharding.

## Decision

1. Fragments are marshaled (OTLP proto) on ingest. pdata is never retained
   past `ConsumeTraces`.
2. Storage: append-only segment files, rolled at 32 MiB (config), stamped
   with their `[t_min, t_max]`. Record = length prefix + traceID + CRC32C +
   fragment bytes. On roll, a per-segment footer directory (traceID →
   offsets) is written.
3. Expiry deletes whole segments oldest-first once `t_max < now − W`. No
   per-trace timers, no per-record deletes, no capacity-ring eviction.
4. Keep flush reads fragments via index locs (`ReadAt`); locs pointing into
   expired segments are skipped → partial trace (ADR-003 rule 4).
5. Index: in-memory traceID → packed `{segment gen, offset, len}`.
   **Budget: ≤150 B per live trace** — the naive map-of-slices measured
   ~900 B/trace, which at ~9M live traces (target rate × `W`, ~4.2 KB/trace)
   is ~8 GB vs ~1.3 GB. Flat loc arena + open-addressing table; the full 16-byte key is
   stored or truncation is verified on hit — a false trace merge is a
   correctness bug. Layout is TDD'd against the budget (ADR-004 heap floor).
6. Restart: index rebuilt from segment footers; a torn tail truncates to the
   last valid CRC. fsync on segment roll only — data-loss bound is the
   current segment. Disk survives pod restart via the deployment's volume.
7. Reads of the not-yet-rolled segment flush the write buffer first
   (keep-path only; never on ingest).

## Consequences

- Disk sizing: ~38 GB per instance at target (plus overload headroom,
  ADR-007). Page cache serves warm keep reads.
- The compact index is the main open implementation risk; it is gated by the
  150 B/trace budget test, not by review judgement.
- CPU spent re-marshaling is the accepted price; measured, it still clears
  the target 3.5× per core.
