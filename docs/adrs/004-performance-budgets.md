# ADR-004: Performance budgets are tests

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-001, ADR-003, ADR-006

## Context

The component runs at line rate 24/7. A performance regression must fail CI,
not surface in production. Numbers derive from ADR-003 targets and the
2026-08-10 buffer spike (table in ADR-006).

## Decision

1. **Hot path** = `ConsumeTraces` → shard routing → marshal → segment append →
   index insert, plus the expiry tick and keep-receive flush.
2. **Bookkeeping allocations on the hot path: 0 per span**, asserted with
   `testing.AllocsPerRun` (buffers pre-warmed). Covers all processor
   machinery and the ADR-008 built-in conditions. Serialization uses one
   reused buffer per shard: ≤1 transient alloc per fragment until
   marshal-into-buffer lands, then 0. GC pressure is designed out (noscan
   buffer, ADR-006), not tuned out.
   User-configured OTTL policies (ADR-008 rule 2) are the one exclusion:
   their cost is measured, not zero — `BenchmarkIngest` includes an
   OTTL-policy variant with its own committed baseline, and the benchstat
   gate applies to it like everything else.
3. **Testbed floors** (`make testbed`, CI):
   - sustained ≥125 MB/s payload per instance config,
   - heap ≤4 GB at target rate with `W`=5m,
   - GC CPU <5%, max GC pause <10 ms (asserted via `runtime/metrics`),
   - all floors hold for the full scaled run, not at a sampled instant.
4. **Floors are absolute and assume capable hardware.** Testbed and benchmark
   gates run only on machines with CPU and disk headroom above the ADR-003
   target; a shortfall on lesser hardware is an environment error, not a
   regression. `benchstat` baselines are per machine class — comparing runs
   across classes is invalid.
5. **Benchmark regression gate.** `BenchmarkIngest`, `BenchmarkKeepFlush`,
   `BenchmarkExpiry` with a committed baseline; `benchstat` fails CI on >10%
   time/op or any alloc/op regression. The baseline changes only in a commit
   whose message says why.
6. **`GOMEMLIMIT`** set to ~90% of the container limit in deployment config;
   `GOGC` stays default.
7. **pprof labels per shard**; hot-path counters exported via component
   telemetry defined in `metadata.yaml`.
8. Enforcement wiring (CI jobs, benchstat scripts, testbed scenarios) lands at
   scaffolding. The budgets above are the normative spec for it.

## Consequences

- A change that speeds up code but adds an alloc/span fails the gate; that is
  intended — alloc regressions compound at 24/7 line rate.
- Budgets loosen only by superseding this ADR.
