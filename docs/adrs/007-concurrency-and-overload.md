# ADR-007: Concurrency and overload behaviour

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-004, ADR-006

## Context

groupbytrace's shard-by-traceID single-writer workers are the right shape:
serialized per-shard state, no locks. Its mechanisms are not: `any`-boxed
events, a goroutine + timer per handled event, a `time.AfterFunc` per trace.
Overload behaviour must be explicit — at 24/7 line rate the watermark is
routine, and unspecified means OOM.

## Decision

1. Shard by traceID hash (`hash/maphash` class). One goroutine per shard owns
   that shard's entire state: index shard, current segment stream, decided/
   pending set, expiry. Zero cross-shard coordination; a trace's spans,
   keeps, and expiry all route to the same shard.
2. Single-writer discipline: no locks or atomics on the hot path. Ingest and
   bus-received keeps enter a shard over one bounded typed queue. No `any`
   payloads, no goroutine-per-event, no timer-per-trace; one expiry ticker
   per shard.
3. `ConsumeTraces` routes spans to shards without per-span heap allocation
   (reused per-shard staging, asserted by the ADR-004 alloc gate).
4. Worker count: `min(GOMAXPROCS, config)`, default `GOMAXPROCS`.
5. **Overload, in order:**
   - disk watermark (config, % of budget) → expire oldest segments early;
     effective window shrinks; exported as `effective_window_seconds`;
   - window floor (config, default 60s) → `ConsumeTraces` returns a
     retryable error (backpressure to the pipeline);
   - shard queue full → same retryable error. Never a silent drop: every
     shed or refusal increments a counter.
   `memory_limiter` stays first in the pipeline; it is not this component's
   memory strategy.
6. `Shutdown(ctx)`: stop intake, drain shard queues, flush + fsync current
   segments, honour the deadline (required test, ADR-002).
7. Goroutine census is static: shards + bus subscriber + expiry tickers.
   `goleak` enforces it.

## Consequences

- Per-shard segment streams multiply open files by shard count; the disk
  budget and watermark of ADR-006 are global across shards.
- Backpressure at the floor turns sustained overload into an explicit,
  alertable pipeline signal instead of a silently useless window.
- A blocked downstream exporter never blocks ingest: decided traces stay decided
  and their fragments stay in the buffer, so flush retries are free until
  segment expiry. An exporter outage longer than `W` loses fragments — the
  partial-trace consequence, counted, not a hang.
