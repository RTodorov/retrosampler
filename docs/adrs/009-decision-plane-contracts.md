# ADR-009: Decision-plane contracts — keep-on-error, flusher, required buffer

- **Date:** 2026-08-13
- **Status:** Accepted
- **Related:** ADR-004, ADR-007, ADR-008

## Context

Stage 3 lands the decision plane and ends shadow mode. Building it surfaced
six contract points ADR-007/008 left open or now amends, plus an ADR-004
amendment and the decided set's real memory shape.

## Decision

1. **Keep-on-error is a built-in**, amending ADR-008 r1's list (span latency,
   `elapsed_ms`, trace age, baseline). ADR-008's criterion — "OTTL cannot
   express or cannot evaluate without allocating" — does not cover it (a
   status compare measured 28 ns/0 allocs). It joins anyway: it is the
   design's headline condition, and OTTL zero-alloc is expression-shape-
   dependent, never guaranteed. Config: `keep_on_error` bool, default true,
   beside the threshold built-ins.
   **A shed fragment must not suppress the verdict.** An `Offer` refusal
   still attempts `Keep`: a shed governs new volume, while the verdict
   governs a trace whose earlier fragments are already on disk and whose
   peers wait on the broadcast — so the shard layer does not floor-gate
   `Keep` and the verdict lands even under a rung-2 shed. A lost verdict
   alongside accepted fragments would silently un-decide an error trace;
   either refusal fails the batch retryably instead.
2. **The flusher goroutine joins the ADR-007 r7 census**: shard workers +
   bus subscriber + flusher + expiry tickers. It is the processor's single
   emission point; all unbounded-latency work (publish, decode,
   `ConsumeTraces`) lives there, preserving ADR-007's blocked-exporter
   consequence.
   **Shutdown order is normative**: intake off → bus unsubscribe → flusher
   stop → shard-set shutdown. A flusher mid-job still owes its shard a
   `Retry` that blocks on the free ring and aborts only via the flusher's
   stop channel; stopping the shards first would leave `Set.Shutdown`
   burning its whole context waiting on a sender only `stop` can release.
   The unsubscribe leads for the same reason: cancel waits for an in-flight
   callback whose `KeepFromBus` progresses only while workers live.
   **A timed-out quiesce leaves workers live, deliberately** — conservation
   over teardown. The processor restores its set pointer so a retried
   shutdown is real rather than a false nil, which makes that retry the
   only worker-stopper: it is load-bearing, not a formality.
3. **`storage_dir` and `disk_budget` are required.** A retroactive sampler
   that cannot buffer cannot sample; empty is a startup config error, never
   a silent passthrough or a silent drop. (Supersedes the stage-1 opt-in
   shadow reading of empty `storage_dir`.)
4. **The Loopback bus is the default.** With no external bus client
   configured the processor runs single-instance: local keeps drive the full
   decide→flush loop. The bus is optional infrastructure, not a
   prerequisite. External clients (stage 6) implement the same
   `internal/bus.Bus` contract.
5. **Span delivery is at-least-once, stated normatively.** Decisions are
   exactly-once (the decided set suppresses duplicate flushes and
   publishes); spans are not: batch retries after a partial refusal,
   pending-intent re-collection after a partial flush, and durable-bus
   replay after restart may each duplicate spans. The backstop is the
   backend (Tempo dedup), as everywhere else in OTel pipelines.
6. **Accepted crash window.** Fragments survive restart on disk and a
   durable bus replays broadcast keeps within its horizon, but a locally
   detected keep that dies before its publish is lost: detection runs at
   ingest only, never over recovered fragments. A decided-set WAL could
   close a seconds-wide window and is not worth its complexity. Accepted,
   monitored via the `published.keeps` vs `kept.local` gap in the exported
   telemetry (18 instruments under `otelcol.processor.retrosampler.*`).
7. **ADR-004 r5 amendment — the gated set is five.** `BenchmarkOffer` and
   `BenchmarkDecode` join `BenchmarkIngest`, `BenchmarkKeepFlush`,
   `BenchmarkExpiry`. Moving the set takes two edits: the Makefile `-bench`
   regex decides what runs, `bench_gate.sh`'s `gated=5` decides what must
   pair, and the selftest carries a five-benchmark stand-in set.
   - `BenchmarkOffer` was **reshaped onto the accepted path**: 298.6 ns/op
     ±7%, 0 allocs/op, 0 sheds/op. The prior ~40 ns rows measured a
     saturated shed regime at 0.87–0.97 sheds/op — the early return, not the
     work. The step is a regime change, not a regression.
   - **`-p 1` is load-bearing.** Benchmarks run serially; parallel package
     execution distorted `BenchmarkDecode` by +274% in a sample, which is
     noise a >10% time gate cannot survive.
   - `BenchmarkDecode`'s 566 allocs/op is the **flush path**, exempt from
     ADR-004 r2's zero-alloc hot-path rule but counted: decode is off the
     ingest path by construction (ADR-007 r7), and gating it pins the cost
     rather than blessing it.
   - **Gated is not ratcheted.** The alloc arm compares integer-truncated
     allocs/op and B/op is ungated, so sub-unit drift and byte growth pass.
     The gate pins step changes, not every byte.
   - The **pairing floor rose from three to five**, counted per distinct
     benchmark name. It is a floor: extra informational benchmarks that
     pair only raise the count, and baseline rows with no counterpart —
     a baseline outlives the run that recorded it — count for nothing.
8. **The decided set is an open-addressed table over an insertion-ordered
   ring**, not a map. Slots hold a sequence number; each id and deadline is
   stored once in the ring, whose head order is deadline order (monotone per
   shard), making expiry exact-`W` and O(1) amortized. The map-keyed sketch
   measured 65.4 B/entry against the 48 B budget; the split ships 41.9 B/entry
   at the gated 1M population. **The honest envelope is 40–64 B/entry across
   populations**: both structures are powers of two, so the ring holds up to
   ~2x live just above a growth boundary and the cost sawtooths between
   those bounds.

## Consequences

- The generated docs must present `keep_on_error` with the other built-ins
  and state the alloc-free guarantee (ADR-008 consequence carries over).
- **Capacity sizing uses ~64 B/entry × global keep rate × `W`** — the top of
  the envelope, since the set holds global keeps and a population may sit
  anywhere in the sawtooth. The 48 B/entry gate is a regression pin at a
  fixed 1M population, not a universal bound; reading it as a capacity
  number undersizes by up to a third. The baseline built-in (stage 4) will
  not publish and so does not grow the set beyond local marks.
- Rule 3 is a breaking config change at development stability; e2e fixtures
  already comply.
- The e2e and compose assertions flip to kept-conservation in the next
  change. Rule 3 makes passthrough impossible, so the assertions written
  against shadow-mode conservation are structurally red until then — a
  known consequence of this ADR, not a regression to chase.
