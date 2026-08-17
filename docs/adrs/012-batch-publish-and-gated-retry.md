# ADR-012: Batch publish, gated retry drain, provoked saturation

- **Date:** 2026-08-17
- **Status:** Accepted
- **Related:** ADR-004, ADR-007, ADR-008, ADR-009, ADR-011

## Context

Stage 6 made the bus real (ADR-011) and closed the pending map's
unbounded path with the r3 publish deadline. Running it at the ADR-004
r3 floors then produced two facts the contracts had not accounted for.

The single flusher goroutine (ADR-009 r2) did one synchronous acked
JetStream publish per keep, plus decode and export, and measured
~990 keeps/s at the 125 MB/s shape — barely above the offered keep
rate, so any hiccup parked intents. And the parked-intent retry
replayed *every* intent against the buffer on every 1 s tick, each
replay a fresh whole-trace `Collect` (a `ReadAt` plus CRC per
fragment). Pending grows exactly when the flusher is slowest, so the
retry cost grew with the backlog precisely when the shard workers
could least afford it: their disk reads went to re-collecting instead
of draining their queues, which makes `Offer` shed, which parks more.
One run wedged at 27,592 parked intents with `flush_retries` 0 (the
flusher was behind, not failing) and queue-full sheds still climbing;
the successful full run of the same shape peaked at 777. A
bistability, not a hard limit — and it could not be made to reproduce
on demand, which is its own finding.

## Decision

1. **`bus.Bus.Publish` takes a batch.** The seam is now
   `Publish(ctx, keeps []Keep) (failed []Keep, err error)`, amending
   ADR-011 r1's seam list. The ~990 keeps/s ceiling was one *mode* of
   one adapter — durable JetStream's per-keep acked publish,
   serialized on the flusher — and no global contract may encode one
   adapter's delivery-guarantee latency. Contract: Publish returns
   once every keep is as durable as this bus's guarantee makes it, or
   ctx is done with every still-unresolved keep reported failed;
   `failed ⊆ keeps` and is exhaustive; `err` is nil iff `failed` is
   empty, so an empty batch is vacuously durable and returns
   `nil, nil` even against a dead ctx. Wire format is unchanged
   (ADR-011 r6 stands): a batch is K 17-byte messages, and the batch
   exists only at this API. Adapters internalize their strategy —
   Loopback and core NATS loop, and durable JetStream pipelines a
   chunked async ack window whose chunk size *is* the client's
   `MaxPending` of 64, with the acks joined between chunks so a chunk
   starting from an empty window can never stall inside one call. A
   keep whose publish landed but whose ack was lost retries as a
   duplicate, which the decided set absorbs — the same ambiguity a
   failed synchronous publish always had.

2. **The flusher batches by occupancy, never by timer.** The batch is
   whatever backlog is present when the flusher frees: one blocking
   receive plus a non-blocking gather of the queue, never a wait. At
   idle that is size 1 and byte-for-byte the old path (no added
   latency by construction, not by tuning); under load it is
   arrival rate × ack RTT. Publish-before-consume becomes a batch
   barrier: every ack joins before any consume, because other
   clusters' retention windows are burning. A keep the batch reports
   failed skips its consume and re-parks both bits with the deadline
   the job already carried, never a fresh one. This is still one
   goroutine: ADR-009 r2's single emission point is intact and the
   goroutine census is unchanged, and the publish join is bounded by
   the stop signal through a channel-backed context *type* rather than
   a watcher goroutine. Post-stop drain batches publish under a
   bounded per-batch timeout (2 s) instead: the shutdown backlog must
   be attempted, not refused on sight by an already-dead context.
   `publishedKeeps` and `publishErrors` now count per keep rather than
   per call (identical at batch size 1). The split-pipeline and
   flusher-pool alternatives stay deferred behind a measurement
   trigger: revisit only if a provoked run shows the ceiling
   decode+consume-bound.

3. **`retryPending` splits into a memory-only sweep and a gated
   drain.** The sweep walks the pending map touching no disk, so
   ADR-011 r3 abandonment stays tick-exact regardless of backlog size
   (the deadline is cleared with the bit it bounds, and an intent left
   owing nothing is deleted). The drain walks a FIFO of first-park
   order — a `pendq` beside the pend map, where merges never re-queue
   and the sweep leaves tombstones the drain pops and skips — oldest
   first, at most one attempt per intent per tick, and stops at the
   first full-channel refusal. On that stop the head intent keeps its
   place: `pendqHead` advances only past an intent the pass actually
   disposed of. Re-parking the refused intent at the back would demote
   the oldest intent behind every newer one on exactly the tick it was
   refused service, inverting the oldest-first order the queue exists
   to hold. A `Collect` read error is not a capacity signal, so that
   intent re-parks at the back — it has had its attempt — and the walk
   continues. Wasted disk work per blocked tick falls from
   O(pending) whole-trace Collects to at most one per shard: measured
   at the pin, 599 Collects ungated against 14-15 gated on the same
   fixture. `compactPendq` bounds both the consumed prefix and the
   episode-peak capacity, so a burst does not leave its backing array
   resident.

4. **Saturation is provoked, not chased.**
   `TESTBED_OUTAGE=<start>:<dur>` pauses the NATS container mid-run,
   which saturates the flusher deterministically at whatever the
   ceiling happens to be, replacing knife-edge rate tuning that must
   be re-found every time the ceiling moves. Gate polarity follows
   the shape: the standard
   run and any outage ≤ W gate `publishes_abandoned == 0`; an outage
   longer than W gates `> 0`. Any outage shape additionally gates
   pending peak `> 0` — the outage must be shown to have fired, so a
   `docker pause` that silently failed cannot pass the abandonment
   gate vacuously — and pending at end `< 1000`, which is the wedge
   signature. This supersedes ADR-011's consequence sentence that the
   past-W abandonment path stays unit-tier only: the W=30 s smoke
   shape proves it at testbed scale in 180 s without touching CI.

## Consequences

- The internal interface break moved the `bus.Bus` seam and everything
  behind it — both adapters, the `bustest` tiers, the flusher call
  site — in one commit; ADR-011 r1's third seam, the discriminated
  `bus.type` config block, needed no change at all. Cheap because the
  interface is internal and has no external implementors; a fourth
  adapter would have paid for it later at a worse time.
- `publishErrors` counts failed keeps, not failed calls. Any alert or
  dashboard reading it as a call count is reading a different number
  above batch size 1.
- Operators reading `pending.flushes` should expect transient linear
  growth for the duration of a bus outage and full drainage after it:
  58,735 intents parked during a 60 s full-rate outage, drained to 0
  in 278 s with 0 abandoned, while new keeps kept arriving. The wedge
  signature — unbounded growth plus climbing queue-full sheds — is now
  a bug rather than a mode.
- `corrupt.fragments` re-counts skipped fragments on every retry
  `Collect`, so a head intent wedged at a full channel inflates it
  once per tick for up to W. A counting artifact of the retry, not new
  corruption; accepted rather than fixed, since the alternative is
  per-intent state to remember what was already counted.
- A stop landing mid-publish abandons that batch's publish and consume
  halves. The publish half is counted (`publishErrors`, exported as
  `publish.errors`); the consume half is silent — its re-park is
  best-effort against an already-closed stop signal, and no flush-side
  counter moves — so what keeps it from being loss is that the
  fragments stay on disk for a durable bus to replay, not a number an
  operator can watch. The blast radius scales with batch size rather
  than being one keep. Accepted: the drain's bounded-context publish
  is what keeps it small, and shutdown that waits for a wedged bus is
  the worse failure.
