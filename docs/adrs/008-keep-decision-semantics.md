# ADR-008: Keep decisions, pending keeps, and the bus contract

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-003, ADR-007

## Context

Keep conditions are detected locally (that collapse is the whole design,
ADR-003). What remains to fix: the decision lifecycle, the verdict-before-
spans race, duplicate delivery, and the exact contract with the bus.

## Decision

1. **Built-in conditions** — native machinery, zero-alloc under the strict
   ADR-004 gate, evaluated per span at ingest, thresholds config. The
   conditions the processor guarantees alloc-free — because OTTL either
   cannot express them or cannot evaluate them without allocating:
   - **span latency**: span duration > per-span threshold — plain timestamp
     math (OTTL's equivalent arithmetic allocates);
   - `elapsed_ms` (baggage) > trace-latency threshold — accumulated in-hop
     time; skew-free, blind to inter-hop gaps;
   - **trace age** (optional): `now − T0` (baggage) > threshold, `now` from
     the injected clock — catches queue-wait and gap latency `elapsed_ms`
     cannot see; wall-clock skew accepted (clamped, insignificant at
     practical thresholds);
   - **deterministic baseline**: `hash(traceID) < baseline_rate` — computed
     identically by every instance, so complete healthy traces need no
     broadcast at all.
2. **User rules are OTTL span conditions** — the only user-rule mechanism;
   status, attribute, and regex rules all live here. Measured (2026-08-11):
   ~30–110 ns and 0–3 allocs per span per condition; enum and simple attr
   compares are 0-alloc (status: 28 ns/0), regex and arithmetic are not, and
   zero-alloc is expression-shape-dependent — never guaranteed. Zero
   configured policies ⇒ provably alloc-free hot path; each policy chain carries its
   own benchmark baseline (ADR-004). Semantics are **locally decidable,
   OR-of-spans**: any matching span keeps the whole trace. Whole-trace
   policies (span_count, AND-across-spans, assembled-trace anything) are out
   of scope by design. Exclusion/drop rules belong to `filterprocessor`
   upstream (ADR-005 rule 2), never here.
   Non-ingress/async roots must not be invisible: baseline covers them;
   span-level OTTL rules apply to them like any span.
   Threshold defaults are derived from measured workload data before
   stability promotion.
3. **Lifecycle.** Condition fires and trace not yet decided → mark decided,
   flush own fragments, publish the trace_id (baseline keeps are never
   published). Keep received from the bus → mark decided, flush buffered
   fragments. Spans arriving for an already-decided trace flush straight
   through. Decided entries live for `W`, time-evicted, sharded per ADR-007.
4. **Verdict-before-spans race**: the decided set *is* the pending-keeps set.
   A keep with no buffered spans still records decided for `W`; matching
   spans flush as they arrive.
5. **Idempotency.** Duplicate keeps are expected (multiple detectors,
   redelivery). The decided set suppresses duplicate flushes and duplicate
   publishes. At-least-once delivery is therefore free, correctness-wise.
6. **Bus contract.** The processor requires only broadcast fan-out of
   trace_ids (16-byte id + optional reason byte). It is duplicate-tolerant
   and late-tolerant up to `W`. Delivery guarantee is the **operator's
   choice**, surfaced in config (plain pub/sub vs durable consumer):
   - durable (recommended default): reconnect replays missed keeps;
     constraint: bus replay horizon ≤ `W`, validated in `Config.Validate()`
     where the client exposes it;
   - at-most-once: a reconnect gap silently loses fragments of kept traces —
     documented operator trade, not a bug.
   Bus technology (NATS/Redis/other) and placement are ops decisions at
   provisioning time, behind the internal client interface (ADR-005).
7. **Time hygiene.** Negative durations from skew clamp to zero.
   `T0`-vs-`elapsed_ms` divergence is exported as the skew/propagation-gap
   metric (ADR-003 rule 5).

## Consequences

- The cost dial is thresholds + baseline rate (ADR-003); changing them needs
  config, not code.
- **The config surface must not present two rule systems.** Built-ins appear
  as plain scalar settings (`span_latency_threshold`, `trace_latency_threshold`,
  `trace_age_threshold`, `baseline_rate`); `policies` is the one place rules
  are written, and it is OTTL. The generated `README.md`/`documentation.md`
  (from `metadata.yaml`) must state explicitly: built-ins are guaranteed
  alloc-free; OTTL policies cost ~30–110 ns and 0–3 allocs per span per
  condition depending on expression shape — with the span-latency example
  called out (use the setting, not an OTTL arithmetic expression).
- The built-in/OTTL split is an accepted wart, owed to OTTL's
  expression-shape-dependent allocation. If OTTL later guarantees
  alloc-free evaluation for these shapes, supersede this ADR and fold the
  built-ins into `policies`.
- Bus outage degrades gracefully: local keeps still flush own fragments;
  cross-instance completeness suffers until reconnect (durable mode replays).
  Subscriber lag is the alert.
- Everything here is per-shard state already owned by ADR-007 workers; no new
  concurrency is introduced.
