# ADR-011: Bus client and integration-hardening contracts

- **Date:** 2026-08-13
- **Status:** Accepted
- **Related:** ADR-003, ADR-004, ADR-005, ADR-008, ADR-009

## Context

Stages 3–5 run the whole decide→flush loop against the in-process
Loopback. Stage 6 makes the bus real: ADR-008 r6 fixed the contract
(dumb broadcast, duplicate/late-tolerant ≤ W, operator-chosen delivery
guarantee) and left technology, client shape, and hardening proof open.

## Decision

1. **NATS is the one bus client** ADR-005 r3 allows, via
   `github.com/nats-io/nats.go`. Both ADR-008 r6 modes through one
   client: core pub/sub (`at_most_once`) and JetStream (`durable`,
   the default). Future backends (e.g. Redis) arrive as a new package
   behind the same three seams — the `bus.Bus` interface, the
   discriminated `bus.type` config block, and the `bustest`
   conformance suite — each under its own ADR. No plugin registry.
2. **Durable mode = JetStream, ensure-at-start.** The client
   idempotently creates-or-updates the stream with `MaxAge = W` from
   this instance's config, then validates the live stream's horizon
   ≤ W (the ADR-008 r6 "validated where the client exposes it" hook).
   A fleet must share one W; a mixed-W fleet is a documented operator
   error. Subscribe replays deliver-all: a restarted pod re-receives
   the ≤W keep backlog and flushes what its disk buffer preserved;
   the decided set absorbs the duplicates.
3. **Publish intents carry a deadline.** A parked `NeedPublish` past
   its decided deadline (~keep time + W, stamped at first park) is
   dropped and counted (`pending.publishes_abandoned`). Correctness:
   past W every peer's fragments of that trace have aged out, so the
   broadcast can no longer cause any flush — dropping it loses
   nothing that still exists. This closes the pending map's one
   unbounded-growth path; it becomes reachable exactly when Publish
   becomes fallible.
4. **`github.com/nats-io/nats-server/v2` is authorized test-only**, on
   ADR-005's testify/goleak precedent: the hardening tier needs a real
   server it can kill and restart in-process, or bus failure modes are
   testable only at compose speed.
5. **Conformance is tiered** (`internal/bus/bustest`): a contract tier
   every implementation passes (fan-out, echo, duplicates, idempotent
   cancel), a hardening tier every real client passes (cancel-from-
   inside-fn without deadlock, no cross-subscriber head-of-line
   blocking, reconnect resume — the documented Loopback constraints,
   which exempt Loopback), and a durable tier for the replay promises
   (ctx-honoring refusal while unreachable, fresh-subscribe backlog
   replay). Implementations compose the tiers that state their
   contract; skipping inside a tier is banned with the rest of
   ADR-002 r2.
6. **Wire format: 16-byte trace id + optional reason byte.** 17 bytes
   normal, 16 read as reason 0 (an "unspecified" telemetry label,
   never a detector reason); any other length is malformed — counted,
   dropped, never fatal. Echo is allowed; the decided set absorbs it.
7. **The loadgen is `cmd/loadgen`** in the main module (pdata's
   `ptraceotlp`, zero new dependencies) and replaces telemetrygen in
   compose: cross-instance assertions need spans of one trace split
   across endpoints, which telemetrygen cannot do. Single-instance
   e2e keeps telemetrygen and the Loopback default.
8. **The testbed lives outside the harness inner loop.** Manual
   `make testbed` plus the dispatch-only perf.yml job, nothing else —
   an 8-minute gate in lefthook or the stop-gate would be routed
   around, which is ADR-001 failure by another door. Scenario: native
   ocb-built collector + NATS container (durable) + loadgen at the
   ADR-004 r3 floors, run ≥1.5×W, environment errors exiting distinct
   from floor failures (ADR-004 r4).
9. **`flush.age.ratio` is the W instrument**: a flusher-side histogram
   of (now − oldest span start in the flushed fragment) / W, static
   0→1+ buckets. Flusher-side deliberately — a first-seen stamp in the
   index would spend ADR-006 r5 budget headroom that does not exist
   (149.4/150 B). Mass near 1.0 ⇒ W too tight. This series is what
   lets the W=5m open decision (ADR-003 r3) close on production data.

## Consequences

- `go.mod` gains exactly two modules; the depguard list grows the
  matching entries. Anything further needs a new ADR first.
- Operators choosing `at_most_once` accept counted slow-consumer loss
  (`bus.dropped`); the durable default's cost is running JetStream.
- The compose gate now proves cross-instance keeps and outage replay;
  the past-W abandonment path stays unit-tier (a >W outage in CI
  wall-clock is not reasonable).
