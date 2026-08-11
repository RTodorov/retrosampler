# ADR-003: Deployment topology and per-instance targets

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-001

## Context

A single trace's spans arrive at many collector instances, often separated
by network boundaries where transport is expensive. Any design that moves
all spans — or per-span metadata — across those boundaries pays proportional
to total volume. This processor makes cross-boundary traffic scale with the
kept fraction (a few percent) instead.

## Decision

Two planes:

- **Payload plane.** Spans never leave their local domain unsampled. Each
  participating collector runs this processor: buffer marshaled spans on
  local disk for window `W`, flush only this instance's fragments of kept
  traces to the pipeline's exporter.
- **Metadata plane.** A keep-notification bus: dumb broadcast fan-out carrying
  trace_ids only. No state, no logic. Every instance publishes keeps and
  subscribes to all keeps.

1. Only trace_ids and kept-span fragments cross network boundaries. Per-span
   cross-boundary metadata for unkept traffic violates the design.
2. Per-instance target: **1 Gbps (125 MB/s) sustained OTLP payload**. This is
   the testbed floor (ADR-004) and sizes the disk window (~38 GB at `W`=5m).
3. `W` defaults to 5m, configurable. Validate against real trace-duration
   distribution and pod lifetime before promoting stability past
   `development`.
4. A trace outliving `W` flushes as a partial trace. Accepted behaviour.
5. End-to-end latency is approximate, from W3C baggage: `T0` (root start,
   wall-anchored) + `elapsed_ms` (per-hop accumulated, skew-free in-hop).
   Baggage propagation coverage is a platform prerequisite, not this
   component's job. `T0`-vs-`elapsed_ms` divergence is a skew/gap health
   signal (ADR-008).
6. Complete healthy baseline traces come from a deterministic trace-id ratio
   evaluated identically by every instance (ADR-008), not from head-sampling
   infrastructure.
7. 100%-coverage RED aggregates: `spanmetrics` connector in the deployment
   config. Not this processor.

## Rejected

- **Head sampling as primary**: spends the budget on successes; catches
  failures only at the base rate. Kept only as the deterministic baseline.
- **`tail_sampling` + `loadbalancing` exporter**: routes all spans of a trace
  to one decision point — pays exactly the cross-boundary transport this
  design removes.
- **Central stub server** (per-span ~33 B stubs to a stateful sharded server
  over gRPC): exact whole-trace decisions, but ships a stub per span across
  boundaries and rebuilds the hardest infrastructure. Superseded once keep
  conditions proved locally detectable.

## Consequences

- Instance count scales linearly with total sustained ingest.
- Bus provisioning (technology, placement, delivery mode) is an ops decision;
  the processor's contract with it is fixed in ADR-008.
- The cost dial is the keep-rule thresholds and baseline rate, not topology.
