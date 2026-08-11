# ADR-005: Scope and dependency policy

- **Date:** 2026-08-11
- **Status:** Accepted
- **Related:** ADR-001, ADR-002

## Context

The goal is the least code doing exactly what is needed. Every dependency is
supply-chain and upgrade surface; every adjacent feature is buffer capacity
and latency spent off-mission.

## Decision

1. **Scope is exactly:** disk-backed span buffer, local keep detection,
   decided/pending-keeps sets, bus publish/subscribe, idempotent fragment
   flush. Nothing else. Explicitly out: span enrichment, transformation,
   aggregation (`spanmetrics` connector exists), storage-backend logic,
   receivers, exporters.
2. A capability that can live in an adjacent existing component does. A new
   config field needs a stated keep/cost justification in its PR.
3. **Allowed direct dependencies:** stdlib; the collector framework and
   `pdata`; `pkg/ottl` + `ottlfuncs` (contrib) for user keep-rules
   (ADR-008 rule 2); one bus client behind an internal interface; `testify`,
   `goleak`, `pdatatest`/`golden` test-only. Anything else requires an ADR
   before the import.
4. depguard floor from ADR-001 plus the mock-framework ban from ADR-002.
   No CGO.
5. Prefer stdlib (`hash/maphash`, `container/*`) and small in-repo utility
   code over importing a module for one function.
6. Every config field is exercised by a test (ADR-002 tier obligation);
   a field no test needs is a field to delete.

## Consequences

- Feature requests outside rule 1 become deployment-config answers or
  separate components, not patches here.
- The dependency review for a PR is `go.mod` diff = ADR reference, mechanical.
