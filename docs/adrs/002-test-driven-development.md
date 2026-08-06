# ADR-002: Test-Driven Development discipline

- **Date:** 2026-08-06
- **Status:** Accepted
- **Related:** ADR-001

## Context

ADR-001 rule 6 makes tests the spec. A test written after the code passes by
construction and specifies nothing. This component buffers spans and decides
later, so its behaviour depends on elapsed time and accumulated state —
unreachable through a real clock or a stubbed next-consumer.

## Decision

Processor logic — buffer, decision engine, `Config.Validate`, factory,
consumer plumbing — is test-first. Doubles are restricted to the process
boundary.

1. **Red → Green → Refactor.** A failing test exists in the working tree
   before the production code that makes it pass. Procedure:
   `/superpowers:test-driven-development`.

2. **Bug fix = regression test first.** The test must fail for the actual
   reason, not a bogus assertion. Confirm red, then fix. The test stays.
   ADR-001 rule 3 blocks `t.Skip(` and `go test -run`, so it cannot be parked.

3. **Permitted doubles:** `consumertest.TracesSink`, `consumertest.NewNop`,
   `componenttest.NewNopHost`, `processortest.NewNopSettings`,
   `componenttest.NewNopTelemetrySettings`; an injected clock; a hand-written
   fake for an external interface such as a storage extension.

   **Forbidden:** mock-generation frameworks — `github.com/vektra/mockery`,
   `go.uber.org/mock`, `github.com/stretchr/testify/mock`; an interface
   introduced only to mock a project-owned type; mocking pure functions;
   mocking `Config` (construct it, call `Validate()`); mocking telemetry
   settings (use generated `metadatatest` against an in-memory reader).
   Prefer deterministic trace-ID hashing to an RNG — ADR-001 already blocks
   `math/rand`.

4. **Clock is injected.** The processor takes `func() time.Time` from the
   factory, defaulting to `time.Now`. Bare `time.Now()` in processor packages
   is blocked by forbidigo, with one allow entry for the factory. No
   `time.Sleep` for synchronisation in tests — advance the fake clock or
   synchronise on a channel.

5. **Tiers.**

   | Tier        | Doubles                   | Command                | Must cover                                  |
   | ----------- | ------------------------- | ---------------------- | ------------------------------------------- |
   | unit/golden | fake clock, `TracesSink`  | `go test -race ./...`  | every config field, every decision path      |
   | e2e         | none                      | `make e2e`             | every release-blocking behaviour             |
   | testbed     | none                      | `make testbed`         | throughput and memory ceilings               |

   Golden comparison is `testdata/*.yaml` via `ptracetest.CompareTraces`
   (ADR-001 rule 8). E2E runs the ocb binary built from `builder-config.yaml`:
   real config unmarshalling, real pipeline wiring, real shutdown.

6. **Name behaviour, not wiring.** `TestProcessor_LateErrorSpanFlipsVerdict`,
   not `TestProcessor_CallsNextConsumerOnce`. A table case's `name` states the
   promise, not the input. A test that breaks on refactor without a behaviour
   change was wrong.

7. **Theatre is deleted, not tightened.** Reject: `require.NoError` as the
   sole assertion; `consumertest.NewNop()` where the emitted traces needed
   assertions; a golden regenerated with `-update` without the commit
   explaining the behaviour change.

8. **Test and code in one commit,** test first in diff order. Two commits in
   sequence is fine; "code now, tests later" is not. The `test:` type is for
   standalone test work, not for debt from the previous commit.

9. **Coverage.** ADR-001 rule 12's ≥80% floor stands as a tripwire. The
   governing rule is "every behaviour has a test". A test that only moves the
   number is rejected under 7.

10. **Required in the initial `.golangci.yml`.** The file does not exist yet;
    ADR-001 rule 3 protects it once written, so these land at scaffolding:
    - depguard blocks the three mock frameworks in 3,
    - forbidigo blocks `time.Now` in processor packages, allowing the factory.

    Loosening either requires superseding this ADR.

## Consequences

- Everything in 3 and 4 is a linter failure. Red-before-green is the only rule
  left to habit — nothing in the working tree reveals authoring order.
- `make e2e` needs an ocb build, so it stays in CI. Pre-push remains
  `go test -race ./...`.

## References

- opentelemetry-collector `docs/coding-guidelines.md` — `Shutdown` deadline,
  `Config.Validate()`.
- opentelemetry-collector-contrib `CONTRIBUTING.md` — `consumertest`,
  `componenttest`, `processortest`, testbed.
