# ADR Index

Architecture Decision Records for Retrosampler. Numbered, timestamped,
append-only. To revise a decision, write a new ADR that supersedes the old
one — never edit a merged ADR.

| #   | Title                                            | Status   | Date       |
| --- | ------------------------------------------------ | -------- | ---------- |
| 001 | [Harness philosophy — mechanisms over prompts](001-harness-philosophy.md) | Accepted | 2026-08-06 |
| 002 | [Test-Driven Development discipline](002-test-driven-development.md) | Accepted | 2026-08-06 |
| 003 | [Deployment topology and per-instance targets](003-deployment-topology.md) | Accepted | 2026-08-11 |
| 004 | [Performance budgets are tests](004-performance-budgets.md) | Accepted | 2026-08-11 |
| 005 | [Scope and dependency policy](005-scope-and-dependencies.md) | Accepted | 2026-08-11 |
| 006 | [Buffer architecture — marshal-on-ingest disk segments](006-buffer-architecture.md) | Accepted | 2026-08-11 |
| 007 | [Concurrency and overload behaviour](007-concurrency-and-overload.md) | Accepted | 2026-08-11 |
| 008 | [Keep decisions, pending keeps, and the bus contract](008-keep-decision-semantics.md) | Accepted | 2026-08-11 |
| 009 | [Decision-plane contracts — keep-on-error, flusher, required buffer](009-decision-plane-contracts.md) | Accepted | 2026-08-13 |

## Conventions

- Filename: `NNN-kebab-case-title.md` with zero-padded three-digit number.
- Frontmatter (first lines): Date, Status, optional Scope / Supersedes / Related.
- Status values: `Proposed`, `Accepted`, `Superseded by ADR-NNN`, `Retracted`.
- Never renumber. Never edit content of a merged ADR — supersede it instead.
- Update this index in the same commit as a new or status-changed ADR.
