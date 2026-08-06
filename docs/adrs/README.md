# ADR Index

Architecture Decision Records for Retrosampler. Numbered, timestamped,
append-only. To revise a decision, write a new ADR that supersedes the old
one — never edit a merged ADR.

| #   | Title                                            | Status   | Date       |
| --- | ------------------------------------------------ | -------- | ---------- |
| 001 | [Harness philosophy — mechanisms over prompts](001-harness-philosophy.md) | Accepted | 2026-08-06 |
| 002 | [Test-Driven Development discipline](002-test-driven-development.md) | Accepted | 2026-08-06 |

## Conventions

- Filename: `NNN-kebab-case-title.md` with zero-padded three-digit number.
- Frontmatter (first lines): Date, Status, optional Scope / Supersedes / Related.
- Status values: `Proposed`, `Accepted`, `Superseded by ADR-NNN`, `Retracted`.
- Never renumber. Never edit content of a merged ADR — supersede it instead.
- Update this index in the same commit as a new or status-changed ADR.
