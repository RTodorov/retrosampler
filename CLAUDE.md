# Retrosampler — Routing

This file is a router, not a manual. Project truth lives in code, tests,
`metadata.yaml`, and ADRs (`docs/adrs/`). ADRs are timestamped and superseded
— never edited.

## Commands

- `make fmt` — gofumpt + gci write (auto-fix)
- `make lint` — golangci-lint run
- `make test` — `go test -race ./...`
- `make cover` — race tests with coverage profile
- `make generate` — mdatagen from `metadata.yaml` (regenerates docs and tests)
- `make build` — ocb build → `./bin/retrosamplercol`
- `make e2e` — run the built collector against a pipeline fixture
- `make testbed` — throughput and memory load test
- `make golden` — regenerate `testdata/*.yaml` golden files
- `make vuln` — govulncheck

## Workflow

- Plan → approve → execute. Use Plan Mode for non-trivial work.
- One feature at a time. Do not one-shot multiple features.
- `metadata.yaml` first, then `make generate`, then implement.
- Trunk-based: commit to `main`. No long-lived branches.
- TDD for processor logic. Failing test first, then implement.

## Prohibitions

- Do not silence the harness — linters, hooks, generated files, golden files.
  Fix the code (ADR-001).
- Do not write hand-authored architecture or design docs. Use ADRs for decisions.
- Do not save planning prose to the repo. Use `docs/progress.json` for
  cross-session state. JSON shape resists overwriting.
- Do not declare the processor done without an ocb-built collector actually
  running it.

## Cross-session startup

Before any work: `git log --oneline -20 && cat docs/progress.json`. The
SessionStart hook prints this automatically.

## ADRs

Read `docs/adrs/README.md` for the current index of architecture decisions.
Update the index in the same commit as any new or status-changed ADR.
