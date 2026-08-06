# ADR-001: Harness philosophy — mechanisms over prompts

- **Date:** 2026-08-06
- **Status:** Accepted

## Context

LLM agent compliance with prose instructions degrades as instruction count
grows. Performance starts to fall off around 150–200 instructions, and the
Claude Code system prompt already consumes ~50 of that budget. Quality must
therefore be enforced where the agent cannot ignore it: in linters, hooks,
tests, and CI.

A second pressure: stale text in the repo is indistinguishable from current
truth to an agent doing `grep`/`find`/`cat`. Hand-authored architecture docs
rot, then get adopted as truth.

A third pressure specific to this stack: a large share of an OpenTelemetry
Collector component is generated from `metadata.yaml` by `mdatagen`. A
hand-edited generated file reads as truth, survives review, and is then
silently overwritten by the next `go generate` — rot with a delayed fuse.

## Decision

We enforce quality with mechanisms, not prompts, organised across four
feedback layers — fastest first:

| Layer       | Latency | What runs                                                                                                                                       |
| ----------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| PostToolUse | ms      | `gofumpt -w` + `gci write` on the edited file; report remaining `golangci-lint` findings as `additionalContext`                                    |
| Pre-commit  | s       | `golangci-lint run` on changed packages + `go build ./...`                                                                                         |
| Commit-msg  | ms      | Conventional Commits regex; rejection prints the allowed type list                                                                                |
| Pre-push    | s–min   | `go test -race ./...`                                                                                                                             |
| CI          | min     | Full suite `-race -cover`, `go vet`, `govulncheck`, `go generate ./... && git diff --exit-code`, ocb-built collector pipeline smoke, testbed load test |

### Concrete rules

1. **CLAUDE.md is a router**, not a manual. Target <50 lines. Commands,
   prohibitions (each pointing at an ADR or linter rule), and the
   cross-session startup procedure only.
2. **PostToolUse hook auto-formats** on every Edit/Write/MultiEdit:
   `gofumpt` with extra rules, then `gci` with sections
   `standard`, `default`, `prefix(<module path>)`. Remaining lint
   violations are returned to the agent as
   `hookSpecificOutput.additionalContext` JSON so they enter context. Plain
   stdout from a hook does not.
3. **PreToolUse config protection.** Edits to `.golangci.yml`,
   `lefthook.yml`, `Makefile`, `go.mod`, `go.sum`, `builder-config.yaml`,
   `.claude/hooks/**`, and `.claude/settings.json` are blocked. So are the
   Go-native escape hatches: writing a `//nolint` directive, adding
   `t.Skip(`, and narrowing committed test invocations with `go test -run`.
   `git commit --no-verify`, `--no-gpg-sign`, and equivalents are blocked.
   Override requires superseding this ADR.
4. **Stop hook verifies.** Agent cannot end a session while
   `go build ./...`, `go test -race ./...`, or `golangci-lint run` fail.
5. **SessionStart hook prints state**: `git log --oneline -20`, `git status`,
   and `docs/progress.json` are injected as `additionalContext` so a new
   session opens with the previous session's record loaded.
6. **Only executable artifacts in the repo.** Code, tests, `metadata.yaml`,
   linter configs, CI configs, ADRs. No hand-written design docs,
   architecture overviews, or status memos. The component's `README.md`,
   `documentation.md`, and `config.schema.yaml` are generated *from*
   `metadata.yaml` — the YAML is the spec. Specs become tests where possible.
7. **Cross-session state is JSON.** `docs/progress.json` is the agent
   handover record. JSON shape resists inappropriate overwriting; the git
   log is the secondary record.
8. **Golden files on disk, not fixtures in context.** Expected pdata lives in
   `testdata/*.yaml`, read with `golden.ReadTraces` and compared with
   `ptracetest.CompareTraces`. Regenerate with the `-update` flag; never
   hand-tune expected output. Snapshots stay out of context, and a fixture
   cannot be quietly bent to match a wrong result.
9. **Plan → approve → execute.** Plan Mode for non-trivial work; one
   feature at a time.
10. **The harness grows reactively.** Every agent mistake ends in a new test
    or linter rule, not a new sentence in CLAUDE.md.
11. **Generated files are never hand-edited.** `internal/metadata/generated_*.go`,
    `generated_component_test.go`, `generated_package_test.go`,
    `documentation.md`, and `config.schema.yaml` come from `metadata.yaml`
    via `mdatagen`. Change the YAML and run `make generate`. Blocked by
    PreToolUse; CI re-runs generation and fails on any diff.
12. **Collector invariants are mechanisms, not prose.** Each rule the
    upstream coding guidelines state in English has an enforcer here:

    | Invariant                                     | Enforced by                                     |
    | --------------------------------------------- | ----------------------------------------------- |
    | No panic / `os.Exit` / `log.Fatal` outside `main` | `forbidigo`                                     |
    | Don't ignore errors; wrap, don't string-match  | `errcheck`, `errorlint`                         |
    | No goroutine leaks                            | `goleak.VerifyTestMain` (generated `TestMain`)  |
    | `Shutdown(ctx)` honours the deadline          | Required test; buffered state must flush        |
    | `Config.Validate()` exists and rejects bad input | Generated lifecycle test + table-driven tests |
    | Banned dependencies                           | `depguard`                                      |
    | No CGO                                        | `CGO_ENABLED=0` in CI                           |
    | Coverage does not regress                     | CI threshold, ≥80% for new packages             |
13. **Commit messages are Conventional Commits**, enforced by a Lefthook
    `commit-msg` stage:
    `<type>(<scope>)!: <subject>`, types
    `feat|fix|docs|refactor|perf|test|build|ci|chore|revert`, scope is the
    package when present, subject in the imperative and under 72 characters.
    The hook's rejection message lists the allowed types, so the format never
    needs to occupy a line of CLAUDE.md. Implemented as a shell regex, not
    `commitlint` — that would drag an npm toolchain into a Go repo for one
    regex.

## Stack choices (recorded here so the rules above have a referent)

- **Language:** Go. Single module (standalone repo, contrib-shaped layout —
  not contrib's per-component multi-module scheme).
- **Formatter:** `gofumpt` (extra rules) + `gci`, both run through
  `golangci-lint` as formatters, matching upstream contrib configuration.
- **Linter:** `golangci-lint`, seeded with contrib's enabled set — `errcheck`,
  `errorlint`, `exhaustive`, `forbidigo`, `gocritic`, `gosec`, `govet`,
  `revive`, `staticcheck`, `testifylint`, `thelper`, `unparam`, `unused`,
  `usetesting`, `nolintlint`, `modernize`, `copyloopvar`, `misspell`,
  `predeclared`, `reassign`, `unconvert`, `usestdlibvars`, `wastedassign`,
  `whitespace`, `decorder` — plus a `depguard` blocklist
  (`github.com/pkg/errors`, `math/rand`, `go.uber.org/atomic`,
  `github.com/hashicorp/go-multierror`; semconv pinned to one version).
- **Type checking:** the compiler. `go build ./...` and `go vet ./...`. No
  separate typecheck step exists or is needed.
- **Unit tests:** stdlib `testing` with `testify` (`require`/`assert`),
  table-driven, `t.Parallel()` where possible, always `-race`.
- **Leak detection:** `go.uber.org/goleak` via the generated `TestMain`.
- **Golden/integration:** `pkg/pdatatest/ptracetest` + the `golden` package.
- **E2E:** a collector binary built with `ocb` (OpenTelemetry Collector
  Builder) from `builder-config.yaml`, run against a pipeline fixture.
- **Load/perf:** the collector `testbed` harness — non-negotiable for a
  stateful buffering processor, where throughput and memory are the
  failure modes that unit tests cannot see.
- **Code generation:** `mdatagen`, driven by `metadata.yaml` and a
  `//go:generate` pragma in `doc.go`.
- **Vulnerability scanning:** `govulncheck`.
- **License headers:** `addlicense`, Apache-2.0 SPDX.
- **Task runner:** `Makefile` (contrib convention; single entry point).
- **Pre-commit runner:** Lefthook. Kept from the previous stack — it is a
  Go binary and language-agnostic, so there is nothing to replace. Runs the
  `pre-commit`, `commit-msg`, and `pre-push` stages above.
- **Commit convention:** Conventional Commits (see rule 13).
- **Component:** a stateful traces processor on
  `go.opentelemetry.io/collector/processor` and `pdata/ptrace`. Declared
  stability starts at `development` in `metadata.yaml` and is promoted only
  when the coverage and testbed gates hold.

If any of these choices changes, supersede this ADR — don't edit it.

## Consequences

- Adding a quality rule means a test or a `golangci-lint` rule, not a
  sentence in CLAUDE.md. CLAUDE.md should not grow.
- A post-mortem on a bad agent action ends in a new mechanism.
- Bypassing the harness requires editing this ADR's status to Superseded,
  which is visible in `git log` and code review.
- The toolchain (`make install-tools && lefthook install`) must be installed
  before the harness runs at full strength. Hooks degrade gracefully (skip
  silently) when binaries are absent.
- Because generation is enforced in CI, a `metadata.yaml` change that is not
  followed by `make generate` fails the build rather than drifting.

## References

- Sakasegawa, "How to approach your harness" (2026).
- Cloudflare's Boris Tane on plan-approve-execute as the single most
  important agent practice.
- opentelemetry-collector-contrib, `CONTRIBUTING.md` and
  `docs/new-components.md` — component layout, required files, stability
  ladder, generation and lint targets.
- opentelemetry-collector, `docs/coding-guidelines.md` — the invariants
  mechanised in rule 12.
