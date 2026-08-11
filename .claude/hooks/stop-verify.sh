#!/usr/bin/env bash
# ADR-001 r4: session cannot end red. Skips when tree unchanged since last green.
set -uo pipefail
root=${CLAUDE_PROJECT_DIR:-$PWD}
cd "$root" || exit 0
command -v go >/dev/null 2>&1 || exit 0
[[ -f go.mod ]] || exit 0
export PATH="$root/.tools:$PATH"
tree_state() {
  {
    git rev-parse HEAD 2>/dev/null
    git status --porcelain=v2 2>/dev/null
    git diff 2>/dev/null
    git diff --cached 2>/dev/null
    git ls-files --others --exclude-standard -z 2>/dev/null | xargs -0 shasum -a 256 2>/dev/null
  } | shasum -a 256 | cut -d' ' -f1
}
marker=.git/retrosampler-stop-green
state=$(tree_state)
[[ -f "$marker" && "$(cat "$marker")" == "$state" ]] && exit 0
fail() { printf 'STOP BLOCKED (%s). Fix before ending the session:\n%s\n' "$1" "$2" >&2; exit 2; }
out=$(go build ./... 2>&1) || fail "go build" "$out"
if command -v golangci-lint >/dev/null 2>&1; then
  out=$(golangci-lint run 2>&1) || fail "golangci-lint" "$out"
fi
out=$(go test -race ./... 2>&1) || fail "go test -race" "$out"
if ! git diff --quiet HEAD -- metadata.yaml 2>/dev/null; then
  out=$(make generate 2>&1) || fail "make generate" "$out"
  git diff --quiet -- internal/metadata documentation.md 2>/dev/null ||
    fail "generated files out of sync" "run make generate and include the diff"
fi
tree_state >"$marker"
exit 0
