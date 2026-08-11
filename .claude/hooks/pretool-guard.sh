#!/usr/bin/env bash
# ADR-001 r3/r8/r11 + ADR-002 r2. Blocks protected paths and banned content.
set -euo pipefail
command -v jq >/dev/null 2>&1 || { echo "jq missing: guards cannot run" >&2; exit 2; }
input=$(cat)
file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
[[ -z "$file" ]] && exit 0
rel=${file#"${CLAUDE_PROJECT_DIR:-$PWD}"/}
deny() { echo "BLOCKED: $1" >&2; exit 2; }
case "$rel" in
  .golangci.yml|lefthook.yml|Makefile|go.mod|go.sum|builder-config.yaml|.claude/*)
    deny "$rel is harness-protected; changing it requires superseding ADR-001 (r3)" ;;
  documentation.md|config.schema.yaml|*generated_*.go|internal/metadata/*)
    deny "$rel is generated; edit metadata.yaml then 'make generate' (ADR-001 r11)" ;;
  testdata/*.yaml|*/testdata/*.yaml)
    deny "golden files change only via 'go test <pkg> -update' (ADR-001 r8)" ;;
esac
content=$(jq -r '(.tool_input.content // "") + "\n" + (.tool_input.new_string // "")' <<<"$input")
if grep -q '//nolint' <<<"$content"; then
  deny "//nolint is banned; fix the finding (ADR-001 r3)"
fi
if grep -q 't\.Skip(' <<<"$content"; then
  deny "t.Skip is banned; fix or delete the test (ADR-002 r2)"
fi
exit 0
