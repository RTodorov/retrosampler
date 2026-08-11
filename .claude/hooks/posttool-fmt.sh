#!/usr/bin/env bash
# ADR-001 r2: autoformat edited Go file, report remaining lint as context.
set -euo pipefail
command -v jq >/dev/null 2>&1 || exit 0
input=$(cat)
file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
[[ "$file" == *.go && -f "$file" ]] || exit 0
root=${CLAUDE_PROJECT_DIR:-$PWD}
export PATH="$root/.tools:$PATH"
if command -v gofumpt >/dev/null 2>&1; then gofumpt -extra -w "$file" || true; fi
if command -v gci >/dev/null 2>&1; then
  gci write -s standard -s default \
    -s 'prefix(github.com/rtodorov/retrosampler)' --skip-generated "$file" >/dev/null 2>&1 || true
fi
command -v golangci-lint >/dev/null 2>&1 || exit 0
pkg=$(dirname "$file")
if findings=$(cd "$root" && golangci-lint run "$pkg" 2>&1); then
  exit 0
fi
jq -n --arg ctx "golangci-lint findings after your edit (fix now):
$findings" '{hookSpecificOutput:{hookEventName:"PostToolUse",additionalContext:$ctx}}'
exit 0
