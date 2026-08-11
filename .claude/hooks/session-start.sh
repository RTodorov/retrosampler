#!/usr/bin/env bash
# ADR-001 r5: inject cross-session state.
set -uo pipefail
cd "${CLAUDE_PROJECT_DIR:-$PWD}" || exit 0
ctx=$(printf '## git log\n%s\n\n## git status\n%s\n\n## docs/progress.json\n%s\n' \
  "$(git log --oneline -20 2>/dev/null)" \
  "$(git status --short 2>/dev/null)" \
  "$(cat docs/progress.json 2>/dev/null)")
command -v jq >/dev/null 2>&1 || { echo "$ctx"; exit 0; }
jq -n --arg ctx "$ctx" '{hookSpecificOutput:{hookEventName:"SessionStart",additionalContext:$ctx}}'
