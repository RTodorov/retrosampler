#!/usr/bin/env bash
# L5/C6: Conventional Commits (ADR-001 r13).
set -euo pipefail
if [[ -f "${1:-}" ]]; then
  subject=$(head -n1 "$1")
else
  subject="${1:-}"
fi
[[ "$subject" == Merge* ]] && exit 0
regex='^(feat|fix|docs|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9/_-]+\))?!?: [^ ].*$'
if ! grep -qE "$regex" <<<"$subject" || ((${#subject} > 72)); then
  cat >&2 <<'EOF'
Commit message rejected (ADR-001 r13). Required:
  <type>(<scope>)!: <subject>   subject line <=72 chars, imperative
  types: feat fix docs refactor perf test build ci chore revert
EOF
  exit 1
fi
