#!/usr/bin/env bash
# ADR-001 r3: no hook/verification bypass via Bash.
set -euo pipefail
command -v jq >/dev/null 2>&1 || { echo "jq missing: guards cannot run" >&2; exit 2; }
cmd=$(jq -r '.tool_input.command // empty' <<<"$(cat)")
[[ -z "$cmd" ]] && exit 0
deny() { echo "BLOCKED: $1 (ADR-001 r3)" >&2; exit 2; }
if grep -qE 'git[^|;&]*commit[^|;&]*(--no-verify|--no-gpg-sign|[[:space:]]-n([[:space:]]|$))' <<<"$cmd"; then
  deny "git commit with hook bypass"
fi
if grep -qE 'git[^|;&]*push[^|;&]*--no-verify' <<<"$cmd"; then
  deny "git push --no-verify"
fi
if grep -qE '(^|[[:space:];&|])LEFTHOOK(_EXCLUDE)?=' <<<"$cmd"; then
  deny "LEFTHOOK env bypass"
fi
if grep -qE 'lefthook[[:space:]]+uninstall' <<<"$cmd"; then
  deny "lefthook uninstall"
fi
if grep -qE 'core\.hooksPath' <<<"$cmd"; then
  deny "git hooksPath override"
fi
exit 0
