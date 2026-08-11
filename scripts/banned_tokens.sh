#!/usr/bin/env bash
# C9: lint suppressions and test skips are banned in committed Go code
# (ADR-001 r3, ADR-002 r2). Session-level guards only cover interactive
# tools; this gate holds for every writer — subagents, heredocs, humans.
# Usage: banned_tokens.sh [file...]   (no args: scan all tracked .go files)
set -euo pipefail
pattern='(//[[:space:]]*nolint|t\.Skip\()'
if [[ $# -eq 0 ]]; then
  # No repo paths contain whitespace; plain word splitting is fine here.
  # shellcheck disable=SC2046
  set -- $(git ls-files -- '*.go')
fi
[[ $# -eq 0 ]] && { echo "banned tokens OK (no files)"; exit 0; }
hits=$(grep -nE "$pattern" -- "$@" 2>/dev/null || true)
if [[ -n "$hits" ]]; then
  echo "BLOCKED: banned token (lint suppression or test skip):" >&2
  echo "$hits" >&2
  echo "fix the finding or the test; overriding requires superseding ADR-001/ADR-002" >&2
  exit 1
fi
echo "banned tokens OK"
