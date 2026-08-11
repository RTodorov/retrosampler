#!/usr/bin/env bash
# L3: metadata.yaml staged => generated output must match.
set -euo pipefail
git diff --cached --name-only | grep -q '^metadata.yaml$' || exit 0
export PATH="$PWD/.tools:$PATH"
make generate >/dev/null
if ! git diff --quiet -- internal/metadata documentation.md 2>/dev/null; then
  echo "BLOCKED: generated files stale; run 'make generate' and stage the result (ADR-001 r11)" >&2
  exit 1
fi
