#!/usr/bin/env bash
# C1: per-package coverage floor >=80% (ADR-001 r12). internal/metadata exempt.
set -euo pipefail
floor=80
fail=0
while IFS= read -r line; do
  pkg=$(awk '{print $2}' <<<"$line")
  [[ "$pkg" == *"/internal/metadata"* ]] && continue
  if grep -q '\[no test files\]' <<<"$line"; then
    echo "FAIL $pkg: no test files" >&2
    fail=1
    continue
  fi
  pct=$(grep -oE 'coverage: [0-9.]+' <<<"$line" | grep -oE '[0-9.]+' || true)
  [[ -z "$pct" ]] && continue
  if (($(echo "$pct < $floor" | bc -l))); then
    echo "FAIL $pkg: ${pct}% < ${floor}%" >&2
    fail=1
  else
    echo "ok   $pkg: ${pct}%"
  fi
done < <(go test -race -cover ./... 2>&1 | grep -E '^(ok|\?)' || true)
exit $fail
