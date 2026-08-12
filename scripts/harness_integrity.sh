#!/usr/bin/env bash
# C7: committed harness files must not narrow test runs (ADR-001 r3).
# The bench target's -run '^$' (skip tests, run benchmarks) is the one sanctioned shape.
set -euo pipefail
hits=$(grep -nE "go test[^\"]*[[:space:]]-run[[:space:]]" \
  Makefile lefthook.yml .github/workflows/*.yml 2>/dev/null |
  grep -vE -- "-run '\^\\\$+'" || true)
if [[ -n "$hits" ]]; then
  echo "BLOCKED: test narrowing in committed harness files:" >&2
  echo "$hits" >&2
  exit 1
fi
# C8: the bench gate must still be able to fail (see bench_gate_selftest.sh).
scripts/bench_gate_selftest.sh
# C9: whole-tree banned-token sweep (see banned_tokens.sh).
scripts/banned_tokens.sh
echo "harness integrity OK"
