#!/usr/bin/env bash
# C1: per-package coverage floor >=80% (ADR-001 r12). internal/metadata and
# internal/bus/bustest exempt, for different reasons - see each below.
set -euo pipefail
floor=80
fail=0
while IFS= read -r line; do
  pkg=$(awk '{print $2}' <<<"$line")
  [[ "$pkg" == *"/internal/metadata"* ]] && continue
  # bustest is a test LIBRARY: its statements are the ADR-011 r5 conformance
  # tiers, which run inside other packages' test binaries and never its own, so
  # self-coverage measures the wrong thing here - not a gap. The tiers are
  # gated by the natsbus composition tests (TestHardeningAtMostOnce and
  # TestHardeningDurable), which execute every one of them in both modes.
  # Matched in full rather than by glob, so this exempts exactly one package
  # and cannot widen to a future bustest-adjacent one unnoticed.
  if [[ "$pkg" == "github.com/rtodorov/retrosampler/internal/bus/bustest" ]]; then
    echo "skip $pkg: test library, gated by the natsbus composition tests"
    continue
  fi
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
