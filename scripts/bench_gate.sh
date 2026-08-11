#!/usr/bin/env bash
# C8: benchstat gate - >10% time/op or ANY alloc/op regression fails (ADR-004 r5).
set -euo pipefail
export PATH="$PWD/.tools:$PATH"
mode=${1:?usage: bench_gate.sh compare|baseline}
class=${BENCH_CLASS:-$(cat benchmarks/CLASS 2>/dev/null || true)}
[[ -z "$class" ]] && {
  echo "no machine class (benchmarks/CLASS or BENCH_CLASS)" >&2
  exit 1
}
base="benchmarks/baseline-$class.txt"
[[ -f bench-new.txt ]] && grep -q 'Benchmark' bench-new.txt || {
  echo "no benchmarks ran; gates cannot pass vacuously (ADR-004)" >&2
  exit 1
}
if [[ "$mode" == baseline ]]; then
  mkdir -p benchmarks
  cp bench-new.txt "$base"
  echo "baseline updated: $base — commit must state why (ADR-004 r5)"
  exit 0
fi
[[ -f "$base" ]] || {
  echo "no baseline for class '$class' — run make bench-baseline on the reference machine" >&2
  exit 1
}
# benchstat CSV: rows grouped per metric table; delta column is last.
benchstat -format csv "$base" bench-new.txt | awk -F, '
  tolower($0) ~ /sec\/op/ { metric = "time" }
  tolower($0) ~ /allocs\/op/ { metric = "allocs" }
  $NF ~ /^[+-][0-9.]+%$/ {
    d = $NF; gsub(/[+%]/, "", d); delta = d + 0
    if (metric == "time" && delta > 10) { print "FAIL time/op +" delta "% : " $1; bad = 1 }
    if (metric == "allocs" && delta > 0) { print "FAIL allocs/op +" delta "% : " $1; bad = 1 }
  }
  END { exit bad }
'
echo "bench gate OK (class $class)"
