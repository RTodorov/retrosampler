#!/usr/bin/env bash
# C8 selftest: the bench gate's failure path is itself gated (ADR-001 r1,
# ADR-004 r5). A gate that cannot fail is worse than no gate - it reports
# green while regressions land - and this one silently could: it read
# benchstat's trailing P-value column instead of the delta column.
# Every case below runs the real script against synthetic benchmark output.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
export PATH="$repo/.tools:$PATH"
command -v benchstat >/dev/null 2>&1 || {
  echo "benchstat missing: run make install-tools" >&2
  exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/benchmarks"
export BENCH_CLASS=selftest

base="$work/benchmarks/baseline-selftest.txt"
new="$work/bench-new.txt"

# block appends one package section: $1 file, $2 pkg, then one
# name/ns-per-op/sheds-per-op/allocs-per-op quadruple per benchmark.
block() {
  local file=$1 pkg=$2
  shift 2
  {
    echo "goos: selftest"
    echo "goarch: selftest"
    echo "pkg: $pkg"
    echo "cpu: selftest"
    while (($# >= 4)); do
      for _ in $(seq 10); do
        printf '%s\t1000000\t%s ns/op\t%s sheds/op\t0 B/op\t%s allocs/op\n' \
          "$1" "$2" "$3" "$4"
      done
      shift 4
    done
    echo "PASS"
    printf 'ok  \t%s\t1.000s\n' "$pkg"
  } >>"$file"
}

# results writes a fresh single-package result file.
results() {
  local file=$1
  shift
  : >"$file"
  block "$file" example.com/selftest "$@"
}

# The gated set stands in for ADR-004 r5's three buffer benchmarks.
alpha=(BenchmarkAlpha-8 100.0 0.5000 0)
beta=(BenchmarkBeta-8 200.0 0.5000 0)
gamma=(BenchmarkGamma-8 300.0 0.5000 0)

pass=0
# expect runs the gate over the fixtures just written: $1 is pass|fail,
# $2 the case label, $3 a string the output must contain (optional).
expect() {
  local want=$1 label=$2 needle=${3:-} out rc
  set +e
  out=$(cd "$work" && "$repo/scripts/bench_gate.sh" compare 2>&1)
  rc=$?
  set -e
  if [[ "$want" == pass && $rc -ne 0 ]]; then
    echo "SELFTEST FAIL [$label]: gate exited $rc, expected 0" >&2
    echo "$out" >&2
    exit 1
  fi
  if [[ "$want" == fail && $rc -eq 0 ]]; then
    echo "SELFTEST FAIL [$label]: gate exited 0, expected nonzero" >&2
    echo "$out" >&2
    exit 1
  fi
  if [[ -n "$needle" && "$out" != *"$needle"* ]]; then
    echo "SELFTEST FAIL [$label]: output lacks '$needle'" >&2
    echo "$out" >&2
    exit 1
  fi
  pass=$((pass + 1))
  echo "  ok  $label"
}

# 1. Identical runs pass, and say how many benchmarks they compared.
results "$base" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"
results "$new" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"
expect pass "identical runs pass" "bench gate OK (class selftest, 3 compared)"

# 2. A time/op regression beyond 10% fails (ADR-004 r5).
results "$new" BenchmarkAlpha-8 130.0 0.5000 0 "${beta[@]}" "${gamma[@]}"
expect fail "time/op +30% fails" "FAIL time/op"

# 3. Time/op within the band passes.
results "$new" BenchmarkAlpha-8 105.0 0.5000 0 "${beta[@]}" "${gamma[@]}"
expect pass "time/op +5% passes" "bench gate OK"

# 4. Any alloc/op regression fails, including the 0 -> 1 step that matters
#    most (ADR-004 r2). benchstat reports that one as "?", not a percentage,
#    because the ratio has a zero denominator.
results "$new" BenchmarkAlpha-8 100.0 0.5000 1 "${beta[@]}" "${gamma[@]}"
expect fail "allocs/op 0 -> 1 fails" "FAIL allocs/op"

# 5. The same "?" delta in the improving direction passes.
results "$base" BenchmarkAlpha-8 100.0 0.5000 1 "${beta[@]}" "${gamma[@]}"
results "$new" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"
expect pass "allocs/op 1 -> 0 passes" "bench gate OK"
results "$base" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"

# 6. An ungated metric moving does not fail the gate: metric identity must
#    reset per benchstat table rather than leak from the sec/op table above.
results "$new" BenchmarkAlpha-8 100.0 1.0000 0 "${beta[@]}" "${gamma[@]}"
expect pass "ungated sheds/op +100% passes" "bench gate OK"

# 7. Nothing paired is a failure, not a pass: benchmark names carry the
#    GOMAXPROCS suffix, so a baseline from another machine class compares
#    nothing at all (ADR-004 r4).
results "$new" BenchmarkAlpha-16 100.0 0.5000 0 \
  BenchmarkBeta-16 200.0 0.5000 0 BenchmarkGamma-16 300.0 0.5000 0
expect fail "unpaired baseline fails" "0 of 3 gated benchmarks paired"

# 8. Losing one benchmark of the gated set is a failure too - a rename, a
#    package move or a narrowed -bench regex must not let its rows vanish
#    quietly while the survivors report OK.
results "$new" "${alpha[@]}"
expect fail "one of three gated benchmarks fails" "1 of 3 gated benchmarks paired"

# 9. A baseline-only benchmark under its own package - what the committed,
#    ungated BenchmarkOffer rows are - yields a whole one-sided table with no
#    delta column at all. It must neither count nor fail.
results "$base" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"
block "$base" example.com/selftest/other BenchmarkDelta-8 400.0 0.5000 0
results "$new" "${alpha[@]}" "${beta[@]}" "${gamma[@]}"
expect pass "baseline-only benchmark is ignored" "bench gate OK (class selftest, 3 compared)"

echo "bench gate selftest OK ($pass cases)"
