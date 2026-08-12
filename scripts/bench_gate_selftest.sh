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

# fixture writes 10 samples of one benchmark: $1 file, $2 name, $3 ns/op,
# $4 sheds/op, $5 allocs/op.
fixture() {
  local file=$1 name=$2 ns=$3 sheds=$4 allocs=$5
  {
    echo "goos: selftest"
    echo "goarch: selftest"
    echo "pkg: example.com/selftest"
    echo "cpu: selftest"
    for _ in $(seq 10); do
      printf '%s\t1000000\t%s ns/op\t%s sheds/op\t0 B/op\t%s allocs/op\n' \
        "$name" "$ns" "$sheds" "$allocs"
    done
    echo "PASS"
    echo "ok  	example.com/selftest	1.000s"
  } >"$file"
}

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

base="$work/benchmarks/baseline-selftest.txt"
new="$work/bench-new.txt"

# 1. Identical runs pass. Guards against a gate that fails on noise.
fixture "$base" BenchmarkFixture-8 100.0 0.5000 0
fixture "$new" BenchmarkFixture-8 100.0 0.5000 0
expect pass "identical runs pass" "bench gate OK"

# 2. A time/op regression beyond 10% fails (ADR-004 r5).
fixture "$new" BenchmarkFixture-8 130.0 0.5000 0
expect fail "time/op +30% fails" "FAIL time/op"

# 3. Time/op within the band passes.
fixture "$new" BenchmarkFixture-8 105.0 0.5000 0
expect pass "time/op +5% passes" "bench gate OK"

# 4. Any alloc/op regression fails, including the 0 -> 1 step that matters
#    most (ADR-004 r2). benchstat reports that one as "?", not a percentage,
#    because the ratio has a zero denominator.
fixture "$new" BenchmarkFixture-8 100.0 0.5000 1
expect fail "allocs/op 0 -> 1 fails" "FAIL allocs/op"

# 5. The same "?" delta in the improving direction passes.
fixture "$base" BenchmarkFixture-8 100.0 0.5000 1
fixture "$new" BenchmarkFixture-8 100.0 0.5000 0
expect pass "allocs/op 1 -> 0 passes" "bench gate OK"
fixture "$base" BenchmarkFixture-8 100.0 0.5000 0

# 6. An ungated metric moving does not fail the gate: metric identity must
#    reset per benchstat table rather than leak from the sec/op table above.
fixture "$new" BenchmarkFixture-8 100.0 1.0000 0
expect pass "ungated sheds/op +100% passes" "bench gate OK"

# 7. Nothing paired is a failure, not a pass: benchmark names carry the
#    GOMAXPROCS suffix, so a baseline from another machine class compares
#    nothing at all (ADR-004 r4).
fixture "$new" BenchmarkFixture-16 100.0 0.5000 0
expect fail "unpaired baseline fails" "compared nothing"

echo "bench gate selftest OK ($pass cases)"
