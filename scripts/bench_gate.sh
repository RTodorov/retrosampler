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
# benchstat CSV: one table per metric, each opened by header rows whose first
# field is empty. The metric header also names the delta column ("vs base"),
# which is NOT last - a P-value column follows it - and a table without one
# (a benchmark only one side ran) compares nothing.
# Both the metric and the delta column reset per table, so an ungated metric
# cannot inherit "time" from the table above it.
# scripts/bench_gate_selftest.sh holds this parsing to both directions.
#
# gated is the size of ADR-004 r5's gated set - Ingest, KeepFlush, Expiry,
# Offer, Decode, PolicyEval (Offer and Decode joined per ADR-009's amendment;
# PolicyEval is the OTTL path's own committed baseline that ADR-004 r2 and
# ADR-008 r2 require - the one benchmark whose allocs are priced rather than
# forbidden). That many benchmarks must actually pair, or benchmarks have gone
# missing from the run and their regressions with them. Baseline rows with no
# counterpart by design simply do not count toward it: a baseline outlives the
# run that recorded it.
#
# Moving the set takes TWO edits, and this number is the harmless one. What
# decides whether a benchmark runs at all is the -bench regex in the Makefile's
# bench target, which is harness-protected (ADR-001 r3). Raise this number
# without widening that regex and the gate fails every run with "N of M gated
# benchmarks paired"; widen the regex without raising this number and the new
# benchmarks ARE gated for regressions but may vanish from the run unnoticed.
# The floor is a minimum, not a filter - every benchmark that pairs is checked
# either way - so this number says only which ones must not go missing.
gated=6
paired=$(benchstat -format csv "$base" bench-new.txt | awk -F, -v gated="$gated" '
  $1 == "" {
    metric = ""
    if ($2 == "sec/op") { metric = "time" }
    if ($2 == "allocs/op") { metric = "allocs" }
    delta_col = 0; base_col = 0; new_col = 0
    for (i = 2; i <= NF; i++) {
      if ($i == "vs base") { delta_col = i }
      else if ($i == $2) {
        if (base_col == 0) { base_col = i } else if (new_col == 0) { new_col = i }
      }
    }
    next
  }
  $1 == "geomean" { next }
  metric != "" && delta_col > 0 && NF >= delta_col && $base_col != "" && $new_col != "" {
    # A benchmark only one side ran keeps its table but yields a truncated,
    # one-sided row - never a comparison, whatever the header promises.
    compared[$1] = 1
    d = $delta_col
    if (d ~ /^[+-][0-9.]+%$/) {
      gsub(/[+%]/, "", d); delta = d + 0
      if (metric == "time" && delta > 10) { print "FAIL time/op +" delta "% : " $1 > "/dev/stderr"; bad = 1 }
      if (metric == "allocs" && delta > 0) { print "FAIL allocs/op +" delta "% : " $1 > "/dev/stderr"; bad = 1 }
    } else if (d == "?" && metric == "allocs" && base_col > 0 && new_col > 0 &&
               $new_col + 0 > $base_col + 0) {
      # benchstat cannot express a ratio against a zero baseline and prints
      # "?", so the percentage branch above never sees the one regression the
      # zero-alloc hot path can have: 0 -> N (ADR-004 r2).
      print "FAIL allocs/op " $base_col " -> " $new_col " : " $1 > "/dev/stderr"; bad = 1
    }
  }
  END {
    n = 0
    for (b in compared) { n++ }
    if (n < gated) {
      print "FAIL " n " of " gated " gated benchmarks paired with the baseline" > "/dev/stderr"
      bad = 1
    }
    if (bad) { exit 1 }
    print n
  }
')
echo "bench gate OK (class $class, $paired compared)"
