#!/usr/bin/env bash
# Compose e2e gate: two instances behind a real NATS bus, paced load, exact
# kept-span conservation ACROSS instances.
# Phase 1 (ADR-011): every trace's spans are split over both instances and
# only instance 1 receives the error marker, so instance 2 holds fragments
# that look healthy in isolation. Those fragments reaching the exporter is
# the keep bus working — nothing local can explain it.
# Retention (ADR-009): every span of a kept trace out, every healthy one
# absent.
# Functional assertions only — perf floors live in make testbed (ADR-004 r4).
set -euo pipefail
cd "$(dirname "$0")/../e2e/compose"
TRACES=6000
ERRORS=600
SPANS=4
compose() { docker compose -f compose.yaml "$@"; }
rm -rf out
mkdir -p out/1 out/2
chmod -R 777 out
# -v takes the JetStream volume with it: the durable consumers replay from
# the start of the stream, so a previous run's keeps left on disk would
# flush traces this run never sent.
compose down -v --remove-orphans >/dev/null 2>&1 || true
trap 'compose down -v >/dev/null 2>&1 || true' EXIT
compose up -d retrosampler-1 retrosampler-2
for port in 43181 43182; do
  ok=0
  for _ in $(seq 1 60); do
    nc -z 127.0.0.1 "$port" 2>/dev/null && {
      ok=1
      break
    }
    sleep 0.5
  done
  [[ $ok -eq 1 ]] || {
    echo "FAIL: collector on :$port never became ready" >&2
    compose logs
    exit 1
  }
done
run_loadgen() { # $1 = seed; prints the summary JSON on stdout
  compose run --rm -T loadgen \
    --endpoints retrosampler-1:4317,retrosampler-2:4317 \
    --traces "$TRACES" --spans-per-trace "$SPANS" \
    --error-traces "$ERRORS" --seed "$1" --rate 400
}
kept() { # $1 = instance, $2 = summary; spans of kept traces in the output
  jq -s --argjson ids "$(jq -c '.kept_trace_ids' <<<"$2")" \
    '[.[].resourceSpans[]?.scopeSpans[]?.spans[]? | select((.traceId | ascii_downcase) as $t | $ids | index($t))] | length' \
    "out/$1/traces.json" 2>/dev/null || echo 0
}
# No fallback: a file this cannot parse must abort rather than read as
# "nothing leaked".
leaked() { # $1 = kept ids, $2 = instance; spans outside every kept set
  jq -s --argjson ids "$1" \
    '[.[].resourceSpans[]?.scopeSpans[]?.spans[]? | select(((.traceId | ascii_downcase) as $t | $ids | index($t)) | not)] | length' \
    "out/$2/traces.json"
}
summary1=$(run_loadgen 1)
jq -e . >/dev/null 2>&1 <<<"$summary1" || {
  echo "FAIL phase1: the loadgen printed no summary document" >&2
  echo "  stdout was: $summary1" >&2
  exit 1
}
want1=$(jq -r '.expected_kept_spans_per_endpoint[0]' <<<"$summary1")
want2=$(jq -r '.expected_kept_spans_per_endpoint[1]' <<<"$summary1")
# A run that expected nothing would let every assert below pass on an
# empty output directory.
[[ "$want1" -gt 0 && "$want2" -gt 0 ]] || {
  echo "FAIL phase1: the load expected no kept spans: $want1 + $want2" >&2
  exit 1
}
k1=0
k2=0
for _ in $(seq 1 60); do
  k1=$(kept 1 "$summary1")
  k2=$(kept 2 "$summary1")
  [[ "$k1" -eq "$want1" && "$k2" -eq "$want2" ]] && break
  sleep 1
done
[[ "$k1" -eq "$want1" && "$k2" -eq "$want2" ]] || {
  echo "FAIL phase1: cross-instance kept conservation violated" >&2
  echo "  instance 1: want $want1 kept spans got $k1" >&2
  echo "  instance 2: want $want2 kept spans got $k2" >&2
  compose logs nats retrosampler-1 retrosampler-2 | tail -100
  exit 1
}
ids1=$(jq -c '.kept_trace_ids' <<<"$summary1")
l1=$(leaked "$ids1" 1)
l2=$(leaked "$ids1" 2)
[[ "$l1" -eq 0 && "$l2" -eq 0 ]] || {
  echo "FAIL phase1: healthy spans leaked through retention" >&2
  echo "  instance 1: $l1 unkept spans" >&2
  echo "  instance 2: $l2 unkept spans" >&2
  exit 1
}
echo "phase1 OK: $k1 + $k2 kept spans across instances, healthy spans dropped"
