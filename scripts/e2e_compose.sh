#!/usr/bin/env bash
# Compose e2e gate: two instances, paced load, exact span conservation.
# Functional assertions only — perf floors live in make testbed (ADR-004 r4).
set -euo pipefail
cd "$(dirname "$0")/../e2e/compose"
TRACES_PER_INSTANCE=5000
WANT=$((TRACES_PER_INSTANCE * 2))
compose() { docker compose -f compose.yaml "$@"; }
rm -rf out
mkdir -p out/1 out/2
chmod -R 777 out
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
compose run --rm loadgen-1 &
p1=$!
compose run --rm loadgen-2 &
p2=$!
wait $p1 || {
  echo "FAIL: loadgen-1 errored" >&2
  exit 1
}
wait $p2 || {
  echo "FAIL: loadgen-2 errored" >&2
  exit 1
}
count() {
  jq -s '[.[].resourceSpans[]?.scopeSpans[]?.spans[]?] | length' "out/$1/traces.json" 2>/dev/null || echo 0
}
c1=0
c2=0
for _ in $(seq 1 30); do
  c1=$(count 1)
  c2=$(count 2)
  [[ "$c1" -eq "$WANT" && "$c2" -eq "$WANT" ]] && break
  sleep 1
done
if [[ "$c1" -ne "$WANT" || "$c2" -ne "$WANT" ]]; then
  echo "FAIL: span conservation violated" >&2
  echo "  instance 1: want $WANT got $c1" >&2
  echo "  instance 2: want $WANT got $c2" >&2
  exit 1
fi
echo "e2e-compose OK: $c1 + $c2 spans conserved across 2 instances"
