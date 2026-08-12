#!/usr/bin/env bash
# C5: ocb-built collector runs the processor end to end.
# Retention (ADR-009): the gate is kept-conservation — every error span
# out, every healthy span absent. Healthy load goes first so a
# pass-through regression cannot transiently match the error count.
set -euo pipefail
export PATH="$PWD/.tools:$PATH"
mkdir -p e2e/out
: >e2e/out/traces.json
./bin/retrosamplercol --config e2e/config.yaml &
col=$!
trap 'kill $col 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  nc -z 127.0.0.1 43170 2>/dev/null && break
  sleep 0.5
done
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5 --status-code Error
want=10 # 5 error traces x 2 spans per trace
count=0
for _ in $(seq 1 30); do
  count=$(grep -o traceId e2e/out/traces.json | wc -l | tr -d ' ') || count=0
  [[ "$count" -eq "$want" ]] && break
  sleep 0.5
done
kill $col 2>/dev/null || true
wait $col 2>/dev/null || true
# Counted after shutdown, so a drain that duplicated keeps or flushed an
# unkept trace fails here too.
count=$(grep -o traceId e2e/out/traces.json | wc -l | tr -d ' ') || count=0
if [[ "$count" -ne "$want" ]]; then
  echo "e2e FAILED: kept-conservation violated, want $want error spans got $count" >&2
  exit 1
fi
if grep -q '"status":{}' e2e/out/traces.json; then
  echo "e2e FAILED: a healthy (status-unset) span leaked through retention" >&2
  exit 1
fi
echo "e2e OK: $count error spans kept, healthy spans dropped"
