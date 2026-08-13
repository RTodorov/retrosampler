#!/usr/bin/env bash
# C5: ocb-built collector runs the processor end to end.
# Retention (ADR-009): the gate is kept-conservation — every span of an
# error, slow, or policy-matched trace out, every healthy span absent.
# Healthy load goes first so a pass-through regression cannot
# transiently match the kept count.
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
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5 \
  --otlp-attributes 'e2e.cohort="unkept"'
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5 --status-code Error
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5 --span-duration 600ms
telemetrygen traces --otlp-insecure --otlp-endpoint 127.0.0.1:43170 --traces 5 \
  --telemetry-attributes 'e2e.keep="yes"' --otlp-attributes 'e2e.cohort="policy"'
want=30 # (5 error + 5 slow + 5 policy) traces x 2 spans each
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
  echo "e2e FAILED: kept-conservation violated, want $want kept spans got $count" >&2
  exit 1
fi
# Cohort markers, not status: latency and policy keeps are status-unset
# by design. The positive check keeps the negative one honest — both read
# a resource attribute, so a decode path that dropped resources would
# fail loudly here instead of passing the leak check vacuously.
if grep -q unkept e2e/out/traces.json; then
  echo "e2e FAILED: a healthy baseline-cohort span leaked through retention" >&2
  exit 1
fi
if ! grep -q policy e2e/out/traces.json; then
  echo "e2e FAILED: the policy cohort's resource attribute did not survive decode" >&2
  exit 1
fi
echo "e2e OK: $count spans kept across error+latency+policy, healthy spans dropped"
