#!/usr/bin/env bash
# C5: ocb-built collector runs the processor end to end.
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
for _ in $(seq 1 20); do
  grep -q traceId e2e/out/traces.json 2>/dev/null && break
  sleep 0.5
done
kill $col
wait $col 2>/dev/null || true
if ! grep -q traceId e2e/out/traces.json; then
  echo "e2e FAILED: no traces through pipeline" >&2
  exit 1
fi
count=$(grep -o traceId e2e/out/traces.json | wc -l | tr -d ' ')
echo "e2e OK: $count traceId occurrences"
