#!/usr/bin/env bash
# Compose e2e gate: two instances behind a real NATS bus, paced load, exact
# kept-span conservation ACROSS instances.
# Phase 1 (ADR-011): every trace's spans are split over both instances and
# only instance 1 receives the error marker, so instance 2 holds fragments
# that look healthy in isolation. Those fragments reaching the exporter is
# the keep bus working — nothing local can explain it.
# Phase 2 (ADR-011 r2): a second wave is detected while the bus is stopped,
# and the bounce has to make both instances whole again.
# Retention (ADR-009): every span of a kept trace out, every healthy one
# absent.
# Functional assertions only — perf floors live in make testbed (ADR-004 r4).
set -euo pipefail
cd "$(dirname "$0")/../e2e/compose"
TRACES=6000
# The kept classes must stay under the loadgen's 10000-id summary cap:
# past it kept_trace_ids stops listing, and every assert here reads that
# list. Counts alone stay exact, so the cap fails quietly.
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
assert_kept() { # $1 = which reading; reads the phase/k/want globals
  [[ "$k1" -eq "$want1" && "$k2" -eq "$want2" ]] || {
    echo "FAIL $phase: kept-span conservation violated ($1)" >&2
    echo "  instance 1: want $want1 kept spans got $k1" >&2
    echo "  instance 2: want $want2 kept spans got $k2" >&2
    compose logs nats retrosampler-1 retrosampler-2 | tail -100
    exit 1
  }
}
# Two consecutive equal polls, never one: a loop that breaks the instant
# the numbers match cannot see what arrives next, and late or duplicate
# delivery is exactly what the replay phase produces.
converge() { # $1 = summary, $2 = poll budget; leaves the last reading in k1/k2
  local stable=0
  k1=0
  k2=0
  for _ in $(seq 1 "$2"); do
    k1=$(kept 1 "$1")
    k2=$(kept 2 "$1")
    if [[ "$k1" -eq "$want1" && "$k2" -eq "$want2" ]]; then
      stable=$((stable + 1))
      [[ "$stable" -ge 2 ]] && return
    else
      stable=0
    fi
    sleep 1
  done
}
# A wave that expected nothing would let every assert below pass on an
# empty output directory.
assert_wanted() { # $1 = which wave; reads the want globals
  [[ "$want1" -gt 0 && "$want2" -gt 0 ]] || {
    echo "FAIL $phase: $1 expected no kept spans: $want1 + $want2" >&2
    exit 1
  }
}
phase=phase1
summary1=$(run_loadgen 1)
jq -e -s 'length == 1' >/dev/null 2>&1 <<<"$summary1" || {
  echo "FAIL phase1: the loadgen printed no summary document" >&2
  echo "  stdout was: $summary1" >&2
  exit 1
}
want1=$(jq -r '.expected_kept_spans_per_endpoint[0]' <<<"$summary1")
want2=$(jq -r '.expected_kept_spans_per_endpoint[1]' <<<"$summary1")
assert_wanted "the load"
converge "$summary1" 60
assert_kept "on convergence"
ids1=$(jq -c '.kept_trace_ids' <<<"$summary1")
l1=$(leaked "$ids1" 1)
l2=$(leaked "$ids1" 2)
[[ "$l1" -eq 0 && "$l2" -eq 0 ]] || {
  echo "FAIL phase1: healthy spans leaked through retention" >&2
  echo "  instance 1: $l1 unkept spans" >&2
  echo "  instance 2: $l2 unkept spans" >&2
  exit 1
}
# The last word on the counts, after the leak assert rather than before:
# leaked() selects the ids OUTSIDE the kept set, so a second copy of a
# kept trace is invisible to it. Only kept() can see a duplicate, so the
# final kept() reading has to be the final reading.
k1=$(kept 1 "$summary1")
k2=$(kept 2 "$summary1")
assert_kept "settled"
echo "phase1 OK: $k1 + $k2 kept spans across instances, healthy spans dropped"

# Phase 2 (ADR-011 r2): the second wave is detected with the bus stopped.
# Instance 1's publish intents park and its own flush parks behind them,
# so nothing of this wave reaches either exporter while the outage lasts;
# instance 2 holds fragments nothing has told it to keep. The bounce is
# what makes both whole — JetStream file storage outlives the container
# (compose.yaml's named volume) and the ordered consumer replays
# deliver-all.
phase=phase2
compose stop nats >/dev/null
# Seed 1001, not 2: the loadgen seeds each BATCH with seed+batchIndex, so
# wave 1 has already used 1 through 60 (6000 traces in batches of 100) and
# seed 2 would re-emit 59 of those batches under their original ids. Those
# traces are decided from phase 1, so instance 2 would forward the second
# wave's fragments without ever consulting the bus — the phase would pass
# on ids that prove nothing, and its counts would double.
summary2=$(run_loadgen 1001)
jq -e -s 'length == 1' >/dev/null 2>&1 <<<"$summary2" || {
  echo "FAIL phase2: the loadgen printed no summary document" >&2
  echo "  stdout was: $summary2" >&2
  exit 1
}
# Hold the outage past the loadgen's exit: the parked intents retry on the
# 1 s tick, so the publish DRAIN has to fail against a dead server too and
# not just the detection that queued it.
sleep 5
want1=$(jq -r '.expected_kept_spans_per_endpoint[0]' <<<"$summary2")
want2=$(jq -r '.expected_kept_spans_per_endpoint[1]' <<<"$summary2")
assert_wanted "the outage-window load"
# Instance 2 stays off the bus across the bounce, and this is what makes
# the phase test replay at all. Without it the two clients race to
# reconnect: when the subscriber wins it takes the keeps LIVE, nothing is
# replayed from the stream, and an at_most_once fleet — whose reconnect
# buffer flushes the same broadcasts publisher-side — passes this assert
# too (measured, both ways). Frozen here, the broadcasts can only land
# while nobody is subscribed, so the ≤W backlog replay of ADR-011 r2 is
# the one path left that can complete wave 2 on instance 2.
compose pause retrosampler-2 >/dev/null
compose start nats >/dev/null
# Observed, never slept through. The flusher publishes BEFORE it flushes
# and re-parks both bits together on failure, so instance 1's own kept
# spans cannot appear until its broadcasts have been accepted by the
# stream — which makes its kept count the one directly observable proof
# that there is a backlog for instance 2 to replay. Unpausing on a timer
# instead would silently degrade the phase to a live-broadcast test on any
# machine slower than the timer.
drained=0
for poll in $(seq 1 90); do
  [[ "$(kept 1 "$summary2")" -ge "$want1" ]] && {
    drained=$poll
    break
  }
  sleep 1
done
[[ "$drained" -gt 0 ]] || {
  echo "FAIL phase2: instance 1 never drained its parked keeps after the bounce" >&2
  echo "  want $want1 kept spans, got $(kept 1 "$summary2")" >&2
  compose logs nats retrosampler-1 | tail -100
  exit 1
}
# Printed, because the number is the evidence: a drain that completed on
# poll 1 would mean instance 1 had flushed before the bounce, and this
# whole freeze would be guarding nothing.
echo "phase2: instance 1 drained $want1 kept spans by poll $drained after the bounce"
compose unpause retrosampler-2 >/dev/null
# 90 polls, not phase 1's 60: parked intents drain on 1 s ticks behind a
# reconnect backoff. If CI flakes, the bound is the knob, never the
# assertion.
converge "$summary2" 90
assert_kept "wave 2, on convergence"
# Both waves' ids, so a wave-2 span outside its own kept set is a leak and
# not counted as one of wave 1's.
idsAll=$(jq -c -n --argjson a "$(jq -c '.kept_trace_ids' <<<"$summary1")" \
  --argjson b "$(jq -c '.kept_trace_ids' <<<"$summary2")" '$a + $b')
l1=$(leaked "$idsAll" 1)
l2=$(leaked "$idsAll" 2)
[[ "$l1" -eq 0 && "$l2" -eq 0 ]] || {
  echo "FAIL phase2: healthy spans leaked through retention" >&2
  echo "  instance 1: $l1 unkept spans" >&2
  echo "  instance 2: $l2 unkept spans" >&2
  exit 1
}
k1=$(kept 1 "$summary2")
k2=$(kept 2 "$summary2")
assert_kept "wave 2, settled"
# The replay is deliver-all, so wave 1's keeps cross the bus a second time.
# Re-reading wave 1 here is what proves the decided set absorbed them:
# nothing above can see a duplicate of a trace that was already complete.
want1=$(jq -r '.expected_kept_spans_per_endpoint[0]' <<<"$summary1")
want2=$(jq -r '.expected_kept_spans_per_endpoint[1]' <<<"$summary1")
k1=$(kept 1 "$summary1")
k2=$(kept 2 "$summary1")
assert_kept "wave 1, re-read after the replay"
echo "phase2 OK: wave-2 keeps conserved across the bus outage, replay duplicates absorbed"
echo "e2e-compose OK: cross-instance keeps + outage replay"
