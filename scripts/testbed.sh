#!/usr/bin/env bash
# Testbed: the ADR-004 r3 floors over the deployed shape — native
# ocb-built collector + NATS container in durable mode + loadgen at the
# ADR-003 target rate, with the keep loop live.
#
# Manual `make testbed` and the dispatch-only perf.yml job ONLY
# (ADR-011 r8). An 8-minute gate in lefthook or the stop gate would be
# routed around, which is ADR-001 failure by another door.
#
# TESTBED_MBPS is decimal MEGABYTES per second of OTLP payload — ADR-003's
# 125 MB/s = 1 Gbps — and NOT the megabits the loadgen's --target-mbps
# takes. The x8 conversion happens in exactly one place below.
#
# Knobs: TESTBED_WINDOW (W, 5m), TESTBED_MBPS (125), TESTBED_DURATION
# (8m, >=1.5xW per ADR-011 r8), TESTBED_DIR (scratch volume).
#
#   TESTBED_WINDOW=30s TESTBED_DURATION=60s TESTBED_MBPS=20 scripts/testbed.sh
#
# is a SMOKE of this script's own logic — it exercises every floor
# expression in about 90 seconds. It is NEVER the gate: a floor means
# nothing below the ADR-003 rate and under 1.5xW of run.
#
# Exit codes: 0 floors hold; 1 floor failure; 2 environment error
# (ADR-004 r4: a shortfall in the machine is not a regression).
set -euo pipefail
cd "$(dirname "$0")/.."

W="${TESTBED_WINDOW:-5m}"
RATE_MBPS="${TESTBED_MBPS:-125}"
DUR="${TESTBED_DURATION:-8m}"
SCRATCH="${TESTBED_DIR:-$(mktemp -d)}"
mkdir -p "$SCRATCH"

# The buffer's own numbers: 40 GiB budget at a 95% watermark. The default
# 80% would put the watermark at 32 GiB, under the 34.9 GiB a 5m window of
# 125 MB/s payload occupies, so the ladder would early-expire segments and
# shrink the effective window for a reason that is the budget rather than
# W — which is exactly what the flush.age.ratio report would then
# misattribute.
DISK_BUDGET=42949672960
WATERMARK_PCT=95
SHARDS=8
# Keep classes: 3% error traces plus the 0.5% deterministic baseline.
ERROR_PCT=3
BASELINE_RATE=0.005

secs() { # go duration -> whole seconds; -1 for a form this subset cannot read
  awk -v d="$1" 'BEGIN {
    n = substr(d, 1, length(d) - 1) + 0
    if (d ~ /^[0-9]+(\.[0-9]+)?s$/) { printf "%d\n", n; exit }
    if (d ~ /^[0-9]+(\.[0-9]+)?m$/) { printf "%d\n", n * 60; exit }
    if (d ~ /^[0-9]+(\.[0-9]+)?h$/) { printf "%d\n", n * 3600; exit }
    print -1
  }'
}
w_s=$(secs "$W")
dur_s=$(secs "$DUR")

# --- preflight (ADR-004 r4: a shortfall here is environment, exit 2) ---
# What the run puts on this volume: the buffer settles at the smaller of
# its watermark and one window of payload (+15% for record framing and the
# up-to-one-tick expiry lag), plus the exporter's kept stream for the
# whole run (~3.5% of ingest kept, JSON at roughly 2x the protobuf), plus
# 2 GiB of slack. The rates are decimal but both df forms report GiB
# (macOS -g, GNU -BG), so the total converts to GiB before comparing -
# against decimal GB the check would run ~7% lenient.
need_gib=$(awk -v budget="$DISK_BUDGET" -v pct="$WATERMARK_PCT" -v mbps="$RATE_MBPS" \
  -v w="$w_s" -v dur="$dur_s" -v kept=0.035 'BEGIN {
    mark = budget / 100 * pct
    win = (w > 0 ? mbps * 1e6 * w * 1.15 : mark)
    buf = (win < mark ? win : mark)
    out = (dur > 0 ? mbps * 1e6 * dur * kept * 2 : 0)
    printf "%d\n", (buf + out) / 1073741824 + 3
  }')
free_gib=$(df -g "$SCRATCH" 2>/dev/null | awk 'NR==2 {print $4}') ||
  free_gib=$(df -BG --output=avail "$SCRATCH" | awk 'NR==2 {gsub("G", ""); print}')
[[ "${free_gib:-0}" -ge "$need_gib" ]] || {
  echo "ENV: need ${need_gib}GiB free at $SCRATCH, have ${free_gib:-0}GiB" >&2
  echo "     (${RATE_MBPS} MB/s x W=$W buffered, capped by the ${WATERMARK_PCT}% watermark, plus the kept stream for $DUR)" >&2
  exit 2
}
cores=$(getconf _NPROCESSORS_ONLN)
[[ "$cores" -ge 8 ]] || {
  echo "ENV: need >=8 cores, have $cores" >&2
  exit 2
}
for tool in docker go jq curl nc; do
  command -v "$tool" >/dev/null || {
    echo "ENV: $tool is required" >&2
    exit 2
  }
done
[[ -x bin/retrosamplercol ]] || {
  echo "ENV: bin/retrosamplercol missing - run make build first" >&2
  exit 2
}
# macOS SIGKILLs a binary whose code signature no longer matches its
# inode, which is exactly what copying over a file that has already been
# executed leaves behind: exit 137, no output, no log, and otherwise
# nothing but a mystifying readiness timeout two screens further down.
help_status=0
bin/retrosamplercol --help >/dev/null 2>&1 || help_status=$?
[[ "$help_status" -eq 0 ]] || {
  echo "ENV: bin/retrosamplercol will not execute (exit $help_status)" >&2
  echo "     137 is macOS refusing a stale signature: rm the binary and make build again" >&2
  exit 2
}
# Before the port check, not after it: this script owns testbed-nats by
# name, and one left behind by a kill -9 holds 4222 forever. Pre-cleaning
# afterwards would wedge every subsequent run behind a port message that
# blames the machine for a container this script is entitled to remove.
docker rm -f testbed-nats >/dev/null 2>&1 || true
for port in 4317 8888 4222; do
  nc -z 127.0.0.1 "$port" 2>/dev/null && {
    echo "ENV: something already listens on 127.0.0.1:$port" >&2
    echo "     (a previous testbed collector still shutting down will free it shortly)" >&2
    exit 2
  }
done
short_run=""
if [[ "$dur_s" -ge 0 && "$w_s" -ge 0 ]] &&
  awk -v d="$dur_s" -v w="$w_s" 'BEGIN {exit !(d < 1.5 * w)}'; then
  short_run="the run is $DUR against W=$W, under the 1.5xW of ADR-011 r8"
fi

# The overload ladder's window floor has to sit below W, and its 1m
# default does not when the smoke shortens W to 30s. Scaling it to W/5
# lands exactly on that default at W=5m and keeps a short W legal; a W
# this subset cannot parse falls back to the default, which is only ever
# wrong below 1m.
floor_line=""
[[ "$w_s" -le 0 ]] || floor_line="    window_floor: $((w_s / 5))s"

cleanup() {
  [[ -z "${SAMPLER_PID:-}" ]] || kill "$SAMPLER_PID" 2>/dev/null || true
  # Wait the collector out rather than just signalling it: shutdown closes
  # segments and drains the flusher, and until that finishes the process
  # still holds :4317 and :8888 - which the NEXT run then fails to bind,
  # with an error naming the port and nothing at all naming this script.
  if [[ -n "${COL_PID:-}" ]]; then
    kill "$COL_PID" 2>/dev/null || true
    for _ in $(seq 1 60); do
      kill -0 "$COL_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "$COL_PID" 2>/dev/null || true
    wait "$COL_PID" 2>/dev/null || true
  fi
  docker rm -f testbed-nats >/dev/null 2>&1 || true
  # The segments and the kept stream are tens of gigabytes of opaque
  # bytes; the logs and the summary are the evidence, so those stay.
  [[ -z "${SCRATCH:-}" ]] || rm -rf "${SCRATCH:?}/buf" "${SCRATCH:?}/kept.json"
}
trap cleanup EXIT

# --- bus: durable mode needs JetStream, -sd puts its store on disk ---
docker run -d --rm --name testbed-nats -p 4222:4222 nats:2.12-alpine \
  -js -sd /tmp/js >/dev/null || {
  echo "ENV: could not start the testbed-nats container" >&2
  exit 2
}

cat >"$SCRATCH/config.yaml" <<YAML
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:4317

processors:
  retrosampler:
    storage_dir: $SCRATCH/buf
    window: $W
$floor_line
    disk_budget: $DISK_BUDGET
    watermark_pct: $WATERMARK_PCT
    shards: $SHARDS
    keep_on_error: true
    baseline_rate: $BASELINE_RATE
    bus:
      type: nats
      nats:
        url: nats://127.0.0.1:4222
        mode: durable

exporters:
  file:
    path: $SCRATCH/kept.json

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [retrosampler]
      exporters: [file]
  telemetry:
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 127.0.0.1
                port: 8888
YAML

echo "testbed: W=$W rate=${RATE_MBPS}MB/s duration=$DUR scratch=$SCRATCH"
GODEBUG=gctrace=1 bin/retrosamplercol --config "$SCRATCH/config.yaml" \
  2>"$SCRATCH/collector.log" &
COL_PID=$!
ready=0
for _ in $(seq 1 60); do
  nc -z 127.0.0.1 4317 2>/dev/null && nc -z 127.0.0.1 8888 2>/dev/null && {
    ready=1
    break
  }
  sleep 0.5
done
[[ "$ready" -eq 1 ]] || {
  echo "ENV: the collector never opened :4317 and :8888" >&2
  kill -0 "$COL_PID" 2>/dev/null || {
    col_status=0
    wait "$COL_PID" || col_status=$?
    echo "     the process is gone, exit $col_status" >&2
  }
  tail -20 "$SCRATCH/collector.log" >&2
  exit 2
}

# RSS is the one floor with no cumulative counter behind it, so it is
# sampled through the run and the floor takes the maximum: ADR-004 r3 asks
# for floors that hold for the whole run, not at a sampled instant. The
# shed counters ride along stamped, which is what turns a shed total into
# a WHEN - a burst while the pipeline warms reads very differently from
# one that starts once the buffer is full.
: >"$SCRATCH/samples.txt"
started=$(date +%s)
while :; do
  printf '# t=%ss\n' "$(($(date +%s) - started))" >>"$SCRATCH/samples.txt"
  curl -sf http://127.0.0.1:8888/metrics 2>/dev/null |
    grep -E '^otelcol_(process_memory_rss|processor_retrosampler_(shed_|pending_flushes))' \
      >>"$SCRATCH/samples.txt" || true
  sleep 5
done &
SAMPLER_PID=$!

# --- load ---
# --target-mbps is MEGABITS, hence the x8; the extra 1% is what makes the
# floor reachable. The generator sleeps whenever it is ahead of schedule
# and never makes the time back, so it converges on its target from below
# and lands a hair under it (measured: 19.998 against a 20.000 target).
# Paced 1% high, an achieved rate at or above RATE_MBPS is a real
# measurement rather than a rounding tolerance.
mbits=$(awk -v m="$RATE_MBPS" 'BEGIN {printf "%.6g\n", m * 8 * 1.01}')
load_ok=1
go run ./cmd/loadgen --endpoints 127.0.0.1:4317 \
  --duration "$DUR" --target-mbps "$mbits" \
  --spans-per-trace 4 --span-bytes 1024 --error-pct "$ERROR_PCT" --seed 7 \
  >"$SCRATCH/summary.json" 2>"$SCRATCH/loadgen.log" || load_ok=0

curl -sf http://127.0.0.1:8888/metrics >"$SCRATCH/metrics.txt" || {
  echo "ENV: the collector stopped answering :8888/metrics" >&2
  tail -20 "$SCRATCH/collector.log" >&2
  exit 2
}
{ kill "$SAMPLER_PID" && wait "$SAMPLER_PID"; } 2>/dev/null || true
SAMPLER_PID=""

# --- floors (ADR-004 r3) ---
# Series names are PINNED from a live :8888 dump, not derived from the
# metadata.yaml ids: the reader emits the dotted ids underscored and with
# no _total suffix on the monotonic sums, which no amount of reading
# metadata.yaml would tell you. Every series a floor reads has to be
# present, checked below - a name spelled differently here would read as 0
# and pass the shed floors while measuring nothing at all.
gauge_rss=otelcol_process_memory_rss
sheds=(
  otelcol_processor_retrosampler_shed_floor_protected
  otelcol_processor_retrosampler_shed_nothing_reclaimable
  otelcol_processor_retrosampler_shed_queue_full
)
counter_keeps=otelcol_processor_retrosampler_kept_local
gauge_window=otelcol_processor_retrosampler_effective_window_seconds
hist_age=otelcol_processor_retrosampler_flush_age_ratio
missing=()
# Anchored on the WHOLE name - the next character has to be the label brace
# or the value separator - because sum() and peak() match the name exactly
# and a bare prefix check does not. A reader that started appending _total
# or _bytes would satisfy "^name" while sum() found nothing, and the shed
# and rss floors would both go green over a series they never read.
for series in "$gauge_rss" "${sheds[@]}" "$counter_keeps" "$gauge_window"; do
  grep -qE "^$series[{ ]" "$SCRATCH/metrics.txt" || missing+=("$series")
done
# The histogram is the exception: its base name only ever appears carrying
# a _bucket, _sum or _count suffix, and it feeds a report rather than a
# floor, so an absence here prints nothing instead of passing something.
grep -q "^$hist_age" "$SCRATCH/metrics.txt" || missing+=("$hist_age")
[[ "${#missing[@]}" -eq 0 ]] || {
  echo "ENV: the collector emitted none of: ${missing[*]}" >&2
  echo "     (a floor over an absent series reads 0 and passes while measuring nothing)" >&2
  exit 2
}
grep -q '^gc [0-9]' "$SCRATCH/collector.log" || {
  echo "ENV: no gctrace lines in $SCRATCH/collector.log - GODEBUG did not take" >&2
  exit 2
}
# Only a complete loadgen run prints a summary, so an empty one means the
# load ended early. That is a FLOOR failure and not an environment error
# whenever the generator got as far as exporting: the refusal it hit is
# the overload ladder working, and what it reports is that this shape did
# not hold the rate. A generator that never sent a byte is environment.
have_summary=1
jq -e '.bytes_sent > 0 and .elapsed_seconds > 0' "$SCRATCH/summary.json" >/dev/null 2>&1 ||
  have_summary=0
if [[ "$have_summary" -eq 0 ]] && ! grep -q 'export to' "$SCRATCH/loadgen.log"; then
  echo "ENV: the loadgen never exported anything" >&2
  tail -20 "$SCRATCH/loadgen.log" >&2
  exit 2
fi

sum() { # $1 = series, $2 = file; total over every label set
  awk -v n="$1" '{s = $1; sub(/\{.*/, "", s); if (s == n) t += $2} END {printf "%.10g\n", t + 0}' "$2"
}
peak() { # $1 = series, $2 = file; maximum over every sample
  awk -v n="$1" '{s = $1; sub(/\{.*/, "", s); if (s == n && $2 + 0 > m) m = $2 + 0} END {printf "%.10g\n", m + 0}' "$2"
}

mbps=0
[[ "$have_summary" -eq 0 ]] ||
  mbps=$(jq -r '.bytes_sent / .elapsed_seconds / 1000000' "$SCRATCH/summary.json")
shed_total=0
for series in "${sheds[@]}"; do
  shed_total=$(awk -v a="$shed_total" -v b="$(sum "$series" "$SCRATCH/metrics.txt")" \
    'BEGIN {printf "%.10g\n", a + b}')
done
rss_max=$(peak "$gauge_rss" "$SCRATCH/samples.txt")
# Nothing above validates samples.txt, and peak() over an empty file is 0,
# which sails through a <= 4 GiB floor having measured no process at all.
awk -v r="$rss_max" 'BEGIN {exit !(r > 0)}' || {
  echo "ENV: no RSS samples in $SCRATCH/samples.txt - the sampler never read :8888" >&2
  echo "     (a zero here would pass the memory floor while measuring nothing)" >&2
  exit 2
}
rss_gib=$(awk -v r="$rss_max" 'BEGIN {printf "%.2f\n", r / 1073741824}')
keeps=$(sum "$counter_keeps" "$SCRATCH/metrics.txt")

# gctrace, Go 1.25:
#   gc 5 @0.207s 1%: 0.10+3.7+0.026 ms clock, 1.2+0.28/6.3/0+0.31 ms cpu, ...
# The clock triple is sweep-termination STW, concurrent mark wall, then
# mark-termination STW, so the pauses are the first and third numbers and
# the middle one is not a pause at all. The percentage is GC CPU SINCE
# PROGRAM START, cumulative, so the whole-run figure is the last line's -
# a maximum over a cumulative series would fail the run on a startup
# transient it has already amortized away.
maxpause=$(awk '/^gc [0-9]/ {
    if (match($0, /: [0-9.]+\+[0-9.]+\+[0-9.]+ ms clock/) == 0) next
    split(substr($0, RSTART + 2, RLENGTH - 11), p, "+")
    if (p[1] + 0 > m) m = p[1] + 0
    if (p[3] + 0 > m) m = p[3] + 0
  } END {printf "%.3f\n", m + 0}' "$SCRATCH/collector.log")
gccpu=$(awk '/^gc [0-9]/ {
    if (match($0, / [0-9.]+%:/)) v = substr($0, RSTART + 1, RLENGTH - 3) + 0
  } END {printf "%.10g\n", v + 0}' "$SCRATCH/collector.log")
gccycles=$(grep -c '^gc [0-9]' "$SCRATCH/collector.log")

fail=0
floor() { # $1 = name, $2 = value, $3 = unit, $4 = awk test over v, $5 = bound
  local verdict=OK
  awk -v v="$2" "BEGIN {exit !($4)}" || {
    verdict=FAIL
    fail=1
    echo "FLOOR: $1 = $2 $3, need $5" >&2
  }
  printf '  %-12s %12s %-6s %-12s %s\n' "$1" "$2" "$3" "$5" "$verdict"
}

echo
echo "=== ADR-004 r3 floors ==="
floor throughput "$mbps" MB/s "v >= $RATE_MBPS" ">= $RATE_MBPS"
floor sheds "$shed_total" events "v == 0" "== 0"
# The one floor most likely to fail is the one a single number explains
# least: a queue-full refusal is the shard workers falling behind, while
# floor_protected and nothing_reclaimable are the disk ladder out of room.
# Which of the three fired IS the question a reader has to answer before
# they can judge the ==0 bound against ADR-007's design.
if awk -v v="$shed_total" 'BEGIN {exit !(v > 0)}'; then
  for series in "${sheds[@]}"; do
    printf '    %-24s %s\n' "${series#otelcol_processor_retrosampler_}" \
      "$(sum "$series" "$SCRATCH/metrics.txt")"
  done
fi
floor rss "$rss_gib" GiB "v <= 4" "<= 4"
floor "gc pause" "$maxpause" ms "v < 10" "< 10"
floor "gc cpu" "$gccpu" % "v < 5" "< 5"
# Not a performance number: a run whose keep loop never fired would pass
# every floor above while proving nothing about the deployed shape.
floor keeps "$keeps" traces "v > 0" "> 0"
[[ "$load_ok" -eq 1 ]] || {
  echo "FLOOR: the load ended early - the collector refused its exports" >&2
  tail -3 "$SCRATCH/loadgen.log" >&2
  fail=1
}

echo
echo "=== observed, never a floor ==="
printf '  effective window   %s s of a configured W=%s\n' \
  "$(sum "$gauge_window" "$SCRATCH/metrics.txt")" "$W"
printf '  gc cycles          %s over %s\n' "$gccycles" "$DUR"
# The parked-intent population belongs next to the sheds because the two
# move together: an intent parks when the flush channel is full, and every
# parked one costs its shard a fresh whole-trace Collect on the next tick.
printf '  pending flushes    %s intents at the end of the run\n' \
  "$(sum otelcol_processor_retrosampler_pending_flushes "$SCRATCH/metrics.txt")"
awk '/^# t=/ {t = $2} /_pending_flushes/ {if ($2 + 0 > m) {m = $2 + 0; at = t}}
  END {if (m > 0) printf "  pending peak       %d intents at %s\n", m, at}' \
  "$SCRATCH/samples.txt"
# When the sheds happened, if they did: a burst while the pipeline warms
# reads very differently from one that accumulates once the buffer is
# full, and only a timeline spanning the WHOLE run tells those apart. The
# samples are decimated to about eight rows plus the last one - a head -N
# would have shown the first forty seconds of an eight-minute run and
# called it a shape.
if awk -v v="$shed_total" 'BEGIN {exit !(v > 0)}'; then
  awk '/^# t=/ {t = $2; if (!(t in seen)) {seen[t] = 1; order[++n] = t}}
    /_shed_/ {v[t] += $2}
    END {
      step = int(n / 8)
      if (step < 1) step = 1
      for (i = 1; i <= n; i += step) printf "  sheds by %-8s %s\n", order[i], v[order[i]] + 0
      if ((n - 1) % step != 0) printf "  sheds by %-8s %s\n", order[n], v[order[n]] + 0
    }' "$SCRATCH/samples.txt"
fi
# The ADR-011 r9 W instrument: mass gathering near 1.0 says keeps are only
# just beating expiry and W is too tight. Reported, never gated - the
# whole point is to close ADR-003 r3 (W=5m, open) on data.
awk -v n="$hist_age" '{
    s = $1; sub(/\{.*/, "", s)
    if (s == n"_count") count = $2
    if (s == n"_sum") sum = $2
    if (s != n"_bucket" || match($1, /le="[^"]*"/) == 0) next
    b[++i] = substr($1, RSTART + 4, RLENGTH - 5); c[i] = $2
  } END {
    printf "  flush.age.ratio    %d flushes, mean %.5f of W (cumulative buckets)\n",
      count, (count > 0 ? sum / count : 0)
    for (j = 1; j <= i; j++) printf "    le=%-6s %s\n", b[j], c[j]
  }' "$SCRATCH/metrics.txt"
[[ -z "${short_run:-}" ]] || echo "  CAVEAT: $short_run"
echo
echo "evidence: $SCRATCH/{summary.json,metrics.txt,samples.txt,collector.log}"
exit "$fail"
