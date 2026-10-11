#!/bin/bash
# One profiling run on this machine: N server nodes against a local Kafka,
# the load generator at a fixed open-loop rate, and, over a window in the
# middle of the run, CPU per request for every process plus a perf profile of
# node 1. Writes everything to $OUT/$NAME and prints the scorecard line.
#
#   NAME=base PARTITIONS=24 RATE=2000 ./prof.sh
#
# Environment (defaults in brackets):
#   NAME        run name [run]               OUT       results dir [./results]
#   NODES       server nodes [2]             PARTITIONS  [24]
#   MODE        lifecycle|workflow [lifecycle]
#   RATE        operations per second [1000] SECONDS   measured seconds [40]
#   ROUTE       any|owner [any]              WINDOW    profiled seconds [15]
#   BROKERS     [127.0.0.1:9092]             BIN       dir with server, load
#   PIPELINE    pipeline_depth [1]           LIBRDKAFKA  'key = "value"' lines
#   PERF        perf binary                  FREQ      perf sample rate [999]
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
NAME=${NAME:-run} OUT=${OUT:-$here/results} NODES=${NODES:-2} PARTITIONS=${PARTITIONS:-24}
MODE=${MODE:-lifecycle} RATE=${RATE:-1000} SECONDS_=${SECONDS:-40} ROUTE=${ROUTE:-any}
WINDOW=${WINDOW:-15} BROKERS=${BROKERS:-127.0.0.1:9092} PIPELINE=${PIPELINE:-1}
BIN=${BIN:?BIN must name the directory holding the server and load binaries}
PERF=${PERF:-$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)}
FREQ=${FREQ:-999}
dir=$OUT/$NAME
rm -rf "$dir" && mkdir -p "$dir"
topic="p$(date +%s)"   # fresh topics and group per run: no replay, no history

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

for i in $(seq 1 "$NODES"); do
  nd=$dir/node$i && mkdir -p "$nd"
  printf '[servers.server_kafka.librdkafka]\n%b\n' "${LIBRDKAFKA:-}" > "$nd/resonate.toml"
  ( cd "$nd" && exec env \
      RESONATE_SERVERS__ACTIVE=server_kafka \
      RESONATE_SERVERS__SERVER_KAFKA__BROKERS="$BROKERS" \
      RESONATE_SERVERS__SERVER_KAFKA__PARTITIONS="$PARTITIONS" \
      RESONATE_SERVERS__SERVER_KAFKA__REPLICATION_FACTOR=1 \
      RESONATE_SERVERS__SERVER_KAFKA__TOPIC_PREFIX="$topic" \
      RESONATE_SERVERS__SERVER_KAFKA__NODE_ID="n$i" \
      RESONATE_SERVERS__SERVER_KAFKA__PEER_BIND="127.0.0.$i:8002" \
      RESONATE_SERVERS__SERVER_KAFKA__PEER_URL="http://127.0.0.$i:8002" \
      RESONATE_SERVERS__SERVER_KAFKA__DATA_DIR="$nd/rocks" \
      RESONATE_SERVERS__SERVER_KAFKA__BLOCK_CACHE_MB=256 \
      RESONATE_SERVERS__SERVER_KAFKA__PIPELINE_DEPTH="$PIPELINE" \
      RESONATE_GATEWAYS__GATEWAY_HTTP__BIND="127.0.0.$i:8001" \
      RESONATE_GATEWAYS__GATEWAY_METRICS__BIND="127.0.0.$i:9090" \
      RESONATE_LEVEL=warn \
      "$BIN/server" > "$nd/server.log" 2>&1 ) &
  pids+=($!)
done

# Ready when every partition is served by someone.
served() {
  local s=0
  for i in $(seq 1 "$NODES"); do
    v=$(curl -sf "127.0.0.$i:9090/metrics" | awk '/^resonate_kafka_partitions_served /{print $2}') || v=0
    s=$((s + ${v:-0}))
  done
  echo $s
}
for _ in $(seq 1 120); do [ "$(served)" -ge "$PARTITIONS" ] && break; sleep 1; done
[ "$(served)" -ge "$PARTITIONS" ] || { echo "partitions never all served" >&2; exit 1; }

urls=$(for i in $(seq 1 "$NODES"); do printf 'http://127.0.0.%s:8001,' "$i"; done); urls=${urls%,}
metrics=$(for i in $(seq 1 "$NODES"); do printf 'http://127.0.0.%s:9090/metrics,' "$i"; done); metrics=${metrics%,}
args=(--url "$urls" --metrics "$metrics" --mode "$MODE" --rate "$RATE" --seconds "$SECONDS_"
      --warmup 5 --route "$ROUTE" --verify true --timeline "$dir/timeline.csv")
[ "$ROUTE" = owner ] && args+=(--brokers "$BROKERS" --topic-prefix "$topic" --partitions "$PARTITIONS"
                              --gateway-port 8001 --group "$topic")
[ "$MODE" = workflow ] && args+=(--worker-bind 127.0.0.1:7001 --worker-url http://127.0.0.1:7001/)
"$BIN/load" "${args[@]}" > "$dir/load.txt" 2>&1 &
lpid=$!

# The window: after warm-up and settling, well before the end.
sleep $((5 + 8))
node_pids="${pids[*]}"
broker=$(pgrep -f 'kafka\.Kafka' | head -1)
ticks() { awk '{print $14 + $15}' "/proc/$1/stat"; }
count() {  # gateway requests answered, all nodes
  for i in $(seq 1 "$NODES"); do curl -sf "127.0.0.$i:9090/metrics"; done |
    awk '/^resonate_request_duration_seconds_count/{s+=$2} END{print s+0}'
}
declare -A t0
for p in $node_pids $broker $lpid; do t0[$p]=$(ticks "$p"); done
c0=$(count); w0=$(date +%s.%N)

n1=$(echo "$node_pids" | awk '{print $1}')
"$PERF" record -F "$FREQ" -g -p "$n1" -o "$dir/perf.data" -- sleep "$WINDOW" > /dev/null 2>&1 &
perfpid=$!
"$PERF" stat -x, -e task-clock,context-switches,cpu-migrations,page-faults,raw_syscalls:sys_enter \
  -p "$(echo $node_pids | tr ' ' ,)" -o "$dir/perfstat.csv" -- sleep "$WINDOW" > /dev/null 2>&1 || true
wait $perfpid || true

c1=$(count); w1=$(date +%s.%N)
hz=$(getconf CLK_TCK)
{
  echo "window_s $(echo "$w1 - $w0" | bc)"
  echo "requests $((c1 - c0))"
  for p in $node_pids; do echo "cpu node $p $(( $(ticks "$p") - ${t0[$p]} ))"; done
  echo "cpu broker $broker $(( $(ticks "$broker") - ${t0[$broker]} ))"
  echo "cpu load $lpid $(( $(ticks "$lpid") - ${t0[$lpid]} ))"
  echo "hz $hz"
} > "$dir/cpu.txt"

wait $lpid || true
"$PERF" script -i "$dir/perf.data" -F comm,tid,ip,sym,dso 2> /dev/null > "$dir/perf.script" || true
python3 "$here/score.py" "$dir" | tee "$dir/score.txt"
