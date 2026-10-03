#!/bin/bash
# Soak: 40,000 req/s (20,000 lifecycles/s) on Redpanda for 70 minutes, a
# kill -9 of node 2 at minute 60 and a restart a minute later. Samples node
# CPU, RocksDB and broker disk every minute; summarizes latency per minute.
cd "$(dirname "$0")"; source env.sh
RATE=${RATE:-20000} SECS=${SECS:-4200} KILL_AT=${KILL_AT:-3600}
export TOPIC=soak-$(date +%H%M) PARTITIONS=12
export LIBRDKAFKA='"topic.metadata.refresh.interval.ms" = "10000"'
./run.sh start 3 | tail -1
mkdir -p results/soak
( while :; do
    line="$(date -u +%T)"
    for i in 1 2 3; do line+=" | n$i cpu $(ssh_to $PREFIX-n$i $(zone_of $i) "top -bn2 -d2 | grep '^%Cpu' | tail -1" 2>/dev/null | awk '{print 100-$8}')% rocks $(ssh_to $PREFIX-n$i $(zone_of $i) 'sudo du -sm /mnt/ssd/rocks 2>/dev/null' 2>/dev/null | cut -f1)MB"; done
    for i in 1 2 3; do line+=" | b$i $(ssh_to $PREFIX-b$i $(zone_of $i) 'sudo du -sm /var/lib/redpanda/data' 2>/dev/null | cut -f1)MB"; done
    echo "sample $line"; sleep 60
  done ) > results/soak/samples.txt 2>&1 &
sampler=$!
start=$(date +%s)
echo "## load: $RATE lifecycles/s ($((RATE*2)) req/s) for ${SECS}s $(date -u +%T)"
ROUTE=owner NODES=3 ./run.sh load soak 2 --rate $RATE --seconds $SECS --warmup 30 --retry-ms 30000 --max-inflight 400000 > results/soak/load.log 2>&1 &
load=$!
sleep $(( KILL_AT - ($(date +%s) - start) ))
echo "## kill node 2 at t+$(( $(date +%s) - start ))s"; ./run.sh kill-node 2
sleep 60
echo "## restart node 2 at t+$(( $(date +%s) - start ))s"; ./run.sh restart-node 2
wait $load; kill $sampler
echo "## result"; cat results/soak/summary.txt
echo "## takeovers after the kill"; for i in 1 2 3; do echo "n$i: $(./run.sh metrics $i 2>/dev/null | grep -E 'takeovers_total|replayed|takeover_seconds_(sum|count)|partitions_served' | tr '\n' ' ')"; done
echo "## per minute (load generator 1: ops ok, failed, worst-second p99, max)"
awk '/^--- timeline/{t=1;next} t && NR>1' results/soak/l1.txt | awk -F, 'NR>1 {m=int($1/60); ok[m]+=$2; f[m]+=$3; if ($5>p[m]) p[m]=$5; if ($6>x[m]) x[m]=$6} END {for (m in ok) printf "min %3d  ok %8d  failed %6d  p99 %8.1f ms  max %8.1f ms\n", m, ok[m], f[m], p[m], x[m]}' | sort -k2 -n
echo "## seconds around the kill"
awk '/^--- timeline/{t=1;next} t' results/soak/l1.txt | awk -F, -v k=$KILL_AT 'NR>1 && $1>=k-10 && $1<=k+150 && ($1%5==0 || $3>0)' | head -60
echo "## samples"; cat results/soak/samples.txt
