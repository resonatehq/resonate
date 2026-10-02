#!/bin/bash
# A failure experiment: load in the background, a fault at FAULT_AT seconds,
# its repair at HEAL_AT (both from the start of the load), then the takeover
# metrics of every node.
#
#   ./fail.sh NAME FAULT_AT "FAULT CMD" HEAL_AT "HEAL CMD" -- LOAD ARGS..
#   ./fail.sh kill-node 35 "kill-node 2" 65 "restart-node 2" -- --mode workflow --rate 600 --seconds 120
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
: "${NODES:=3}" "${LOADGENS:=2}"
name=$1 fault_at=$2 fault=$3 heal_at=$4 heal=$5; shift 6
mkdir -p results/$name
for i in $(seq 1 $NODES); do echo "n$i"; ./run.sh metrics $i || true; done > results/$name/metrics-before.txt 2>&1
start=$(date +%s)
NODES=$NODES ./run.sh load $name $LOADGENS "$@" > results/$name/load.log 2>&1 &
load=$!
at() { local t=$(( start + $1 - $(date +%s) )); [ $t -gt 0 ] && sleep $t; true; }
at $fault_at; echo "t+$(( $(date +%s) - start ))s fault: $fault"; ./run.sh $fault
at $heal_at;  echo "t+$(( $(date +%s) - start ))s heal: $heal";   ./run.sh $heal
wait $load
for i in $(seq 1 $NODES); do echo "n$i"; ./run.sh metrics $i || true; done > results/$name/metrics-after.txt 2>&1
cat results/$name/summary.txt
# seconds with failures or a p99 over 1 s, from load generator 1
awk '/^--- timeline/{t=1;next} t' results/$name/l1.txt | awk -F, 'NR==1||$3>0||$5>1000' | head -60
