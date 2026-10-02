#!/bin/bash
# Owner routing against round-robin: the same rates on the same topic, back
# to back, with node 1's CPU sampled mid-run.  ./ab.sh NODES RATE...
# PARTITIONS, ROUTES (default "any owner") and MODE (default lifecycle) select the rest.
set -uo pipefail
cd "$(dirname "$0")"; source env.sh
n=$1; shift
export LIBRDKAFKA="${LIBRDKAFKA:-\"topic.metadata.refresh.interval.ms\" = \"10000\"}" TOPIC=ab$n-p$PARTITIONS-$(date +%H%M)
./run.sh start $n | tail -1
for _ in $(seq 1 12); do
  served=0; for i in $(seq 1 $n); do s=$(./run.sh metrics $i 2>/dev/null | awk '/partitions_served/{print $2}'); served=$((served + ${s:-0})); done
  [ $served -eq $PARTITIONS ] && break; sleep 5
done
echo "partitions served: $served"; [ $served -eq $PARTITIONS ] || { echo "not all partitions served"; exit 1; }
cpu() { ssh_to $PREFIX-n1 $(zone_of 1) "sleep 18; top -bn2 -d5 | grep '^%Cpu' | tail -1" | awk '{print 100-$8}'; }
for r in "$@"; do for route in ${ROUTES:-any owner}; do
  cpu > /tmp/ab-cpu &
  echo "-- $n nodes, $r ops/s, route=$route"
  ROUTE=$route NODES=$n ./run.sh load ab$n-p$PARTITIONS-${MODE:-lifecycle}-$route-$r 2 --mode ${MODE:-lifecycle} --rate $r --seconds 30 --warmup 5 --max-inflight 30000 2>&1 | sed -n 2,4p | cut -c1-170
  wait; echo "   node 1 CPU: $(cat /tmp/ab-cpu)%"
done; done
