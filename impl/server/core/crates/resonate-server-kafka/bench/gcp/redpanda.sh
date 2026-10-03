#!/bin/bash
# The Kafka half of the Kafka/Postgres comparison, on Redpanda: 3 nodes, 12
# partitions, owner routing, no pipelining; lifecycles and workflows.
cd "$(dirname "$0")"; source env.sh
show() { grep -E '^--|^ops|node 1|server mean|verify|not all|errors|started' | cut -c1-170; }
ssh_to $PREFIX-b1 $(zone_of 1) "sudo docker exec redpanda rpk cluster info 2>&1 | head -12; sudo docker exec redpanda rpk redpanda admin brokers list 2>&1 | head -5"
echo "==== redpanda: lifecycles $(date -u +%T)"
PARTITIONS=12 ROUTES=owner ./ab.sh 3 1000 2000 5000 10000 15000 20000 25000 2>&1 | show
echo "==== redpanda: workflows $(date -u +%T)"
PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 3 250 500 1000 2000 3000 2>&1 | show
