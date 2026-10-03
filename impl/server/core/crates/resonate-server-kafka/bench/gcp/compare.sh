#!/bin/bash
# Kafka against Cloud SQL Postgres at equal cost: the same 3 nodes, load and
# harness. Kafka: 12 partitions, owner routing, no pipelining. Postgres: HA
# (priced like the 3 brokers), round-robin clients, a fresh database per run.
cd "$(dirname "$0")"; source env.sh
show() { grep -E '^--|^ops|node 1|served|server mean|verify|not all|errors|started' | cut -c1-170; }
life="5000 10000 15000 20000 25000"; wf="1000 2000 3000"
[ -s .pg ] || ./up.sh postgres
read -r pghost pgpw < .pg
fresh_db() {
  local db=rb_$(date +%H%M%S)
  $G sql databases create $db --instance=$PREFIX-pg >/dev/null
  echo "postgres://postgres:$pgpw@$pghost:5432/$db"
}
echo "==== kafka: lifecycles $(date -u +%T)"
PARTITIONS=12 ROUTES=owner ./ab.sh 3 $life 2>&1 | show
echo "==== postgres: lifecycles $(date -u +%T)"
PG_URL=$(fresh_db) BACKEND=postgres ROUTES=any ./ab.sh 3 $life 2>&1 | show
echo "==== kafka: workflows $(date -u +%T)"
PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 3 $wf 2>&1 | show
echo "==== postgres: workflows $(date -u +%T)"
PG_URL=$(fresh_db) BACKEND=postgres ROUTES=any MODE=workflow ./ab.sh 3 $wf 2>&1 | show
