#!/bin/bash
# Kafka against Cloud SQL Postgres at equal cost: the same 3 nodes, load and
# harness. Kafka: 12 partitions, owner routing, no pipelining. Postgres: HA
# (priced like the 3 brokers), round-robin clients, a fresh database per run.
cd "$(dirname "$0")"; source env.sh
show() { grep -E '^--|^ops|node 1|served|server mean|verify|not all|errors|started' | cut -c1-170; }
# Kafka's ramp, and Postgres's: a request is ~12 ms on HA Cloud SQL, so the
# pool (PG_POOL per node) caps it far below Kafka's ceiling.
life="${LIFE:-5000 10000 15000 20000 25000}"; wf="${WF:-1000 2000 3000}"
pg_life="${PG_LIFE:-1000 2000 3000 4000 5000}"; pg_wf="${PG_WF:-250 500 750 1000}"
export PG_POOL=${PG_POOL:-64}
[ -s .pg ] || ./up.sh postgres || { echo "postgres setup failed"; exit 1; }
read -r pghost pgpw < .pg
fresh_db() {
  local db=rb_$(date +%H%M%S)
  $G sql databases create $db --instance=$PREFIX-pg >/dev/null
  echo "postgres://postgres:$pgpw@$pghost:5432/$db"
}
# Phases to run (default all): kafka-life postgres-life kafka-wf postgres-wf
phases=${*:-kafka-life postgres-life kafka-wf postgres-wf}
for phase in $phases; do
  echo "==== $phase $(date -u +%T)"
  case $phase in
    kafka-life)    PARTITIONS=12 ROUTES=owner ./ab.sh 3 $life 2>&1 | show ;;
    postgres-life) PG_URL=$(fresh_db) BACKEND=postgres ROUTES=any ./ab.sh 3 $pg_life 2>&1 | show ;;
    kafka-wf)      PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 3 $wf 2>&1 | show ;;
    postgres-wf)   PG_URL=$(fresh_db) BACKEND=postgres ROUTES=any MODE=workflow ./ab.sh 3 $pg_wf 2>&1 | show ;;
    kafka-life-low) PARTITIONS=12 ROUTES=owner ./ab.sh 3 $pg_life 2>&1 | show ;;
    kafka-wf-low)   PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 3 $pg_wf 2>&1 | show ;;
  esac
done
