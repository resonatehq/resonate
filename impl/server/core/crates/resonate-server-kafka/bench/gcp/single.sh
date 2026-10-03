#!/bin/bash
# One Resonate node on each backend: closed-loop ramp (clients per loadgen),
# then open-loop rates; lifecycles and workflows. Postgres is sampled while
# its phases run (pgwatch.sh).
cd "$(dirname "$0")"; source env.sh
show() { grep -E '^--|^ops|node 1|server mean|verify|not all|errors|started' | cut -c1-170; }
[ -s .pg ] || ./up.sh postgres || { echo "postgres setup failed"; exit 1; }
read -r pghost pgpw < .pg
fresh_db() { local db=rb_$(date +%H%M%S); $G sql databases create $db --instance=$PREFIX-pg >/dev/null; echo "postgres://postgres:$pgpw@$pghost:5432/$db"; }
export PG_POOL=${PG_POOL:-64}
pg() {  # pg MODE RATES..: a Postgres phase, sampled
  local mode=$1; shift
  ./pgwatch.sh > /tmp/pgwatch.log 2>&1 & local w=$!
  PG_URL=$(fresh_db) BACKEND=postgres ROUTES=any MODE=$mode ./ab.sh 1 "$@" 2>&1 | show
  kill $w; echo "-- postgres samples (every 5 s):"; cut -c1-230 /tmp/pgwatch.log | sed 's/^/   /'
}
echo "==== postgres, 1 node, lifecycles $(date -u +%T)"; pg lifecycle c4 c16 c64 250 500 1000 2000
echo "==== kafka, 1 node, lifecycles $(date -u +%T)";    PARTITIONS=12 ROUTES=owner ./ab.sh 1 c4 c16 c64 1000 2000 5000 10000 2>&1 | show
echo "==== postgres, 1 node, workflows $(date -u +%T)";  pg workflow c4 c16 c64 100 250 500
echo "==== kafka, 1 node, workflows $(date -u +%T)";     PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 1 c4 c16 c64 250 500 1000 2000 2>&1 | show
