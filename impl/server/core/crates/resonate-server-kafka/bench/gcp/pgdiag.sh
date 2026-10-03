#!/bin/bash
# Why does one node on Postgres plateau near 1,600 lifecycles/s? One change
# per step: pool size, commit path (diagnosis only), nodes. Postgres sampled.
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg
fresh_db() { local db=rb_$1_$(date +%H%M%S); $G sql databases create $db --instance=$PREFIX-pg >/dev/null; echo "postgres://postgres:$pgpw@$pghost:5432/$db"; }
flags() { $G sql instances patch $PREFIX-pg --database-flags="$1" >/dev/null 2>&1; for _ in $(seq 1 60); do [ "$($G sql instances describe $PREFIX-pg --format='value(state)')" = RUNNABLE ] && break; sleep 10; done; echo "flags: $1"; }
show() { grep -E '^--|^ops|node 1|errors' | cut -c1-150; }
phase() {  # phase NAME NODES POOL CLIENTS..
  local name=$1 nodes=$2 pool=$3; shift 3
  echo "## $name: $nodes node(s), pool $pool per node $(date -u +%T)"
  ./pgwatch.sh > /tmp/pgw.log 2>&1 & local w=$!
  PG_POOL=$pool PG_URL=$(fresh_db $name) BACKEND=postgres ROUTES=any ./ab.sh $nodes "$@" 2>&1 | show
  kill $w
  awk '/waits:/ {n++; split($0,a,"waits: "); split(a[2],b," top"); s=s" | "b[1]} END {print "   pg active/waits:" substr(s,1,600)}' /tmp/pgw.log
}
phase A 1 64 c32 c64 c128
phase B 1 256 c64 c128 c256
flags "max_connections=600,synchronous_commit=off"
phase C 1 256 c64 c128 c256
flags "max_connections=600"
./up.sh nodes 2 2>&1 | grep -E "ERROR|stockout|trying" | head -3
for _ in $(seq 1 40); do ssh_to $PREFIX-n2 $(zone_of 2) "systemctl is-enabled resonate" 2>/dev/null | grep -q enabled && break; sleep 10; done
phase D 2 128 c64 c128 c256
