#!/bin/bash
# What makes Postgres contend: WAL per transaction, full-page images, HOT
# updates — and which change moves throughput. One node, closed loop.
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg; export PGPASSWORD=$pgpw
which psql >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1
q() { psql -h $pghost -U postgres -d "$1" -At -F' ' -c "$2"; }
echo "## build $(date -u +%T)"
./up.sh builder >/dev/null 2>&1; for _ in $(seq 1 30); do ssh_to $PREFIX-builder $(zone_of 1) true 2>/dev/null && break; sleep 10; done
SRC_TGZ=$HOME/src.tgz ./build.sh 2>&1 | tail -1
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &
for l in 1 2; do ssh_to $PREFIX-l$l $(zone_of $l) "sudo gcloud storage cp -q $BUCKET/bin/load /opt/resonate/load && sudo chmod +x /opt/resonate/load" & done; wait
SPREAD="--deadline-spread-ms 100000000000 --random-ids true"
phase() {  # phase NAME "SQL" LOAD-ARGS
  local name=$1 sql=$2 extra=$3 db=rb_w$1_$(date +%H%M%S)
  $G sql databases create $db --instance=$PREFIX-pg >/dev/null
  PG_POOL=64 PG_URL="postgres://postgres:$pgpw@$pghost:5432/$db" BACKEND=postgres ./run.sh start 1 >/dev/null
  [ -n "$sql" ] && q $db "set search_path to resonate; $sql" >/dev/null
  echo "## $name: ${sql:-no schema change} ${extra:+| $extra} $(date -u +%T)"
  for c in 64 128; do
    read -r r0 f0 b0 <<< "$(q postgres "select wal_records, wal_fpi, wal_bytes from pg_stat_wal")"
    x0=$(q $db "select xact_commit from pg_stat_database where datname = current_database()")
    read -r u0 h0 <<< "$(q $db "select n_tup_upd, n_tup_hot_upd from pg_stat_user_tables where relname = 'promises'")"
    ./pgwatch.sh > /tmp/pgw.log 2>&1 & w=$!
    out=$(ROUTE=any NODES=1 ./run.sh load w$name-c$c 2 --clients $c --seconds 30 --warmup 5 $extra 2>&1 | sed -n 2p | cut -c1-80)
    kill $w
    read -r r1 f1 b1 <<< "$(q postgres "select wal_records, wal_fpi, wal_bytes from pg_stat_wal")"
    x1=$(q $db "select xact_commit from pg_stat_database where datname = current_database()")
    read -r u1 h1 <<< "$(q $db "select n_tup_upd, n_tup_hot_upd from pg_stat_user_tables where relname = 'promises'")"
    awk -v c=$c -v o="$out" -v r=$((r1-r0)) -v f=$((f1-f0)) -v b0=$b0 -v b1=$b1 -v x=$((x1-x0)) -v u=$((u1-u0)) -v h=$((h1-h0)) 'BEGIN {
      b = b1 - b0; printf "-- %3d clients/loadgen: %s\n   per transaction: %.0f WAL bytes, %.1f records, %.2f full-page images; HOT updates %.0f%% of %d\n", c, o, b/x, r/x, f/x, (u ? 100*h/u : 0), u }'
    awk '/waits:/ {split($0,a,"waits: "); split(a[2],b," top"); s=s" | "b[1]} END {print "   waits:" substr(s,1,300)}' /tmp/pgw.log
  done
}
phase A "" ""
phase B "" "$SPREAD"
phase C "alter table promises set (fillfactor = 70)" "$SPREAD"
phase D "drop index idx_promises_callbacks" "$SPREAD"
phase E "drop index idx_promises_timeout_at" "$SPREAD"
phase F "alter table promises set (fillfactor = 70); drop index idx_promises_callbacks; drop index idx_promises_timeout_at" "$SPREAD"
