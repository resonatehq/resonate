#!/bin/bash
# Is the ~1,700 ops/s ceiling Resonate's or Cloud SQL's? pgbench (no Resonate
# at all) and the Resonate lifecycle load, first on the HA instance, then on a
# zonal instance of the same tier and storage (no synchronous standby).
# The zonal instance is created here and deleted at the end.
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg; export PGPASSWORD=$pgpw
which psql >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1
ZI=$PREFIX-pgz
trap '$G sql instances delete $ZI >/dev/null 2>&1; echo "## deleted $ZI $(date -u +%T)"' EXIT

echo "## create zonal $ZI in the background $(date -u +%T)"
( $G sql instances describe $ZI >/dev/null 2>&1 || $G sql instances create $ZI \
    --database-version=POSTGRES_16 --edition=ENTERPRISE --tier=$PG_TIER \
    --zone=$(zone_of 1) --availability-type=ZONAL --storage-type=SSD --storage-size=$PG_STORAGE_GB \
    --network=default --no-assign-ip --database-flags=max_connections=400 >/tmp/pgz.log 2>&1
  $G sql users set-password postgres --instance=$ZI --password=$pgpw >>/tmp/pgz.log 2>&1 ) &
zjob=$!

# pgbench on loadgen 1; Debian ships it with the server package.
ssh_to $PREFIX-l1 $(zone_of 1) "command -v pgbench >/dev/null || ls /usr/lib/postgresql/*/bin/pgbench >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client postgresql >/dev/null 2>&1; sudo systemctl stop postgresql 2>/dev/null; true"

pgb() {  # pgb HOST LABEL
  local host=$1 label=$2 db=pgb_$(date +%H%M%S)
  psql -h $host -U postgres -d postgres -qc "create database $db" >/dev/null
  psql -h $host -U postgres -d $db -q >/dev/null <<'SQL'
create table tiny (id bigserial primary key, v int);
-- shaped like a promise row: text key, ~600 B, two secondary indexes
create table prom (id text primary key, state text, timeout_at bigint, payload text, callbacks text[]);
create index on prom (timeout_at) where state = 'pending';
create index on prom using gin (callbacks);
SQL
  ssh_to $PREFIX-l1 $(zone_of 1) "cat > /tmp/tiny.sql <<'EOF'
insert into tiny (v) values (1);
EOF
cat > /tmp/prom.sql <<'EOF'
\set k random(1, 1000000000)
begin;
insert into prom values ('p' || :client_id || '-' || :k, 'pending', 9000000000000, repeat('x', 600), '{}') on conflict do nothing;
update prom set state = 'resolved' where id = 'p' || :client_id || '-' || :k;
commit;
EOF"
  echo "## pgbench $label ($host) $(date -u +%T)"
  for s in tiny prom; do for c in 16 64 128 256; do
    out=$(ssh_to $PREFIX-l1 $(zone_of 1) "export PGPASSWORD=$pgpw PATH=\$PATH:\$(dirname \$(ls /usr/lib/postgresql/*/bin/pgbench | tail -1)); pgbench -h $host -U postgres -n -M prepared -c $c -j 8 -T 20 -f /tmp/$s.sql $db 2>&1" | awk '/^tps/ {t=$3} /latency average/ {l=$4} END {printf "%8.0f tps  mean %6.2f ms", t, l}')
    echo "-- $s c=$c: $out"
  done; done
  echo "-- tiny c=128, synchronous_commit=off: $(ssh_to $PREFIX-l1 $(zone_of 1) "export PGPASSWORD=$pgpw PGOPTIONS='-c synchronous_commit=off' PATH=\$PATH:\$(dirname \$(ls /usr/lib/postgresql/*/bin/pgbench | tail -1)); pgbench -h $host -U postgres -n -M prepared -c 128 -j 8 -T 20 -f /tmp/tiny.sql $db 2>&1" | awk '/^tps/ {t=$3} /latency average/ {l=$4} END {printf "%8.0f tps  mean %6.2f ms", t, l}')"
  psql -h $host -U postgres -d postgres -qc "drop database $db" >/dev/null
}

lifecycle() {  # lifecycle HOST LABEL: Resonate on one node, closed loop
  local host=$1 label=$2 db=rb_f_$(date +%H%M%S)
  psql -h $host -U postgres -d postgres -qc "create database $db" >/dev/null
  PG_POOL=64 PG_URL="postgres://postgres:$pgpw@$host:5432/$db" BACKEND=postgres ./run.sh start 1 >/dev/null
  echo "## resonate lifecycles $label ($host) $(date -u +%T)"
  for c in 32 64 128; do
    echo "-- $c clients/loadgen: $(ROUTE=any NODES=1 ./run.sh load f$label-c$c 2 --clients $c --seconds 30 --warmup 5 --deadline-spread-ms 100000000000 --random-ids true 2>&1 | sed -n 2p | cut -c1-110)"
  done
}

pgb $pghost HA
lifecycle $pghost HA

wait $zjob; cat /tmp/pgz.log | grep -vE '^Creating|^$' | tail -3
zhost=$($G sql instances describe $ZI --format='value(ipAddresses[0].ipAddress)')
[ -n "$zhost" ] || { echo "zonal instance has no address"; exit 1; }
echo "## zonal ready: $zhost $(date -u +%T)"
pgb $zhost ZONAL
lifecycle $zhost ZONAL
./run.sh stop 2 >/dev/null 2>&1
