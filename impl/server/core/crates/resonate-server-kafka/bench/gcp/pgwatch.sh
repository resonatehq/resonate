#!/bin/bash
# While it runs: every 5 s, what Postgres's client connections wait on and
# what the busiest active statements are. Uses .pg; prints compact lines.
cd "$(dirname "$0")"
read -r host pw < .pg
which psql >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1
export PGPASSWORD=$pw
while :; do
  w=$(psql -h $host -U postgres -d postgres -At -F' ' -c "select count(*), coalesce(wait_event_type,'CPU')||'/'||coalesce(wait_event,'run') from pg_stat_activity where backend_type='client backend' and state='active' group by 2 order by 1 desc limit 4" 2>&1 | tr '\n' ',')
  q=$(psql -h $host -U postgres -d postgres -At -F' ' -c "select count(*), left(regexp_replace(query,'\s+',' ','g'),60) from pg_stat_activity where backend_type='client backend' and state='active' group by 2 order by 1 desc limit 2" 2>&1 | tr '\n' ',')
  echo "pg $(date -u +%T) waits: $w top: $q"
  sleep 5
done
