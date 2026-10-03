#!/bin/bash
# The plan of every statement Postgres ran in database $1 (from
# pg_stat_statements), flagging any sequential scan on promises; and the
# table's scan counters.
cd "$(dirname "$0")"; read -r h p < .pg; export PGPASSWORD=$p; db=$1
psql -h $h -U postgres -d $db -qAt -c "create extension if not exists pg_stat_statements" >/dev/null
echo "table counters in $db:"
psql -h $h -U postgres -d $db -At -F' ' -c "select 'promises seq_scan', seq_scan, 'seq_tup_read', seq_tup_read, 'idx_scan', idx_scan, 'rows', n_live_tup from pg_stat_user_tables where relname = 'promises'"
psql -h $h -U postgres -d $db -At -c "select queryid from pg_stat_statements where dbid = (select oid from pg_database where datname = '$db') and query ~* '(promises|schedules)' and query !~* '(pg_stat|explain|create extension)' order by calls desc" > /tmp/qids
scans=0; n=0
while read -r qid; do
  n=$((n+1))
  psql -h $h -U postgres -d $db -At -c "select query from pg_stat_statements where queryid = $qid limit 1" > /tmp/q.sql
  calls=$(psql -h $h -U postgres -d $db -At -c "select calls||' calls, '||round(mean_exec_time::numeric,2)||' ms' from pg_stat_statements where queryid = $qid limit 1")
  { echo "set search_path to resonate, public;"; echo "EXPLAIN (GENERIC_PLAN, COSTS OFF)"; cat /tmp/q.sql; } > /tmp/e.sql
  plan=$(psql -h $h -U postgres -d $db -f /tmp/e.sql 2>&1)
  head=$(tr -s ' \n' ' ' < /tmp/q.sql | cut -c1-90)
  if echo "$plan" | grep -q 'Seq Scan on promises'; then scans=$((scans+1)); echo "  SEQ SCAN  [$calls] $head"; echo "$plan" | grep -B2 -A2 'Seq Scan on promises' | sed 's/^/      /' | head -8
  elif echo "$plan" | grep -q ERROR; then echo "  (no plan) [$calls] $head"
  else echo "  ok        [$calls] $head"; fi
done < /tmp/qids
echo "statements audited: $n, with a sequential scan on promises: $scans"
