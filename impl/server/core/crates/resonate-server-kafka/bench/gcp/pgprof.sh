#!/bin/bash
# Where a Resonate request's time goes inside Postgres. One node, closed loop,
# fresh database per concurrency level. For each level: every statement from
# pg_stat_statements (calls per transaction, mean time, rows, WAL), what the
# client connections are doing once a second (running, waiting on what, or
# idle in a transaction while the server thinks), the lock waits with their
# blockers, and the node's CPU and thread count.
#
#   ./pgprof.sh [CLIENTS..]     default 8 32 128; MODE=workflow for workflows,
#                               LABEL names the results, RESONATE_BIN the build
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg; export PGPASSWORD=$pgpw
which psql >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1
q() { psql -h $pghost -U postgres -d "$1" -At -F' | ' -c "$2"; }

sampler() {  # sampler DB: once a second, the state of every client connection of DB
  while :; do
    q postgres "select to_char(now(),'HH24:MI:SS'),
      count(*) filter (where state='active' and wait_event is null) as cpu,
      count(*) filter (where state='active' and wait_event is not null) as waiting,
      count(*) filter (where state like 'idle in transaction%') as idle_in_tx,
      count(*) filter (where state='idle') as idle,
      coalesce(string_agg(distinct wait_event_type||'/'||wait_event, ',') filter (where state='active' and wait_event is not null),'-')
      from pg_stat_activity where datname='$1' and backend_type='client backend'" | sed 's/^/S | /'
    q postgres "select 'L', w.pid, l.locktype, coalesce(l.relation::regclass::text,''), l.mode,
        left(regexp_replace(w.query,'\s+',' ','g'),70) as waiter,
        left(regexp_replace(b.query,'\s+',' ','g'),70) as blocker, b.state as blocker_state
      from pg_stat_activity w
      join pg_locks l on l.pid = w.pid and not l.granted
      cross join lateral unnest(pg_blocking_pids(w.pid)) bp
      join pg_stat_activity b on b.pid = bp
      where w.datname='$1' limit 5"
    sleep 1
  done
}

profile() {  # profile CLIENTS_PER_LOADGEN
  local c=$1 db=rb_${LABEL:-prof}_c$1_$(date +%H%M%S) mode=${MODE:-lifecycle}
  $G sql databases create $db --instance=$PREFIX-pg >/dev/null
  PG_POOL=64 PG_URL="postgres://postgres:$pgpw@$pghost:5432/$db" BACKEND=postgres ./run.sh start 1 >/dev/null
  q $db "create extension if not exists pg_stat_statements" >/dev/null
  # warm-up outside the window, then reset and measure
  ROUTE=any NODES=1 ./run.sh load ${LABEL:-prof}-warm-c$c 2 --mode $mode --clients $c --seconds 5 --warmup 1 --deadline-spread-ms 100000000000 --random-ids true >/dev/null 2>&1
  q $db "drop table if exists pss0; create table pss0 as select queryid, calls, total_exec_time, rows, shared_blks_hit, wal_bytes from pg_stat_statements where dbid=(select oid from pg_database where datname=current_database())" >/dev/null
  read -r x0 r0 <<< "$(q $db "select xact_commit, xact_rollback from pg_stat_database where datname=current_database()" | tr -d '|')"
  sampler $db > /tmp/samples-c$c.txt 2>&1 & local s=$!
  ( ssh_to $PREFIX-n1 $(zone_of 1) "sleep 8; top -bn2 -d5 | grep '^%Cpu' | tail -1; ps -o nlwp= -p \$(pgrep -f '/opt/resonate/resonate serve' | head -1)" > /tmp/node-c$c.txt 2>&1 ) &
  echo "## ${LABEL:-prof}: $mode, $c clients per loadgen (2 loadgens), pool 64, db $db $(date -u +%T)"
  echo "   $(ROUTE=any NODES=1 ./run.sh load ${LABEL:-prof}-$mode-c$c 2 --mode $mode --clients $c --seconds 25 --warmup 0 --deadline-spread-ms 100000000000 --random-ids true 2>&1 | sed -n 2p | cut -c1-120)"
  kill $s; wait
  read -r x1 r1 <<< "$(q $db "select xact_commit, xact_rollback from pg_stat_database where datname=current_database()" | tr -d '|')"
  local x=$((x1-x0))
  echo "   transactions: $x committed, $((r1-r0)) rolled back"
  echo "   node 1: cpu $(head -1 /tmp/node-c$c.txt | awk '{print 100-$8}')%, threads $(tail -1 /tmp/node-c$c.txt | tr -d ' ')"
  echo "   connection states, mean over $(grep -c '^S' /tmp/samples-c$c.txt) samples (of 64 pooled):"
  awk -F' [|] ' '/^S/ {n++; cpu+=$3; w+=$4; it+=$5; id+=$6; for (i=1;i<=split($7,e,",");i++) if (e[i]!="-") ev[e[i]]++}
    END {printf "     on cpu %.1f  waiting %.1f  idle in transaction %.1f  idle %.1f\n", cpu/n, w/n, it/n, id/n;
         for (k in ev) printf "     wait seen in %3.0f%% of samples: %s\n", 100*ev[k]/n, k}' /tmp/samples-c$c.txt | sort -k5 -rn
  echo "   lock waits (waiter <- blocker), most frequent:"
  grep '^L' /tmp/samples-c$c.txt | awk -F' [|] ' '{print $3" "$4" "$5" | "$6" <- ["$8"] "$7}' | sort | uniq -c | sort -rn | head -6 | sed 's/^/     /'
  echo "   statements by time in the window (per transaction = calls / $x):"
  # the window's share of each statement: now minus the snapshot before it
  local d="with d as (select s.query, s.calls-coalesce(o.calls,0) calls, s.total_exec_time-coalesce(o.total_exec_time,0) t,
      s.rows-coalesce(o.rows,0) r, s.shared_blks_hit-coalesce(o.shared_blks_hit,0) h, s.wal_bytes-coalesce(o.wal_bytes,0) w
    from pg_stat_statements s left join pss0 o using (queryid)
    where s.dbid=(select oid from pg_database where datname=current_database()) and s.query !~* 'pg_stat|pg_database|pss0')"
  q $db "$d select round(100*t/sum(t) over ()) || '%', round(calls::numeric/$x, 2) || '/tx', round((t/greatest(calls,1))::numeric, 2) || ' ms',
      round(r::numeric/greatest(calls,1), 1) || ' rows', round(h::numeric/greatest(calls,1)) || ' hit', round(w::numeric/greatest(calls,1)) || ' walB',
      left(regexp_replace(query,'\s+',' ','g'), 110) from d where calls > 0 order by t desc limit 14" | sed 's/^/     /'
  echo "   total statement time per transaction: $(q $db "$d select round((sum(t)/$x)::numeric, 2) || ' ms over ' || round(sum(calls)::numeric/$x, 1) || ' statements' from d")"
  cp /tmp/samples-c$c.txt results/samples-${LABEL:-prof}-$mode-c$c.txt 2>/dev/null
}

mkdir -p results
for c in ${@:-8 32 128}; do profile $c; done
