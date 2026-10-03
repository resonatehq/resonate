#!/bin/bash
# The suspended-awaiters fix, and group commit: build, differential, workflow
# ramp, lifecycle ramp with commit_delay off and on, plan audit.
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg
fresh_db() { local db=rb_$1_$(date +%H%M%S); $G sql databases create $db --instance=$PREFIX-pg >/dev/null; echo "postgres://postgres:$pgpw@$pghost:5432/$db"; }
flags() { $G sql instances patch $PREFIX-pg --database-flags="$1" >/dev/null 2>&1; for _ in $(seq 1 60); do [ "$($G sql instances describe $PREFIX-pg --format='value(state)')" = RUNNABLE ] && break; sleep 10; done; echo "flags: $1"; }
show() { grep -E '^--|^ops|node 1|server mean|verify|not all|errors|started' | cut -c1-170; }
export PG_POOL=${PG_POOL:-64}
echo "## build $(date -u +%T)"
./up.sh builder >/dev/null 2>&1; for _ in $(seq 1 30); do ssh_to $PREFIX-builder $(zone_of 1) true 2>/dev/null && break; sleep 10; done
SRC_TGZ=$HOME/src.tgz ./build.sh 2>&1 | tail -1
echo "## differential $(date -u +%T)"
url=$(fresh_db diff)
ssh_to $PREFIX-builder $(zone_of 1) "source \$HOME/.cargo/env; cd ~/src/impl/server/core && TEST_POSTGRES_URL='$url' CARGO_TARGET_DIR=~/target cargo test --release --test differential 2>&1 | grep -E 'test result|FAILED|panicked' | head -5"
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &
echo "## workflows, 1 node $(date -u +%T)"
PG_URL=$(fresh_db wf) BACKEND=postgres ROUTES=any MODE=workflow ./ab.sh 1 c4 c16 c64 100 250 500 1000 2>&1 | show
echo "## lifecycles, commit_delay off $(date -u +%T)"
PG_URL=$(fresh_db life) BACKEND=postgres ROUTES=any ./ab.sh 1 c4 c16 c64 c128 2>&1 | show
flags "max_connections=400,commit_delay=500,commit_siblings=5"
echo "## lifecycles, commit_delay 500 us $(date -u +%T)"
PG_URL=$(fresh_db gc) BACKEND=postgres ROUTES=any ./ab.sh 1 c4 c16 c64 c128 2>&1 | show
echo "## workflows, commit_delay 500 us $(date -u +%T)"
PG_URL=$(fresh_db wfgc) BACKEND=postgres ROUTES=any MODE=workflow ./ab.sh 1 c16 c64 250 500 2>&1 | show
flags "max_connections=400"
echo "## plan audit $(date -u +%T)"
for db in $($G sql databases list --instance=$PREFIX-pg --format='value(name)' | grep -E '^rb_wf_' | tail -1); do ./pgaudit.sh $db | grep -E 'counters|seq_scan|SEQ SCAN|audited|^      '; done
