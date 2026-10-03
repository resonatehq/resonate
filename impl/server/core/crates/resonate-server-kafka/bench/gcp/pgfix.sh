#!/bin/bash
# After the full-scan fix in the Postgres backend: build it, check it against
# the oracle on Cloud SQL, rerun the single-node Postgres phases, and show the
# plans of the statements that scanned.
cd "$(dirname "$0")"; source env.sh
read -r pghost pgpw < .pg
fresh_db() { local db=rb_$1_$(date +%H%M%S); $G sql databases create $db --instance=$PREFIX-pg >/dev/null; echo "postgres://postgres:$pgpw@$pghost:5432/$db"; }
echo "## build $(date -u +%T)"
./up.sh builder >/dev/null 2>&1; for _ in $(seq 1 30); do ssh_to $PREFIX-builder $(zone_of 1) true 2>/dev/null && break; sleep 10; done
SRC_TGZ=$HOME/src.tgz ./build.sh 2>&1 | tail -2
echo "## differential against the oracle, on Cloud SQL $(date -u +%T)"
url=$(fresh_db diff)
ssh_to $PREFIX-builder $(zone_of 1) "source \$HOME/.cargo/env; cd ~/src/impl/server/core && TEST_POSTGRES_URL='$url' TEST_BACKENDS=postgres CARGO_TARGET_DIR=~/target cargo test --release --test differential 2>&1 | grep -E 'test result|FAILED|panicked|diverge|mismatch' | head -20"
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &
echo "## postgres, 1 node, after the fix $(date -u +%T)"
export PG_POOL=${PG_POOL:-64}
show() { grep -E '^--|^ops|node 1|server mean|verify|not all|errors|started' | cut -c1-170; }
./pgwatch.sh > /tmp/pgwatch.log 2>&1 & w=$!
PG_URL=$(fresh_db life) BACKEND=postgres ROUTES=any ./ab.sh 1 c4 c16 c64 250 500 1000 2000 5000 2>&1 | show
PG_URL=$(fresh_db wf) BACKEND=postgres ROUTES=any MODE=workflow ./ab.sh 1 c4 c16 c64 100 250 500 1000 2>&1 | show
kill $w; echo "-- postgres samples:"; cut -c1-200 /tmp/pgwatch.log | sed 's/^/   /' | tail -20
echo "## plans and scans $(date -u +%T)"
bash /tmp/pgscan.sh 2>/dev/null | grep -E 'promises' | tail -2
