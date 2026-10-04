#!/bin/bash
# Builds of the Postgres engine side by side: same fleet, same Cloud SQL
# instance, a fresh database per run, lifecycles and workflows, profiled
# (pgprof.sh): closed loops for the ceiling, fixed rates for latency.
#
#   ./pgab.sh LABEL=SRC.tgz ...   source tarballs (impl/server/core); the first
#                                 also provides the load generator.
# LIFECYCLE and WORKFLOW override the levels (pgprof.sh: N clients, rN per second).
cd "$(dirname "$0")"; source env.sh
labels=(); for spec in "$@"; do labels+=("${spec%%=*}"); done
echo "## builder $(date -u +%T)"
./up.sh builder >/dev/null 2>&1
for _ in $(seq 1 30); do ssh_to $PREFIX-builder $(zone_of 1) true 2>/dev/null && break; sleep 10; done
first=1
for spec in "$@"; do
  l=${spec%%=*} tgz=${spec#*=}
  if [ $first = 1 ]; then
    echo "## build $l (engine, load generator) $(date -u +%T)"
    SRC_TGZ=$tgz ./build.sh 2>&1 | tail -1
    $G storage cp -q $BUCKET/bin/resonate $BUCKET/bin/resonate-$l; first=0
  else
    echo "## build $l (engine) $(date -u +%T)"
    $G compute scp ${SSH_FLAGS:-} $tgz $PREFIX-builder:/tmp/src-$l.tgz --zone $(zone_of 1) >/dev/null
    ssh_to $PREFIX-builder $(zone_of 1) "set -e; source \$HOME/.cargo/env
      rm -rf ~/src-$l && mkdir -p ~/src-$l && tar -xzf /tmp/src-$l.tgz -C ~/src-$l && cd ~/src-$l/impl/server/core
      CARGO_TARGET_DIR=~/target-b cargo build --release -p resonate --bin resonate 2>&1 | tail -1
      gcloud storage cp -q ~/target-b/release/resonate $BUCKET/bin/resonate-$l"
  fi
  $G storage ls $BUCKET/bin/resonate-$l >/dev/null || { echo "build $l is missing"; exit 1; }
done
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &

echo "## node and loadgens $(date -u +%T)"
./up.sh nodes 1 2>&1 | grep -E 'unavailable|ERROR' | head -3
./up.sh loadgens 2 2>&1 | grep -E 'unavailable|ERROR' | head -3
for i in 1 2; do for _ in $(seq 1 40); do ssh_to $PREFIX-l$i $(zone_of $i) "test -x /opt/resonate/load" 2>/dev/null && break; sleep 10; done; done
for _ in $(seq 1 40); do ssh_to $PREFIX-n1 $(zone_of 1) "test -x /opt/resonate/resonate" 2>/dev/null && break; sleep 10; done
ssh_to $PREFIX-n1 $(zone_of 1) "sudo systemctl stop resonate" 2>/dev/null

# Interleaved, so drift in the instance (cache, checkpoints) hits every build.
for mode in lifecycle workflow; do
  levels=${LIFECYCLE:-8 32 128 r1000 r2000 r2500}; [ $mode = workflow ] && levels=${WORKFLOW:-8 32 r100 r200}
  for l in "${labels[@]}"; do
    MODE=$mode LABEL=$l RESONATE_BIN=resonate-$l ./pgprof.sh $levels
  done
done
./run.sh stop 1 >/dev/null 2>&1
echo "## summary"
grep -A1 -E '^## [a-z0-9-]+: (lifecycle|workflow)' ctl.log | grep -vE '^--$' | sed 's/ (2 loadgens), pool 64, db .*//' | paste - - | cut -c1-190
