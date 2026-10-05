#!/bin/bash
# Configurations of the Postgres engine side by side: same fleet, same Cloud
# SQL instance, a fresh database per run, lifecycles and workflows, profiled
# (pgprof.sh): closed loops for the ceiling, fixed rates for latency.
#
#   LOADGEN_TGZ=K.tgz ./pgab.sh LABEL[:LANES]=SRC.tgz ...
#
# Each SRC.tgz (impl/server/core) is built once, however many labels share it;
# LANES sets its group commit lanes. LOADGEN_TGZ provides the load generator.
# TEST_TGZ (and TEST_LANES) run pgtest.sh on the builder meanwhile.
# LIFECYCLE and WORKFLOW override the levels (pgprof.sh: N clients, rN per second).
cd "$(dirname "$0")"; source env.sh
z=$(zone_of 1)
echo "## builder $(date -u +%T)"
./up.sh builder >/dev/null 2>&1
for _ in $(seq 1 30); do ssh_to $PREFIX-builder $z true 2>/dev/null && break; sleep 10; done
echo "## build load generator $(date -u +%T)"
SRC_TGZ=$LOADGEN_TGZ ./build.sh 2>&1 | tail -1
declare -A bin
for spec in "$@"; do
  tgz=${spec#*=}; b=$(basename $tgz .tgz)
  [ -n "${bin[$tgz]:-}" ] && continue; bin[$tgz]=$b
  echo "## build engine $b $(date -u +%T)"
  $G compute scp ${SSH_FLAGS:-} $tgz $PREFIX-builder:/tmp/$b.tgz --zone $z >/dev/null
  ssh_to $PREFIX-builder $z "set -e; source \$HOME/.cargo/env
    rm -rf ~/$b && mkdir -p ~/$b && tar -xzf /tmp/$b.tgz -C ~/$b && cd ~/$b/impl/server/core
    # Its own target directory: the sources carry their commit's mtime, so in
    # a shared one cargo can find another tree's newer build fresh and reuse it.
    CARGO_TARGET_DIR=~/target-$b cargo build --release -p resonate --bin resonate 2>&1 | tail -1
    echo \"sha256 \$(sha256sum ~/target-$b/release/resonate | cut -c1-16)\"
    gcloud storage cp -q ~/target-$b/release/resonate $BUCKET/bin/resonate-$b"
  $G storage ls $BUCKET/bin/resonate-$b >/dev/null || { echo "build $b is missing"; exit 1; }
done

if [ -n "${TEST_TGZ:-}" ]; then
  echo "## correctness suites on the builder, lanes ${TEST_LANES:-0 1 4} $(date -u +%T)"
  $G compute scp ${SSH_FLAGS:-} pgtest.sh $PREFIX-builder:~/pgtest.sh --zone $z >/dev/null
  ssh_to $PREFIX-builder $z "src=~/$(basename $TEST_TGZ .tgz); DIFF=${TEST_DIFF:-1} nohup bash ~/pgtest.sh \$src ${TEST_LANES:-0 1 4} > ~/pgtest.out 2>&1 < /dev/null &"
fi

echo "## node and loadgens $(date -u +%T)"
./up.sh nodes 1 2>&1 | grep -E 'unavailable|ERROR' | head -3
./up.sh loadgens 2 2>&1 | grep -E 'unavailable|ERROR' | head -3
for i in 1 2; do for _ in $(seq 1 40); do ssh_to $PREFIX-l$i $(zone_of $i) "test -x /opt/resonate/load" 2>/dev/null && break; sleep 10; done; done
for _ in $(seq 1 40); do ssh_to $PREFIX-n1 $z "test -x /opt/resonate/resonate" 2>/dev/null && break; sleep 10; done
ssh_to $PREFIX-n1 $z "sudo systemctl stop resonate" 2>/dev/null

# Interleaved, so drift in the instance (cache, checkpoints) hits every configuration.
for mode in lifecycle workflow; do
  levels=${LIFECYCLE:-8 32 128 r1000 r2000 r2500 r3500}; [ $mode = workflow ] && levels=${WORKFLOW:-8 32 r100 r200 r300}
  for spec in "$@"; do
    label=${spec%%=*} tgz=${spec#*=}; lanes=; [[ $label == *:* ]] && lanes=${label#*:} label=${label%%:*}
    MODE=$mode LABEL=$label RESONATE_BIN=resonate-${bin[$tgz]} PG_BATCH_LANES=$lanes ./pgprof.sh $levels
  done
done
./run.sh stop 1 >/dev/null 2>&1

if [ -n "${TEST_TGZ:-}" ]; then
  echo "## waiting for the correctness suites $(date -u +%T)"
  for _ in $(seq 1 120); do ssh_to $PREFIX-builder $z "pgrep -f \"[p]gtest.sh\" >/dev/null" 2>/dev/null || break; sleep 30; done
  echo "## correctness"; ssh_to $PREFIX-builder $z "cat ~/pgtest.out; tar -czf /tmp/pgtest.tgz -C ~ pgtest" 2>/dev/null
  mkdir -p results/pgtest && $G compute scp ${SSH_FLAGS:-} $PREFIX-builder:/tmp/pgtest.tgz results/pgtest/ --zone $z >/dev/null 2>&1
fi
$G compute instances delete $PREFIX-builder --zone $z >/dev/null 2>&1

echo "## summary"
grep -A1 -E '^## [a-z0-9-]+: (lifecycle|workflow)' ctl.log | grep -vE '^--$' | sed 's/ (2 loadgens), pool 64, db .*//' | paste - - | cut -c1-190
