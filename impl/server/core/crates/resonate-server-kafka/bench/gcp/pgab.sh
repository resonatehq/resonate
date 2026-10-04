#!/bin/bash
# Two builds of the Postgres engine, same fleet, same Cloud SQL instance, a
# fresh database per run: lifecycles and workflows, profiled (pgprof.sh).
#
#   ./pgab.sh A.tgz B.tgz       source tarballs (impl/server/core); A also
#                               provides the load generator. Labels: A_LABEL, B_LABEL.
cd "$(dirname "$0")"; source env.sh
a=$1 b=$2 al=${A_LABEL:-a} bl=${B_LABEL:-b}
echo "## builder $(date -u +%T)"
./up.sh builder >/dev/null 2>&1
for _ in $(seq 1 30); do ssh_to $PREFIX-builder $(zone_of 1) true 2>/dev/null && break; sleep 10; done
echo "## build $al (engine, load generator) $(date -u +%T)"
SRC_TGZ=$a ./build.sh 2>&1 | tail -1
$G storage cp -q $BUCKET/bin/resonate $BUCKET/bin/resonate-$al
echo "## build $bl (engine) $(date -u +%T)"
$G compute scp ${SSH_FLAGS:-} $b $PREFIX-builder:/tmp/src-b.tgz --zone $(zone_of 1) >/dev/null
ssh_to $PREFIX-builder $(zone_of 1) "set -e; source \$HOME/.cargo/env
  rm -rf ~/src-b && mkdir -p ~/src-b && tar -xzf /tmp/src-b.tgz -C ~/src-b && cd ~/src-b/impl/server/core
  CARGO_TARGET_DIR=~/target-b cargo build --release -p resonate --bin resonate 2>&1 | tail -1
  gcloud storage cp -q ~/target-b/release/resonate $BUCKET/bin/resonate-$bl"
$G storage ls $BUCKET/bin/resonate-$al $BUCKET/bin/resonate-$bl || { echo "a build is missing"; exit 1; }
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &

echo "## node and loadgens $(date -u +%T)"
./up.sh nodes 1 2>&1 | grep -E 'unavailable|ERROR' | head -3
./up.sh loadgens 2 2>&1 | grep -E 'unavailable|ERROR' | head -3
for i in 1 2; do for _ in $(seq 1 40); do ssh_to $PREFIX-l$i $(zone_of $i) "test -x /opt/resonate/load" 2>/dev/null && break; sleep 10; done; done
for _ in $(seq 1 40); do ssh_to $PREFIX-n1 $(zone_of 1) "test -x /opt/resonate/resonate" 2>/dev/null && break; sleep 10; done
ssh_to $PREFIX-n1 $(zone_of 1) "sudo systemctl stop resonate" 2>/dev/null

# Interleaved, so drift in the instance (cache, checkpoints) hits both builds.
for mode in lifecycle workflow; do
  for l in $al $bl; do
    levels="8 32 128"; [ $mode = workflow ] && levels="8 32"
    MODE=$mode LABEL=$l RESONATE_BIN=resonate-$l ./pgprof.sh $levels
  done
done
./run.sh stop 1 >/dev/null 2>&1
echo "## summary"
grep -h -A1 '^## ' ctl.log 2>/dev/null | grep -E '^## (a|b|'"$al|$bl"')|ops/s|wf/s' | sed 's/, pool 64, db .*//' | paste - - | cut -c1-200
