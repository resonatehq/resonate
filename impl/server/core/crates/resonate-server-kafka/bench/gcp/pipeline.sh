#!/bin/bash
# On the controller: stand up the fleet, build, run an experiment, publish.
# (controller.sh around it publishes results and tears down.)
#   ./pipeline.sh NODES LOADGENS ./experiment.sh args..
set -uo pipefail
cd "$(dirname "$0")"; source env.sh
nodes=$1 loadgens=$2; shift 2
step() { echo "## $* $(date -u +%T)"; }
wait_for() {  # wait_for VM ZONE CHECK
  for _ in $(seq 1 60); do ssh_to "$1" "$2" "$3" >/dev/null 2>&1 && return 0; sleep 10; done
  echo "$1 never passed: $3"; ssh_to "$1" "$2" "sudo journalctl -u google-startup-scripts --no-pager | tail -25" 2>&1; return 1
}
step builder;  ./up.sh builder 2>&1 | grep -vE "^NAME|^$"
wait_for $PREFIX-builder $(zone_of 1) true || exit 1
step build;    SRC_TGZ=$HOME/src.tgz ./build.sh 2>&1 | tail -20; gcloud storage cat $BUCKET/bin/REVISION
$G compute instances delete $PREFIX-builder --zone $(zone_of 1) >/dev/null 2>&1 &
step brokers;  for t in 1 2 3; do ./up.sh brokers 2>&1 | grep -vE "^NAME|^$|Updated"; [ -s .brokers ] && break; sleep 30; done
for i in 1 2 3; do wait_for $PREFIX-b$i $(zone_of $i) "sudo docker ps | grep -q kafka" || exit 1; done
step nodes;    for t in 1 2 3; do ./up.sh nodes $nodes 2>&1 | grep -E "ERROR|stockout" || break; sleep 30; done
step loadgens; for t in 1 2 3; do ./up.sh loadgens $loadgens 2>&1 | grep -E "ERROR|stockout" || break; sleep 30; done
for i in $(seq 1 $nodes); do wait_for $PREFIX-n$i $(zone_of $i) "systemctl is-enabled resonate" || exit 1; done
for l in $(seq 1 $loadgens); do wait_for $PREFIX-l$l $(zone_of $l) "test -x /opt/resonate/load" || exit 1; done
step "fleet ready: $(cat .brokers)"
"$@"
