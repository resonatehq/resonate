#!/bin/bash
# Run on the controller VM: an experiment script, then publish its results to
# the bucket and, unless KEEP=1, tear the whole fleet down (this VM included).
#
#   nohup ./controller.sh NAME ./some-experiment.sh args.. > ctl.log 2>&1 &
#
# Results land in $BUCKET/results/NAME/ as they are written, then DONE.
set -uo pipefail
cd "$(dirname "$0")"; source env.sh
export SSH_FLAGS=--internal-ip
# The ssh key, once, before anything runs in parallel: concurrent first-time
# `gcloud compute ssh` calls race to create it and all but one fail at an
# interactive overwrite prompt.
[ -f ~/.ssh/google_compute_engine ] || ssh-keygen -t rsa -b 3072 -N "" -q -f ~/.ssh/google_compute_engine
name=$1; shift
dest=$BUCKET/results/$name
mkdir -p results
( while sleep 60; do gcloud storage rsync -r -q results $dest/results >/dev/null 2>&1; gcloud storage cp -q ctl.log $dest/ctl.log >/dev/null 2>&1; done ) &
sync=$!
"$@"; rc=$?
kill $sync
gcloud storage rsync -r -q results $dest/results
gcloud storage cp -q ctl.log $dest/ctl.log
echo "exit $rc $(date -u +%FT%TZ)" | gcloud storage cp - $dest/DONE
# A failed run keeps the fleet for inspection; a successful one tears it down.
[ "${KEEP:-0}" = 1 ] || [ $rc -ne 0 ] || ./down.sh
