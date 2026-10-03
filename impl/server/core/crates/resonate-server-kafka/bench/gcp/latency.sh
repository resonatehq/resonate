#!/bin/bash
# Pipelining and leader placement against the baseline: 3 nodes, 12
# partitions, owner routing; lifecycles at low and high load, workflows at
# low and high load. Each variant on a fresh topic.
cd "$(dirname "$0")"
show() { grep -E '^--|^ops|node 1|served|commit|verify|not all|errors|leaders placed' | cut -c1-170; }
for variant in "1 0" "4 0" "4 1"; do
  set -- $variant; depth=$1 place=$2
  echo "==== pipeline depth $depth, leaders placed: $place $(date -u +%T)"
  PIPELINE=$depth PLACE=$place PARTITIONS=12 ROUTES=owner ./ab.sh 3 2000 15000 2>&1 | show
  PIPELINE=$depth PLACE=$place PARTITIONS=12 ROUTES=owner MODE=workflow ./ab.sh 3 500 2000 2>&1 | show
done
