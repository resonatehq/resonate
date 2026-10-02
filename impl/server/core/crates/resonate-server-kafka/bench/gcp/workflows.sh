#!/bin/bash
# 12 against 128 partitions, owner routing, workflows (3 local steps and a
# remote one, 13 requests each), at 3 and 6 nodes.
cd "$(dirname "$0")"
for n in 3 6; do for p in 12 128; do
  echo "==== workflows, $n nodes, $p partitions $(date -u +%T)"
  if [ $n = 3 ]; then rates="1000 1500 2000 2500"; else rates="2000 3000 4000 5000"; fi
  MODE=workflow PARTITIONS=$p ROUTES=owner ./ab.sh $n $rates 2>&1 | grep -E '^--|^ops|node 1|served|commit|verify|not all|errors' | cut -c1-170
done; done
