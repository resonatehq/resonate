#!/bin/bash
# 12 against 128 partitions, owner routing, at 3 and 6 nodes.
cd "$(dirname "$0")"
for n in 3 6; do for p in 12 128; do
  echo "==== $n nodes, $p partitions $(date -u +%T)"
  if [ $n = 3 ]; then rates="10000 12500 15000 17500"; else rates="15000 20000 25000 30000"; fi
  PARTITIONS=$p ROUTES=owner ./ab.sh $n $rates 2>&1 | grep -E '^--|^ops|node 1|served|commit|verify|not all' | cut -c1-170
done; done
