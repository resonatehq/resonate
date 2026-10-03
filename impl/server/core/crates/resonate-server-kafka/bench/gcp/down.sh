#!/bin/bash
# Delete every instance this benchmark created (label $PREFIX=1). The bucket
# stays, with the results; `./down.sh all` removes it too.
set -euo pipefail
cd "$(dirname "$0")"; source env.sh
$G compute instances list --filter="labels.$PREFIX=1" --format='value(name,zone.basename())' |
  while read -r name zone; do echo "deleting $name"; $G compute instances delete "$name" --zone "$zone" & done
wait
$G sql instances describe $PREFIX-pg >/dev/null 2>&1 && { echo "deleting $PREFIX-pg"; $G sql instances delete $PREFIX-pg; }
[ "${1:-}" = all ] && gcloud storage rm -r "$BUCKET" --project "$PROJECT"
true
