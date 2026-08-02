#!/usr/bin/env bash
# Replay every adapted trace through THE model.
#
#   ./validate-traces.sh
#
# There is no profile to choose.  The model is the specification, so a trace
# either replays -- the recorded execution was conformant -- or it does not,
# and the run names the event where it stopped.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"
OUT="$HERE/output"; mkdir -p "$OUT"

fail=0
for t in "$HERE"/traces/*.ndjson; do
  name="$(basename "$t" .ndjson)"
  read -r ids addrs workers retry ttl ttls <<<"$(python3 - "$t" <<'PYX'
import json,sys
lines=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
c=lines[0]["config"]
def st(xs): return "{"+",".join('"%s"'%x for x in xs)+"}"
vals={int(c["ttl"])}
for d in lines:
    if d.get("tag")!="trace": continue
    for tk in d["event"]["state"].get("tasks",{}).values():
        if "ttl" in tk: vals.add(int(tk["ttl"]))
print(st(c["ids"]), st(c["addrs"]), st(c.get("pids") or ["w1"]),
      c["retry"], c["ttl"], "{"+",".join(str(v) for v in sorted(vals))+"}")
PYX
)"

  cat > "$HERE/UTrace.cfg" <<CFG
SPECIFICATION TraceSpec
CONSTANTS
    Ids     = $ids
    Addrs   = $addrs
    Workers = $workers
    MaxTime    = 64
    MaxVersion = 16
    Retry = $retry
    Ttl   = $ttl
    TTLs  = $ttls
    FaultsOn = TRUE
    TraceFile = "$t"
CHECK_DEADLOCK FALSE
INVARIANT NotReplayed
CFG

  java -XX:+UseParallelGC -cp "$LIB/tla2tools.jar:$LIB/CommunityModules-deps.jar" \
       tlc2.TLC -config "$HERE/UTrace.cfg" -workers 1 "$HERE/UTrace.tla" \
       > "$OUT/trace-$name.out" 2>&1

  events=$(python3 -c "import json;print(sum(1 for l in open('$t') if l.strip() and json.loads(l)['tag']=='trace'))")
  # a full replay VIOLATES NotReplayed -- see the header of UTrace.tla
  if grep -q "Invariant NotReplayed is violated" "$OUT/trace-$name.out"; then
    printf "  %-28s CONFORMANT      %2s events\n" "$name" "$events"
  else
    printf "  %-28s NOT CONFORMANT  %2s events -- output/trace-%s.out\n" "$name" "$events" "$name"
    fail=1
  fi
done
exit $fail
