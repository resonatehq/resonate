#!/usr/bin/env bash
# Mutation experiments: remove one guard, check, report which property fails.
# See MUTATIONS.md.  Unified.tla is restored on exit, always.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"
cp "$HERE/Unified.tla" "$HERE/.Unified.orig"
trap 'cp "$HERE/.Unified.orig" "$HERE/Unified.tla"; rm -f "$HERE/.Unified.orig"' EXIT

mut () {
  python3 - "$2" "$HERE" <<'PYX'
import sys
h=sys.argv[2]; src=open(h+"/.Unified.orig").read()
old,new=sys.argv[1].split('||')
assert old in src, "anchor not found: "+old[:70]
open(h+"/Unified.tla","w").write(src.replace(old,new,1))
PYX
  timeout 900 java -XX:+UseParallelGC -Xmx3g -cp "$LIB/tla2tools.jar" \
       tlc2.TLC -workers 2 -config "$HERE/MC.cfg" "$HERE/MC.tla" > /tmp/mut.out 2>&1
  local prop depth
  prop=$(grep -oE "Invariant [A-Za-z]+ is violated" /tmp/mut.out | head -1 | awk '{print $2}')
  depth=$(grep -c "^State " /tmp/mut.out)
  [ -z "$prop" ] && { prop="SURVIVES -- no property caught it"; depth="-"; }
  printf "  %-44s %-28s %s\n" "$1" "$prop" "$depth"
}

printf "  %-44s %-28s %s\n" "MUTATION" "PROPERTY THAT FAILS" "STATES IN CEX"
mut "drop TIMEOUT-WINS from the cascade"  'Eligible(j)     == Live(j)||Eligible(j)     == TRUE'
mut "over-arm (arm every promise)"        '    /\ External(i)||    /\ TRUE'
mut "ungate task.halt"                    '    /\ Live(i)                                  \* spec T-09||    /\ TRUE'
mut "task.create serves the raw row"      '    /\ RespondP(i, Proj(i))                     \* responses are projected||    /\ RespondP(i, promises[i].state)'
mut "ungate task.continue"                '    /\ Live(i)                                  \* spec T-10||    /\ TRUE'
mut "drop external guard on callbacks"    '    /\ External(aw)                             \* 422 external-only waiters||    /\ TRUE'
