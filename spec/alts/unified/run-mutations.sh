#!/usr/bin/env bash
# Mutation experiments.  One per defect an implementation actually has: the
# mutation reproduces that defect IN THE SPECIFICATION, and the run reports
# which property catches it.  See MUTATIONS.md.
# Unified.tla is restored on exit, always.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"
# Serialise: two concurrent runs would race on Unified.tla and leave a
# mutation applied.  That happened once and silently poisoned the module.
exec 9>"$HERE/.mutate.lock"; flock -n 9 || { echo "another mutation run holds the lock"; exit 2; }
cp "$HERE/Unified.tla" "$HERE/.Unified.orig"
trap 'cp "$HERE/.Unified.orig" "$HERE/Unified.tla"; rm -f "$HERE/.Unified.orig"' EXIT

mut () {
  if ! python3 - "$2" "$HERE" <<'PYX'
import sys
h=sys.argv[2]; src=open(h+"/.Unified.orig").read()
old,new=sys.argv[1].split('|||')
assert src.count(old)>=1, "anchor not found: "+old[:70]
open(h+"/Unified.tla","w").write(src.replace(old,new,1))
PYX
  then
    # CRITICAL: without this the stale Unified.tla from the PREVIOUS mutation
    # is still in place and TLC would report that mutant's verdict under this
    # mutation's name.  An unapplied mutation is an error, never a result.
    printf "  %-38s %-32s %s\n" "$1" "!! ANCHOR DID NOT APPLY" "-"
    cp "$HERE/.Unified.orig" "$HERE/Unified.tla"
    return
  fi
  timeout 600 java -XX:+UseParallelGC -Xmx3g -cp "$LIB/tla2tools.jar" \
       tlc2.TLC -workers 4 -config "$HERE/MC.cfg" "$HERE/MC.tla" > /tmp/mut-$3.out 2>&1
  local prop depth
  prop=$(grep -oE "Invariant [A-Za-z]+ is violated" /tmp/mut-$3.out | head -1 | awk '{print $2}')
  depth=$(grep -c "^State " /tmp/mut-$3.out)
  [ -z "$prop" ] && { prop="!! SURVIVES -- nothing caught it"; depth="-"; }
  printf "  %-38s %-32s %s\n" "$1" "$prop" "$depth"
}

printf "  %-38s %-32s %s\n" "MUTATION (= a real defect)" "PROPERTY THAT CATCHES IT" "CEX"
mut "resume a dead awaiter"        'Eligible(j)     == Live(j)|||Eligible(j)     == TRUE' m1
mut "under-arm: only targeted"     '    \* accepted would strand that obligation.
    /\ External(i)|||    \* MUTANT: under-arm
    /\ Targeted(i)' m2
mut "over-arm: arm everything"     '    \* accepted would strand that obligation.
    /\ External(i)|||    \* MUTANT: over-arm
    /\ TRUE' m3
mut "listener on internal promise" '    /\ External(aw)                             \* 422 external-only waiters
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners|||    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners' m4
mut "callback on internal promise" '    /\ \A j \in S : External(j)                  \* 422 external-only waiters|||    /\ TRUE' m5
mut "ungate task.create claim"     '    /\ Live(i)                                  \* 409 -- spec T-02|||    /\ TRUE' m6
mut "ungate task.halt"             '    /\ Live(i)                                  \* spec T-09|||    /\ TRUE' m7
mut "ungate task.continue"         '    /\ Live(i)                                  \* spec T-10|||    /\ TRUE' m8
mut "redispatch a dead task (R6)"  '    /\ Live(i)                    \* no new work for the dead (R6)|||    /\ TRUE' m9
mut "task.create serves raw row"   '    /\ RespondP(i, Proj(i))|||    /\ RespondP(i, promises[i].state)' m10
