#!/usr/bin/env bash
# Generate a scenario + canonical trace from each mutation's counterexample.
# Unified.tla is restored on exit, always.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"
mkdir -p "$HERE/scenarios"
# Serialise: two concurrent runs would race on Unified.tla and leave a
# mutation applied.  That happened once and silently poisoned the module.
exec 9>"$HERE/.mutate.lock"; flock -n 9 || { echo "another mutation run holds the lock"; exit 2; }
cp "$HERE/Unified.tla" "$HERE/.Unified.orig"
trap 'cp "$HERE/.Unified.orig" "$HERE/Unified.tla"; rm -f "$HERE/.Unified.orig"' EXIT

# gen <bug> <spec>...   -- several specs are applied together, for defects
# that are only observable in combination.
gen () {
  local bug="$1"; shift
  python3 - "$HERE" "$@" <<'PYX' || { echo "  !! ANCHOR DID NOT APPLY: $bug"; return 1; }
import sys
h=sys.argv[1]; src=open(h+"/.Unified.orig").read()
for spec in sys.argv[2:]:
    old,new=spec.split('|||')
    assert old in src, "anchor: "+old[:60]
    src=src.replace(old,new,1)
open(h+"/Unified.tla","w").write(src)
PYX
  timeout 600 java -XX:+UseParallelGC -Xmx3g -cp "$LIB/tla2tools.jar" tlc2.TLC -workers 4 \
      -dumpTrace json "/tmp/cex-$bug.json" -config "$HERE/MC.cfg" "$HERE/MC.tla" \
      > "/tmp/cex-$bug.out" 2>&1
  cp "$HERE/.Unified.orig" "$HERE/Unified.tla"
  if ! grep -q "is violated" "/tmp/cex-$bug.out"; then echo "  !! no violation: $bug"; return 1; fi
  ( cd "$HERE" && python3 gen-scenarios.py "/tmp/cex-$bug.out" "/tmp/cex-$bug.json" "$bug" )
}

gen halt-on-dead        '    /\ Live(i)                                  \* spec T-09|||    /\ TRUE'
gen dead-dispatch-claim '    /\ Live(i)                                  \* 409 -- spec T-02|||    /\ TRUE'
gen dead-redispatch     '    /\ Live(i)                    \* no new work for the dead (R6)|||    /\ TRUE'
gen stranded-listener   '    /\ External(aw)                             \* 422 external-only waiters
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners|||    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners'
gen resume-dead-awaiter 'Eligible(j)     == Live(j)|||Eligible(j)     == TRUE'
# resonate-pg BUG-2 as SHIPPED: task.create is ungated AND serves the raw
# row.  The two are not independent -- with the T-02 liveness guard in place
# the promise is live whenever task.create fires, so the raw row EQUALS the
# projection and serving it is unobservable.  Mutating only the response
# yields no violation; TLC searches the whole space and finds nothing.
gen unprojected-response '    /\ Live(i)                                  \* 409 -- spec T-02|||    /\ TRUE' \
'does.
    /\ RespondP(i, Proj(i))|||does.
    /\ RespondP(i, promises[i].state)'
