#!/usr/bin/env bash
# Check the model.
#
#   ./check.sh                        Abstract, every invariant expected to hold
#   ./check.sh Concrete               Concrete, including the refinement
#   ./check.sh Abstract WheelComplete one name, to watch it fail
#
# TLC is the checker. Apalache could not reach depth 6 of this machine in 25
# minutes -- its cost roughly triples per step and a create-then-settle is
# about ten transitions -- while TLC covers the whole reachable state space in
# seconds. Apalache still runs first, on types only: that pass is what forced
# the alphabet into a variant and split `None` into the four different
# questions it was hiding.
set -uo pipefail
cd "$(dirname "$0")"

TLA_TOOLS=${TLA_TOOLS:-tla2tools.jar}
APALACHE=${APALACHE:-apalache-mc}

if command -v "$APALACHE" >/dev/null 2>&1; then
    echo "== types =="
    "$APALACHE" typecheck Abstract.tla 2>&1 | grep -E "purrfect|error|\[FAILED\]" || true
else
    echo "== types == (skipped: $APALACHE not on PATH)"
fi

if [ ! -f "$TLA_TOOLS" ]; then
    echo "tla2tools.jar not found. Set TLA_TOOLS, or:"
    echo "  curl -sSLO https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar"
    exit 1
fi

SPEC=${1:-Abstract}
echo "== $SPEC =="
if [ $# -le 1 ]; then
    CFG=$SPEC.cfg
else
    CFG=$(mktemp /tmp/one-XXXX.cfg)
    sed -n "1,/MaxVersion/p" "$SPEC.cfg" > "$CFG"
    # a T_ or CT_ prefix marks a wrapped .trans entry, which TLC reads as a
    # temporal property; Refinement is one too. Everything else is a state
    # invariant.
    case "$2" in
        T_*|CT_*|Refinement) echo "PROPERTY $2"  >> "$CFG" ;;
        *)                   echo "INVARIANT $2" >> "$CFG" ;;
    esac
fi
# -metadir keeps TLC's state pool and fingerprints OUT of the repo. They
# are large, they churn, and a stray `rm -rf` of build output inside the
# tree will kill a running check by deleting the pool underneath it.
java -XX:+UseParallelGC -cp "$TLA_TOOLS" tlc2.TLC \
     -config "$CFG" -workers 4 -deadlock \
     -metadir "${TLC_METADIR:-/tmp/tlc-$SPEC}" "$SPEC.tla"
