#!/usr/bin/env bash
# Check the model.
#
#   ./check.sh                  every invariant expected to hold
#   ./check.sh WheelComplete    one named invariant or property, to see it fail
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
    "$APALACHE" typecheck Resonate.tla   2>&1 | grep -E "purrfect|error|\[FAILED\]" || true
    "$APALACHE" typecheck MCResonate.tla 2>&1 | grep -E "purrfect|error|\[FAILED\]" || true
else
    echo "== types == (skipped: $APALACHE not on PATH)"
fi

if [ ! -f "$TLA_TOOLS" ]; then
    echo "tla2tools.jar not found. Set TLA_TOOLS, or:"
    echo "  curl -sSLO https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar"
    exit 1
fi

echo "== model =="
if [ $# -eq 0 ]; then
    CFG=MCResonate.cfg
else
    CFG=$(mktemp /tmp/one-XXXX.cfg)
    sed -n "1,/MaxVersion/p" MCResonate.cfg > "$CFG"
    # a name with a T_ prefix is a wrapped .trans entry, which TLC reads as a
    # temporal property; everything else is a state invariant
    case "$1" in
        T_*) echo "PROPERTY $1"  >> "$CFG" ;;
        *)   echo "INVARIANT $1" >> "$CFG" ;;
    esac
fi
java -XX:+UseParallelGC -cp "$TLA_TOOLS" tlc2.TLC \
     -config "$CFG" -workers 4 -deadlock MCResonate.tla
