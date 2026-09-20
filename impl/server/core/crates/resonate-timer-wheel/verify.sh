#!/usr/bin/env bash
# Verify the wheel: the spec, the proofs, and the tie to the executable code.
#   ./verify.sh              everything
#   ./verify.sh wheel        one module (spec | proof | wheel | comparator)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
VERUS="${VERUS:-$HOME/verus-build/verus/source/target-verus/release/verus}"
ARGS=(--crate-type=lib src/lib.rs --multiple-errors 12 --triggers-mode silent)
[ $# -gt 0 ] && ARGS+=(--verify-only-module "$1")
exec "$VERUS" "${ARGS[@]}"
