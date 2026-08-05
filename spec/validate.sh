#!/usr/bin/env bash
#
# validate.sh — check a recorded Resonate run against the Lean specification.
#
#   cat trace.ndjson | ./validate.sh
#   ./validate.sh < trace.ndjson
#   ./validate.sh < trace.ndjson 50000        # raise the candidate cap
#
# Reads the trace on stdin. There is no FILE argument: `< file` already
# does that, and one way in beats two.
#
# Input is NDJSON, one event per line, exactly as `capture.py` tees it:
#
#   {"kind":"promise.create","now":1000,"req":{...},"res":{"kind":...,"head":{...},"data":{...}}}
#
# `req` is the data the client sent, `res` is the whole envelope the server
# returned. Internal steps (timeouts firing, resumes draining) are NOT in
# the file and must not be — the checker's job is to work out whether some
# schedule of them explains what you did see.
#
# Exit codes:
#   0  ADMISSIBLE    some execution the specification permits has these events
#   1  REFUTED       none does; the offending event is printed
#   2  usage or parse error
#   3  INCONCLUSIVE  the search hit a bound, or the trace uses schedules
#
# So a sweep reads:
#
#   for f in traces/*.ndjson; do
#     ./validate.sh < "$f" || echo "FAILED: $f"
#   done
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
spec="$here/spec"
bin="$spec/.lake/build/bin/checktrace"

export PATH="${HOME}/.elan/bin:/root/.elan/bin:$PATH"

if ! command -v lake >/dev/null 2>&1; then
  echo "validate.sh: lake not found. Install Lean via elan:" >&2
  echo "  curl https://elan.lean-lang.org/elan-init.sh -sSf | sh" >&2
  exit 2
fi

# Build only when a source is newer than the binary. `lake` is fast when
# there is nothing to do, but not free, and this script is meant to sit in
# a pipe.
newest_src="$(find "$spec/06-trace" "$spec/01-protocol" "$spec/03-concrete" \
                   "$spec/04-theorems" -name '*.lean' -newer "$bin" -print -quit 2>/dev/null || true)"
if [[ ! -x "$bin" || -n "$newest_src" ]]; then
  echo "validate.sh: building…" >&2
  (cd "$spec" && lake build checktrace >&2)
fi

if [[ -t 0 ]]; then
  echo "validate.sh: no trace on stdin. Try: ./validate.sh < trace.ndjson" >&2
  exit 2
fi

exec "$bin" "$@"
