#!/usr/bin/env bash
#
# validate.sh — check a recorded Resonate run against the Lean specification.
#
#   ./validate.sh trace.ndjson              # from a file
#   cat trace.ndjson | ./validate.sh        # from a pipe
#   N=200 python3 spec/06-trace/traces/capture.py && ./validate.sh trace.ndjson
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

# Build only when the sources are newer than the binary. `lake` is fast when
# there is nothing to do, but not free, and this script is meant to sit in a
# pipe.
newest_src="$(find "$spec/06-trace" "$spec/01-protocol" "$spec/03-concrete" \
                   "$spec/04-theorems" -name '*.lean' -newer "$bin" -print -quit 2>/dev/null || true)"
if [[ ! -x "$bin" || -n "$newest_src" ]]; then
  echo "validate.sh: building…" >&2
  (cd "$spec" && lake build checktrace >&2)
fi

# A FILE argument is redirected here rather than parsed in Lean: the
# checker reads stdin and nothing else, which keeps the source dispatch
# out of the verified code and costs two lines of bash.
case "${1:-}" in
  -h|--help) exec "$bin" --help ;;
  ""|-)      shift 2>/dev/null || true
             exec "$bin" "$@" ;;
  -*)        echo "validate.sh: unknown option $1" >&2; exit 2 ;;
  [0-9]*)    exec "$bin" "$@" ;;                       # bare cap, still stdin
  *)         file="$1"; shift
             [[ -r "$file" ]] || { echo "validate.sh: cannot read $file" >&2; exit 2; }
             echo "validate.sh: $file" >&2
             exec "$bin" "$@" < "$file" ;;
esac
