#!/usr/bin/env bash
# Replay every adapted trace through the unified model.
#
#   ./validate-traces.sh [PROFILE]
#
# PROFILE defaults to `impl` -- replay each trace under its OWN
# implementation's profile, which asks "does the model describe the
# implementation?".  Pass `spec` to replay under the specification profile
# instead, which asks "was this recorded execution conformant?".
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"
PROFILE="${1:-impl}"
OUT="$HERE/output"; mkdir -p "$OUT"

fail=0
for t in "$HERE"/traces/*.ndjson; do
  name="$(basename "$t" .ndjson)"
  impl="$(python3 -c "import json;print(json.loads(open('$t').readline())['config']['impl'])")"

  if [ "$PROFILE" = spec ]; then
    GUARDS='CallbackExternalGuard = TRUE
    ListenerExternalGuard = TRUE
    PromiseLivenessGuard  = TRUE
    TimeoutLivenessGuard  = TRUE
    ResumeLivenessGuard   = TRUE
    HeartbeatGuard        = TRUE
    ProjectedResponses    = TRUE
    ArmPolicy             = "external"'
  else
    case "$impl" in
      resonate-pg) GUARDS='CallbackExternalGuard = TRUE
    ListenerExternalGuard = FALSE
    PromiseLivenessGuard  = FALSE
    TimeoutLivenessGuard  = FALSE
    ResumeLivenessGuard   = FALSE
    HeartbeatGuard        = TRUE
    ProjectedResponses    = FALSE
    ArmPolicy             = "external"' ;;
      *) echo "no profile for impl=$impl"; exit 2 ;;
    esac
  fi

  ids="$(python3 -c "import json;print('{'+','.join('\"%s\"'%i for i in json.loads(open('$t').readline())['config']['ids'])+'}')")"
  addrs="$(python3 -c "import json;print('{'+','.join('\"%s\"'%i for i in json.loads(open('$t').readline())['config']['addrs'])+'}')")"
  retry="$(python3 -c "import json;print(json.loads(open('$t').readline())['config']['retry'])")"
  ttl="$(python3 -c "import json;print(json.loads(open('$t').readline())['config']['ttl'])")"

  cat > "$HERE/UTrace.cfg" <<CFG
SPECIFICATION TraceSpec
CONSTANTS
    Ids     = $ids
    Addrs   = $addrs
    Workers = {"w1"}
    MaxTime    = 64
    MaxVersion = 16
    Retry = $retry
    Ttl   = $ttl
    $GUARDS
    SequencedDriver = FALSE
    WorkerLayer     = FALSE
    FaultsOn        = FALSE
    TraceFile = "$t"
CHECK_DEADLOCK FALSE
INVARIANT NotReplayed
CFG

  java -XX:+UseParallelGC -cp "$LIB/tla2tools.jar:$LIB/CommunityModules-deps.jar" \
       tlc2.TLC -config "$HERE/UTrace.cfg" -workers 1 "$HERE/UTrace.tla" \
       > "$OUT/trace-$PROFILE-$name.out" 2>&1

  events=$(python3 -c "import json;print(sum(1 for l in open('$t') if l.strip() and json.loads(l)['tag']=='trace'))")
  # a full replay VIOLATES NotReplayed -- see the header of UTrace.tla
  if grep -q "Invariant NotReplayed is violated" "$OUT/trace-$PROFILE-$name.out"; then
    printf "  %-28s REPLAYED  %2s events\n" "$name" "$events"
  else
    printf "  %-28s NOT REPLAYED  %2s events -- output/trace-%s-%s.out\n" "$name" "$events" "$PROFILE" "$name"
    fail=1
  fi
done
exit $fail
