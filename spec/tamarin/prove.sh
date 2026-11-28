#!/usr/bin/env bash
# Prove one lemma per process, with a wall-clock and a heap cap, so a lemma
# that diverges is reported as such instead of taking the whole run with it.
#
#   ./prove.sh <model.spthy> [timeout-seconds] [extra tamarin flags...]
#
# exists-trace lemmas get two attempts: depth-first on a short budget, then
# breadth-first on the full one.  Neither order dominates.  The model has rules
# that read state without changing it -- promise.get, task.get, the retry
# dispatch -- and each is a source for the very linear fact it consumes, so
# depth-first can walk into a cycle and not come back; but where it does not,
# it finds the witness in a handful of steps where breadth-first takes
# hundreds.  All-traces lemmas keep depth-first, which is what the
# completeness argument needs.
set -u
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
MODEL="$1"; shift
TMO="${1:-900}"; shift || true
HEAP="${TAMARIN_HEAP:-8G}"
DFS_TMO=$(( TMO / 4 )); [ "$DFS_TMO" -lt 30 ] && DFS_TMO=30

mapfile -t LEMMAS < <(grep -oE '^lemma [A-Za-z0-9_]+' "$MODEL" | awk '{print $2}' | awk '!seen[$0]++')

is_exists() {
  awk -v want="$1" '
    $1 == "lemma" { name=$2; sub(/[^A-Za-z0-9_].*/, "", name); seen=(name==want); next }
    seen && /exists-trace/ { print "yes"; exit }
    seen && /^ *"/ { exit }
  ' "$MODEL"
}

# run <lemma> <timeout> [flags...] -> prints the verdict, or nothing on timeout
run_one() {
  local L="$1" T="$2"; shift 2
  local OUT RC
  OUT=$(timeout "$T" tamarin-prover --prove="$L" "$@" "${PASSTHRU[@]}" "$MODEL" \
        +RTS -M"$HEAP" -RTS 2>&1)
  RC=$?
  if [ $RC -eq 124 ]; then return 1; fi
  if [ $RC -eq 251 ] || echo "$OUT" | grep -q "heap overflow"; then
    echo "HEAP OVERFLOW (${HEAP})"; return 0
  fi
  if [ $RC -ne 0 ] && [ $RC -ne 1 ]; then echo "ERROR (rc=$RC)"; return 0; fi
  local R
  R=$(echo "$OUT" | sed -n "/summary of summaries/,\$p" | grep -E "^  $L " | sed -E 's/.*\): //')
  [ -z "$R" ] && return 1
  echo "$R"; return 0
}

PASSTHRU=("$@")
printf '%-52s %s\n' "LEMMA" "RESULT"
printf '%s\n' "--------------------------------------------------------------------"
for L in "${LEMMAS[@]}"; do
  if [ -n "$(is_exists "$L")" ]; then
    R=$(run_one "$L" "$DFS_TMO") || R=""
    if [ -z "$R" ]; then
      R=$(run_one "$L" "$TMO" --stop-on-trace=BFS) || R="TIMEOUT (${TMO}s, DFS and BFS)"
      [ "${R#TIMEOUT}" = "$R" ] && R="$R [BFS]"
    fi
  else
    R=$(run_one "$L" "$TMO") || R="TIMEOUT (${TMO}s)"
  fi
  printf '%-52s %s\n' "$L" "$R"
done
