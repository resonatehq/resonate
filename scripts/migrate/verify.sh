#!/usr/bin/env bash
#
# Post-conditions for the grafted monorepo branch.
#
# This is the review artifact. The graft's diff is unreadable — thousands of
# files, thousands of commits — so correctness is established by re-running
# migrate.sh and comparing the tree hash printed here, not by reading it.
#
# Read-only. Usage:
#   BUILD_DIR=/tmp/resonate-monorepo ./verify.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.sh
source "$HERE/config.sh"

BUILD_DIR="${BUILD_DIR:-${TMPDIR:-/tmp}/resonate-monorepo}"
OUT="${OUT:-$BUILD_DIR/monorepo}"
[[ -d "$OUT/.git" ]] || { echo "no repository at $OUT" >&2; exit 1; }
cd "$OUT"

FAIL=0
pass() { printf '  \033[32mok\033[0m   %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=1; }
head2() { printf '\n\033[1m%s\033[0m\n' "$*"; }

head2 "repository"
if [[ -f .git/shallow ]]; then
  fail "repository is shallow — history was truncated"
else
  pass "not shallow"
fi
[[ -z "$(git status --porcelain)" ]] && pass "working tree clean" \
                                     || fail "working tree dirty"

head2 "layout"
# After a pure-prefix graft the root holds only the destination roots. Anything
# else means a component leaked files into the root tree.
mapfile -t roots < <(git ls-tree --name-only HEAD | sort)
declare -A want=()
want["${BASE_DEST%%/*}"]=1
for record in "${COMPONENTS[@]}"; do
  IFS='|' read -r _ _ _ dest _ <<< "$record"
  want["${dest%%/*}"]=1
done
mapfile -t expected < <(printf '%s\n' "${!want[@]}" | sort)
if [[ "${roots[*]}" == "${expected[*]}" ]]; then
  pass "root contains exactly: ${roots[*]}"
else
  fail "root is ${roots[*]} — expected ${expected[*]}"
fi

head2 "subtrees"
for dest in "${!SENTINEL[@]}"; do
  sentinel="$dest/${SENTINEL[$dest]}"
  if ! git cat-file -e "HEAD:$sentinel" 2>/dev/null; then
    fail "$dest — missing sentinel ${SENTINEL[$dest]}"
    continue
  fi
  files=$(git ls-tree -r --name-only HEAD -- "$dest" | wc -l | tr -d ' ')
  commits=$(git rev-list --count HEAD -- "$dest")
  if (( commits > 1 )); then
    pass "$(printf '%-22s %5s files  %5s commits' "$dest" "$files" "$commits")"
  else
    fail "$dest has $commits commit(s) — history did not survive the graft"
  fi
done

head2 "history"
# The grafted components carry their prefixed path in every commit, so plain
# `git log -- <path>` already reaches their full history (checked above).
# The base is different: it was MOVED, so reaching its pre-migration history
# requires rename detection. This is the check that proves the move was a
# rename and not a delete-plus-add.
base_sentinel="$BASE_DEST/${SENTINEL[$BASE_DEST]}"
follow=$(git log --follow --oneline -- "$base_sentinel" | wc -l | tr -d ' ')
plain=$(git log --oneline -- "$base_sentinel" | wc -l | tr -d ' ')
if (( follow > plain )); then
  pass "base rename traversable ($plain without --follow, $follow with)"
else
  fail "base history not reachable through the rename ($plain/$follow)"
fi

# Blame must still attribute to the original authors. The most recent commit on
# this path is necessarily the migration's own move commit, so the check is
# that the path's history contains commits by anyone else.
others=$(git log --follow --format=%an -- "$base_sentinel" \
         | grep -vcx "$GIT_AUTHOR_NAME" || true)
if (( others > 0 )); then
  pass "authorship preserved ($others commit(s) by original authors)"
else
  fail "no pre-migration authorship on $base_sentinel"
fi

head2 "tags"
total=$(git tag | wc -l | tr -d ' ')
unprefixed=$(git tag | grep -c '^v' || true)
pass "$total tags"
for record in "${COMPONENTS[@]}"; do
  IFS='|' read -r name _ _ _ tagprefix <<< "$record"
  n=$(git tag --list "${tagprefix}*" | wc -l | tr -d ' ')
  printf '       %-24s %s tag(s) under %s\n' "$name" "$n" "$tagprefix"
done
if (( unprefixed )); then
  printf '       %s unprefixed tag(s) from the base repo (expected — its history was not rewritten)\n' "$unprefixed"
fi
# The Go module proxy resolves a subdirectory module only via tags named
# exactly "<subdir>/vX.Y.Z". Any other prefix makes the module unfetchable.
go_tags=$(git tag --list 'impl/sdk/go/*' | wc -l | tr -d ' ')
if (( go_tags )); then
  pass "$go_tags go tag(s) carry the path-derived prefix the module proxy requires"
else
  printf '       note: the go sdk has no release tags yet — its first must be\n'
  printf '             named impl/sdk/go/vX.Y.Z, not sdk-go/vX.Y.Z\n'
fi

head2 "result"
printf '  commits  %s\n' "$(git rev-list --count HEAD)"
printf '  tree     %s\n' "$(git rev-parse 'HEAD^{tree}')"
printf '  head     %s\n' "$(git rev-parse HEAD)"
echo
echo "  The tree hash is the reproducibility check: re-run migrate.sh from the"
echo "  recorded pins on another machine and it must match exactly."

echo
(( FAIL )) && { echo "VERIFY FAILED"; exit 1; }
echo "All checks passed. Nothing has been pushed."
