#!/usr/bin/env bash
#
# Build the Resonate monorepo branch.
#
# WHAT THIS TOUCHES
#   - a scratch build directory (default $TMPDIR/resonate-monorepo), and
#   - nothing else.
#
# It never pushes. It never writes to any source repository. The seven
# component repos are cloned read-only into throwaway directories and rewritten
# THERE; their remotes are untouched. The only thing you can lose by running
# this is the build directory.
#
# Usage:
#   ./migrate.sh                 # build from current branch tips, record pins
#   ./migrate.sh --clean         # wipe the build directory first
#   PINS=build/MANIFEST ./migrate.sh --clean
#                                # rebuild from exact recorded SHAs
#
# Afterwards, review the result and push it yourself:
#   git -C <build>/monorepo push -u origin monorepo
#
# Note the absence of --tags: the renamed tags stay local until cutover.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.sh
source "$HERE/config.sh"

BUILD_DIR="${BUILD_DIR:-${TMPDIR:-/tmp}/resonate-monorepo}"
OUT="$BUILD_DIR/monorepo"
SRC="$BUILD_DIR/src"
MANIFEST="$BUILD_DIR/MANIFEST"
PINS="${PINS:-}"
CLEAN=0

for arg in "$@"; do
  case "$arg" in
    --clean) CLEAN=1 ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

log()  { printf '\033[1m==>\033[0m %s\n' "$*"; }
step() { printf '    %s\n' "$*"; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- preflight ----------------------------------------------------------

preflight() {
  log "preflight"

  (( BASH_VERSINFO[0] >= 4 )) || die "bash 4.4+ required (found $BASH_VERSION)"

  command -v git >/dev/null || die "git not found"
  local gv
  gv="$(git --version | awk '{print $3}')"
  step "git $gv"

  if ! git filter-repo --version >/dev/null 2>&1; then
    die "git-filter-repo not found.

  Install one of:
    pip install git-filter-repo
    brew install git-filter-repo
    apt install git-filter-repo

  Do NOT substitute 'git filter-branch'. It is orders of magnitude slower and
  its mangling of tags and merge commits is exactly what this script must not do."
  fi
  step "git-filter-repo $(git filter-repo --version 2>&1 | head -1)"

  if [[ -e "$BUILD_DIR" ]]; then
    (( CLEAN )) || die "build directory exists: $BUILD_DIR
  Re-run with --clean to wipe it, or set BUILD_DIR to somewhere else."
    step "wiping $BUILD_DIR"
    rm -rf "$BUILD_DIR"
  fi

  mkdir -p "$SRC"
  : > "$MANIFEST"
}

# A full clone is not a nicety here: filter-repo on a shallow clone silently
# produces a monorepo with one commit of history per component, which looks
# like success and throws away the entire point of the migration.
assert_full_clone() {
  local dir="$1" name="$2" n
  [[ -f "$dir/.git/shallow" ]] && die "$name was cloned shallow — refusing to continue"
  n="$(git -C "$dir" rev-list --count HEAD)"
  (( n > 1 )) || die "$name has $n commit(s); expected full history"
  step "$name: $n commits"
}

pinned_sha() {
  local name="$1"
  [[ -n "$PINS" && -f "$PINS" ]] || return 1
  awk -v n="$name" '$1 == n { print $2; found = 1 } END { exit !found }' "$PINS"
}

# --- base ---------------------------------------------------------------

clone_base() {
  log "cloning base: $BASE_URL"
  git clone --quiet --branch "$BASE_BRANCH" "$BASE_URL" "$OUT"
  assert_full_clone "$OUT" "resonate"

  local sha
  sha="$(pinned_sha resonate || git -C "$OUT" rev-parse HEAD)"
  git -C "$OUT" checkout --quiet -B "$TARGET_BRANCH" "$sha"
  printf '%s %s\n' resonate "$sha" >> "$MANIFEST"
  step "branch $TARGET_BRANCH at $sha"
}

# The base is moved, not rewritten. One `git mv` commit relocates the whole
# tree into $BASE_DEST; every prior commit keeps its SHA, and `git log --follow`
# walks straight through the rename.
move_base() {
  log "moving base tree -> $BASE_DEST"
  cd "$OUT"

  local entries=()
  mapfile -d '' entries < <(git ls-tree --name-only -z HEAD)
  (( ${#entries[@]} )) || die "base repo appears empty"
  step "${#entries[@]} top-level entries"

  mkdir -p "$BASE_DEST"
  git mv -- "${entries[@]}" "$BASE_DEST/"

  git commit --quiet -m "move server to $BASE_DEST

The Resonate server moves into the monorepo layout. This is a pure rename:
no file content changes, and this repository's history is not rewritten, so
existing commit SHAs, permalinks and issue references remain valid.

The server's own .github/ moves with it and its workflows become inert;
root-level workflows are added in a follow-up change."

  step "committed $(git rev-parse --short HEAD)"
}

# --- components ---------------------------------------------------------

prepare_component() {
  local name="$1" url="$2" branch="$3" dest="$4" tagprefix="$5"
  local dir="$SRC/$name"

  log "preparing $name -> $dest"
  git clone --quiet --branch "$branch" "$url" "$dir"
  assert_full_clone "$dir" "$name"

  local sha
  sha="$(pinned_sha "$name" || git -C "$dir" rev-parse HEAD)"
  git -C "$dir" checkout --quiet -B "$branch" "$sha"
  printf '%s %s\n' "$name" "$sha" >> "$MANIFEST"
  step "pinned at $sha"

  # --to-subdirectory-filter prefixes every path in every commit.
  # --tag-rename ':<prefix>' namespaces the tags so that eight repos' worth of
  # v0.1.0 do not collide in one tag namespace.
  git -C "$dir" filter-repo \
    --to-subdirectory-filter "$dest" \
    --tag-rename ":$tagprefix" \
    --quiet

  step "rewritten under $dest/"
}

graft_component() {
  local name="$1" branch="$3" dest="$4"
  local dir="$SRC/$name"

  cd "$OUT"
  git fetch --quiet --tags "$dir" "$branch:refs/heads/import/$name"
  git merge --quiet --allow-unrelated-histories --no-ff \
    -m "graft $name into $dest

Imported with 'git filter-repo --to-subdirectory-filter $dest' from
$name at the commit recorded in scripts/migrate MANIFEST. Full history is
preserved; tags are namespaced. Content is unchanged." \
    "import/$name"
  git branch --quiet -D "import/$name"

  step "grafted, HEAD now $(git rev-parse --short HEAD)"
}

# --- main ---------------------------------------------------------------

preflight
clone_base
move_base

for record in "${COMPONENTS[@]}"; do
  IFS='|' read -r name url branch dest tagprefix <<< "$record"
  prepare_component "$name" "$url" "$branch" "$dest" "$tagprefix"
  graft_component   "$name" "$url" "$branch" "$dest" "$tagprefix"
done

cd "$OUT"
log "done"
step "build:  $OUT"
step "commits: $(git rev-list --count HEAD)"
step "tree:    $(git rev-parse 'HEAD^{tree}')"
step "pins:    $MANIFEST"
echo
echo "Verify before doing anything else:"
echo "  BUILD_DIR=$BUILD_DIR $HERE/verify.sh"
echo
echo "Then, when you are satisfied:"
echo "  git -C $OUT push -u origin $TARGET_BRANCH"
echo
echo "This script has pushed nothing."
