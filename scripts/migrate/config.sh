# shellcheck shell=bash
#
# Configuration for the monorepo consolidation.
#
# Every move here is a PURE PATH PREFIX. No file is edited, split, renamed
# within its tree, or restructured. That is what makes the graft mechanically
# verifiable: the result is a function of the eight input commits and this
# file, and two people running it get byte-identical output.
#
# Restructuring (splitting spec/ into lean/ and tla/, rewriting build files,
# adding root workflows) happens AFTERWARDS, in ordinary reviewable PRs.

# --- base ---------------------------------------------------------------
#
# The base repo becomes the monorepo. Its history is NOT rewritten: its tree
# is moved with a single `git mv` commit, so every existing SHA, permalink and
# issue cross-reference in resonatehq/resonate stays valid.

BASE_URL="https://github.com/resonatehq/resonate.git"
BASE_BRANCH="main"
BASE_DEST="impl/server/core"

TARGET_BRANCH="monorepo"

# --- components ---------------------------------------------------------
#
# Grafted in with `git filter-repo --to-subdirectory-filter`. Their histories
# ARE rewritten (locally, in throwaway clones — the source repos are never
# written to).
#
# Format: name | url | branch | dest | tag-prefix
#
# On tag prefixes: they are free-form EXCEPT for Go. The module proxy requires
# a subdirectory module's tags to be exactly "<module subdir>/vX.Y.Z", so the
# Go SDK's prefix is forced to match its destination path. Do not "tidy" it.

COMPONENTS=(
  "resonate-pg|https://github.com/resonatehq/resonate-pg.git|main|impl/server/postgres|server-postgres/"
  "resonate-sdk-ts|https://github.com/resonatehq/resonate-sdk-ts.git|main|impl/sdk/ts|sdk-ts/"
  "resonate-sdk-py|https://github.com/resonatehq/resonate-sdk-py.git|main|impl/sdk/py|sdk-py/"
  "resonate-sdk-go|https://github.com/resonatehq/resonate-sdk-go.git|main|impl/sdk/go|impl/sdk/go/"
  "resonate-sdk-rs|https://github.com/resonatehq/resonate-sdk-rs.git|main|impl/sdk/rs|sdk-rs/"
  "resonate-sdk-java|https://github.com/resonatehq/resonate-sdk-java.git|main|impl/sdk/java|sdk-java/"
  "resonate-specification|https://github.com/resonatehq/resonate-specification.git|main|spec|spec/"
)

# A file that must exist at <dest> afterwards, used by verify.sh to prove the
# subtree landed and that `git log --follow` reaches its pre-migration history.
declare -A SENTINEL=(
  ["impl/server/core"]="Cargo.toml"
  ["impl/server/postgres"]="resonate.sql"
  ["impl/sdk/ts"]="package.json"
  ["impl/sdk/py"]="pyproject.toml"
  ["impl/sdk/go"]="go.mod"
  ["impl/sdk/rs"]="Cargo.toml"
  ["impl/sdk/java"]="build.gradle.kts"
  ["spec"]="lakefile.lean"
)

# --- determinism --------------------------------------------------------
#
# filter-repo preserves original author/committer dates, so the rewritten
# histories are already deterministic. The commits this script AUTHORS (the
# base move and the graft merges) are pinned here so that the whole run is
# reproducible: same inputs -> same commit SHAs, not merely the same tree.
#
# Set MIGRATE_DATE to the planned cutover date before the real run, so the
# graft commits don't sort oddly in `git log`.

export GIT_AUTHOR_NAME="${MIGRATE_NAME:-resonate migration}"
export GIT_AUTHOR_EMAIL="${MIGRATE_EMAIL:-migration@resonatehq.io}"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export GIT_AUTHOR_DATE="${MIGRATE_DATE:-2026-09-05T00:00:00+00:00}"
export GIT_COMMITTER_DATE="$GIT_AUTHOR_DATE"
