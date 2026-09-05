# Monorepo migration

Builds the consolidated `monorepo` branch from eight repositories.

## Safety

Running `migrate.sh` writes to a scratch build directory and nothing else.

- The seven component repos are cloned read-only into throwaway directories and
  rewritten **there**. Their remotes are never written to.
- `resonatehq/resonate` is cloned; the result lands on a new local branch.
  `main` is not touched.
- Nothing is pushed. No tags are pushed. No GitHub Release is created — which
  matters, because all five publish pipelines (`resonate` cd.yml, ts/py/java
  cd.yml, rs publish.yml) trigger on `release: published`/`released`, never on
  a branch or tag push. A branch push therefore cannot ship anything to npm,
  PyPI, crates.io, Maven Central, DockerHub, GHCR or GCR.

The only thing a run can destroy is its own build directory.

Two local hazards the script guards against, worth knowing anyway:

- **Never run `git filter-repo` in a clone you care about.** It rewrites in
  place and drops `origin` afterwards, deliberately. This script only ever runs
  it inside `$BUILD_DIR/src/*`.
- **Shallow clones silently defeat the exercise.** `filter-repo` on a
  `--depth 1` clone yields a monorepo with one commit per component: it looks
  like it worked and the history is gone. `assert_full_clone` refuses.

## Layout produced

| source | destination | tag prefix |
|---|---|---|
| `resonate` | `impl/server/core` | *(unchanged)* |
| `resonate-pg` | `impl/server/postgres` | `server-postgres/` |
| `resonate-sdk-ts` | `impl/sdk/ts` | `sdk-ts/` |
| `resonate-sdk-py` | `impl/sdk/py` | `sdk-py/` |
| `resonate-sdk-go` | `impl/sdk/go` | `impl/sdk/go/` |
| `resonate-sdk-rs` | `impl/sdk/rs` | `sdk-rs/` |
| `resonate-sdk-java` | `impl/sdk/java` | `sdk-java/` |
| `resonate-specification` | `spec` | `spec/` |

The Go prefix is not a style inconsistency. The module proxy resolves a
subdirectory module only through tags named exactly `<subdir>/vX.Y.Z`, so that
one prefix is forced by the destination path.

`resonate` is the base and is **moved, not rewritten** — a single `git mv`
commit. Every existing SHA, permalink and issue cross-reference in that
repository stays valid. Only the seven incoming histories are rewritten.

## What this deliberately does not do

Every move is a pure path prefix. No file content changes. In particular:

- `resonate-specification` lands whole at `spec/`, so its Lean project still
  builds. Splitting it into `spec/lean` and `spec/tla` needs `lakefile.lean`
  edits, so it is a follow-up PR, not part of the graft.
- No root `README`, `.github/workflows`, `dependabot.yml`, `CODEOWNERS` or
  release config is created. Those are the genuinely reviewable ~400 lines and
  belong in ordinary PRs on top of this branch.
- No root `package.json`. The two npm projects (`impl/sdk/ts`, and the gateway
  UI inside `impl/server/core`) stay independent, with their own lockfiles.
- Open branches in the source repos are not carried over. Port them per-branch:
  `git format-patch main..topic --stdout | git am --directory=impl/sdk/ts`

## Use

```sh
./migrate.sh --clean
BUILD_DIR=${TMPDIR:-/tmp}/resonate-monorepo ./verify.sh
```

`migrate.sh` records the resolved input SHAs to `$BUILD_DIR/MANIFEST`. Rebuild
from exactly those:

```sh
PINS=$BUILD_DIR/MANIFEST ./migrate.sh --clean
```

The commits this script authors are pinned to a fixed identity and timestamp
(see `config.sh`), so a rebuild from the same pins reproduces the same commit
SHAs, not merely the same tree. **Set `MIGRATE_DATE` to the cutover date before
the real run** so the graft commits sort sensibly in `git log`.

## Review

The graft's diff is unreadable by construction — thousands of files, thousands
of commits, and GitHub gives up on rendering long before that. Review the
script, then verify the artifact:

```sh
# reviewer, independently:
PINS=MANIFEST ./migrate.sh --clean && ./verify.sh
# compare the printed tree hash against the branch under review
```

## Pushing

`migrate.sh` prints the push command; it does not run it.

```sh
git -C "$BUILD_DIR/monorepo" push -u origin monorepo
```

No `--tags`. Renamed tags stay local until cutover — pushing eight repos' worth
of tags into the shared repo is noise someone else has to clean up.

Check `.github/workflows/remove-stale-branches.yml` before parking the branch
there for review, so it doesn't get reaped mid-review.

## Cutover

**The graft branch must be merged with a merge commit, never squashed.**
Squashing flattens eight histories into one commit with no ancestors: every
commit, all blame, and `git log --follow` are lost, and the migration's entire
purpose with them. This repository squash-merges by convention, so merge
commits must be enabled and the branch-protection rule relaxed for this one
merge. It is the single highest-consequence step in the migration and it is one
dropdown.
