# Workflows

GitHub Actions reads workflows from this directory and nowhere else. Not from
subdirectories of it, and not from a `.github/` inside a subproject.

Each grafted subproject brought its own `.github/workflows/` when it was
merged in — `impl/server/core/`, `impl/sdk/*/`, `spec/`. Every one of those was
inert. Between the graft and this commit the repository ran no CI at all: the
only workflow Actions could see was the one still sitting at the root of
`main`, which is the pre-graft layout.

The CI workflows are copied here, one file per subproject, so they run again.
Three things change in the move, and nothing else does:

- `defaults.run.working-directory` points at the subproject, so every `run:`
  step behaves as it did when the subproject was its own repository.
- Anything an *action* resolves for itself — a cache key, a lint directory, a
  Gradle build root — is spelled from the workspace root, because
  `working-directory` applies only to `run:` steps.
- `paths:` keeps a change to one subproject from running every other
  subproject's CI. Each file lists itself, so editing a workflow re-runs it.

`pull_request` is deliberately not filtered by base branch. The restructuring
is not on `main` yet, and a workflow answering only to `main` would sit out
every pull request that matters right now.

The inert copies under each subproject (`impl/*/.github/`, `spec/.github/`)
have since been removed, along with the subprojects' `dependabot.yml` files and
the TypeScript SDK's issue and PR templates, none of which GitHub reads from a
subdirectory. They are in git history at `b0ab037` and earlier; the release
workflows among them were moved here, manual only (below).

## Release workflows: manual only

The subprojects' release and publish workflows are here too, but **none of them
runs on its own**: their release, tag and push triggers were replaced with
`workflow_dispatch`, so they run only when someone starts them from the Actions
tab. Publishing is outward-facing and needs secrets this repository may not
carry yet; enabling it is a decision for whoever owns the release process.

| Workflow | Publishes | Was |
| --- | --- | --- |
| `release-sdk-ts.yml` | npm (trusted publishing) | `impl/sdk/ts/.github/workflows/cd.yml` |
| `release-sdk-py.yml` | PyPI | `impl/sdk/py/.github/workflows/cd.yml` |
| `release-sdk-java.yml` | Maven Central | `impl/sdk/java/.github/workflows/cd.yml` |
| `release-sdk-rs.yml` | crates.io (runs `sdk-rs-ci.yml` first) | `impl/sdk/rs/.github/workflows/publish.yml` |
| `release-server.yml` | Docker Hub, GHCR, Artifact Registry, release binaries, Homebrew PR | `impl/server/core/.github/workflows/cd.yml` |
| `release-spec-binaries.yml` | `lincheck` / `scenarios` binaries, optionally a GitHub Release | `spec/.github/workflows/binaries.yml` |
| `docs-sdk-ts.yml` | TypeScript SDK docs to GitHub Pages | `impl/sdk/ts/.github/workflows/docs.yaml` |

Each takes a `tag` input in place of the release tag it used to read (the docs
workflow takes none), and each file's header lists the secrets it needs. Two
things to settle before the first real run: the secrets, and a tag scheme, since
every subproject used to tag `vX.Y.Z` in its own repository and those collide
here. None has been exercised from the monorepo yet.

Not carried over (still in history at `b0ab037`): `impl/sdk/ts/.github/workflows/dst.yml`
(`workflow_dispatch` only, opens issues) and
`impl/server/core/.github/workflows/remove-stale-branches.yml` (deletes branches).
