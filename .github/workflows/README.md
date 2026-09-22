# Workflows

GitHub Actions reads workflows from this directory and nowhere else. Not from
subdirectories of it, and not from a `.github/` inside a subproject.

Each grafted subproject brought its own `.github/workflows/` when it was
merged in — `impl/server/core/`, `impl/sdk/*/`, `spec/`. Every one of those is
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

The copies under each subproject are left in place as that project's own
record. They are not read by anything.

`server-s3-ci.yml` is the exception to "copied": `impl/server/s3/` grew up
inside this repository, its steps were already written from the workspace
root, and the copy only gains the paths it depends on and the cache location.

## Deliberately not moved

These are outward-facing or destructive, and enabling them is a decision for
whoever owns the release process, not a side effect of repairing CI:

| Workflow | Why it stayed |
| --- | --- |
| `impl/sdk/*/cd.yml`, `impl/sdk/rs/publish.yml` | Publish to npm, PyPI, Maven and crates.io on release. Moving them arms real publishing, and they need secrets this repository may not carry. |
| `spec/.github/workflows/binaries.yml` | Publishes GitHub Releases on `v*` tags, and builds on every push to every branch. |
| `impl/server/core/.github/workflows/remove-stale-branches.yml` | Deletes branches. It is also the one workflow still running today, from the root of `main`. |
| `impl/sdk/ts/.github/workflows/dst.yml` | `workflow_dispatch` only, and it opens issues. |

Moving any of them is the same mechanical change as above.
