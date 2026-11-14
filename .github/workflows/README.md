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
subdirectory. They are in git history at `b0ab037` and earlier.

## Deliberately not moved

These are outward-facing or destructive, and enabling them is a decision for
whoever owns the release process, not a side effect of repairing CI. They
were never active in this repository and were removed with the other inert
copies; to enable one, restore it from `b0ab037` (e.g.
`git show b0ab037:impl/sdk/ts/.github/workflows/cd.yml`) into the root
`.github/workflows/` and adapt it as above:

| Workflow (path at `b0ab037`) | Why it was not moved |
| --- | --- |
| `impl/{sdk/*,server/core}/.github/workflows/cd.yml`, `impl/sdk/rs/.github/workflows/publish.yml` | Publish to npm, PyPI, Maven and crates.io on release. Moving them arms real publishing, and they need secrets this repository may not carry. |
| `spec/.github/workflows/binaries.yml` | Publishes GitHub Releases on `v*` tags, and builds on every push to every branch. |
| `impl/server/core/.github/workflows/remove-stale-branches.yml` | Deletes branches. |
| `impl/sdk/ts/.github/workflows/dst.yml` | `workflow_dispatch` only, and it opens issues. |
| `impl/sdk/ts/.github/workflows/docs.yaml` | Publishes the TypeScript SDK's docs site. |

