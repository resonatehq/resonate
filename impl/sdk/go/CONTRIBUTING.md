# How to Contribute

Welcome to the Resonate project! We appreciate your interest in helping us build reliable
and scalable distributed systems. To get started, follow these simple steps:

## Initial Setup

We follow the fork and branch workflow. There will be three Git repositories involved:

1. **upstream** - the [resonate-sdk-go](https://github.com/resonatehq/resonate-sdk-go) repository on GitHub.
2. **origin** - your GitHub fork of `upstream`.
3. **local** - your local clone of `origin`.

These steps are only needed once and not for subsequent changes you might want to make:

1. Fork the `resonate-sdk-go` repository on GitHub to create `origin`.
   Visit [resonate-sdk-go](https://github.com/resonatehq/resonate-sdk-go) GitHub repository and click the `Fork` button.

2. Make a `local` clone of your fork.

   ```shell
   git clone git@github.com:<your-user-name>/resonate-sdk-go.git
   ```

3. Add a remote pointing from `local` to `upstream`.

   ```shell
   cd resonate-sdk-go
   git remote add upstream git@github.com:resonatehq/resonate-sdk-go.git
   ```

4. Double check the two remotes are referencing the expected URL.

   ```shell
   git remote get-url origin   # git@github.com:<your-user-name>/resonate-sdk-go.git
   git remote get-url upstream # git@github.com:resonatehq/resonate-sdk-go.git
   ```

## Development Workflow

Here is an outline of the steps needed to make changes to the resonate project.

1. Make a local branch in your clone and pull any recent changes into it.

   ```shell
   git switch -c awesome_branch
   git pull upstream main
   ```

2. Make changes and commit to local branch.

   ```shell
   git add .
   git commit -m "dead simple"
   ```

3. Pull any changes that may have been made in the upstream repository main branch.

   ```shell
   git pull --rebase upstream main # may result in merge conflicts
   ```

4. Run tests locally.

   **Unit tests** (no server required):

   ```shell
   go test ./...
   ```

   **E2E tests** (requires a running Resonate server):

   ```shell
   docker run -d --name resonate -p 8001:8001 resonatehqio/resonate:latest serve
   RESONATE_URL=http://localhost:8001 go test -race -timeout 5m ./...
   docker stop resonate && docker rm resonate
   ```

5. Ensure code is properly formatted and passes linting.

   ```shell
   gofmt -w .
   go vet ./...
   golangci-lint run
   ```

6. Push your branch to the corresponding branch in your fork.

   ```shell
   git push origin awesome_branch
   ```

7. Select the branch you are working on in the drop-down menu of branches in
   your fork. Then hit the `Compare and pull request` button.

8. Once your pull request has been reviewed and approved by a maintainer, select
   the `Squash and merge` option. Edit the commit message as appropriate for the
   squashed commit.

9. Delete the branch from `origin`:

   ```shell
   git push origin --delete awesome_branch
   ```

10. Delete the branch from `local`:

    ```shell
    git switch main
    git branch -D awesome_branch
    ```

## What to Contribute To?

We're pre-semver, so the highest-leverage contributions right now are documentation
improvements, examples, and small bugfixes. If you're thinking about a larger change —
especially anything touching the public API — please open an issue to discuss it first
so we can make sure the direction is stable before you invest the time.

Here are some areas where your contributions would be valuable:

- Bug fixes for existing packages.
- Refactoring efforts to improve code quality.
- Enhancements to our testing and reliability efforts.
- Documentation improvements and examples.
- Custom `Encryptor` implementations (e.g., AES-GCM encryption at the durability boundary).

Thank you for your contributions and support in building a better Resonate! 🚀
