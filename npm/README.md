# resonate-cli

Resonate as an npm package.

```shell
npx resonate-cli@latest dev
```

Resonate is a single Rust binary. This package contains no implementation — it
downloads the release artifact for your platform and runs it, so that npm can
serve as the install channel for people who already have Node on their machine.

For the project itself, see
[resonatehq/resonate](https://github.com/resonatehq/resonate).

## Install

Per project, or globally:

```shell
npm install --save-dev resonate-cli
npm install --global resonate-cli
```

Either way the `resonate` command becomes available. Everything after it goes
straight to the binary:

```shell
npx resonate dev                 # in-memory, ephemeral, for development
npx resonate serve               # persistent, backed by SQLite by default
npx resonate invoke foo --func f # talk to a running server
npx resonate --help
```

The published package version matches the Resonate version it installs, so
`resonate-cli@0.9.8` is Resonate `v0.9.8`.

## Platforms

The release pipeline builds macOS and Linux, on x86_64 and aarch64. There is no
Windows build to download, and npm will refuse to install there rather than fail
later; use [WSL](https://learn.microsoft.com/windows/wsl/install) or
[Docker](https://github.com/resonatehq/resonate#docker).

## Environment variables

| Variable | Effect |
|---|---|
| `RESONATE_BINARY_PATH` | Run this binary instead of downloading one. For air-gapped machines, an unsupported platform, or a local `cargo build`. |
| `RESONATE_DOWNLOAD_BASE_URL` | Fetch release assets from here instead of GitHub. The layout must match: `<base>/v<version>/resonate_<os>_<arch>.tar.gz` and a `.sha256` beside it. |

The download is checked against the `.sha256` published with the release; a
mismatch fails the install rather than leaving an unverified binary behind.

## Other ways to install

```shell
brew install resonatehq/tap/resonate
```

Or take a binary straight from the
[releases page](https://github.com/resonatehq/resonate/releases).

## License

[Apache-2.0](https://github.com/resonatehq/resonate/blob/main/LICENSE)
