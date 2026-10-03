# Sandbox plugin

`worker_sandbox` runs each task dispatched to `sandbox://` inside an
isolated sandbox booted from that image. The plugin is the only component that
talks to the Resonate server; the guest has no network by default and reaches
the server only through its own stdio, relayed by **rn8**, a static binary baked
into the image as its entrypoint. SDKs run unchanged.

| Crate | What it is |
|---|---|
| `resonate-worker-sandbox` | The plugin: sandbox lifecycle, relay, scope check, lease watch |
| `resonate-sandbox` | The frame protocol and the `Backend` / `Process` traits, shared by both ends |
| `resonate-sandbox-microsandbox` | A microsandbox microVM per task, through the `msb` CLI |
| `resonate-sandbox-tensorlake` | A Tensorlake sandbox per task, through Tensorlake's HTTP API |
| `resonate-sandbox-rn8` | `rn8`, the relay in the guest |

## Addressing

```text
sandbox://<image>              the default provider (`backend`)
sandbox://<provider>/<image>   microsandbox, tensorlake or local
```

`<image>` is an OCI reference pinned by digest, e.g.
`sandbox://tensorlake/ghcr.io/acme/worker@sha256:…`. The provider is the first
segment when it is a provider's name; those names have no `.` or `:`, so they
never read as a registry host. They can read as a Docker Hub namespace, and the
provider wins — name such an image in full: `sandbox://docker.io/tensorlake/…`.

A provider named in an address must be enabled; if it is not, the message is
refused as unroutable rather than sent to the default.

## Task flow

1. The server dispatches a task to a `sandbox://` target.
2. The plugin calls `create`, then `exec` on the image's entrypoint.
3. The plugin writes the task message as the first frame.
4. rn8 starts the worker and pushes the task to it over loopback HTTP.
5. The SDK sends its requests to rn8, which relays them as frames. The plugin
   checks each one against the claimed task, forwards it to the server with
   auth attached, and returns the response.
6. The process exits when the step ends, and the plugin calls `destroy`.

The plugin destroys the sandbox when the task's lease expires, when the guest
has not acquired its task within `start_timeout`, or when the guest outlives
its step by more than `exit_grace`.

## Configuration

```toml
[workers.worker_sandbox]
enabled = true
backend = "microsandbox"   # the default provider: microsandbox, tensorlake, local
cpus = 2                   # per sandbox; absent = provider default
memory_mib = 1024          # per sandbox; absent = provider default
egress = "none"            # "all", or { allow = ["example.com", …] }
require_digest = true      # refuse sandbox://<image> not pinned by @sha256:…
command = []               # empty = the image's own entrypoint (rn8)
env = {}                   # extra, non-secret environment for that command
concurrency = 16           # sandboxes at once
token = "…"                # attached to every forwarded request; the guest's is dropped
start_timeout = 120000     # ms from dispatch to acquire, create included
exit_grace = 5000          # ms the guest has to exit once its step has ended

# Per image, first match wins: cpus, memory_mib and egress instead of the
# defaults above. `image` is exact, or a prefix ending in '*', and is matched
# without the provider.
[[workers.worker_sandbox.images]]
image = "cas-v1:4f2a…"
egress = { allow = ["books.toscrape.com"] }   # hostnames, IPs or CIDRs
memory_mib = 2048

# Each provider: on when it is `backend`, or when its own section says so.
[workers.worker_sandbox.microsandbox]
enabled = false
msb = "msb"                # the microsandbox CLI

[workers.worker_sandbox.tensorlake]
enabled = false
api_key = "…"              # absent: TENSORLAKE_API_KEY; required either way
api_url = "https://api.tensorlake.ai"
proxy_url = "https://sandbox.tensorlake.ai"
timeout_secs = 900         # Tensorlake reaps the sandbox after this, plugin or not
ready_timeout = 120000     # ms for a new sandbox to start running

[workers.worker_sandbox.local]
enabled = false            # no isolation: development and tests only
```

The `tensorlake` provider creates a sandbox with `POST /sandboxes` (image,
`resources`, `network.allow_internet_access` from `egress`, `timeout_secs`),
waits for it to run, and starts the command with
`POST /api/v1/processes` and `stdin_mode: "pipe"`. Frames to the guest go as
`POST …/stdin` (and `…/stdin/close` at EOF); frames from it arrive as the
server-sent events of `…/stdout/follow`, one output line per frame. With
`command` empty it runs the entrypoint Tensorlake reports for the sandbox.
`timeout_secs` doubles as cleanup for a sandbox orphaned by a plugin crash.

The `microsandbox` backend needs `msb` on the host (Linux with KVM, or macOS on
Apple Silicon). It drives the CLI rather than linking the `microsandbox` SDK
crate, which links a SQLite this workspace's sqlx already links at another
version. `msb exec --stream` gives the same live, byte-faithful stdio.

An allow-list is enforced by the provider: microsandbox as
`--net-default-egress deny` with one `--net-rule allow@<host>` each, Tensorlake
as `allow_out` (which is default-deny) with DNS left on. The `local` provider
enforces nothing, so it insists on `egress = "all"`.

For a worked example — a browser in a sandbox per page, orchestrated from the
host — see `impl/sdk/ts/examples/sandbox/`.

## Building an image

```dockerfile
FROM rust:1 AS rn8
RUN rustup target add x86_64-unknown-linux-musl
COPY . /src
RUN cargo build --release --manifest-path /src/impl/server/core/Cargo.toml \
      -p resonate-sandbox-rn8 --target x86_64-unknown-linux-musl

FROM node:22-slim
COPY --from=rn8 /src/impl/server/core/target/x86_64-unknown-linux-musl/release/rn8 /usr/local/bin/rn8
COPY worker.js package.json ./
RUN npm install
ENTRYPOINT ["rn8", "--", "node", "worker.js"]
```

Dispatch to the image by digest: `sandbox://ghcr.io/acme/worker@sha256:…`.

The worker is started with `RESONATE_URL` set to rn8's loopback relay and `PORT`
set to where rn8 pushes the task (`--worker-port`, default 8080; `--push-path`,
default `/`). If the task message carries `head.serverUrl`, rn8 rewrites it to
the relay too. The worker answers the push when the step ends — the function
completed or suspended — and rn8 exits then.

## Frames

Newline-delimited JSON on rn8's stdin and stdout; rn8's own diagnostics go to
stderr.

```text
plugin → rn8
{"type":"task","v":1,"task":{...}}              first frame, exactly once
{"type":"res","id":7,"status":200,"body":{...}}

rn8 → plugin
{"type":"req","id":7,"body":{...}}              a protocol request from the SDK
{"type":"log","stream":"stdout","data":"..."}   worker output
```

| rn8 exit | Meaning |
|---|---|
| 0 | The step ended normally |
| 1 | The worker crashed, the push failed, or stdin closed |
| 2 | Framing or version error |

## Scope

A request is forwarded only if it is the claimed task's to make: `task.acquire`
at the claimed version; `task.heartbeat`, `task.get`, `task.release`,
`task.fulfill`, `task.suspend` and `task.fence` on the claimed task;
`promise.get`, `promise.create` and `promise.settle` within the task's origin
(plus creating, and reading back, a new root, as `task.fence` allows);
`promise.register_callback` with the claimed task as awaiter. Everything else —
`task.create`, schedules, searches, listeners, `task.halt`/`task.continue`,
`debug.*` — is answered 403 without reaching the server.

## Not in this version

Snapshot as continuation, host-side secrets, and cleaning up sandboxes orphaned
by a plugin crash.
