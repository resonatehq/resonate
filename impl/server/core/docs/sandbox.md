# Sandbox plugin

`worker_sandbox` runs each task dispatched to `sandbox://<image>` inside an
isolated sandbox booted from that image. The plugin is the only component that
talks to the Resonate server; the guest has no network by default and reaches
the server only through its own stdio, relayed by **rn8**, a static binary baked
into the image as its entrypoint. SDKs run unchanged.

| Crate | What it is |
|---|---|
| `resonate-worker-sandbox` | The plugin: sandbox lifecycle, relay, scope check, lease watch |
| `resonate-sandbox` | The frame protocol and the `Backend` / `Process` traits, shared by both ends |
| `resonate-sandbox-microsandbox` | The first backend: a microsandbox microVM per task, through the `msb` CLI |
| `resonate-sandbox-rn8` | `rn8`, the relay in the guest |

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
backend = "microsandbox"   # or "local": no isolation, for development and tests
cpus = 2                   # per sandbox; absent = backend default
memory_mib = 1024          # per sandbox; absent = backend default
egress = "none"            # or "all"
require_digest = true      # refuse sandbox://<image> not pinned by @sha256:…
command = []               # empty = the image's own entrypoint (rn8)
env = {}                   # extra, non-secret environment for that command
concurrency = 16           # sandboxes at once
token = "…"                # attached to every forwarded request; the guest's is dropped
start_timeout = 120000     # ms from dispatch to acquire, create included
exit_grace = 5000          # ms the guest has to exit once its step has ended
msb = "msb"                # the microsandbox CLI
```

The `microsandbox` backend needs `msb` on the host (Linux with KVM, or macOS on
Apple Silicon). It drives the CLI rather than linking the `microsandbox` SDK
crate, which links a SQLite this workspace's sqlx already links at another
version. `msb exec --stream` gives the same live, byte-faithful stdio.

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
