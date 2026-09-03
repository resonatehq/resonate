<div align="center">

![Resonate](./assets/resonate-banner.png)

[![License](https://img.shields.io/badge/license-Apache--2.0-1EE3CF?style=flat-square)](./LICENSE)
[![Rust](https://img.shields.io/badge/built%20with-Rust-1EE3CF?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Discord](https://img.shields.io/badge/Discord-join-1EE3CF?style=flat-square&logo=discord&logoColor=white)](https://resonatehq.io/discord)
[![Docs](https://img.shields.io/badge/docs-resonatehq.io-1EE3CF?style=flat-square)](https://docs.resonatehq.io/)

[Example](#example) · [Install](#install-and-run) · [Console](#console) · [Architecture](#architecture) · [Backends](#backends) · [Workers](#workers) · [Plugins](#plugins) · [Deploy](#deploy) · [Docs](https://docs.resonatehq.io/)

</div>

---

[Resonate](https://resonatehq.io/) is an AI-native, extensible durable execution platform for agentic and classic workloads. Resonate features a dead simple programming model and a dead simple operational model: functions and promises on a single binary. Write normal code and get durable, scalable, and reliable applications.

---

## Example

A deep research agent: plan the searches, fan them out, synthesize the results.

```typescript
async function research(context: Context, question: string) {
  // Plan the searches
  const queries = await context.run(agent,
    `Plan the searches for: ${question}`
  );
  // Fan out the searches
  const results = await Promise.allSettled(
    queries.map((q) => context.rpc(search, q))
  );
  // Synthesize the results
  const cited = results.filter((r) => r.status === "fulfilled").map((r) => r.value);
  return await context.run(agent,
    `Write a cited report. ${question}: ${cited}`
  );
}
```

That is the whole orchestration — no queue to drain, no state machine to
advance, no scheduler to configure.

- **`context.run`** calls a function and persists its result. On recovery the
  call is not made again, its result is read back — you never pay for the same
  tokens twice.
- **`context.rpc`** calls a function on another worker, on another machine, in
  another language. It returns a promise, so `Promise.allSettled` gives you
  fan-out with per-branch failure handling — the same code you would write
  in-process.
- **Everything in between survives.** Kill the worker mid-flight and the
  execution is still there, waiting for the next one to pick it up.

---

## Install and run

**1. Install Resonate**

```shell
brew install resonatehq/tap/resonate
```

**2. Install an SDK**

```shell
npm install @resonatehq/sdk
```

**3. Write the worker** — `research.ts`

`agent` and `search` are your code: a model call and a search API. Resonate does
not care what is inside them, only that their results are worth keeping.

```typescript
import { type Context, type Info, Resonate } from "@resonatehq/sdk/async";

async function agent(info: Info, prompt: string) {
  // your model call
}

async function search(info: Info, query: string) {
  // your search API
}

async function research(context: Context, question: string) {
  // as above
}

const resonate = new Resonate({ url: "http://localhost:8001" });
resonate.register("research", research);
resonate.register("search", search);
resonate.register("agent", agent);
```

**4. Start Resonate, then the worker**

```shell
resonate dev
npx tsx research.ts
```

`resonate dev` keeps state in memory, for development. `resonate serve` keeps it
in a database — see [Backends](#backends).

**5. Activate the function**

```shell
resonate invoke research.1 --func research --arg "What is durable execution?"
```

```
[agent]  Plan the searches for: What is durable execution?
[search] durable execution
[search] workflow recovery
[search] sagas
[agent]  Write a cited report. What is durable execution? ...
```

Kill the worker while the searches are in flight and start it again. The
execution waits in the meantime, then resumes: the searches that never finished
run again, and the plan — already persisted — does not.

---

## Console

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/console-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/console-light.png">
    <img alt="Resonate console showing durable executions" src="./assets/console-light.png">
  </picture>
</div>

Every durable execution, live: status, function, when it was created, when it settled, and when it times out. Filter by status, function, or time window, and search by id, function, or tag.

The console is compiled into the binary, fonts included, so it works on an
air-gapped or on-prem host. Turn it off with `[gateways.gateway_web] enabled =
false`, or `RESONATE_GATEWAYS__GATEWAY_WEB__ENABLED=false`.

---

## Architecture

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/architecture-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./assets/architecture-light.svg">
    <img alt="Resonate architecture" src="./assets/architecture-light.svg">
  </picture>
</div>

Resonate sits in the middle of the stack you already run — your language, your compute, your storage, your transport — and a plugin for everything it does not reach yet.

### Build a server with the plugins you want

A Resonate server is a list of plugins and a `run`. Everything that varies —
which storage backend answers requests, which transports deliver, which edges
listen — is a plugin, and which ones a binary carries is decided by the binary.
Cargo is the plugin manager: name what you want as a dependency, name it again
in the registry, and only that is resolved, downloaded and compiled.

There is no cargo feature per engine. Choosing plugins is what a *binary* does,
by naming them, and the binary this repository ships is simply the one that
carries all of them.

`Cargo.toml`:

```toml
[dependencies]
resonate-base = { git = "https://github.com/resonatehq/resonate" }
resonate-server-postgres = { git = "https://github.com/resonatehq/resonate" }
resonate-transport-http-push = { git = "https://github.com/resonatehq/resonate" }
resonate-gateway-http = { git = "https://github.com/resonatehq/resonate" }
tokio = { version = "1", features = ["full"] }
```

These crates are not on crates.io yet, so depend on them by git — or by `path`
against a local checkout. Once they are published a version requirement
(`resonate-base = "0.10"`) will say the same thing.

`src/main.rs`:

```rust
use resonate_base::{Options, Registry};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    resonate_base::main(
        Registry::new()
            .server(&resonate_server_postgres::PLUGIN)
            .worker(&resonate_transport_http_push::PLUGIN)
            .gateway(&resonate_gateway_http::PLUGIN),
        Options::default().default_server("server_postgres"),
    )
    .await
}
```

That is the whole binary: PostgreSQL for state, HTTP push for delivery, the
HTTP API at the edge, and nothing else compiled in. It will not fall back to
SQLite — with no `servers.server_postgres.url` configured it refuses to start
and says so. A worker plugin someone else publishes joins the list as one more
`.worker(&…::PLUGIN)` line.

It reads the same `resonate.toml` and the same `RESONATE_*` variables as the
server this repository ships. Each plugin reads its own section — `servers.<id>`,
`workers.<id>` or `gateways.<id>`, where `<id>` is the crate name with the
`resonate-` prefix dropped and dashes turned into underscores, so
`acme-worker-kafka` configures itself under `[workers.acme_worker_kafka]`.

It has no command-line flags of its own. `Options` is where a binary adds them:
`resonate serve`'s flags are `Options::set` calls over those same keys, and a
custom binary can parse whatever it likes and do the same — or parse nothing at
all and let the file and the environment say everything.

| Plugin kind | What it is | Configured under |
|---|---|---|
| **server** | the storage backend that answers requests | `servers.<id>` |
| **worker** | how work reaches your code | `workers.<id>` |
| **gateway** | an edge that listens — the HTTP API, the console, metrics | `gateways.<id>` |

The plugins this repository carries: servers `server_sqlite`, `server_postgres`,
`server_mysql`, `server_scylladb` and `server_blob`; workers
`transport_http_push`, `transport_http_poll`, `transport_gcps` and
`worker_bash`; gateways `gateway_http`, `gateway_web` and `gateway_metrics`.

---

## Why Resonate

|  | |
|---|---|
| **Durable by construction** | Promises, tasks, and schedules are persisted before they are acted on. A crash mid-flight is a resume, not a loss. |
| **Formally specified** | The protocol has a machine-checked specification in [resonate-specification](https://github.com/resonatehq/resonate-specification), with mechanized invariants — not a prose document that drifted. |
| **Differentially tested** | Every storage engine is compared step-for-step against an executable oracle on randomized traffic, across SQLite, PostgreSQL, and MySQL, with a snapshot diff after every request. |
| **One binary** | `brew install`, `resonate serve`, done. No control plane to operate, no cluster to bootstrap. |
| **Boring where it counts** | Your existing database is the state store. Your existing observability stack gets Prometheus metrics and OpenTelemetry traces. |

---

## Backends

Resonate keeps its state in a database you already run.

| Backend | Best for | Select with |
|---|---|---|
| **SQLite** | local development, single-node deployments | the default — `resonate serve` |
| **PostgreSQL** | the production default | `--storage-type postgres`, or `servers.active = "server_postgres"` |
| **MySQL** | wherever it already runs | `--storage-type mysql`, or `servers.active = "server_mysql"` |

`server_scylladb` and `server_blob` ship in the same binary and are selected the
same way.

All three are held to the same behaviour by the differential test suite — the same requests go to every engine and to an executable model of the specification, and any divergence fails the build.

---

## Workers

A worker is your code. Resonate does not care where it runs.

- **In-process** — embed the SDK in your application and let it serve its own executions.
- **Out-of-process** — run a fleet of workers with their own lifecycle, scaled independently.

Resonate reaches them however suits your network:

| Transport | Shape |
|---|---|
| **HTTP push** | Resonate calls your endpoint. Ideal for Cloud Run, Cloud Functions, and anything with a URL. |
| **HTTP long-poll** | Your worker holds a connection open. Ideal behind NAT, in a laptop, or in a private cluster. |
| **Google Cloud Pub/Sub** | Resonate publishes; your subscribers pick up the work. |

---

## Plugins

A plugin represents an **external system's unit of work** — anything with a beginning and an end — as a durable promise. The plugin begins the work, sees it through to its terminal state, and settles the promise with the outcome.

These are integration plugins, and they are a different thing from the server
plugins in [Architecture](#build-a-server-with-the-plugins-you-want) — those are
the crates a server binary is built from.

The [catalogue](https://github.com/resonatehq/resonate-plugins/blob/main/Plugins.md) lists **447** systems on the roadmap. Seven are built today: Apache Airflow, Bannerbear, Baserow, Gotify, n8n, Rundeck, and Zendesk.

→ [resonatehq/resonate-plugins](https://github.com/resonatehq/resonate-plugins)

---

## SDKs

| Language | Repository |
|---|---|
| TypeScript | [resonate-sdk-ts](https://github.com/resonatehq/resonate-sdk-ts) |
| Python | [resonate-sdk-py](https://github.com/resonatehq/resonate-sdk-py) |
| Go | [resonate-sdk-go](https://github.com/resonatehq/resonate-sdk-go) |
| Java | [resonate-sdk-java](https://github.com/resonatehq/resonate-sdk-java) |
| Rust | [resonate-sdk-rs](https://github.com/resonatehq/resonate-sdk-rs) |

---

## Deploy

For the full guide see [Set up and run Resonate](https://docs.resonatehq.io/operate/run-server).

### Homebrew

```shell
brew install resonatehq/tap/resonate
resonate serve
```

Every release and its artifacts are on the [releases page](https://github.com/resonatehq/resonate/releases).

On start you will see:

```shell
INFO resonate_base: Server plugin selected server=server_sqlite
INFO resonate_base: Worker plugin registered worker=transport_http_push schemes=["http", "https"]
INFO resonate_base: Gateway plugin registered gateway=gateway_http
INFO resonate_base: Gateway plugin registered gateway=gateway_web
INFO resonate_gateway_http: Server listening bind=0.0.0.0:8001
INFO resonate_gateway_metrics: Metrics listening bind=0.0.0.0:9090
```

HTTP on `8001`, metrics on `9090`. These are the defaults every SDK assumes, and both are configurable.

### Docker

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
docker-compose up
```

### From source

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
cargo build --release
./target/release/resonate serve
```

---

## Configuration

Configuration comes from a TOML file, environment variables (`RESONATE_` prefix, `__` for nesting), or CLI flags — in that order of increasing precedence.

```shell
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=0.0.0.0:3000
RESONATE_SERVERS__ACTIVE=server_postgres
RESONATE_SERVERS__SERVER_POSTGRES__URL=postgres://...
```

Every flag is one key, and `--set` says the same thing in the general case —
`--server-port 3000` and `--set gateways.gateway_http.bind=0.0.0.0:3000` are the
same override. That is what lets a binary carrying a plugin this repository has
never heard of be configured without a flag being added for it.

### Outbound authentication for HTTP push

When Resonate delivers execute messages to protected Cloud Functions or Cloud Run services, it can attach an outbound authentication header. Configure it under `[workers.transport_http_push.auth]`.

**Google Cloud ID token** (recommended for Cloud Run / Cloud Functions)

```toml
[workers.transport_http_push.auth]
mode = "gcp"
# audience = "https://my-function.example.com"  # optional; defaults to the delivery URL
```

```shell
RESONATE_WORKERS__TRANSPORT_HTTP_PUSH__AUTH__MODE=gcp
RESONATE_WORKERS__TRANSPORT_HTTP_PUSH__AUTH__AUDIENCE=https://...   # optional
```

```shell
resonate serve --transports-http-push-auth-mode gcp
```

Tokens come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials); on Cloud Run this resolves to the service account identity automatically. Acquisition and refresh are handled by the `google-cloud-auth` crate.

**Static bearer token**

```toml
[workers.transport_http_push.auth]
mode = "bearer"
token = "my-static-token"
```

**No auth** (default)

```toml
[workers.transport_http_push.auth]
mode = "none"
```

**Custom header name** — defaults to `Authorization`.

```toml
[workers.transport_http_push.auth]
mode = "gcp"
header = "X-Custom-Auth"
```

---

## Learn more

- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [The concepts that power Resonate](https://www.distributed-async-await.io/)
- [Example application library](https://github.com/resonatehq-examples)

## Community

[Discord](https://resonatehq.io/discord) · [Blog](https://journal.resonatehq.io/subscribe) · [X](https://x.com/resonatehqio) · [LinkedIn](https://www.linkedin.com/company/resonatehqio) · [YouTube](https://www.youtube.com/@resonatehqio)

## License

[Apache-2.0](./LICENSE)

<div align="center">
<sub>Logos are the trademarks of their respective owners and appear here to identify the systems Resonate integrates with.</sub>
</div>
