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
  return await context.run(agent,
    `Write a cited report. ${question}: ${results}`
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

`resonate dev` keeps state in memory for development. `resonate serve` keeps it
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

The console is compiled into the binary, so it works on an air-gapped or on-prem host.

---

## Architecture

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/architecture-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./assets/architecture-light.svg">
    <img alt="Resonate architecture" src="./assets/architecture-light.svg">
  </picture>
</div>

Resonate sits in the middle of the stack you already run — your language, your compute, your storage, your transport — and a plugin for everything it does not natively support yet.

### Build a server with the plugins you want

Resonate lets you build a server from three kinds of plugins: **servers** for
storage, **workers** for transport, and **gateways** for the edge. Name the
crates in `Cargo.toml`, register them in a `Registry`, and call `run` — Cargo
resolves, downloads, and compiles only what you named.

You pick plugins by naming them in `Cargo.toml` and registering them in `main.rs`.

`Cargo.toml`:

```toml
[dependencies]
resonate-base = { git = "https://github.com/resonatehq/resonate" }
resonate-server-postgres = { git = "https://github.com/resonatehq/resonate" }
resonate-transport-http-push = { git = "https://github.com/resonatehq/resonate" }
resonate-gateway-http = { git = "https://github.com/resonatehq/resonate" }
tokio = { version = "1", features = ["full"] }
```

These crates are not on crates.io yet, so specify them by git or by `path`.

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

Resonate ships with the following plugins:

| Kind | Plugin |
|---|---|
| Server | `server_sqlite` |
| Server | `server_postgres` |
| Server | `server_mysql` |
| Server | `server_scylladb` |
| Server | `server_blob` |
| Worker | `transport_http_push` |
| Worker | `transport_http_poll` |
| Worker | `transport_gcps` |
| Worker | `worker_bash` |
| Gateway | `gateway_http` |
| Gateway | `gateway_web` |
| Gateway | `gateway_metrics` |

---

## Why Resonate

**Durable by construction.** Promises, tasks, and schedules are persisted before they are acted on. A crash mid-flight is a resume, not a loss.

**Formally specified.** The protocol has a machine-checked specification in [resonate-specification](https://github.com/resonatehq/resonate-specification), with mechanized invariants — not a prose document that drifted.

**Differentially tested.** Every storage engine is compared step-for-step against an executable oracle on randomized traffic, across SQLite, PostgreSQL, and MySQL, with a snapshot diff after every request.

**One binary.** `brew install`, `resonate serve`, done. No control plane to operate, no cluster to bootstrap.

**Boring where it counts.** Your existing database is the state store. Your existing observability stack gets Prometheus metrics and OpenTelemetry traces.

---

## Backends

Supported backends:

| Backend | Flag |
|---|---|
| **SQLite** | *(default)* |
| **PostgreSQL** | `--storage-type postgres` |
| **MySQL** | `--storage-type mysql` |
| **ScyllaDB** | `--storage-type scylladb` |
| **Blob** | `--storage-type blob` |

All backends are held to the same behaviour by the differential test suite — the same requests go to every engine and to an executable model of the specification, and any divergence fails the build.

---

## Workers

A worker is your code. Run it anywhere.

- **In-process** — embed the SDK in your application and let it serve its own executions.
- **Out-of-process** — run a fleet of workers with their own lifecycle, scaled independently.

Resonate supports the following transports:

| Transport | Shape |
|---|---|
| **HTTP push** | Resonate calls your endpoint. Ideal for Cloud Run, Cloud Functions, and anything with a URL. |
| **HTTP long-poll** | Your worker holds a connection open. Ideal behind NAT, in a laptop, or in a private cluster. |
| **Google Cloud Pub/Sub** | Resonate publishes; your subscribers pick up the work. |

---

## Plugins

**Integration plugins** connect external systems to Resonate. Start a job in Airflow, render an image in Bannerbear, send a notification in Gotify — Resonate begins the work, tracks it to completion, and settles the promise with the result.

These are different from the [server plugins](#build-a-server-with-the-plugins-you-want) that define storage, transport, and the API edge.

See the [catalogue](https://github.com/resonatehq/resonate-plugins) for all **447** systems on the roadmap.

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

See the [full guide](https://docs.resonatehq.io/operate/run-server) for deployment instructions.

### Homebrew

```shell
brew install resonatehq/tap/resonate
resonate serve
```

Or download binaries directly from the [releases page](https://github.com/resonatehq/resonate/releases).


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

Configuration layers in order of increasing precedence:

1. TOML file (`resonate.toml`)
2. Environment variables
3. CLI flags

```shell
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=0.0.0.0:3000
RESONATE_SERVERS__ACTIVE=server_postgres
RESONATE_SERVERS__SERVER_POSTGRES__URL=postgres://...
```

### Outbound authentication for HTTP push

When Resonate calls protected endpoints, it can attach an auth header under `[workers.transport_http_push.auth]`:

| Mode | TOML | CLI |
|---|---|---|
| **GCP ID token** (Cloud Run / Cloud Functions) | `mode = "gcp"` | `--transports-http-push-auth-mode gcp` |
| **Static bearer** | `mode = "bearer"` <br> `token = "my-token"` | `--transports-http-push-auth-mode bearer` <br> `--transports-http-push-auth-token my-token` |
| **None** (default) | `mode = "none"` | — |

Tokens come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials); on Cloud Run this resolves to the service account identity automatically. Acquisition and refresh are handled by the `google-cloud-auth` crate.

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
