<div align="center">

![Resonate](./assets/resonate-banner.png)

[![License](https://img.shields.io/badge/license-Apache--2.0-1EE3CF?style=flat-square)](./LICENSE)
[![Rust](https://img.shields.io/badge/built%20with-Rust-1EE3CF?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Discord](https://img.shields.io/badge/Discord-join-1EE3CF?style=flat-square&logo=discord&logoColor=white)](https://resonatehq.io/discord)
[![Docs](https://img.shields.io/badge/docs-resonatehq.io-1EE3CF?style=flat-square)](https://docs.resonatehq.io/)

[Example](#example) · [Console](#console) · [Quickstart](#quickstart) · [Architecture](#architecture) · [Backends](#backends) · [Workers](#workers) · [Plugins](#plugins) · [Deploy](#deploy) · [Docs](https://docs.resonatehq.io/)

</div>

---

[Resonate](https://resonatehq.io/) is an AI-native, extensible durable execution platform for agentic and classic workloads. Resonate features a dead simple programming model and a dead simple operational model: functions and promises on a single binary. Write normal code and get durable, scalable, and reliable applications.

1. Write durable functions in ordinary code with any of [our language SDKs](#sdks)
2. Run `resonate dev` while you build — one binary, in memory, nothing else to install
3. Deploy your workers wherever your code already runs, in-process or out-of-process
4. Point Resonate at the database you already operate — SQLite, PostgreSQL, or MySQL
5. Resonate invokes your functions over HTTP or Pub/Sub, persisting every step along the way

No DAGs. No YAML. No new language. You write a function; Resonate makes sure it finishes.

---

## Example

A deep research agent — plan the searches, fan them out, synthesize the results:

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

That is the whole orchestration. There is no queue to drain, no state machine to advance, and no scheduler to configure — just `await`.

- **`context.run`** executes a function and persists its result. On recovery the step is not run again, its result is read back — you never pay for the same tokens twice.
- **`context.rpc`** invokes a durable function on another worker, on another machine, in another language. It returns a promise, so ordinary `Promise.allSettled` gives you fan-out with per-branch failure handling.
- **Everything in between survives.** Kill the worker mid-flight, deploy over it, lose the machine — the function resumes from the last persisted step, minutes or days later.

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

---

## Architecture

![Resonate architecture](./assets/architecture.svg)

One binary in the middle. Your database underneath, your workers wherever they live, and a plugin for every system you need to reach.

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

## Quickstart

![Resonate quickstart](./assets/quickstart-banner.png)

**1. Install Resonate**

```shell
brew install resonatehq/tap/resonate
```

Or, with Node already on the machine, skip the install: every `resonate` command
below also runs as `npx resonate-cli@latest`.

**2. Install an SDK**

```shell
npm install @resonatehq/sdk
```

**3. Write a durable function** — `countdown.ts`

```typescript
import { Resonate, type Context } from "@resonatehq/sdk";

function* countdown(context: Context, count: number, delay: number) {
  for (let i = count; i > 0; i--) {
    // Run a function, persist its result
    yield* context.run((context: Context) => console.log(`Countdown: ${i}`));
    // Sleep
    yield* context.sleep(delay * 1000);
  }
  console.log("Done!");
}

const resonate = new Resonate({ url: "http://localhost:8001" });
resonate.register(countdown);
```

[Working example →](https://github.com/resonatehq-examples/example-quickstart-ts)

**4. Start Resonate, then the worker**

```shell
resonate serve
npx ts-node countdown.ts
```

**5. Activate the function**

```shell
resonate invoke countdown.1 --func countdown --arg 5 --arg 60
```

```
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

Kill the worker halfway through. Start it again. The countdown picks up where it left off.

---

## Backends

Resonate keeps its state in a database you already run.

| Backend | Best for | Configure |
|---|---|---|
| **SQLite** | local development, single-node deployments | default — `resonate serve` |
| **PostgreSQL** | the production default | `RESONATE_STORAGE__TYPE=postgres` |
| **MySQL** | wherever it already runs | `RESONATE_STORAGE__TYPE=mysql` |

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

### npm

```shell
npx resonate-cli@latest dev
```

[`resonate-cli`](./npm) carries no implementation. It downloads the release
binary for your platform — the same artifact Homebrew installs, checked against
the `.sha256` published beside it — and hands your arguments to it. For a
JavaScript project that wants Resonate on hand:

```shell
npm install --save-dev resonate-cli
```

On start you will see:

```shell
INFO resonate: Resonate Server starting port=8001
INFO resonate: Using SQLite backend path=resonate.db
INFO resonate: SQLite initialized
INFO resonate: Metrics server listening port=9090
INFO resonate: Server listening bind=0.0.0.0 port=8001
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
RESONATE_SERVER__PORT=3000
RESONATE_STORAGE__TYPE=postgres
RESONATE_STORAGE__POSTGRES__URL=postgres://...
```

### Outbound authentication for HTTP push

When Resonate delivers execute messages to protected Cloud Functions or Cloud Run services, it can attach an outbound authentication header. Configure it under `[transports.http_push.auth]`.

**Google Cloud ID token** (recommended for Cloud Run / Cloud Functions)

```toml
[transports.http_push.auth]
mode = "gcp"
# audience = "https://my-function.example.com"  # optional; defaults to the delivery URL
```

```shell
RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__MODE=gcp
RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__AUDIENCE=https://...   # optional
```

```shell
resonate serve --transports-http-push-auth-mode gcp
```

Tokens come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials); on Cloud Run this resolves to the service account identity automatically. Acquisition and refresh are handled by the `google-cloud-auth` crate.

**Static bearer token**

```toml
[transports.http_push.auth]
mode = "bearer"
token = "my-static-token"
```

**No auth** (default)

```toml
[transports.http_push.auth]
mode = "none"
```

**Custom header name** — defaults to `Authorization`.

```toml
[transports.http_push.auth]
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
