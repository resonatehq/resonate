![resonate component banner](./assets/resonate-banner.png)

# Resonate Server

[![CI](https://github.com/resonatehq/resonate/actions/workflows/ci.yml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate Server is a highly efficient single binary that pairs with a Resonate SDK to bring durable execution to your application — reliable, distributed function execution that survives process restarts and failures. It acts as both a supervisor and orchestrator for Resonate Workers, persisting execution state so long-running functions always run to completion.

- [Open an issue or pull request](https://github.com/resonatehq/resonate/issues)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [Distributed Async Await — the concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Journal](https://journal.resonatehq.io/subscribe)
- [Follow on X](https://x.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Quickstart

![quickstart banner](./assets/quickstart-banner.png)

1. Install the Resonate Server & CLI

```shell
brew install resonatehq/tap/resonate
```

2. Install the Resonate SDK

```shell
npm install @resonatehq/sdk
```

3. Write your first Resonate Function

A countdown as a loop. Simple, but the function can run for minutes, hours, or days, despite restarts.

Save the following as `countdown.ts`:

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
// Instantiate Resonate
const resonate = new Resonate({ url: "http://localhost:8001" });
// Register the function
resonate.register(countdown);
```

[Clone a working example repo](https://github.com/resonatehq-examples/example-quickstart-ts)

4. Start the server

```shell
resonate serve
```

5. Start the worker

```shell
npx ts-node countdown.ts
```

6. Run the function

Run the function with execution ID `countdown.1`:

```shell
resonate invoke countdown.1 --func countdown --arg 5 --arg 60
```

**Result**

You will see the countdown in the terminal running the worker:

```shell
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

**What to try**

After starting the function, inspect the current state of the execution using the `resonate tree` command. The tree command visualizes the call graph of the function execution as a graph of durable promises.

```shell
resonate tree countdown.1
```

Now try killing the worker mid-countdown and restarting. **The countdown picks up right where it left off without missing a beat.**

## More ways to install the server

For more Resonate Server deployment information see the [Set up and run a Resonate Server](https://docs.resonatehq.io/operate/run-server) guide.

### Install with Homebrew

You can download and install the Resonate Server using Homebrew with the following commands:

```shell
brew install resonatehq/tap/resonate
```

This installs the latest release.
You can see all available releases and associated release artifacts on the [releases page](https://github.com/resonatehq/resonate/releases).

Once installed, you can start the server with:

```shell
resonate serve
```

You will see log output like the following:

```shell
2026-04-02T05:05:32.480430Z  INFO resonate: Resonate Server starting port=8001
2026-04-02T05:05:32.480805Z  INFO resonate: Using SQLite backend path=resonate.db
2026-04-02T05:05:32.486547Z  INFO resonate: SQLite initialized
2026-04-02T05:05:32.492689Z  INFO resonate: Metrics server listening port=9090
2026-04-02T05:05:32.492915Z  INFO resonate: Server listening bind=0.0.0.0 port=8001 server_url=http://localhost:8001
```

The output indicates that the server has HTTP endpoints available at port 8001 and a metrics endpoint at port 9090.

These are the default ports and can be changed via configuration.
The SDKs are all configured to use these defaults unless otherwise specified.

### Run with Docker

The Resonate Server repository contains a Dockerfile that you can use to build and run the server in a Docker container.
You can also clone the repository and start the server using Docker Compose.
The Compose services are gated behind profiles, so select a storage backend with `--profile`:

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
# SQLite backend
docker compose --profile sqlite up
# or, the Postgres backend
docker compose --profile postgres up
```

### Build from source

If Homebrew is unavailable, build from source using Cargo.
Run the following commands to download the repository and build the server:

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
cargo build --release
```

You can build and run the server in one step with Cargo:

```shell
cargo run --release -- serve
```

Or run the compiled binary directly:

```shell
./target/release/resonate serve
```

## Outbound authentication for HTTP push

When the Resonate Server delivers execute messages to protected Cloud Functions or Cloud Run services, it can attach an outbound authentication header. Configure this under `[transports.http_push.auth]`.

### Google Cloud ID token (recommended for Cloud Run / Cloud Functions)

```toml
[transports.http_push.auth]
mode = "gcp"
# audience = "https://my-function.example.com"  # optional; defaults to the delivery URL
```

Equivalent environment variables:
```
RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__MODE=gcp
RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__AUDIENCE=https://...   # optional
```

Equivalent CLI flags:
```
resonate serve --transports-http-push-auth-mode gcp
```

Tokens are obtained via [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). On Cloud Run, this resolves to the service account identity automatically. Token acquisition and refresh are managed by the `google-cloud-auth` crate.

### Static bearer token

```toml
[transports.http_push.auth]
mode = "bearer"
token = "my-static-token"
```

### No auth (default)

```toml
[transports.http_push.auth]
mode = "none"
```

### Custom header name

The auth header defaults to `Authorization`. Override with:

```toml
[transports.http_push.auth]
mode = "gcp"
header = "X-Custom-Auth"
```
