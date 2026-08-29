![resonate banner](./assets/resonate-banner.png)

# Resonate Server

## About this component

The Resonate Server is a highly efficient single binary that pairs with a Resonate SDK to bring durable execution to your application — reliable, distributed function execution that survives process restarts and failures. It acts as both a supervisor and orchestrator for Resonate Workers, persisting execution state so long-running functions always run to completion.

- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [The concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Blog](https://journal.resonatehq.io/subscribe)
- [Follow on X](https://x.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Resonate quickstart

![resonate quickstart banner](./assets/quickstart-banner.png)

### 1. Install the Resonate Server & CLI

```shell
brew install resonatehq/tap/resonate
```

### 2. Install the Resonate SDK

#### TypeScript

```shell
npm install @resonatehq/sdk
```

### 3. Write your first Resonate Function

A countdown as a loop. Simple, but the function can run for minutes, hours, or days, despite restarts.

#### TypeScript (countdown.ts)

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

[Working example](https://github.com/resonatehq-examples/example-quickstart-ts)

### 4. Start the server

```shell
resonate serve
```

### 5. Start the worker

#### TypeScript

```shell
npx ts-node countdown.ts
```

### 6. Activate the function

Activate the function with execution ID `countdown.1`:

```shell
resonate invoke countdown.1 --func countdown --arg 5 --arg 60
```

### 7. Result

You will see the countdown in the terminal

#### TypeScript

```shell
npx ts-node countdown.ts
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

### What to try

## More ways to install the server

For more Resonate Server deployment information see the [Set up and run a Resonate Server](https://docs.resonatehq.io/operate/run-server) guide.

## Install with Homebrew

You can download and install the Resonate Server using Homebrew with the following commands:

```shell
brew install resonatehq/tap/resonate
```

This previous example installs the latest release.
You can see all available releases and associated release artifacts on the [releases page](https://github.com/resonatehq/resonate/releases).

Once installed, you can start the server with:

```shell
resonate serve
```

You will see log output like the following:

```shell
2026-04-02T05:05:32.480430Z  INFO resonate: Resonate Server starting on port 8001
2026-04-02T05:05:32.480805Z  INFO resonate: Using SQLite backend: resonate.db
2026-04-02T05:05:32.486547Z  INFO resonate: SQLite initialized
2026-04-02T05:05:32.492689Z  INFO resonate: Metrics server listening on 0.0.0.0:9090
2026-04-02T05:05:32.492915Z  INFO resonate: Server listening on 0.0.0.0:8001
```

The output indicates that the server has HTTP endpoints available at port 8001 and a metrics endpoint at port 9090.

These are the default ports and can be changed via configuration.
The SDKs are all configured to use these defaults unless otherwise specified.

### Run with Docker

The Resonate Server repository contains a Dockerfile that you can use to build and run the server in a Docker container.
You can also clone the repository and start the server using Docker Compose:

```shell
git clone https://github.com/resonatehq/resonate
cd resonate
docker-compose up
```

### Build from source

If you don't have Homebrew, we recommend building from source using Cargo.
Run the following commands to download the repository and build the server:

```
git clone https://github.com/resonatehq/resonate
cd resonate
cargo build --release
```

After it is built, you can compile and run it as a Go program using the following command:

```
cargo run serve
```

Or, you can run it as an executable using the following command:

```
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
