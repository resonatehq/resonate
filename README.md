![resonate banner](/assets/resonate-banner.png)

# Resonate Server

[![ci](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/cicd.yaml)
[![dst](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml/badge.svg)](https://github.com/resonatehq/resonate/actions/workflows/dst.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/resonatehq/resonate)](https://goreportcard.com/report/github.com/resonatehq/resonate)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate Server acts as a supervisor and orchestrator for Resonate Workers — that is, it provides reliability and scalability to applications built with a Resonate SDK.

**Why does this component exist?**

Popular programming languages like Python and TypeScript are not designed with abstractions that address distribution.
To provide those abstractions at the application level (i.e. to enable API-like abstractions that simplify the implementation of reliable process to process message passing) the system needs a service that acts as both an orchestrator of messages and a supervisor of the processes sending them.

The Resonate Server is a highly efficient single binary that pairs with a Resonate SDK to provide those APIs.

- [How to contribute to this repo](./CONTRIBUTING.md)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [The concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Blog](https://journal.resonatehq.io/subscribe)
- [Follow on Twitter](https://twitter.com/resonatehqio)
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

#### Python

```shell
pip install resonate-sdk
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

#### Python (countdown.py)

```python
from resonate import Resonate, Context
from threading import Event

# Register the function
@resonate.register
def countdown(ctx: Context, count: int, delay: int):
    for i in range(count, 0, -1):
        # Run a function, persist its result
        yield ctx.run(ntfy, i)
        # Sleep
        yield ctx.sleep(delay)
    print("Done!")


def ntfy(_: Context, i: int):
    print(f"Countdown: {i}")


# Instantiate Resonate
resonate = Resonate.remote()
resonate.start()  # Start Resonate threads
Event().wait()  # Keep the main thread alive

[Working example](https://github.com/resonatehq-examples/example-quickstart-py)

### 4. Start the server

```shell
resonate dev
```

### 5. Start the worker

#### TypeScript

```shell
npx ts-node countdown.ts
```

#### Python

```shell
python countdown.py
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

#### Python

```shell
python countdown.py
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

### What to try

After starting the function, inspect the current state of the execution using the `resonate tree` command. The tree command visualizes the call graph of the function execution as a graph of durable promises.

```shell
resonate tree countdown.1
```

Now try killing the worker mid-countdown and restarting. **The countdown picks up right where it left off without missing a beat.**

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
time=2025-09-09T20:54:31.349-06:00 level=INFO msg="starting http server" addr=:8001
time=2025-09-09T20:54:31.349-06:00 level=INFO msg="starting poll server" addr=:8002
time=2025-09-09T20:54:31.351-06:00 level=INFO msg="starting metrics server" addr=:9090
```

The output indicates that the server has HTTP endpoints available at port 8001, a polling endpoint at port 8002, and a metrics endpoint at port 9090.

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

If you don't have Homebrew, we recommend building from source using Go.
Run the following commands to download the repository and build the server:

```
git clone https://github.com/resonatehq/resonate
cd resonate
go build -o resonate
```

After it is built, you can compile and run it as a Go program using the following command:

```
go run main.go serve
```

Or, you can run it as an executable using the following command:

```
./resonate server
```
