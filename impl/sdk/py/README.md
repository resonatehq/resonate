![resonate banner](./assets/resonate-banner.png)

# Resonate Python SDK

[![ci](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/ci.yml/badge.svg)](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/resonatehq/resonate-sdk-py/graph/badge.svg?token=61GYC3DXID)](https://codecov.io/gh/resonatehq/resonate-sdk-py)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate Python SDK enables developers to build reliable and scalable cloud applications across a wide variety of use cases.

- [How to contribute to this SDK](./CONTRIBUTING.md)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [Distributed Async Await — the concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Journal](https://journal.resonatehq.io/subscribe)
- [Follow on X](https://x.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Requirements

- **Python ≥3.12**
- **Resonate Server**: The Python SDK works with the latest **[Resonate server](https://github.com/resonatehq/resonate)** (v0.9.x and up).

## Quickstart

![quickstart banner](./assets/quickstart-banner.png)

1. Install the Resonate Server & CLI

```shell
brew install resonatehq/tap/resonate
```

2. Install the Resonate SDK

```shell
pip install resonate-sdk
```

3. Write your first Resonate Function

A countdown as a loop. Simple, but the function can run for minutes, hours, or days, despite restarts.

```python
import asyncio
from datetime import timedelta

from resonate.context import Context
from resonate.resonate import Resonate


async def countdown(ctx: Context, count: int, delay: int) -> None:
    for i in range(count, 0, -1):
        # Run a function durably, awaiting its persisted result
        await ctx.run(ntfy, i)
        # Sleep durably -- the worker holds no state while suspended
        await ctx.sleep(timedelta(seconds=delay))
    print("Done!")


async def ntfy(ctx: Context, i: int) -> None:
    print(f"Countdown: {i}")


async def main() -> None:
    # Connect to the Resonate server and register the functions
    resonate = Resonate(url="http://localhost:8001")
    resonate.register(countdown)
    resonate.register(ntfy)

    try:
        # Invoke with execution id "countdown.1"; run() returns a handle
        # immediately, result() awaits the durable outcome
        handle = resonate.run("countdown.1", countdown, 5, 1)
        await handle.result()
    finally:
        await resonate.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

4. Start the server

```shell
resonate dev
```

5. Run the worker

```shell
python countdown.py
```

**Result**

You will see the countdown in the terminal

```shell
python countdown.py
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

**What to try**

While the function is running, inspect the current state of the execution using the `resonate tree` command. The tree command visualizes the call graph of the function execution as a graph of durable promises.

```shell
resonate tree countdown.1
```

Now try killing the worker mid-countdown and restarting `python countdown.py`. Because the invocation id is the same (`countdown.1`), the worker reattaches to the existing durable promise and **the countdown picks up right where it left off without missing a beat.**

## Examples

The [`examples/`](./examples) directory contains runnable programs covering the
core patterns. Start a server (`resonate dev`) on `localhost:8001`, then run any
of them, for example:

```shell
uv run python examples/hello-world
uv run python examples/fibonacci --mode rpc --n 10
```

| Example | What it shows |
| --- | --- |
| [`hello-world`](./examples/hello-world) | A minimal `ctx.run` / `ctx.rpc` chain |
| [`fibonacci`](./examples/fibonacci) | Recursive durable invocations via `run`, `rpc`, or a mix |
| [`pipeline`](./examples/pipeline) | A multi-stage DAG with stages running in parallel |
| [`structured-concurrency`](./examples/structured-concurrency) | The runtime never leaks an unawaited durable child |
| [`recovery`](./examples/recovery) | Typed serialize/deserialize across the durability boundary |
| [`retries`](./examples/retries) | Resonate retrying a flaky leaf function until it succeeds |
| [`error-handling`](./examples/error-handling) | How a failure crosses the boundary and is re-raised |
| [`detached`](./examples/detached) | Fire-and-forget invocations decoupled from the parent |
| [`human-in-the-loop`](./examples/human-in-the-loop) | Suspending on a promise an external party resolves |
| [`polling`](./examples/polling) | Non-blocking progress tracking with `handle.done()` |
| [`rpc`](./examples/rpc) | One worker dispatching to another by group |
| [`versioning`](./examples/versioning) | Running several versions of one function side by side |
| [`saga`](./examples/saga) | Compensating completed steps when a later step fails |
| [`pydantic`](./examples/pydantic) | Pydantic `BaseModel` (de)serialization at the durability boundary |
| [`pydantic-ai`](./examples/pydantic-ai) | Durable [Pydantic AI](https://ai.pydantic.dev) agent runs |

## Integrations

### Pydantic AI

`resonate.ext.pydantic_ai` makes [Pydantic AI](https://ai.pydantic.dev) agent
runs durable: wrap any agent in `ResonateAgent` and every `run()` executes as a
durable workflow, with model requests and MCP server communication checkpointed
as durable steps that are served from the journal on recovery instead of
re-hitting the provider.

```shell
pip install "resonate-sdk[pydantic-ai]"
```

```python
from pydantic_ai import Agent

from resonate.ext.pydantic_ai import ResonateAgent
from resonate.resonate import Resonate

resonate = Resonate(url="http://localhost:8001")

agent = Agent(
    "openai:gpt-5.2",
    instructions="You're an expert in geography.",
    name="geography",
)
durable_agent = ResonateAgent(agent, resonate)


async def main():
    result = await durable_agent.run("What is the capital of Mexico?", id="capital-1")
    print(result.output)
```

The optional `id` is the run's durable identity: retrying with the same id
after a crash (or calling it twice) performs the work exactly once and returns
the recovered result. Function tools run inline in the workflow body and are
re-executed on replay; model and MCP steps are not. Because a crashed run
recovers from its serialized parameters, run-time arguments that cannot be
rebuilt from JSON (`model`, `toolsets`, `output_type`, callables) must be
configured on the agent at construction time. Provider-level retries are best
disabled in favor of Resonate's step retry policies (`model_retry_policy` /
`mcp_retry_policy`).
