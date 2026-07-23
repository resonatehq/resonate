![resonate component banner](./assets/resonate-banner.png)

# Resonate TypeScript SDK

[![ci](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/ci.yaml/badge.svg)](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/resonatehq/resonate-sdk-ts/branch/main/graph/badge.svg)](https://codecov.io/gh/resonatehq/resonate-sdk-ts)
[![dst](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/dst.yml/badge.svg)](https://github.com/resonatehq/resonate-sdk-ts/actions/workflows/dst.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate TypeScript SDK enables developers to build reliable and scalable cloud applications across a wide variety of use cases.

- [How to contribute to this SDK](./CONTRIBUTING.md)
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
resonate dev
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

You will see the countdown in the terminal

```shell
npx ts-node countdown.ts
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

## Async/await

The SDK also ships an async/await engine: the same durable model, written with ordinary `async` functions instead of generators. Import it from `@resonatehq/sdk/async`:

```typescript
import { Resonate, type Context } from "@resonatehq/sdk/async";

async function countdown(ctx: Context, count: number, delay: number) {
  for (let i = count; i > 0; i--) {
    // Run a function, persist its result
    await ctx.run((ctx: Context) => console.log(`Countdown: ${i}`));
    // Sleep
    await ctx.sleep(delay * 1000);
  }
  console.log("Done!");
}

// Instantiate Resonate
const resonate = new Resonate({ url: "http://localhost:8001" });
// Register the function
resonate.register(countdown);
```

Same server, same CLI, same durable promises — the rest of the quickstart is unchanged.

One difference to know about: operations are **eager**. Calling `ctx.run(...)` starts the work immediately and returns an awaitable handle, so fan-out is ordinary promise code:

```typescript
async function checkout(ctx: Context) {
  const payment = ctx.run(chargeCard);     // starts now
  const inventory = ctx.run(reserveItems); // runs concurrently
  return await Promise.all([payment, inventory]);
}
```

## Migrating from generators

Both engines live in the same package and speak the same protocol to the same server, so you can migrate one function at a time. The mechanical changes:

| Generator engine | Async engine |
| --- | --- |
| `import { Resonate } from "@resonatehq/sdk"` | `import { Resonate } from "@resonatehq/sdk/async"` |
| `function* (context: Context, ...)` | `async function (ctx: Context, ...)` |
| `yield* context.run(...)`, `yield* context.sleep(...)` | `await ctx.run(...)`, `await ctx.sleep(...)` |
| `yield context.beginRun(...)`, later `yield future` | `const p = ctx.run(...)`, later `await p` — every op is eager |
| `resonate.run(id, func, ...args)` → the result | `resonate.run(id, func, ...args)` → a handle; `await handle.result()` |
| `resonate.beginRun(id, func, ...args)` → a handle | same call — there are no `begin*` variants, `run` *is* begin-run |

Two things to watch:

- **Retries are opt-in.** The generator engine retries plain functions with exponential backoff by default. The async engine never retries by default — an async workflow and a plain async function are indistinguishable at runtime, so there is no safe blanket default. To keep retry behavior, pass a policy explicitly:

  ```typescript
  import { Exponential } from "@resonatehq/sdk/async";

  await ctx.run(chargeCard, ctx.options({ retryPolicy: new Exponential() }));
  ```

- **Only await durable promises inside a workflow.** A plain `await` on a timer or network call is invisible to the engine — the workflow may resume after its execution pass has ended and abort. Wrap side effects in `ctx.run`, the same rule as `context.run` in the generator engine.
