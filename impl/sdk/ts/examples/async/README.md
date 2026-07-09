# Async examples

These mirror the [Go SDK examples](https://github.com/resonatehq/resonate-sdk-go/tree/main/examples),
rewritten for the TypeScript SDK's **async/await** execution engine
(`src/async`). Users write ordinary `async` functions and use `await`;
`ctx.run` / `ctx.rpc` / `ctx.sleep` are eager — calling one starts the work
immediately and returns a `Promise<T>`. There are no `begin*` variants.

| Example | Shows |
|---|---|
| [`hello.ts`](./hello.ts) | minimal registration + durable invocation |
| [`fibonacci.ts`](./fibonacci.ts) | recursive composition via `ctx.run` / `ctx.rpc` (`--mode=run\|rpc\|mix`) |
| [`pipeline.ts`](./pipeline.ts) | DAG-shaped pipeline with a parallel fan-out / fan-in |
| [`saga.ts`](./saga.ts) | multi-step workflow with compensation on failure |

## Running

Start a Resonate server on `localhost:8001`, then run any example with
[`tsx`](https://github.com/privatenumber/tsx):

```shell
npx tsx examples/async/hello.ts
npx tsx examples/async/fibonacci.ts --mode=rpc --n=10
npx tsx examples/async/pipeline.ts
npx tsx examples/async/saga.ts --fail=charge
```

To run fully in-process without a server, drop the `{ url: "..." }` from the
`new Resonate(...)` call — execution then routes through the in-memory
local network.

## Leaf vs. workflow

A function whose first parameter is typed `Info` is a **leaf**: it gets identity
and dependencies but no durable operations, so it completes in a single pass.
A function typed `Context` is a **workflow**: it can call `ctx.run` / `ctx.rpc` /
`ctx.sleep` and suspend on pending durable promises. The stage functions below
are leaves; only the orchestrators take `Context`.
