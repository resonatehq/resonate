# Async examples

These mirror the [Go SDK examples](https://github.com/resonatehq/resonate-sdk-go/tree/main/examples),
rewritten for the TypeScript SDK's **async/await** execution engine
(`src/async`). Users write ordinary `async` functions and use `await`;
`ctx.run` / `ctx.rpc` / `ctx.sleep` are eager — calling one starts the work
immediately and returns a `Promise<T>`. There are no `begin*` variants.

This set matches the generator-engine examples in [`../generator`](../generator)
one-for-one, so you can compare the two engines side by side.

| Example | Shows |
|---|---|
| [`hello.ts`](./hello.ts) | minimal registration + durable invocation |
| [`fibonacci.ts`](./fibonacci.ts) | recursive composition via `ctx.run` / `ctx.rpc` (`--mode=run\|rpc\|mix`) |
| [`rpc.ts`](./rpc.ts) | one worker dispatching to another by group/target |
| [`versioning.ts`](./versioning.ts) | several versions of one function side by side |
| [`error-handling.ts`](./error-handling.ts) | failures across the durability boundary (`--mode`, `--error`) |
| [`human-in-the-loop.ts`](./human-in-the-loop.ts) | suspend on `ctx.promise`, resolve externally (`--decision`) |
| [`recovery.ts`](./recovery.ts) | serialize/deserialize + serve a re-run from the durable promise |
| [`detached.ts`](./detached.ts) | fire-and-forget workflow decoupled from the parent |
| [`polling.ts`](./polling.ts) | non-blocking progress via `handle.done()` |
| [`structured-concurrency.ts`](./structured-concurrency.ts) | the runtime joins never-awaited children |
| [`retries.ts`](./retries.ts) | Resonate retries flaky **leaf** functions (opt-in policy) |
| [`pipeline.ts`](./pipeline.ts) | DAG-shaped pipeline with a parallel fan-out / fan-in |
| [`saga.ts`](./saga.ts) | multi-step workflow with compensation on failure |

## Running

Start a Resonate server on `localhost:8001` (`resonate dev`), then run any
example with [`tsx`](https://github.com/privatenumber/tsx):

```shell
npx tsx examples/async/hello.ts
npx tsx examples/async/fibonacci.ts --mode=rpc --n=10
npx tsx examples/async/saga.ts --fail=charge
```

Most single-instance examples also run fully in-process if you drop the
`{ url: "..." }` from the `new Resonate(...)` call — **except `rpc.ts`**, which
uses two instances that must share a real server.

`just examples` runs the whole set (both engines); see the root `justfile`.

## Leaf vs. workflow

A function whose first parameter is typed `Info` is a **leaf**: it gets identity
and dependencies but no durable operations, so it completes in a single pass.
A function typed `Context` is a **workflow**: it can call `ctx.run` / `ctx.rpc` /
`ctx.sleep` and suspend on pending durable promises. The stage functions below
are leaves; only the orchestrators take `Context`.
