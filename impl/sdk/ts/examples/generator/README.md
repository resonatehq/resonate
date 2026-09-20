# Generator examples

These mirror the [Python SDK examples](https://github.com/resonatehq/resonate-sdk-py/tree/main/examples),
rewritten for the TypeScript SDK's **generator** execution engine (`src/`, the
default `@resonatehq/sdk` entry point). Workflows are generator functions
(`function*`); yielding `ctx.run` / `ctx.rpc` / `ctx.sleep` creates durable
checkpoints. Use `yield*` to delegate to a sub-generator or await a `Future`.

| Example | Shows |
|---|---|
| [`hello.ts`](./hello.ts) | minimal registration + durable invocation |
| [`fibonacci.ts`](./fibonacci.ts) | recursive composition (`--mode run\|rpc\|mix`) |
| [`rpc.ts`](./rpc.ts) | one worker dispatching to another by group/target |
| [`versioning.ts`](./versioning.ts) | several versions of one function side by side |
| [`error-handling.ts`](./error-handling.ts) | failures across the durability boundary (`--mode`, `--error`) |
| [`human-in-the-loop.ts`](./human-in-the-loop.ts) | suspend on `ctx.promise`, resolve externally (`--decision`) |
| [`recovery.ts`](./recovery.ts) | serialize/deserialize + serve a re-run from the durable promise |
| [`detached.ts`](./detached.ts) | fire-and-forget workflow decoupled from the parent |
| [`polling.ts`](./polling.ts) | non-blocking progress via `handle.done()` |
| [`structured-concurrency.ts`](./structured-concurrency.ts) | the runtime joins never-awaited children |
| [`retries.ts`](./retries.ts) | Resonate retries flaky **leaf** functions |
| [`pipeline.ts`](./pipeline.ts) | DAG-shaped pipeline with parallel fan-out / fan-in |
| [`saga.ts`](./saga.ts) | multi-step workflow with compensation on failure (`--fail`) |

## Running

Start a Resonate server on `localhost:8001` (`resonate dev`), then run any
example with [`tsx`](https://github.com/privatenumber/tsx):

```shell
npx tsx examples/generator/hello.ts
npx tsx examples/generator/fibonacci.ts --mode rpc --n 10
npx tsx examples/generator/saga.ts --fail charge
```

Set `RESONATE_URL` to point at a different server. Most single-instance
examples also run fully in-process if you drop the `{ url: "..." }` from the
`new Resonate(...)` call — **except `rpc.ts`**, which uses two instances that
must share a real server.

`just examples` runs the whole set (see the root `justfile`).

## Leaf vs. workflow

A `function*` that yields a durable op is a **workflow**: recovered by replay,
never retried. A plain/`async` function that performs no `ctx.*` op is a
**leaf**: it runs once per attempt and *is* retried on failure (default
`Exponential`; generator workflows default to `Never`). Keep every side effect
(a `console.log`, an external call) in a leaf — a workflow re-executes from the
top on every suspend.
