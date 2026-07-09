# CLAUDE.md

This file helps AI coding agents understand the resonate-sdk-ts repository.

## What this repo is

The Resonate TypeScript SDK (`@resonatehq/sdk`) lets developers write reliable, distributed applications. The SDK coordinates with the Resonate Server to persist function execution state, enabling long-running workflows that survive process restarts. It ships two execution engines that speak the same protocol to the same server:

- **Generator engine** (`@resonatehq/sdk`, `src/`) — workflows are TypeScript generator functions (`function*`) driven by a coroutine.
- **Async engine** (`@resonatehq/sdk/async`, `src/async/`) — workflows are ordinary `async` functions; durable operations are eager and awaited.

## Build

```shell
npm install
npm run build        # compiles TypeScript via tsc (output in dist/)
npm run type-check   # type-check without emitting
```

Requires Node >= 22.

## Test

```shell
npm test             # runs Jest (jest.config.cjs)
npm run dst          # runs the deterministic simulation (sim/main.ts)
npm run dst:diff     # differential test: async engine vs generator engine (see diff-testing.md)
```

Individual test files live in `tests/` (generator engine) and `tests/async/` (async engine). The DST (deterministic simulation testing) lives in `sim/`.

## Lint / Format

```shell
npm run check        # Biome linter check
npm run check:fix    # Biome linter check with auto-fix
npm run fmt          # Biome formatter
```

## Key directories

| Path | Purpose |
|------|---------|
| `src/` | Generator engine source (`@resonatehq/sdk`) |
| `src/resonate.ts` | Main `Resonate` class — entry point for users |
| `src/context.ts` | `Context` type passed to every Resonate function |
| `src/core.ts` | Execution engine |
| `src/coroutine.ts` | Generator coroutine driver |
| `src/promises.ts` | Durable promise primitives |
| `src/schedules.ts` | Schedule API |
| `src/network/` | HTTP networking (remote server communication), shared by both engines |
| `src/async/` | Async engine source (`@resonatehq/sdk/async`): `resonate.ts` (`Resonate`), `context.ts` (eager ops, `DurablePromise`), `core.ts` (task driver) |
| `tests/` | Jest unit and integration tests (`tests/async/` for the async engine, `tests/equivalence/` for cross-engine differential tests) |
| `sim/` | Deterministic simulation (DST) for chaos/reliability testing |
| `dist/` | Compiled output (not committed) |

## Key conventions

- **Two engines, one protocol**: both engines create the same durable promises and tasks against the same server. Changes to shared modules (`src/network/`, `src/codec.ts`, `src/options.ts`, `src/registry.ts`, `src/util.ts`, `src/retries.ts`) affect both.
- **Generator engine**: workflows are generator functions (`function*`). Yielding `context.run(...)` or `context.sleep(...)` creates durable checkpoints. Use `yield*` with `context.run()` to properly delegate to sub-generators. Do not convert generator workflows to async/await — they belong to this engine; the async engine is a separate implementation, not a rewrite target.
- **Async engine**: workflows are ordinary `async` functions. Durable operations are **eager** — `ctx.run(...)` starts immediately and returns a `DurablePromise` — and there are no `begin*` variants. Retries default to `Never` (opt-in via `retryPolicy`). Inside a workflow, `await` must only target durable promises; wrap side effects in `ctx.run`.
- **Registration**: functions must be registered with `resonate.register(fn)` before they can be invoked (both engines).
- **Serialization**: values cross the wire through `src/codec.ts`; a custom `Encryptor` can be provided.
- **Options**: per-call options (timeout, tags, target, retry policy) are built via `src/options.ts` and passed as a trailing argument.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the fork-and-branch workflow. PRs should be squash-merged.
