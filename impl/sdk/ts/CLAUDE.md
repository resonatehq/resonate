# CLAUDE.md

This file helps AI coding agents understand the resonate-sdk-ts repository.

## What this repo is

The Resonate TypeScript SDK (`@resonatehq/sdk`) lets developers write reliable, distributed applications using ordinary TypeScript generator functions. The SDK coordinates with the Resonate Server to persist function execution state, enabling long-running workflows that survive process restarts.

## Build

```shell
npm install
npm run build        # compiles TypeScript via tsc (output in dist/)
npm run type-check   # type-check without emitting
```

Requires Node >= 22.

## Test

```shell
npm test             # runs Jest (jest.config.js)
npm run dst          # runs the deterministic simulation (sim/main.ts)
```

Individual test files live in `tests/`. The DST (deterministic simulation testing) lives in `sim/`.

## Lint / Format

```shell
npm run check        # Biome linter check
npm run check:fix    # Biome linter check with auto-fix
npm run fmt          # Biome formatter
```

## Key directories

| Path | Purpose |
|------|---------|
| `src/` | Core SDK source |
| `src/resonate.ts` | Main `Resonate` class — entry point for users |
| `src/context.ts` | `Context` type passed to every Resonate function |
| `src/core.ts` | Execution engine |
| `src/coroutine.ts` | Generator coroutine driver |
| `src/promises.ts` | Durable promise primitives |
| `src/schedules.ts` | Schedule API |
| `src/tasks.ts` | Task polling and dispatch |
| `src/network/` | HTTP networking (remote server communication) |
| `src/processor/` | Task processor |
| `tests/` | Jest unit and integration tests |
| `sim/` | Deterministic simulation (DST) for chaos/reliability testing |
| `dist/` | Compiled output (not committed) |

## Key conventions

- **Generator functions**: Resonate functions are TypeScript generator functions (`function*`). Yielding `context.run(...)` or `context.sleep(...)` creates durable checkpoints. This is the core programming model — do not convert them to async/await.
- **`yield*` vs `yield`**: Use `yield*` with `context.run()` to properly delegate to sub-generators.
- **Registration**: Functions must be registered with `resonate.register(fn)` before they can be invoked.
- **Encoder**: Results are serialized via `src/encoder.ts`. Custom encoders can be provided.
- **Options**: Per-call options (timeout, tags, retry policy) are set via `src/options.ts`.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the fork-and-branch workflow. PRs should be squash-merged.
