# CLAUDE.md

This file helps AI coding agents understand the resonate-sdk-ts repository.

## What this repo is

The Resonate TypeScript SDK (`@resonatehq/sdk`) lets developers write reliable, distributed applications. The SDK coordinates with the Resonate Server to persist function execution state, enabling long-running workflows that survive process restarts. Workflows are TypeScript generator functions (`function*`) driven by a coroutine.

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
```

Individual test files live in `tests/`. The DST (deterministic simulation testing) lives in `sim/`.

## Lint / Format

```shell
npm run check        # Biome linter check
npm run check:fix    # Biome linter check with auto-fix
npm run fmt          # Biome formatter
```

## Monorepo layout

This is an npm-workspaces monorepo. The root package **is** the core SDK
(`@resonatehq/sdk`, source in `src/`), mirroring the Python SDK where the
workspace root is the core package. Platform FaaS shims live under `packages/*`
and each publish independently:

| Package | Dir | Runtime | Publishes as |
|---------|-----|---------|--------------|
| core | `.` (root) | Node | `@resonatehq/sdk` |
| aws | `packages/aws` | Node (Lambda) | `@resonatehq/aws` |
| gcp | `packages/gcp` | Node (Cloud Functions) | `@resonatehq/gcp` |
| cloudflare | `packages/cloudflare` | Cloudflare Workers | `@resonatehq/cloudflare` |

Shims declare a normal semver dep on `@resonatehq/sdk` (publishable). For local
dev, the root `overrides` field (`"@resonatehq/sdk": "file:."`) symlinks that dep
to the local core so shims build against local source, not the registry — this
is npm's stand-in for uv's `workspace = true` (npm won't self-link the root as
a workspace member). Run `npm install` at the root; build core first
(`npm run build`) since shims resolve it via `dist/`.

## Key directories

| Path | Purpose |
|------|---------|
| `src/` | SDK source (`@resonatehq/sdk`) |
| `packages/` | Platform FaaS shims (see Monorepo layout above) |
| `src/resonate.ts` | Main `Resonate` class — entry point for users |
| `src/context.ts` | `Context` type passed to every Resonate function |
| `src/core.ts` | Execution engine |
| `src/coroutine.ts` | Generator coroutine driver |
| `src/promises.ts` | Durable promise primitives |
| `src/schedules.ts` | Schedule API |
| `src/network/` | HTTP networking (remote server communication) |
| `tests/` | Jest unit and integration tests |
| `sim/` | Deterministic simulation (DST) for chaos/reliability testing |
| `dist/` | Compiled output (not committed) |

## Key conventions

- **Generator workflows**: workflows are generator functions (`function*`). Yielding `context.run(...)` or `context.sleep(...)` creates durable checkpoints. Use `yield*` with `context.run()` to properly delegate to sub-generators.
- **Registration**: functions must be registered with `resonate.register(fn)` before they can be invoked.
- **Serialization**: values cross the wire through `src/codec.ts`; a custom `Encryptor` can be provided.
- **Options**: per-call options (timeout, tags, target, retry policy) are built via `src/options.ts` and passed as a trailing argument.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the fork-and-branch workflow. PRs should be squash-merged.
