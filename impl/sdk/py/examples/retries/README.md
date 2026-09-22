# retries — retry functions, not workflows

Resonate's retry rule:

> **A durable function is retried only if it performs no durable operation.**

A function that never calls `ctx.run` / `ctx.rpc` / `ctx.sleep` / `ctx.promise` /
`ctx.detached` is a plain **leaf**. It has no durable footprint, so re-running it
is safe — on failure Resonate retries it per a
[`RetryPolicy`](../../src/resonate/retry.py) until it succeeds or the policy gives
up. A function that *does* perform a durable op is a **workflow**, recovered by
replay from its durable promises, and is never retried.

Here a flaky `charge` leaf (it fails twice, then succeeds) is invoked through
every entrypoint, and Resonate retries it every time:

| Entrypoint | Where | Policy used |
|------------|-------|-------------|
| `resonate.run(id, charge, ...)` | top-level, as a root task | registered |
| `resonate.rpc(id, "charge", ...)` | top-level, by name | registered |
| `ctx.run(charge, ...)` | inside the `checkout` workflow | per-call (`with_opts`) |
| `ctx.rpc("charge", ...)` | inside the `checkout` workflow | registered |

Every path runs `charge` as a leaf, so every path is retried. `checkout` itself
performs durable ops, so it is a workflow and is never retried — only the leaves
it calls are.

## Where a policy comes from (innermost wins)

| Level | API |
|-------|-----|
| SDK-wide default | `Resonate(retry_policy=...)` — defaults to `Exponential()` |
| Per function | `resonate.register(fn, retry_policy=...)` |
| Per `ctx.run` call | `ctx.with_opts(retry_policy=...).run(fn, ...)` |

For the run-as-root-task paths (`resonate.run`, `resonate.rpc`, `ctx.rpc`), a
remote dispatch carries no policy on the wire — the executing worker resolves it
from the registry, which is why registering `charge` with a policy is what makes
those paths retry. Built-in policies live in
[`resonate.retry`](../../src/resonate/retry.py): `Exponential`, `Linear`,
`Constant`, `Never`.

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
uv run python examples/retries
```

Expected output:

```
resonate.run:
  [resonate.run] attempt 1...
  [resonate.run] attempt 2...
  [resonate.run] attempt 3...
  -> charged $100
resonate.rpc:
  [resonate.rpc] attempt 1...
  [resonate.rpc] attempt 2...
  [resonate.rpc] attempt 3...
  -> charged $200
ctx.run + ctx.rpc (inside the checkout workflow):
  [ctx.run] attempt 1...
  [ctx.run] attempt 2...
  [ctx.run] attempt 3...
  [ctx.rpc] attempt 1...
  [ctx.rpc] attempt 2...
  [ctx.rpc] attempt 3...
  -> charged $300 | charged $400
```

Each path fails twice; the `Constant(max_retries=5, delay=0)` policy re-runs
`charge` immediately, and the third attempt succeeds.
