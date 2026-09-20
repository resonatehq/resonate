# human — human-in-the-loop with a durable promise

An order-fulfillment workflow that *suspends* on a durable promise until an
external party (a human reviewer, a webhook, a UI) resolves it. While the
workflow is suspended the worker holds no state; whenever the resolve
eventually arrives, replay picks up where it left off and proceeds with the
decision.

## What this demonstrates

- **`ctx.promise(timeout)`**: a "dependency-injected" durable promise with a
  global, externally addressable id. The orchestrator awaits it; anyone with
  the id can settle it through `r.promises.resolve(id, ...)`, the CLI, or
  HTTP.
- **Publishing the promise id from a leaf**: `ctx.promise` returns a future
  whose `await fut.id()` is the durable id. We pass that id to a leaf
  (`notify_reviewer`) so the *side effect* — telling the reviewer where to
  resolve — happens exactly once and survives replay.
- **Signalling readiness without polling**: the simulated reviewer doesn't
  retry `r.promises.resolve` until it stops 404-ing. Instead a
  `ReviewerInbox` (an `asyncio.Future[str]`) is injected via
  `Resonate.with_dependency`; `notify_reviewer` completes it the moment the
  durable promise exists, and the reviewer just `await`s the future.
- **Branching on the human decision**: the payload of the resolve is the
  workflow's input for the next step. Approve → `ship_order`. Reject →
  `cancel_order`.
- **Crash-safety**: kill the worker while the workflow is waiting for
  approval; restart it; resolve the promise; the orchestrator picks up at the
  ship/cancel step.

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
# Happy path: simulated reviewer approves.
uv run python examples/human

# Rejected path: simulated reviewer rejects; cancel_order runs instead.
uv run python examples/human --decision reject
```

Expected output (approve):

```
[fulfill_order] starting workflow id=fulfill-... decision='approve'
  [notify_reviewer] order order-42 ($199) needs approval; resolve promise id: 'fulfill-....1'
[reviewer] resolved fulfill-....1 -> approve=True note='looks good'
  [ship_order] shipping order-42 (note: 'looks good')
[fulfill_order] OK: shipped-order-42
```

## Resolving the promise from outside this script

The simulated reviewer in `main()` just calls `r.promises.resolve(...)`. Any
process with the Resonate URL can do the same. With this server running and
a workflow paused on its approval promise, in another terminal:

```python
import asyncio
from resonate.resonate import Resonate
from resonate.types import Value

async def main():
    r = Resonate(url="http://localhost:8001")
    await r.promises.resolve(
        "fulfill-1234567890.1",
        Value.from_serializable({"approve": True, "note": "lgtm"}),
    )
    await r.stop()

asyncio.run(main())
```
