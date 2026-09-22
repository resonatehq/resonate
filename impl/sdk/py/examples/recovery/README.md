# recovery — typed serialize/deserialize across the durability boundary

Every value a durable function exchanges — its arguments and its return — is
written to a durable promise as JSON. The point of this example is that
**recovery is not a special path**: the same (de)serialization runs on every
invocation, so a value rebuilt after a crash, on a re-run, or on a different
worker is rebuilt by the exact steps that ran the first time.

[`DurableFunction`](../../src/resonate/durable.py) is where that happens. On
every call it coerces each argument to its declared parameter type, and the
top-level [`ResonateHandle`](../../src/resonate/handle.py) coerces the settled
value to the function's declared return type (both `msgspec.convert`). Whether
the input is a freshly-packed in-memory object (live) or the JSON builtins it
round-tripped to (recovery), the coercion is identical — that is what keeps the
two paths in lock-step. A function with **no** annotation opts out and gets raw
builtins, exactly like `rpc`.

## What this demonstrates

- **Typed round-trip** through the boundary: `summarize -> Cart` and
  `pay(cart: Cart) -> Receipt` pass a struct out, back in as an argument, and out
  again as a nested `Receipt` — every hop coerced to its declared type.
- **Recovery is the normal path**: re-running with the same id serves the value
  from the durable promise (genuine recovery — no leaf re-executes) and it comes
  back through the very same deserialize, yielding an *equal* struct. No
  recovery-only branch exists to get wrong.
- **The opt-out / footgun**: dispatching the same function by name via `rpc`
  (which is untyped) returns the identical bytes as a raw `dict`. The value
  survives; only the *type* is lost — because nothing told the runtime how to
  rebuild it.

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
uv run python examples/recovery
```

Expected output:

```
[checkout] run id=recovery-...
  [summarize] ['apple', 'pear', 'plum'] -> Cart(items=['apple', 'pear', 'plum'], total=30)
  [pay] charging 30 for 3 items
[checkout] typed result: Receipt(cart=Cart(items=['apple', 'pear', 'plum'], total=30), paid=True)
[checkout] re-run id=recovery-... (served from the durable promise)
[checkout] recovered result equals the original: True
[checkout] rpc  id=recovery-rpc-... (untyped dispatch by name)
  [summarize] ['apple', 'pear', 'plum'] -> Cart(items=['apple', 'pear', 'plum'], total=30)
  [pay] charging 30 for 3 items
[checkout] untyped (rpc) result is a dict: {'cart': {'items': [...], 'total': 30}, 'paid': True}
```

Note the **re-run prints no leaf lines** — the steps already settled, so the
result is recovered from the durable promise rather than recomputed, and it is
deserialized by the same path that produced the first result. The `rpc` call
runs under a fresh id, so its leaves do re-execute, but its result comes back
untyped.

## A note on replay

Like the other examples, `checkout` re-executes from the top on every replay, so
side effects live in the leaf steps, never in the orchestrator. `checkout`
itself only wires the two steps together.

If you want to *see* a crash-and-recover split rather than a clean re-run, add an
`await asyncio.sleep(5)` at the top of `pay`, run with a hard-coded id, Ctrl-C
after `[summarize] ...` prints, and re-run with the same id: `summarize` is
skipped (its promise is settled and its `Cart` is rebuilt by `coerce_result`),
and execution resumes at `pay` — the same deserialize this example already shows,
now spanning a process boundary.

## The rule of thumb

Annotate the return type (and parameter types) of every durable function whose
value is not a plain JSON builtin. Symmetry holds only when the annotation can
losslessly rebuild the value — a `datetime`, `set`, or custom struct returned as
`Any` (or via `rpc`) recovers as its decoded builtin (`str`, `list`, `dict`) and
the live and recovery paths diverge silently. The behavioural matrix across
`msgspec.Struct`, dataclass, attrs, and enum types is pinned in
[`tests/test_durable.py`](../../tests/test_durable.py).

## Code map

| Symbol      | Role                                                          |
|-------------|---------------------------------------------------------------|
| `checkout`  | Orchestrator: `summarize` then `pay`                          |
| `summarize` | Leaf returning a `Cart` (rebuilt for the next step / caller)  |
| `pay`       | Leaf taking a `Cart` arg, returning a nested `Receipt`        |
| `Cart` / `Receipt` | `msgspec.Struct` domain types that cross the boundary  |
