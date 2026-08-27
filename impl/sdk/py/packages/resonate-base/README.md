# resonate-base

The connector seam behind the Resonate Python SDK and every Resonate connector.

`resonate-base` holds what you need to put Resonate on a new substrate, and
nothing else. A connector's job is to **move opaque strings** and to **decide
where they go**. Everything else — promise ids, delivery-address conventions,
codec, context, core, retry, timing, the observability stream, the wire
records, the transport, and the SDK's own error vocabulary — stays in
`resonate-sdk`.

It has **no third-party dependencies** and never imports the SDK, so a
connector built on it is independent of the SDK's release cadence.

## What's in it

| Module | What it gives a connector |
|---|---|
| `resonate_base.connections` | `Network` and `Source` — the two protocols to implement |
| `resonate_base.error` | `ConnectorError`, the one error a connector raises |
| `resonate_base.PROTOCOL_VERSION` | The wire protocol version |

That is the whole package. There is no framework here and nothing to conform
to — implement the protocols however suits your substrate.

## Building a connector

```python
from resonate_base import ConnectorError


class MyConnection:
    """A Network and Source over some new substrate."""

    # -- Network: request/response ------------------------------------------
    async def send(self, req: str, origin: str) -> str:
        try:
            return await self._rpc(self._partition_for(origin), req)
        except OSError as exc:
            raise ConnectorError(exc) from exc

    # -- Source: push -------------------------------------------------------
    def unicast(self) -> str: ...
    def resolve_target(self, target: str) -> str: ...
    def recv(self, callback) -> None: ...

    # -- both ---------------------------------------------------------------
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

Implement `Network`, `Source`, or both — a single connection can serve as both
halves (`resonate-nats` does).

### Routing a request: `origin`

`send` receives the request **and** the `origin` it routes by: the lineage the
request acts on, which is what selects the server's origin-state partition. A
substrate that shards needs it; one that posts everything to a single endpoint
ignores it.

It arrives as an argument on purpose. Digging it out of the payload would mean
knowing both the envelope layout and the promise id format — two SDK-internal
formats free to change under you. The SDK owns both, so the SDK resolves it and
hands it over. Two things follow: **`req` is opaque**, so you never parse it,
and **every request has an origin**, so there is no "unrouted" case.

### Addressing: `unicast` and `resolve_target`

A source advertises two things, and it chooses the shape of both:

- `unicast()` — where the server should push messages meant for **this process
  alone**. Handed to the server verbatim when a listener is registered.
- `resolve_target(target)` — where the work this process *dispatches* to a
  named group should be delivered.

The server parses an address with Go's `url.Parse`, dispatches on the scheme,
and hands the rest straight back to you. So `resonate-nats` advertises
`nats://resonate.recv.workers.7f3a` — a NATS subject already is an address in
the NATS namespace, and nesting a second addressing scheme inside it would buy
nothing.

The one trap: Go lowercases the URL **host**, so an uppercase host does not
round trip — and nothing raises. The server accepts the address, stores it, and
the message is simply never delivered.

Sources are optional. A process that only sends (an HTTP handler, a serverless
function) implements `Network` alone.

### Errors

`ConnectorError` is all a connector needs. The SDK's outermost catch is
`except ResonateError`, so a transport that raises a bare `OSError` through
`send` kills the worker instead of releasing the task — wrap it.

Subclassing is optional. Do it when your connector ships as its own
distribution and its users benefit from a name they can catch specifically:

```python
class NatsError(ConnectorError): ...
```

Add nothing else — the wrapped cause, the `args` tuple (and so the pickle
round-trip the codec depends on), and the message shape are all inherited, so
there is no `__init__` to forget to forward.

Either way, application code can handle *any* transport failure without
importing your package:

```python
from resonate_base import ConnectorError

try:
    await resonate.run("greet", ...)
except ConnectorError as exc:
    ...
```

That is what lets the SDK's `SenderError` union stay closed and exhaustively
type-checked while the set of connectors stays open.

## Installing

Application code should depend on
[`resonate-sdk`](https://pypi.org/project/resonate-sdk/), which depends on this
package. Install `resonate-base` directly only when building a connector.
