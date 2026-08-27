# resonate-base

The connector seam behind the Resonate Python SDK and every Resonate connector.

`resonate-base` holds what you need to put Resonate on a new substrate, and
nothing else. Everything that describes *executing durable functions* — codec,
context, core, retry, timing, the observability stream, the wire records, the
transport, and the SDK's own error vocabulary — stays in `resonate-sdk`.

It has **no third-party dependencies** and never imports the SDK, so a
connector built on it is independent of the SDK's release cadence. Both
properties are asserted in `tests/test_layering.py`.

## What's in it

| Module | What it gives a connector |
|---|---|
| `resonate_base.connections` | `Network` and `Source` — the two protocols to implement |
| `resonate_base.error` | `ConnectorError`, the one error a connector raises |
| `resonate_base.ids` | The `origin:lineage` promise id format you route by |
| `resonate_base.addresses` | Helpers for the delivery address format the server parses |
| `resonate_base.PROTOCOL_VERSION` | The wire protocol version |

That is the whole package. There is no framework here and nothing to conform
to — implement the two protocols however suits your substrate.

## Building a connector

```python
from resonate_base import ConnectorError, origin_of


class MyConnection:
    """A Network and Source over some new substrate."""

    def pid(self) -> str: ...
    def group(self) -> str: ...
    def unicast(self) -> str: ...
    def anycast(self) -> str: ...
    def target_resolver(self, target: str) -> str: ...
    def recv(self, callback) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def send(self, req: str) -> str:
        try:
            return await self._rpc(origin_of(self._id_of(req)), req)
        except OSError as exc:
            raise ConnectorError(exc) from exc
```

### Errors

`ConnectorError` is all a connector needs. The SDK's outermost catch is
`except ResonateError`, so a transport that raises a bare `OSError` through
`send` kills the worker instead of releasing the task — wrap it.

Subclassing is optional. Do it when your connector ships as its own
distribution and its users benefit from a name they can catch specifically:

```python
class NatsError(ConnectorError):
    label = "nats"          # -> "nats error: no responders"
```

Override `label` and nothing else — the wrapped cause, the `args` tuple (and so
the pickle round-trip the codec depends on), and the message shape are all
inherited.

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

### Addresses

A source advertises where the server should push messages. The server parses
those with Go's `url.Parse`, so an address is a URL and its scheme selects the
delivery mechanism. `resonate_base.addresses` mints the form the SDK's own
sources use:

```python
addresses.unicast("mysub", "workers", "7f3a")     # mysub://uni@workers/7f3a
addresses.anycast("mysub", "workers", "7f3a")     # mysub://any@workers/7f3a
addresses.resolve_target("mysub", "elsewhere")    # mysub://any@elsewhere
```

Convenience, not a rule — if your destination is already an address in your own
namespace, say so directly. `resonate-nats` advertises
`nats://resonate.recv.workers.7f3a` and lets the server hand the subject
straight back.

The one thing worth knowing: Go lowercases the URL host, so an uppercase group
or pid does not round trip, and nothing raises — the server accepts the address
and the message is simply never delivered. These helpers refuse rather than
silently folding.

## Installing

Application code should depend on
[`resonate-sdk`](https://pypi.org/project/resonate-sdk/), which depends on this
package. Install `resonate-base` directly only when building a connector.
