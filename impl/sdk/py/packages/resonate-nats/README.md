# resonate-nats

NATS connector for Resonate.

`NatsConnection` is both a `Network` (request/reply to the Resonate server)
and a `Source` (subscriptions carrying `execute` / `unblock` messages), so it
can serve as a Resonate client's only connection — passed as `network`, it
doubles as the source.

```python
import nats

from resonate.resonate import Resonate
from resonate_nats import NatsConnection

conn = NatsConnection(await nats.connect("nats://localhost:4222"), group="workers")
resonate = Resonate(network=conn, group="workers")
```

The NATS client's lifecycle stays yours: pass one already connected, and
`stop()` tears down only this connection's subscriptions.

Name the same `group` on both. The connection uses it to build the subjects it
subscribes to; Resonate uses it as the routing target a `run`/`rpc` defaults
to. Told two different groups, a worker would dispatch its own work to a group
it is not listening on.

## Addressing

Addresses are NATS subjects wearing a `nats://` scheme, which the server's
`url.Parse` hands straight back:

| | Subject | Advertised as |
|---|---|---|
| unicast | `resonate.recv.{group}.{pid}` | `nats://resonate.recv.workers.7f3a` |
| anycast | `resonate.recv.{group}` | queue-subscribed on `{group}`, so exactly one member gets each message |

Requests are published to `{api_prefix}.{base64url(origin)}` — the routing
origin the SDK hands to `send`. The request body is never parsed here: it is
bytes to be moved.

## Errors

Transport failures surface as `ConnectorError`, so application code can handle
*any* transport giving up without importing this package or knowing which
connector is in use:

```python
from resonate_base import ConnectorError

try:
    await resonate.run("greet", ...)
except ConnectorError as exc:
    ...
```

## Install

```bash
pip install resonate-nats
```

Deliberately not an extra of `resonate-sdk`: an extra would put this package in
the SDK's published dependency metadata, pointing the arrow back down the
stack. `resonate-nats` depends only on
[`resonate-base`](https://pypi.org/project/resonate-base/) and `nats-py`, never
on `resonate-sdk`.
