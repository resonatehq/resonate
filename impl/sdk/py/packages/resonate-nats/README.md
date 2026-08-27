# resonate-nats

NATS connector for Resonate.

`NatsConnection` is both a `Network` (request/reply to the Resonate server)
and a `Source` (subscriptions carrying `execute` / `unblock` messages), so it
can serve as a Resonate client's only connection.

```python
from resonate.resonate import Resonate
from resonate_nats import NatsConnection

conn = NatsConnection(servers=["nats://localhost:4222"], group="workers")
resonate = Resonate(network=conn, sources=[conn])
```

Transport failures surface as `NatsError`, defined here rather than in the
base package. Application code that wants to handle *any* transport giving up
catches the category instead, and needs neither this package nor knowledge of
which connector is in use:

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
on `resonate-sdk`, and `packages/resonate-nats/tests/test_layering.py` fails
the build if that ever changes.
