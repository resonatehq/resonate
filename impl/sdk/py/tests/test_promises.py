"""Behaviour tests for :mod:`resonate.promises`.

The :class:`Promises` / :class:`Schedules` clients are built directly over a
real :class:`Sender` + :class:`Transport` + :class:`LocalNetwork` -- the same
wiring ``Resonate.local()`` performs -- with a ``Codec(NoopEncryptor())``.
"""

from __future__ import annotations

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.error import ServerError
from resonate.network import LocalNetwork
from resonate.promises import Promises
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import Value

I64_MAX = 2**63 - 1


def _local() -> Promises:
    """Build promise/schedule clients sharing one local network, like ``Resonate::local()``."""
    net = LocalNetwork()
    sender = Sender(Transport(net), None)
    codec = Codec(NoopEncryptor())
    return Promises(sender, codec)


@pytest.mark.asyncio
async def test_promises_create_get_resolve_roundtrip() -> None:
    promises = _local()

    created = await promises.create("unit-p1", I64_MAX, Value(data={"x": 1}), {})
    assert created.id == "unit-p1"
    assert created.state == "pending"

    fetched = await promises.get("unit-p1")
    assert fetched.id == "unit-p1"

    settled = await promises.resolve("unit-p1", Value(data={"result": "ok"}))
    assert settled.state == "resolved"

    after = await promises.get("unit-p1")
    assert after.state == "resolved"


@pytest.mark.asyncio
async def test_promises_create_get_reject_roundtrip() -> None:
    # Symmetric counterpart to the resolve roundtrip: ``reject`` settles the
    # promise as ``rejected`` (via ``_settle``), and the state survives a re-get.
    promises = _local()

    created = await promises.create("unit-p-reject", I64_MAX, Value(data={"x": 1}), {})
    assert created.state == "pending"

    settled = await promises.reject("unit-p-reject", Value(data={"error": "boom"}))
    assert settled.state == "rejected"

    after = await promises.get("unit-p-reject")
    assert after.state == "rejected"


@pytest.mark.asyncio
async def test_promises_get_missing_returns_server_error() -> None:
    promises = _local()
    with pytest.raises(ServerError):
        await promises.get("does-not-exist")
