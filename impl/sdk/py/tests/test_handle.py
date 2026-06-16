"""Behaviour tests for :mod:`resonate.handle`.

These cases exercise result decoding per promise state and the non-blocking
``done`` check through the public API, using a real :class:`Codec` round-trip
rather than white-box calls.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.error import ApplicationError, TimeoutError as ResonateTimeoutError
from resonate.handle import PromiseResult, ResonateHandle, Subscription
from resonate.types import Value


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def _encoded(codec: Codec, value: Any) -> Value:
    """Build a wire ``Value`` whose ``data`` is the codec-encoded ``value``."""
    return codec.encode(value)


def _ready(result: PromiseResult) -> Subscription:
    sub = Subscription()
    sub.settle(result)
    return sub


def _created() -> asyncio.Future[None]:
    """Build a resolved creation future: these handles model created promises."""
    fut: asyncio.Future[None] = asyncio.Future()
    fut.set_result(None)
    return fut


@pytest.mark.asyncio
async def test_result_resolved_decodes_value() -> None:
    codec = _codec()
    result = PromiseResult(state="resolved", value=_encoded(codec, 42))
    handle = ResonateHandle("p1", _ready(result), codec, int, _created())
    assert await handle.result() == 42


@pytest.mark.asyncio
async def test_result_rejected_raises_application_error() -> None:
    codec = _codec()
    value = _encoded(codec, {"message": "boom"})
    result = PromiseResult(state="rejected", value=value)
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), codec, int, _created()
    )
    with pytest.raises(ApplicationError, match="boom"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_rejected_canceled_raises() -> None:
    result = PromiseResult(state="rejected_canceled", value=Value())
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ApplicationError, match="Promise canceled"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_rejected_timedout_raises_timeout() -> None:
    result = PromiseResult(state="rejected_timedout", value=Value())
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ResonateTimeoutError):
        await handle.result()


@pytest.mark.asyncio
async def test_result_pending_raises() -> None:
    result = PromiseResult(state="pending", value=Value())
    handle: ResonateHandle[int] = ResonateHandle(
        "p1", _ready(result), _codec(), int, _created()
    )
    with pytest.raises(ApplicationError, match="promise still pending"):
        await handle.result()


@pytest.mark.asyncio
async def test_result_empty_data_decodes_to_none() -> None:
    result = PromiseResult(state="resolved", value=Value(data=""))
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), _codec(), Any, _created()
    )
    assert await handle.result() is None


@pytest.mark.asyncio
async def test_result_absent_data_decodes_to_none() -> None:
    result = PromiseResult(state="resolved", value=Value())
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), _codec(), Any, _created()
    )
    assert await handle.result() is None


@pytest.mark.asyncio
async def test_result_object_round_trips() -> None:
    codec = _codec()
    result = PromiseResult(state="resolved", value=_encoded(codec, {"x": 1}))
    handle: ResonateHandle[Any] = ResonateHandle(
        "p1", _ready(result), codec, Any, _created()
    )
    assert await handle.result() == {"x": 1}


@pytest.mark.asyncio
async def test_result_scalar_round_trips() -> None:
    codec = _codec()
    result = PromiseResult(state="resolved", value=_encoded(codec, 7))
    handle = ResonateHandle("p1", _ready(result), codec, int, _created())
    assert await handle.result() == 7


@pytest.mark.asyncio
async def test_done_reflects_channel_state() -> None:
    sub = Subscription()
    handle: ResonateHandle[int] = ResonateHandle("p1", sub, _codec(), int, _created())
    assert handle.done() is False
    sub.settle(PromiseResult(state="resolved", value=Value(data="")))
    assert handle.done() is True


@pytest.mark.asyncio
async def test_id_blocks_until_creation_confirmed() -> None:
    # The id is not exposed until the creation future resolves: until then a
    # caller awaiting it stays blocked, mirroring ``ResonateFuture.id``.
    created: asyncio.Future[None] = asyncio.Future()
    handle = ResonateHandle(
        "p1",
        _ready(PromiseResult(state="pending", value=Value())),
        _codec(),
        int,
        created,
    )
    id_task = asyncio.create_task(handle.id())
    await asyncio.sleep(0)
    assert not id_task.done()
    created.set_result(None)
    assert await id_task == "p1"


@pytest.mark.asyncio
async def test_id_raises_when_creation_fails() -> None:
    # A failed create rejects the creation future, so ``id`` raises that error
    # rather than handing back an id for a promise that was never created.
    created: asyncio.Future[None] = asyncio.Future()
    handle = ResonateHandle(
        "p1",
        _ready(PromiseResult(state="pending", value=Value())),
        _codec(),
        int,
        created,
    )
    created.set_exception(ApplicationError("create failed"))
    with pytest.raises(ApplicationError, match="create failed"):
        await handle.id()
