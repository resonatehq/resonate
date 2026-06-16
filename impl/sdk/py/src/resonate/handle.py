from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import msgspec

from resonate.error import (
    ApplicationError,
    TimeoutError as ResonateTimeoutError,
)
from resonate.types import PromiseState, Value

if TYPE_CHECKING:
    from resonate.codec import Codec


class PromiseResult(msgspec.Struct, frozen=True, kw_only=True):
    """The settled state of a durable promise.

    Carries the ``PromiseState`` and the raw, still-encoded wire ``value``.
    Decoding the value is deferred to :class:`~resonate.codec.Codec` when the
    holder reads the result.
    """

    state: PromiseState
    value: Value


class Subscription:
    """A settle-once signal shared by every handle to one promise id.

    Built on an :class:`asyncio.Event` plus a result slot: it needs no loop
    reference at construction, and a single ``settle`` wakes every waiter at
    once. The settle is driven from the ``unblock`` push handler (or an
    already-settled listener registration).
    """

    def __init__(self) -> None:
        self._done = asyncio.Event()
        self._result: PromiseResult | None = None

    def settle(self, result: PromiseResult) -> None:
        """Record the settled result and wake all waiters. Idempotent."""
        if not self._done.is_set():
            self._result = result
            self._done.set()

    def settled(self) -> bool:
        """Whether the subscription has been settled (non-blocking)."""
        return self._done.is_set()

    async def wait(self) -> PromiseResult:
        """Block until settled, then return the result.

        A subscription woken without a settled result is an SDK bug and
        raises.
        """
        await self._done.wait()
        assert self._result
        return self._result


class ResonateHandle[T]:
    """A handle to a durable promise, returned from ``run``, ``rpc``, and ``get``.

    Allows non-blocking observation and eventual awaiting of a durable promise.
    Python cannot resolve the type variable ``T`` at runtime, so the decode
    type is passed explicitly at construction as ``type_``.

    The promise id is not exposed synchronously. For ``run``/``rpc`` the durable
    promise is created in the background, so the id is only meaningful once that
    creation round-trip has confirmed; :meth:`id` awaits ``created`` before
    handing it back, just like :meth:`~resonate.context.ResonateFuture.id`.
    ``created`` is a :class:`asyncio.Future` carrying the creation *outcome*: it
    resolves to ``None`` once the durable promise exists and is rejected with the
    creation error if the round-trip failed, so :meth:`id` never hands back an id
    for a promise that was never created. The ``get`` path passes an
    already-resolved future, since it only builds a handle after the promise has
    been confirmed to exist.
    """

    def __init__(
        self,
        id: str,
        sub: Subscription,
        codec: Codec,
        type_: type[T],
        created: asyncio.Future[None],
    ) -> None:
        self._id = id
        self._sub = sub
        self._codec = codec
        self._type = type_
        self._created = created

    async def id(self) -> str:
        """Return the durable promise id, once its creation is confirmed.

        Awaits the creation future so a caller never observes an id before the
        backing durable promise is known to exist; if creation failed, the
        future's exception is raised here. Behaves like
        :meth:`~resonate.context.ResonateFuture.id`.
        """
        await self._created
        return self._id

    async def result(self) -> T:
        """Block until the promise completes, returning the result or raising."""
        result = await self._sub.wait()
        return self._decode_result(result)

    def done(self) -> bool:
        """Check if the promise is done (non-blocking).

        Synchronous: the SDK runs single-threaded on asyncio, so no lock is
        needed to read the subscription's state.
        """
        return self._sub.settled()

    def _decode_result(self, result: PromiseResult) -> T:
        """Decode a :class:`PromiseResult` into the final ``T`` or raise.

        The wire ``value`` crosses the durability boundary through the
        :class:`~resonate.codec.Codec` -- the sole owner of that decode. This
        method only maps the settled state onto the decoded value (coerced to
        ``T``) or the matching error. The coercion type-shapes an
        already-decoded value, distinct from the codec's serialization work.
        """
        match result.state:
            case "resolved":
                return self._codec.convert(self._codec.decode(result.value), self._type)
            case "rejected":
                raise self._codec.decode_error(result.value)
            case "rejected_canceled":
                msg = "Promise canceled"
                raise ApplicationError(msg)
            case "rejected_timedout":
                raise ResonateTimeoutError
            case "pending":
                msg = "promise still pending"
                raise ApplicationError(msg)
