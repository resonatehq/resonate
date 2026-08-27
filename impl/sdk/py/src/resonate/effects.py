from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol

from resonate.error import PlatformError, ResonateError, StoppedError
from resonate.observability import (
    Dropped,
    PromiseCreateRequested,
    PromiseCreateReturned,
    PromiseSettleRequested,
    PromiseSettleReturned,
    logging_observer,
)
from resonate.types import PromiseCreateReq, PromiseSettleReq

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.observability import Observer
    from resonate.send import PromiseFencing
    from resonate.types import PromiseRecord


class Effects(Protocol):
    """The two durable operations a :class:`~resonate.context.Context` performs.

    Deliberately *only* the two operations: the promise cache is an
    implementation concern of :class:`ResonateEffects`, not part of the
    contract, so a stand-in has two methods to write and nothing else.

    Implementations are expected to be idempotent (a cached or already-settled
    record short-circuits the network) and to behave as a circuit breaker: once
    a durable op has failed in this attempt, every later one raises rather than
    doing more work, so the task is released and re-delivery retries the whole
    execution.
    """

    async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord: ...
    async def settle_promise[T](
        self, id: str, result: T | Exception
    ) -> PromiseRecord: ...


class ResonateEffects:
    """The two durable operations the SDK needs, built from a Sender and Codec.

    Maintains an internal cache of decoded :class:`PromiseRecord`s. A plain
    ``dict`` is safe here because the SDK runs single-threaded on asyncio --
    individual ``dict`` reads and writes are atomic, and no operation holds
    the cache across an ``await``.
    """

    def __init__(
        self,
        sender: PromiseFencing,
        codec: Codec,
        task_id: str,
        task_version: int,
        preload: list[PromiseRecord],
        observer: Observer = logging_observer,
    ) -> None:
        """Build Effects from a Sender, Codec, task lease, and preloaded promises.

        ``task_id``/``task_version`` are the active task's lease, used as the
        fencing token on every durable promise mutation. Each preloaded record
        is decoded into the cache; a record that fails to decode is skipped and
        reported to ``observer``.
        """
        self.sender = sender
        self.codec = codec
        self.task_id = task_id
        self.task_version = task_version
        self.cache: dict[str, PromiseRecord] = {}
        self._observer = observer
        self._stopped: bool = False
        for p in preload:
            self._absorb(p)

    def _absorb(self, record: PromiseRecord) -> None:
        """Decode and cache a server promise record.

        A record that fails to decode is skipped -- and reported as a
        :class:`~resonate.observability.Dropped` event, so the skip is a
        contract a test can assert rather than a silent ``return``. The decoded
        record is inserted monotonically (see ``_insert_monotonic``).
        """
        try:
            decoded = self.codec.decode_promise(record)
        except ResonateError as exc:
            self._observer(Dropped(what="preload-record", id=record.id, cause=str(exc)))
            return
        self._insert_monotonic(decoded)

    def _insert_monotonic(self, record: PromiseRecord) -> None:
        """Insert a decoded record, preserving monotonicity.

        Promise state is monotonic (pending -> terminal, then immutable), so a
        terminal cache entry is never overwritten by a (possibly stale) record.
        """
        existing = self.cache.get(record.id)
        if existing is not None and existing.state != "pending":
            return
        self.cache[record.id] = record

    async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord:
        """Create a durable promise, returning the decoded record.

        Idempotent: a cached record (from preload or a prior call) is returned
        without touching the network.
        """
        if self._stopped:
            raise PlatformError([StoppedError()])

        cached = self.cache.get(req.id)
        if cached is not None:
            return cached

        try:
            encoded_req = PromiseCreateReq(
                id=req.id,
                timeout_at=req.timeout_at,
                param=self.codec.encode(req.param.data),
                tags=req.tags,
            )

            invocation = _invocation_of(encoded_req.tags.get("resonate:scope"))
            self._observer(
                PromiseCreateRequested(id=encoded_req.id, invocation=invocation)
            )

            res = await self.sender.task_fence_create(
                self.task_id, self.task_version, encoded_req
            )
            for p in res.preload:
                self._absorb(p)
            decoded = self.codec.decode_promise(res.promise)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc
        self._insert_monotonic(decoded)
        self._observer(
            PromiseCreateReturned(
                id=decoded.id, invocation=invocation, state=decoded.state
            )
        )
        return decoded

    async def settle_promise[T](self, id: str, result: T | Exception) -> PromiseRecord:
        """Settle a durable promise with a result.

        Idempotent: a cached non-pending record is returned without touching the
        network. ``result`` is the ``Result[T]`` to settle with -- a plain value
        resolves the promise, any ``Exception`` rejects it (the codec flattens it
        to the error shape, pickling the original where it can).
        """
        if self._stopped:
            raise PlatformError([StoppedError()])

        cached = self.cache.get(id)
        if cached is not None and cached.state != "pending":
            return cached

        state: Literal["resolved", "rejected"]
        state = "rejected" if isinstance(result, Exception) else "resolved"

        # Same conversion boundary as create_promise. Note this covers the
        # encode of the *user's* return value too: an unserializable return is
        # a PlatformError, so the task is released and re-delivered into the
        # same failure (a poison task) -- matching the pre-existing behavior of
        # the root-level encode in execute_until_blocked_inner.
        try:
            req = PromiseSettleReq(id=id, state=state, value=self.codec.encode(result))

            self._observer(PromiseSettleRequested(id=req.id, state=req.state))
            res = await self.sender.task_fence_settle(
                self.task_id, self.task_version, req
            )
            for p in res.preload:
                self._absorb(p)
            decoded = self.codec.decode_promise(res.promise)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc
        self._observer(PromiseSettleReturned(id=decoded.id, state=decoded.state))
        self._insert_monotonic(decoded)
        return decoded


def _invocation_of(scope: str | None) -> Literal["run", "rpc", "unknown"]:
    """Map a promise's ``resonate:scope`` tag to the invocation form that made it."""
    match scope:
        case "local":
            return "run"
        case "global":
            return "rpc"
        case _:
            return "unknown"
