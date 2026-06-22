from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, Protocol

from resonate.error import PlatformError, ResonateError, StoppedError
from resonate.types import PromiseCreateReq, PromiseSettleReq

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.send import Sender
    from resonate.types import PromiseRecord

# The validation harness keys off this logger name rather than the module
# name, so it must stay "resonate.validation".
logger = logging.getLogger("resonate.validation")


class Effects(Protocol):
    cache: dict[str, PromiseRecord]

    # Circuit breaker: flipped True by the first durable-op failure in this
    # attempt. Once set, every later durable op short-circuits with a generic
    # platform error so no further work happens; the task is released and
    # re-delivery retries everything. Rebuilt per attempt (the redirect loop in
    # ``core.py``) and shared by reference root->child, so it is visible at
    # every depth and scoped to exactly one execution.

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
        sender: Sender,
        codec: Codec,
        task_id: str,
        task_version: int,
        preload: list[PromiseRecord],
    ) -> None:
        """Build Effects from a Sender, Codec, task lease, and preloaded promises.

        ``task_id``/``task_version`` are the active task's lease, used as the
        fencing token on every durable promise mutation. Each preloaded record
        is decoded into the cache; a record that fails to decode is skipped.
        """
        self.sender = sender
        self.codec = codec
        self.task_id = task_id
        self.task_version = task_version
        self.cache: dict[str, PromiseRecord] = {}
        self._stopped: bool = False
        for p in preload:
            self._absorb(p)

    def _absorb(self, record: PromiseRecord) -> None:
        """Decode and cache a server promise record.

        Records that fail to decode are skipped. The decoded record is inserted
        monotonically (see ``_insert_monotonic``).
        """
        try:
            decoded = self.codec.decode_promise(record)
        except ResonateError:
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

            # validation logging
            scope = encoded_req.tags.get("resonate:scope")
            if scope == "local":
                invocation = "run"
            elif scope == "global":
                invocation = "rpc"
            else:
                invocation = "unknown"
            logger.info(
                "promise_create_request promise_id=%s invocation=%s",
                encoded_req.id,
                invocation,
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
        logger.info(
            "promise_create_response promise_id=%s invocation=%s state=%s",
            decoded.id,
            invocation,
            decoded.state,
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

            logger.info(
                "promise_settle_request promise_id=%s state=%s", req.id, req.state
            )
            res = await self.sender.task_fence_settle(
                self.task_id, self.task_version, req
            )
            for p in res.preload:
                self._absorb(p)
            decoded = self.codec.decode_promise(res.promise)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc
        logger.info(
            "promise_settle_response promise_id=%s state=%s", decoded.id, decoded.state
        )
        self._insert_monotonic(decoded)
        return decoded
