from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, Protocol

from resonate.error import (
    PlatformError,
    ResonateError,
    SpeculationMismatchError,
    StoppedError,
)
from resonate.observability import (
    Dropped,
    PromiseCreateRequested,
    PromiseCreateReturned,
    PromiseSettleRequested,
    PromiseSettleReturned,
    logging_observer,
)
from resonate.types import PromiseCreateReq, PromiseRecord, PromiseSettleReq, Value

if TYPE_CHECKING:
    from collections.abc import Collection

    from resonate.codec import Codec
    from resonate.observability import Observer
    from resonate.send import PromiseFencing

logger = logging.getLogger(__name__)


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


class _LoggedCreate:
    """A ``promise.create`` the attempt has decided to send, but has not sent."""

    def __init__(self, req: PromiseCreateReq) -> None:
        self.req = req


class _LoggedSettle:
    """A ``promise.settle`` the attempt has decided to send, but has not sent."""

    def __init__(self, req: PromiseSettleReq) -> None:
        self.req = req


_LoggedOp = _LoggedCreate | _LoggedSettle


def prune_speculative_log(
    log: list[_LoggedOp], todos: Collection[str]
) -> list[_LoggedOp]:
    """Drop log entries the server does not need to see.

    A pending *local* create is not a checkpoint. Replay re-executes a local
    body until its promise is terminal, so a still-pending local promise
    changes nothing on the next attempt. It is dropped, unless this suspend
    actually awaits it (it is in ``todos``).

    Everything else is kept, in log order:

    * a create paired with a later settle -- the terminal record *is* the
      checkpoint, and the server must see the create before the settle;
    * a settle whose create was already on the server (preloaded, so it was
      never logged);
    * any non-local create (``rpc``, ``sleep``, ``promise``, ``detached``),
      because some other worker or the timer has to observe it, whether or
      not this task awaits it.

    Settled locals are not dropped. ``ctx.run`` is the side-effect boundary:
    once the body has finished, replay must skip it. Create and settle are
    not collapsed into one op -- the server creates pending and settles
    separately, and the flush sends the pair in order.
    """
    todo_set = set(todos)
    settled: set[str] = set()
    first_create: dict[str, _LoggedCreate] = {}
    for op in log:
        if isinstance(op, _LoggedSettle):
            settled.add(op.req.id)
        elif op.req.id not in first_create:
            first_create[op.req.id] = op

    def keep(id: str) -> bool:
        if id in settled or id in todo_set:
            return True
        create = first_create.get(id)
        # A settle with no logged create is a preloaded promise. Keep it.
        if create is None:
            return True
        return create.req.tags.get("resonate:scope") != "local"

    out: list[_LoggedOp] = []
    seen_create: set[str] = set()
    seen_settle: set[str] = set()
    for op in log:
        if isinstance(op, _LoggedCreate):
            if op.req.id in seen_create or not keep(op.req.id):
                continue
            seen_create.add(op.req.id)
            out.append(op)
        elif op.req.id not in seen_settle and keep(op.req.id):
            seen_settle.add(op.req.id)
            out.append(op)
    return out


def _same_settlement(a: PromiseRecord, b: PromiseRecord) -> bool:
    """Whether two decoded records carry the same terminal outcome."""
    return a.state == b.state and a.value.data == b.value.data


class ResonateEffects:
    """The two durable operations the SDK needs, built from a Sender and Codec.

    ``create_promise`` and ``settle_promise`` do not talk to the server. A
    cache miss is recorded as a pending promise, the intended ``promise.create``
    (or ``promise.settle``) is appended to a log, and the caller continues as
    if the server had already answered. The log is pruned and sent, in order,
    at the next mandatory sync point -- :meth:`flush`, which :class:`Core`
    calls immediately before ``task.suspend`` or ``task.fulfill``.

    The cache is a plain ``dict``. That is safe because the SDK runs
    single-threaded on asyncio: each update is one write with no ``await``
    between the read and the write.
    """

    def __init__(
        self,
        sender: PromiseFencing,
        codec: Codec,
        task_id: str,
        task_version: int,
        preload: list[PromiseRecord],
        observer: Observer = logging_observer,
        carried: Collection[PromiseRecord] = (),
    ) -> None:
        """Build Effects from a Sender, Codec, task lease, and preloaded promises.

        ``task_id``/``task_version`` are the active task's lease, used as the
        fencing token on every durable promise mutation. Each preloaded record
        is decoded into the cache; a record that fails to decode is skipped and
        reported to ``observer``.

        ``carried`` holds records a previous pass of this same attempt already
        decoded -- typically promises the last flush learned were terminal.
        They are inserted as-is (already decoded), and a terminal one wins over
        a stale pending snapshot in ``preload``. A redirect's preload is only
        the task's branch, so a global promise (``sleep``, ``rpc``,
        ``detached``) would otherwise disappear and be speculated pending again.
        """
        self.sender = sender
        self.codec = codec
        self.task_id = task_id
        self.task_version = task_version
        self.cache: dict[str, PromiseRecord] = {}
        self._observer = observer
        self._stopped: bool = False
        self._log: list[_LoggedOp] = []
        # Ids the server already holds in a terminal state, learned while
        # flushing this batch. A logged settle for one of these is not sent.
        self._server_terminal: set[str] = set()
        for p in preload:
            self._absorb(p)
        for record in carried:
            self._insert_monotonic(record)

    @property
    def stopped(self) -> bool:
        """Whether a durable op has already failed this attempt."""
        return self._stopped

    def has_unsent(self) -> bool:
        """Whether the log still holds ops that have not been flushed."""
        return bool(self._log)

    def terminal_records(self) -> list[PromiseRecord]:
        """Return decoded records this attempt already knows are terminal.

        Passed back as ``carried`` on the next pass. Pending speculative
        records are left out: a pruned local create must be re-executed, not
        remembered.
        """
        return [record for record in self.cache.values() if record.state != "pending"]

    def todos_settled(self, todos: Collection[str]) -> bool:
        """Whether every awaited promise is already terminal in the cache.

        An empty ``todos`` is not settled -- there is nothing to skip a
        suspend for.
        """
        if not todos:
            return False
        return all(
            (record := self.cache.get(todo)) is not None and record.state != "pending"
            for todo in todos
        )

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
        """Record a durable promise and return it without talking to the server.

        A cached record (preload, or an earlier call this attempt) is returned
        as-is. Otherwise the promise is cached as pending, a ``promise.create``
        is appended to the log, and that pending record is returned -- the
        same shape a fresh server create would have. The body then runs. The
        create is sent later, by :meth:`flush`.

        Encoding still happens here. An unserializable param fails the attempt
        before the body runs, and before anything is logged.
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
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc

        record = PromiseRecord(
            id=req.id,
            state="pending",
            param=req.param,
            value=Value(),
            tags=dict(req.tags),
            timeout_at=req.timeout_at,
            created_at=0,
            settled_at=None,
        )
        self._insert_monotonic(record)
        self._log.append(_LoggedCreate(encoded_req))
        return record

    async def settle_promise[T](self, id: str, result: T | Exception) -> PromiseRecord:
        """Record a settlement and return it without talking to the server.

        Idempotent: a cached non-pending record is returned with nothing
        logged. Otherwise the cache is updated to the terminal record the
        server will store (encode, then decode, so the caller sees the same
        builtins a replay would) and a ``promise.settle`` is appended to the
        log. ``result`` resolves the promise; any ``Exception`` rejects it.

        An unserializable return is a :class:`PlatformError`, so the task is
        released and re-delivered into the same failure.
        """
        if self._stopped:
            raise PlatformError([StoppedError()])

        cached = self.cache.get(id)
        if cached is not None and cached.state != "pending":
            return cached

        state: Literal["resolved", "rejected"]
        state = "rejected" if isinstance(result, Exception) else "resolved"

        try:
            encoded = self.codec.encode(result)
            decoded_value = self.codec.decode(encoded)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc

        record = PromiseRecord(
            id=id,
            state=state,
            param=cached.param if cached is not None else Value(),
            value=Value(data=decoded_value),
            tags=dict(cached.tags) if cached is not None else {},
            timeout_at=cached.timeout_at if cached is not None else 0,
            created_at=cached.created_at if cached is not None else 0,
            settled_at=0,
        )
        self._insert_monotonic(record)
        self._log.append(
            _LoggedSettle(PromiseSettleReq(id=id, state=state, value=encoded))
        )
        return record

    async def flush(self, todos: Collection[str] = ()) -> None:
        """Prune the log and send what remains, in call order.

        ``todos`` is the set of promises this suspend will await. A pending
        local create that is not in that set is dropped (see
        :func:`prune_speculative_log`). The surviving ops are sent as ordered
        ``task.fence`` calls -- one create or settle each, create before the
        matching settle. This is the batch: nothing in it reaches the server
        before the sync point, and nothing after it runs until the batch has
        been applied.

        A create that comes back already settled with a *different* value than
        the one this attempt computed is a :class:`SpeculationMismatchError`. The
        rest of the batch is not sent.
        """
        # A prior send failure has already abandoned this batch. An encode
        # failure, by contrast, stops *new* ops but leaves the ones logged
        # before it still to be sent -- those bodies already finished.
        if self._stopped and not self._log:
            raise PlatformError([StoppedError()])

        while self._log:
            pending = self._log
            self._log = []
            ops = prune_speculative_log(pending, todos)
            dropped = len(pending) - len(ops)
            if dropped:
                logger.debug("effects: pruned %d speculative ops before flush", dropped)
            for op in ops:
                if isinstance(op, _LoggedCreate):
                    await self._send_create(op.req)
                else:
                    await self._send_settle(op.req)

    async def _send_create(self, req: PromiseCreateReq) -> None:
        invocation = _invocation_of(req.tags.get("resonate:scope"))
        self._observer(PromiseCreateRequested(id=req.id, invocation=invocation))
        try:
            res = await self.sender.task_fence_create(
                self.task_id, self.task_version, req
            )
            for p in res.preload:
                self._absorb(p)
            decoded = self.codec.decode_promise(res.promise)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc

        speculative = self.cache.get(req.id)
        if (
            speculative is not None
            and speculative.state != "pending"
            and decoded.state != "pending"
            and not _same_settlement(speculative, decoded)
        ):
            self._stopped = True
            raise PlatformError([SpeculationMismatchError(req.id)])

        if decoded.state != "pending":
            # A disagreement was already raised above, so a terminal server
            # record either matches what we computed or we never settled.
            self._server_terminal.add(req.id)
            self.cache[req.id] = decoded
        elif speculative is None or speculative.state == "pending":
            self._insert_monotonic(decoded)

        self._observer(
            PromiseCreateReturned(
                id=decoded.id, invocation=invocation, state=decoded.state
            )
        )

    async def _send_settle(self, req: PromiseSettleReq) -> None:
        if req.id in self._server_terminal:
            return

        self._observer(PromiseSettleRequested(id=req.id, state=req.state))
        try:
            res = await self.sender.task_fence_settle(
                self.task_id, self.task_version, req
            )
            for p in res.preload:
                self._absorb(p)
            decoded = self.codec.decode_promise(res.promise)
        except ResonateError as exc:
            self._stopped = True
            raise PlatformError([exc]) from exc

        self._server_terminal.add(req.id)
        self.cache[req.id] = decoded
        self._observer(PromiseSettleReturned(id=decoded.id, state=decoded.state))


def _invocation_of(scope: str | None) -> Literal["run", "rpc", "unknown"]:
    """Map a promise's ``resonate:scope`` tag to the invocation form that made it."""
    match scope:
        case "local":
            return "run"
        case "global":
            return "rpc"
        case _:
            return "unknown"
