from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal, Protocol

import msgspec
from resonate_base import ORIGIN_HEADER, PROTOCOL_VERSION

from resonate.codec import dec_hook
from resonate.error import DecodingError, ServerError
from resonate.ids import origin_of
from resonate.observability import Dropped, logging_observer
from resonate.types import PromiseRecord, ScheduleRecord, TaskRecord, Value

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.observability import Observer
    from resonate.transport import Response, Transport
    from resonate.types import (
        PromiseCreateReq,
        PromiseRegisterCallbackData,
        PromiseSettleReq,
    )


# =============================================================================
# Public result types
# =============================================================================


class TaskAcquireResult(msgspec.Struct, frozen=True, kw_only=True):
    task: TaskRecord
    promise: PromiseRecord
    preload: list[PromiseRecord]


#: Result of creating a task (same structure as acquire).
type TaskCreateResult = TaskAcquireResult


class Redirect(msgspec.Struct, frozen=True, kw_only=True):
    preload: list[PromiseRecord]


SuspendResult = Literal["suspended"] | Redirect


class TaskRef(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    version: int


class TaskFenceResult(msgspec.Struct, frozen=True, kw_only=True):
    """Parsed outcome of a ``task.fence`` call.

    Carries the settled/created promise plus any preloaded sibling promises the
    server returned.
    """

    promise: PromiseRecord
    preload: list[PromiseRecord]


class PromiseSearchResult(msgspec.Struct, frozen=True, kw_only=True):
    promises: list[PromiseRecord]
    cursor: str | None


#: Result of task creation when a conflict is expected: either a
#: :data:`TaskCreateResult` on success, or the literal ``"conflict"`` when the
#: server responds with 409.
#:
#: The 409 response from the server carries no promise data -- callers receiving
#: ``"conflict"`` must subscribe to the existing promise themselves.
type TaskCreateOutcome = TaskCreateResult | Literal["conflict"]


class ScheduleSearchResult(msgspec.Struct, frozen=True, kw_only=True):
    schedules: list[ScheduleRecord]
    cursor: str | None


class ScheduleCreateReq(msgspec.Struct, frozen=True, kw_only=True, rename="camel"):
    id: str
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: Value
    promise_tags: dict[str, str]


# =============================================================================
# Ports -- the narrow interfaces the SDK's internals actually depend on
# =============================================================================


class TaskLifecycle(Protocol):
    """Acquire, fulfill, suspend, release: one task's server-side lifecycle.

    :class:`~resonate.core.Core` needs exactly these four operations, not the
    whole :class:`Sender`. Depending on the narrow port is what lets a test
    hand ``Core`` a four-method recorder and assert the *order* of lifecycle
    calls (that a failed fulfill is followed by a release, say) -- and what
    removes the ``Sender | None`` field that previously existed only so tests
    could pass ``None``.

    :class:`Sender` satisfies this structurally; no registration needed.
    """

    async def task_acquire(
        self, id: str, version: int, pid: str, ttl: int
    ) -> TaskAcquireResult: ...
    async def task_fulfill(
        self, id: str, version: int, action: PromiseSettleReq
    ) -> PromiseRecord: ...
    async def task_suspend(
        self, id: str, version: int, actions: list[PromiseRegisterCallbackData]
    ) -> SuspendResult: ...
    async def task_release(self, id: str, version: int) -> None: ...


class PromiseFencing(Protocol):
    """The two lease-gated durable promise mutations.

    The port :class:`~resonate.effects.ResonateEffects` depends on --
    everything a durable operation needs and nothing else.
    :class:`Sender` satisfies it structurally.
    """

    async def task_fence_create(
        self, id: str, version: int, req: PromiseCreateReq
    ) -> TaskFenceResult: ...
    async def task_fence_settle(
        self, id: str, version: int, req: PromiseSettleReq
    ) -> TaskFenceResult: ...


class TaskHeartbeating(Protocol):
    """Lease extension -- the port :class:`~resonate.heartbeat.AsyncHeartbeat` needs."""

    async def task_heartbeat(self, pid: str, tasks: list[TaskRef]) -> None: ...


#: ``ORIGIN_HEADER`` and ``DEFAULT_ORIGIN`` are imported from
#: :mod:`resonate_base` -- the wire owns both. ``ORIGIN_HEADER`` is the envelope
#: head field the server reads to pick the origin-state partition and the
#: ``headers`` key the origin rides under into
#: :meth:`~resonate_base.connections.Network.send`, for a sharding connector's
#: own routing. ``DEFAULT_ORIGIN`` is where a request that acts on no
#: particular lineage (a search, a schedule) routes.


def default_corr_id() -> str:
    """Mint a fresh correlation id.

    A UUID, not a timestamp: two envelopes built within the same millisecond
    must not collide, or the correlation check in
    :meth:`~resonate.transport.Transport.send` silently validates nothing.
    Injectable via :class:`Sender`'s ``corr_id`` so a golden-file test can pin
    the wire bytes.
    """
    return f"sr-{uuid.uuid4().hex}"


# =============================================================================
# Sender -- typed interface over Transport
# =============================================================================


class Sender:
    """Typed protocol operations over a :class:`~resonate.transport.Transport`.

    ``corr_id`` mints the per-request correlation id (defaults to
    :func:`default_corr_id`); ``observer`` receives a
    :class:`~resonate.observability.Dropped` event for every record in a
    multi-record response that fails to parse, so the SDK's documented
    "skip the bad record" behaviour is assertable instead of invisible.
    """

    def __init__(
        self,
        transport: Transport,
        auth: str | None,
        corr_id: Callable[[], str] = default_corr_id,
        observer: Observer = logging_observer,
    ) -> None:
        self.transport = transport
        self.auth = auth
        self._corr_id = corr_id
        self._observer = observer

    # -- task operations ------------------------------------------------------

    async def task_acquire(
        self, id: str, version: int, pid: str, ttl: int
    ) -> TaskAcquireResult:
        data = {"id": id, "version": version, "pid": pid, "ttl": ttl}
        _, resp = await self._send_envelope(
            "task.acquire", data, allow_409=False, routes_by=id
        )
        return self._parse_task_acquire(resp)

    async def task_fulfill(
        self, id: str, version: int, action: PromiseSettleReq
    ) -> PromiseRecord:
        data = {
            "id": id,
            "version": version,
            "action": SubEnvelope(
                kind="promise.settle", head=self._make_head(), data=action
            ),
        }
        _, resp = await self._send_envelope(
            "task.fulfill", data, allow_409=False, routes_by=id
        )
        return parse_promise(resp)

    async def task_suspend(
        self, id: str, version: int, actions: list[PromiseRegisterCallbackData]
    ) -> SuspendResult:
        """Suspend a task, registering callbacks for awaited promises.

        Returns whether the task was actually suspended or redirected.
        """
        wrapped = [
            SubEnvelope(
                kind="promise.register_callback", head=self._make_head(), data=action
            )
            for action in actions
        ]
        data = {"id": id, "version": version, "actions": wrapped}
        status, resp = await self._send_envelope(
            "task.suspend", data, allow_409=False, routes_by=id
        )
        if status == _REDIRECT_STATUS:
            return Redirect(preload=self._parse_preloaded(resp))
        return "suspended"

    async def task_release(self, id: str, version: int) -> None:
        """Release a task (give up the lock without fulfilling)."""
        await self._send_envelope(
            "task.release",
            {"id": id, "version": version},
            allow_409=False,
            routes_by=id,
        )

    async def task_create(
        self, pid: str, ttl: int, action: PromiseCreateReq
    ) -> TaskCreateResult:
        """Create a task and its associated promise."""
        _, resp = await self._send_task_create(pid, ttl, action, allow_409=False)
        return self._parse_task_acquire(resp)

    async def task_create_or_conflict(
        self, pid: str, ttl: int, action: PromiseCreateReq
    ) -> TaskCreateOutcome:
        """Create a task and its associated promise, returning ``"conflict"`` on 409.

        Unlike :meth:`task_create`, this method does not fail on 409. The
        server's 409 body carries no promise data; callers receiving
        ``"conflict"`` are expected to subscribe to the existing promise via
        :meth:`promise_register_listener`.
        """
        status, resp = await self._send_task_create(pid, ttl, action, allow_409=True)
        if status == _CONFLICT_STATUS:
            return "conflict"
        return self._parse_task_acquire(resp)

    async def task_fence_create(
        self, id: str, version: int, req: PromiseCreateReq
    ) -> TaskFenceResult:
        """Create a promise via ``task.fence``, gated on the task's lease.

        The server applies the create only if the task is still acquired at the
        given ``version`` (the fencing token); a lapsed lease yields a server
        error instead of a split-brain mutation.
        """
        return await self._task_fence(id, version, "promise.create", req)

    async def task_fence_settle(
        self, id: str, version: int, req: PromiseSettleReq
    ) -> TaskFenceResult:
        """Settle a promise via ``task.fence``, gated on the task's lease."""
        return await self._task_fence(id, version, "promise.settle", req)

    async def _task_fence(
        self, id: str, version: int, sub_kind: str, action: Any
    ) -> TaskFenceResult:
        data = {
            "id": id,
            "version": version,
            "action": SubEnvelope(kind=sub_kind, head=self._make_head(), data=action),
        }
        _, resp = await self._send_envelope(
            "task.fence", data, allow_409=False, routes_by=id
        )
        return self._parse_task_fence(resp)

    async def task_heartbeat(self, pid: str, tasks: list[TaskRef]) -> None:
        """Extend the lease for one or more tasks.

        A heartbeat can span lineages, so it routes by the first task's origin
        -- the same choice the server's own client makes.
        """
        await self._send_envelope(
            "task.heartbeat",
            {"pid": pid, "tasks": tasks},
            allow_409=False,
            routes_by=tasks[0].id if tasks else None,
        )

    # -- promise operations ---------------------------------------------------

    async def promise_get(self, id: str) -> PromiseRecord:
        """Get a promise by ID."""
        _, resp = await self._send_envelope(
            "promise.get", {"id": id}, allow_409=False, routes_by=id
        )
        return parse_promise(resp)

    async def promise_create(self, req: PromiseCreateReq) -> PromiseRecord:
        """Create a durable promise."""
        _, resp = await self._send_envelope(
            "promise.create", req, allow_409=False, routes_by=req.id
        )
        return parse_promise(resp)

    async def promise_settle(self, req: PromiseSettleReq) -> PromiseRecord:
        """Settle (resolve/reject) a durable promise."""
        _, resp = await self._send_envelope(
            "promise.settle", req, allow_409=False, routes_by=req.id
        )
        return parse_promise(resp)

    async def promise_register_listener(
        self, awaited: str, address: str
    ) -> PromiseRecord:
        """Register a listener for a promise."""
        data = {"awaited": awaited, "address": address}
        _, resp = await self._send_envelope(
            "promise.register_listener", data, allow_409=False, routes_by=awaited
        )
        return parse_promise(resp)

    async def promise_search(
        self,
        state: str | None,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> PromiseSearchResult:
        """Search for promises matching criteria."""
        data: dict[str, Any] = {}
        if state is not None:
            data["state"] = state
        if tags is not None:
            data["tags"] = tags
        if limit is not None:
            data["limit"] = limit
        if cursor is not None:
            data["cursor"] = cursor
        _, resp = await self._send_envelope("promise.search", data, allow_409=False)
        page = _decode_or_raise(resp, _PromiseSearchPage, "promise.search response")
        return PromiseSearchResult(
            promises=self._decode_lenient(
                page.promises, PromiseRecord, "search-record"
            ),
            cursor=page.cursor,
        )

    # -- schedule operations --------------------------------------------------

    async def schedule_get(self, id: str) -> ScheduleRecord:
        """Get a schedule by ID."""
        _, resp = await self._send_envelope("schedule.get", {"id": id}, allow_409=False)
        return _parse_schedule(resp)

    async def schedule_create(self, req: ScheduleCreateReq) -> ScheduleRecord:
        """Create a schedule."""
        _, resp = await self._send_envelope("schedule.create", req, allow_409=False)
        return _parse_schedule(resp)

    async def schedule_delete(self, id: str) -> None:
        """Delete a schedule."""
        await self._send_envelope("schedule.delete", {"id": id}, allow_409=False)

    async def schedule_search(
        self, tags: dict[str, str] | None, limit: int | None, cursor: str | None
    ) -> ScheduleSearchResult:
        """Search for schedules."""
        data: dict[str, Any] = {}
        if tags is not None:
            data["tags"] = tags
        if limit is not None:
            data["limit"] = limit
        if cursor is not None:
            data["cursor"] = cursor
        _, resp = await self._send_envelope("schedule.search", data, allow_409=False)
        page = _decode_or_raise(resp, _ScheduleSearchPage, "schedule.search response")
        return ScheduleSearchResult(
            schedules=self._decode_lenient(
                page.schedules, ScheduleRecord, "search-record"
            ),
            cursor=page.cursor,
        )

    # -- internal helpers -----------------------------------------------------

    def _make_head(self) -> Head:
        """Build an envelope head; ``origin`` is set on top-level requests only.

        A nested action envelope is routed by the head of the request carrying
        it, so it leaves ``origin`` unset (and off the wire).
        """
        return Head(
            corr_id=self._corr_id(),
            version=PROTOCOL_VERSION,
            auth=self.auth,
        )

    def _decode_lenient[T](self, raw: list[Any], type_: type[T], what: str) -> list[T]:
        """Decode each record, reporting -- not raising on -- the ones that fail.

        One malformed record in a multi-record response must not sink the whole
        page: the server may know about a promise shape this SDK version does
        not. Each skip emits a :class:`~resonate.observability.Dropped` event so
        the behaviour is assertable.
        """
        out: list[T] = []
        for item in raw:
            try:
                out.append(
                    msgspec.convert(
                        _normalize_record(item), type=type_, dec_hook=dec_hook
                    )
                )
            except (TypeError, ValueError, msgspec.MsgspecError) as exc:
                self._observer(Dropped(what=what, id=_id_of(item), cause=str(exc)))
        return out

    def _parse_preloaded(self, data: Any) -> list[PromiseRecord]:
        page = _decode_or_raise(data, _PreloadPage, "preload")
        return self._decode_lenient(page.preload, PromiseRecord, "preload-record")

    def _parse_task_acquire(self, data: Any) -> TaskAcquireResult:
        parsed = _decode_or_raise(data, _TaskAcquirePage, "task.acquire response")
        return TaskAcquireResult(
            task=parsed.task,
            promise=parsed.promise,
            preload=self._decode_lenient(
                parsed.preload, PromiseRecord, "preload-record"
            ),
        )

    def _parse_task_fence(self, data: Any) -> TaskFenceResult:
        parsed = _decode_or_raise(data, _TaskFencePage, "task.fence response")
        return TaskFenceResult(
            promise=parsed.action.data.promise,
            preload=self._decode_lenient(
                parsed.preload, PromiseRecord, "preload-record"
            ),
        )

    async def _send_task_create(
        self, pid: str, ttl: int, action: PromiseCreateReq, *, allow_409: bool
    ) -> tuple[int, Any]:
        """Shared helper for :meth:`task_create` and :meth:`task_create_or_conflict`."""
        data = {
            "pid": pid,
            "ttl": ttl,
            "action": SubEnvelope(
                kind="promise.create", head=self._make_head(), data=action
            ),
        }
        return await self._send_envelope(
            "task.create", data, allow_409=allow_409, routes_by=action.id
        )

    async def _send_envelope(
        self, kind: str, data: Any, *, allow_409: bool, routes_by: str | None = None
    ) -> tuple[int, Any]:
        """Serialize an envelope, send it, and return ``(status, data)``.

        The response arrives already parsed as a
        :class:`~resonate.transport.Response`, so ``status`` and ``data`` are
        read straight off typed fields. A status >= 400 (other than an allowed
        409) raises a :class:`ServerError`.

        ``routes_by`` is the id whose *origin* selects the server's
        origin-state partition -- the promise the request acts on, or the
        promise being awaited. It is resolved to an origin here, once, and then
        travels two ways: on the head, which is where the server reads it, and
        in the ``headers`` under :data:`ORIGIN_HEADER`, which is where a
        sharding connector reads it. Neither the server nor the connector has
        to know how a promise id is built. A request that acts on no particular
        lineage (a search, a schedule) routes by :data:`DEFAULT_ORIGIN`.
        """
        origin = origin_of(routes_by) if routes_by else "default"
        head = self._make_head()
        corr_id = head.corr_id
        envelope = Envelope(kind=kind, head=head, data=data)
        body = msgspec.json.encode(envelope).decode("utf-8")
        resp = await self.transport.send(kind, corr_id, body, {ORIGIN_HEADER: origin})

        status = resp.head.status
        if status >= _ERROR_STATUS and not (allow_409 and status == _CONFLICT_STATUS):
            raise ServerError(status, _error_message(status, resp))

        return status, resp.data


#: HTTP-ish status boundaries used by :meth:`Sender._send_envelope` and
#: :meth:`Sender.task_suspend`.
_ERROR_STATUS = 400
_CONFLICT_STATUS = 409
_REDIRECT_STATUS = 300


def _error_message(status: int, resp: Response) -> str:
    """Extract the server's error text, falling back to a generic message."""
    data = resp.data
    if isinstance(data, str):
        return data
    if isinstance(data, dict) and isinstance(data.get("error"), str):
        return data["error"]
    return f"server error (status {status})"


# =============================================================================
# Typed envelope structs -- serialize directly to wire format
# =============================================================================


class Head(
    msgspec.Struct, frozen=True, kw_only=True, rename="camel", omit_defaults=True
):
    """The ``head`` of a protocol envelope.

    ``auth`` is left out of the wire format when ``None``. ``origin`` is the
    lineage origin the request routes by -- see :meth:`Sender._send_envelope`;
    it is omitted from nested action envelopes, which are routed by the head of
    the request that carries them.
    """

    corr_id: str
    version: str
    auth: str | None = None


class Envelope(msgspec.Struct, frozen=True, kw_only=True):
    """A protocol request envelope: ``{ kind, head, data }``."""

    kind: str
    head: Head
    data: Any


class SubEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """A nested action envelope, embedded in a parent envelope's ``data``."""

    kind: str
    head: Head
    data: Any


# =============================================================================
# Response-data shapes -- parsed once, per operation
# =============================================================================
#
# The ``data`` portion of each response is converted straight into one of these
# before anything reads it, so the operation-specific parsers below never ask
# "is this a dict?" -- the shape question is settled in one ``msgspec.convert``.
# Multi-record fields stay ``list[Any]`` on purpose: they are decoded leniently
# (see :meth:`Sender._decode_lenient`) so one bad record does not sink the page.


class _PromisePage(msgspec.Struct, kw_only=True):
    promise: PromiseRecord


class _SchedulePage(msgspec.Struct, kw_only=True):
    schedule: ScheduleRecord


class _PreloadPage(msgspec.Struct, kw_only=True):
    preload: list[Any] = msgspec.field(default_factory=list)


class _TaskAcquirePage(msgspec.Struct, kw_only=True):
    task: TaskRecord
    promise: PromiseRecord
    preload: list[Any] = msgspec.field(default_factory=list)


class _FenceAction(msgspec.Struct, kw_only=True):
    data: _PromisePage


class _TaskFencePage(msgspec.Struct, kw_only=True):
    action: _FenceAction
    preload: list[Any] = msgspec.field(default_factory=list)


class _PromiseSearchPage(msgspec.Struct, kw_only=True):
    promises: list[Any] = msgspec.field(default_factory=list)
    cursor: str | None = None


class _ScheduleSearchPage(msgspec.Struct, kw_only=True):
    schedules: list[Any] = msgspec.field(default_factory=list)
    cursor: str | None = None


# =============================================================================
# Response parsing helpers (internal)
# =============================================================================

# Value-typed fields whose JSON ``null`` must collapse to an empty Value.
# msgspec rejects ``null`` for a struct field, so these keys are dropped when
# ``null`` and the struct's default factory supplies an empty Value.
_VALUE_FIELDS = ("param", "value", "promiseParam")


def _normalize_record(raw: Any) -> Any:
    """Drop explicit-``null`` Value fields, recursively.

    Applied once at the ``data`` boundary rather than per record, so a
    ``PromiseRecord`` nested inside a typed page struct is normalized too.
    Recursion is safe: user payloads are opaque base64 strings at this point,
    so no application data can be reached by this walk.
    """
    if isinstance(raw, list):
        return [_normalize_record(item) for item in raw]
    if not isinstance(raw, dict):
        return raw
    return {
        key: _normalize_record(val)
        for key, val in raw.items()
        if not (key in _VALUE_FIELDS and val is None)
    }


def _id_of(raw: Any) -> str:
    """Best-effort id of a record that failed to parse, for the drop event."""
    if isinstance(raw, dict):
        id = raw.get("id")
        if isinstance(id, str):
            return id
    return ""


def _decode_or_raise[T](raw: Any, type_: type[T], what: str) -> T:
    """Convert parsed JSON into ``type_``, raising :class:`DecodingError` on failure."""
    try:
        return msgspec.convert(_normalize_record(raw), type=type_, dec_hook=dec_hook)
    except (TypeError, ValueError, msgspec.MsgspecError) as exc:
        msg = f"invalid {what}: {exc}"
        raise DecodingError(msg) from exc


def parse_promise(data: Any) -> PromiseRecord:
    """Parse a promise record from a server response's data portion."""
    return _decode_or_raise(data, _PromisePage, "promise record").promise


def _parse_schedule(data: Any) -> ScheduleRecord:
    return _decode_or_raise(data, _SchedulePage, "schedule record").schedule
