from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import msgspec

from resonate import PROTOCOL_VERSION, now_ms
from resonate.error import DecodingError, ServerError
from resonate.types import (
    PromiseRecord,
    ScheduleRecord,
    TaskRecord,
    Value,
)

if TYPE_CHECKING:
    from resonate.transport import Transport
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
# Sender -- typed interface over Transport
# =============================================================================


class Sender:
    def __init__(self, transport: Transport, auth: str | None) -> None:
        self.transport = transport
        self.auth = auth

    # -- task operations ------------------------------------------------------

    async def task_acquire(
        self, id: str, version: int, pid: str, ttl: int
    ) -> TaskAcquireResult:
        data = {"id": id, "version": version, "pid": pid, "ttl": ttl}
        _, resp = await self._send_envelope("task.acquire", data, allow_409=False)
        return parse_task_acquire(resp)

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
        _, resp = await self._send_envelope("task.fulfill", data, allow_409=False)
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
        status, resp = await self._send_envelope("task.suspend", data, allow_409=False)
        return parse_suspend_result(status, resp)

    async def task_release(self, id: str, version: int) -> None:
        """Release a task (give up the lock without fulfilling)."""
        await self._send_envelope(
            "task.release", {"id": id, "version": version}, allow_409=False
        )

    async def task_create(
        self, pid: str, ttl: int, action: PromiseCreateReq
    ) -> TaskCreateResult:
        """Create a task and its associated promise."""
        _, resp = await self._send_task_create(pid, ttl, action, allow_409=False)
        return parse_task_acquire(resp)

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
        if status == 409:
            return "conflict"
        return parse_task_acquire(resp)

    async def task_heartbeat(self, pid: str, tasks: list[TaskRef]) -> None:
        """Extend the lease for one or more tasks."""
        await self._send_envelope(
            "task.heartbeat", {"pid": pid, "tasks": tasks}, allow_409=False
        )

    # -- promise operations ---------------------------------------------------

    async def promise_get(self, id: str) -> PromiseRecord:
        """Get a promise by ID."""
        _, resp = await self._send_envelope("promise.get", {"id": id}, allow_409=False)
        return parse_promise(resp)

    async def promise_create(self, req: PromiseCreateReq) -> PromiseRecord:
        """Create a durable promise."""
        _, resp = await self._send_envelope("promise.create", req, allow_409=False)
        return parse_promise(resp)

    async def promise_settle(self, req: PromiseSettleReq) -> PromiseRecord:
        """Settle (resolve/reject) a durable promise."""
        _, resp = await self._send_envelope("promise.settle", req, allow_409=False)
        return parse_promise(resp)

    async def promise_register_listener(
        self, awaited: str, address: str
    ) -> PromiseRecord:
        """Register a listener for a promise."""
        data = {"awaited": awaited, "address": address}
        _, resp = await self._send_envelope(
            "promise.register_listener", data, allow_409=False
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
        promises = _decode_list(resp, "promises", PromiseRecord)
        return PromiseSearchResult(promises=promises, cursor=_cursor(resp))

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
        schedules = _decode_list(resp, "schedules", ScheduleRecord)
        return ScheduleSearchResult(schedules=schedules, cursor=_cursor(resp))

    # -- internal helpers -----------------------------------------------------

    def _make_head(self) -> Head:
        return Head(corr_id=f"sr-{now_ms()}", version=PROTOCOL_VERSION, auth=self.auth)

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
        return await self._send_envelope("task.create", data, allow_409=allow_409)

    async def _send_envelope(
        self, kind: str, data: Any, *, allow_409: bool
    ) -> tuple[int, Any]:
        """Serialize an envelope, send it, and return ``(status, data)``.

        ``status`` defaults to 200 and ``data`` to an empty object when absent.
        A status >= 400 (other than an allowed 409) raises a
        :class:`ServerError`.
        """
        head = self._make_head()
        corr_id = head.corr_id
        envelope = Envelope(kind=kind, head=head, data=data)
        body = msgspec.json.encode(envelope).decode("utf-8")
        resp = await self.transport.send(kind, corr_id, body)

        status = _resp_status(resp)
        resp_data = _resp_data(resp)

        if status >= 400 and not (allow_409 and status == 409):
            if isinstance(resp_data, str):
                message = resp_data
            elif isinstance(resp_data, dict) and isinstance(
                resp_data.get("error"), str
            ):
                message = resp_data["error"]
            else:
                message = f"server error (status {status})"
            raise ServerError(status, message)

        return status, resp_data


# =============================================================================
# Typed envelope structs -- serialize directly to wire format
# =============================================================================


class Head(
    msgspec.Struct, frozen=True, kw_only=True, rename="camel", omit_defaults=True
):
    """The ``head`` of a protocol envelope.

    ``auth`` is left out of the wire format when ``None``.
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
# Response parsing helpers (internal)
# =============================================================================

# Value-typed fields whose JSON ``null`` must collapse to an empty Value.
# msgspec rejects ``null`` for a struct field, so these keys are dropped when
# ``null`` and the struct's default factory supplies an empty Value.
_VALUE_FIELDS = ("param", "value", "promiseParam")


def _normalize_record(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return raw
    return {
        key: val
        for key, val in raw.items()
        if not (key in _VALUE_FIELDS and val is None)
    }


def _decode_or_raise[T](raw: Any, type_: type[T], what: str) -> T:
    """Convert parsed JSON into ``type_``, raising :class:`DecodingError` on failure."""
    try:
        return msgspec.convert(_normalize_record(raw), type=type_)
    except (TypeError, ValueError, msgspec.MsgspecError) as exc:
        msg = f"invalid {what}: {exc}"
        raise DecodingError(msg) from exc


def _decode_list[T](data: Any, key: str, type_: type[T]) -> list[T]:
    """Decode the array at ``data[key]``, silently dropping records that fail to parse."""
    arr = data.get(key) if isinstance(data, dict) else None
    if not isinstance(arr, list):
        return []
    out: list[T] = []
    for v in arr:
        try:
            out.append(msgspec.convert(_normalize_record(v), type=type_))
        except (TypeError, ValueError, msgspec.MsgspecError):
            continue
    return out


def _cursor(data: Any) -> str | None:
    """Extract a string ``cursor`` field, defaulting to ``None``."""
    cursor = data.get("cursor") if isinstance(data, dict) else None
    return cursor if isinstance(cursor, str) else None


def _resp_status(resp: Any) -> int:
    """Extract ``head.status``, defaulting to 200."""
    if isinstance(resp, dict):
        head = resp.get("head")
        if isinstance(head, dict):
            status = head.get("status")
            if isinstance(status, int) and not isinstance(status, bool) and status >= 0:
                return status
    return 200


def _resp_data(resp: Any) -> Any:
    """Extract the ``data`` portion, defaulting to ``{}``."""
    if isinstance(resp, dict) and "data" in resp:
        return resp["data"]
    return {}


def parse_promise(data: Any) -> PromiseRecord:
    """Parse a promise record from a server response's data portion."""
    promise = data.get("promise") if isinstance(data, dict) else None
    if promise is None:
        msg = "missing 'promise' in response"
        raise DecodingError(msg)
    return _decode_or_raise(promise, PromiseRecord, "promise record")


def parse_task_acquire(data: Any) -> TaskAcquireResult:
    """Parse a ``task.acquire`` response."""
    task_val = data.get("task") if isinstance(data, dict) else None
    if task_val is None:
        msg = "missing 'task' in task.acquire response"
        raise DecodingError(msg)
    task = _decode_or_raise(task_val, TaskRecord, "task in task.acquire")

    promise_val = data.get("promise") if isinstance(data, dict) else None
    if promise_val is None:
        msg = "missing 'promise' in task.acquire response"
        raise DecodingError(msg)
    promise = _decode_or_raise(promise_val, PromiseRecord, "promise in task.acquire")

    return TaskAcquireResult(task=task, promise=promise, preload=parse_preloaded(data))


def parse_suspend_result(status: int, data: Any) -> SuspendResult:
    """Parse a ``task.suspend`` response -- ``"suspended"`` (200) or ``Redirect`` (300)."""
    if status == 300:
        return Redirect(preload=parse_preloaded(data))
    return "suspended"


def parse_preloaded(data: Any) -> list[PromiseRecord]:
    """Extract preloaded promises from a response, dropping any that fail to parse."""
    return _decode_list(data, "preload", PromiseRecord)


def _parse_schedule(data: Any) -> ScheduleRecord:
    schedule = data.get("schedule") if isinstance(data, dict) else None
    if schedule is None:
        msg = "missing 'schedule' in response"
        raise DecodingError(msg)
    return _decode_or_raise(schedule, ScheduleRecord, "schedule record")
