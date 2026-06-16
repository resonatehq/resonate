from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from resonate import PROTOCOL_VERSION, now_ms
from resonate.error import DecodingError, ServerError
from resonate.types import PromiseState

if TYPE_CHECKING:
    from collections.abc import Callable


# =============================================================================
# CONSTANTS
# =============================================================================

PENDING_RETRY_TTL = 30_000
I64_MAX = (1 << 63) - 1
I64_MIN = -(1 << 63)

# The lifecycle states a task can be in.
type TaskState = Literal["pending", "acquired", "suspended", "halted", "fulfilled"]


# =============================================================================
# SERVER STATE TYPES
# =============================================================================


class DurablePromise(msgspec.Struct, kw_only=True):
    id: str
    state: PromiseState
    param: Any
    value: Any
    tags: dict[str, str]
    timeout_at: int
    created_at: int
    settled_at: int | None
    awaiters: set[str]
    subscribers: set[str]

    def to_record(self) -> dict[str, Any]:
        obj: dict[str, Any] = {
            "id": self.id,
            "state": self.state,
            "param": self.param,
            "value": self.value,
            "tags": self.tags,
            "timeoutAt": self.timeout_at,
            "createdAt": self.created_at,
        }
        if self.settled_at is not None:
            obj["settledAt"] = self.settled_at
        return obj


class Task(msgspec.Struct, kw_only=True):
    id: str
    state: TaskState
    version: int
    pid: str | None
    ttl: int | None
    resumes: set[str]

    def to_record(self) -> dict[str, Any]:
        obj: dict[str, Any] = {
            "id": self.id,
            "state": self.state,
            "version": self.version,
            "promiseId": self.id,
        }
        if self.pid is not None:
            obj["pid"] = self.pid
        if self.ttl is not None:
            obj["ttl"] = self.ttl
        return obj


class PTimeout(msgspec.Struct, kw_only=True):
    id: str
    timeout: int


class TTimeout(msgspec.Struct, kw_only=True):
    id: str
    typ: int
    timeout: int


class OutgoingMessage(msgspec.Struct, kw_only=True):
    address: str
    message: Any


class ScheduleStub(msgspec.Struct, kw_only=True):
    id: str
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: Any
    promise_tags: Any
    created_at: int

    def to_record(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "cron": self.cron,
            "promiseId": self.promise_id,
            "promiseTimeout": self.promise_timeout,
            "promiseParam": self.promise_param,
            "promiseTags": self.promise_tags,
            "createdAt": self.created_at,
            "nextRunAt": 0,
            "lastRunAt": None,
        }


# =============================================================================
# JSON FIELD ACCESSORS
# =============================================================================
#
# Small helpers for reading fields out of decoded JSON (``dict`` / ``list`` /
# scalar values); a missing or wrongly-typed field collapses to a default.
# ``bool`` is excluded from integer reads because ``bool`` subclasses ``int``
# in Python but is not a number in JSON.


def _get(obj: Any, key: str) -> Any:
    """Return ``obj[key]``, or ``None`` when missing or ``obj`` is not a dict."""
    return obj.get(key) if isinstance(obj, dict) else None


def _str(obj: Any, key: str) -> str:
    """Return a string field, defaulting to ``""``."""
    value = _get(obj, key)
    return value if isinstance(value, str) else ""


def _i64(obj: Any, key: str, default: int) -> int:
    """Return an integer field, falling back to ``default``."""
    value = _get(obj, key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return default


def _u64(obj: Any, key: str, default: int) -> int:
    """Return a non-negative integer field, falling back to ``default``."""
    value = _get(obj, key)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return default


def _ttl(obj: Any, key: str, default: int) -> int:
    """Return a TTL field clamped to the 64-bit signed range, else ``default``."""
    value = _get(obj, key)
    if isinstance(value, int) and not isinstance(value, bool):
        if I64_MIN <= value <= I64_MAX:
            return value
        if 0 <= value <= 2**64 - 1:
            return min(value, I64_MAX)
    return default


def _opt_i64(value: Any) -> int | None:
    """Return ``value`` if it is an integer, else ``None``."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def require_str(obj: Any, field: str) -> str:
    """Return a required non-empty string field.

    A missing, non-string, or empty value raises :class:`ServerError` with
    code 400.
    """
    value = _get(obj, field)
    if isinstance(value, str) and value != "":
        return value
    msg = f"missing or empty required field: {field}"
    raise ServerError(400, msg)


def require_task_id(obj: Any) -> str:
    """Extract the required task ID from a request."""
    return require_str(obj, "id")


def _saturating_add(a: int, b: int) -> int:
    """Add two integers, clamping the result to the 64-bit signed range."""
    result = a + b
    if result > I64_MAX:
        return I64_MAX
    if result < I64_MIN:
        return I64_MIN
    return result


def _parse_promise_state(value: Any) -> PromiseState:
    """Parse a settle state, defaulting to ``"resolved"`` for unknown values."""
    match value:
        case "pending":
            return "pending"
        case "resolved":
            return "resolved"
        case "rejected":
            return "rejected"
        case "rejected_canceled":
            return "rejected_canceled"
        case "rejected_timedout":
            return "rejected_timedout"
        case _:
            return "resolved"


# =============================================================================
# SERVER STATE MACHINE
# =============================================================================


class ServerState:
    """The in-process server state machine driving :class:`LocalNetwork`.

    It owns promises, tasks, schedules, pending timeouts, and the queue of
    outgoing messages produced while applying a request or ticking.
    """

    def __init__(self) -> None:
        self.promises: dict[str, DurablePromise] = {}
        self.tasks: dict[str, Task] = {}
        self.schedules: dict[str, ScheduleStub] = {}
        self.p_timeouts: list[PTimeout] = []
        self.t_timeouts: list[TTimeout] = []
        self.outgoing: list[OutgoingMessage] = []

    def apply(self, now: int, req: Any) -> dict[str, Any]:
        """Apply a single (flat) request and return the flat response.

        Auto-times-out any promise the request touches, then dispatches on
        ``kind``. An unknown kind raises :class:`ServerError` (code 400).
        """
        self.outgoing.clear()

        kind = _str(req, "kind")
        corr_id = _get(req, "corrId")

        # Auto-timeout relevant promises before processing.
        if kind in ("promise.get", "promise.create", "promise.settle"):
            self.try_auto_timeout(now, _str(req, "id"))
        elif kind == "promise.register_listener":
            self.try_auto_timeout(now, _str(req, "awaited"))
        elif kind == "task.create":
            pd = extract_action_data(_get(req, "action"))
            self.try_auto_timeout(now, _str(pd, "id"))
        elif kind in ("task.acquire", "task.release", "task.fulfill"):
            self.try_auto_timeout(now, _str(req, "id"))
        elif kind == "task.suspend":
            self.try_auto_timeout(now, _str(req, "id"))
            actions = _get(req, "actions")
            if isinstance(actions, list):
                for action in actions:
                    awaited = _str(extract_action_data(action), "awaited")
                    if awaited:
                        self.try_auto_timeout(now, awaited)

        if kind == "promise.get":
            return self.promise_get(corr_id, req)
        if kind == "promise.create":
            return self.promise_create(now, corr_id, req)
        if kind == "promise.settle":
            return self.promise_settle(now, corr_id, req)
        if kind == "promise.register_listener":
            return self.promise_register_listener(corr_id, req)
        if kind == "task.create":
            return self.task_create(now, corr_id, req)
        if kind == "task.acquire":
            return self.task_acquire(now, corr_id, req)
        if kind == "task.release":
            return self.task_release(now, corr_id, req)
        if kind == "task.fulfill":
            return self.task_fulfill(now, corr_id, req)
        if kind == "task.suspend":
            return self.task_suspend(now, corr_id, req)
        if kind == "task.heartbeat":
            return self.task_heartbeat(now, corr_id, req)
        if kind == "schedule.create":
            return self.schedule_create(now, corr_id, req)
        if kind == "schedule.get":
            return self.schedule_get(corr_id, req)
        if kind == "schedule.delete":
            return self.schedule_delete(corr_id, req)
        if kind == "schedule.search":
            return self.schedule_search(corr_id, req)
        msg = f"unknown request kind: {kind}"
        raise ServerError(400, msg)

    def tick(self, now: int) -> None:
        """Advance time: settle expired promises and retry/release lapsed tasks."""
        promise_settles: list[str] = []
        task_releases: list[tuple[str, int]] = []
        task_retries: list[tuple[str, int]] = []

        for pt in self.p_timeouts:
            if now >= pt.timeout:
                p = self.promises.get(pt.id)
                if p is not None and p.state == "pending":
                    promise_settles.append(pt.id)
        for tt in self.t_timeouts:
            if now < tt.timeout:
                continue
            if tt.typ == 1:
                t = self.tasks.get(tt.id)
                if t is not None and t.state == "acquired":
                    task_releases.append((tt.id, t.version))
            elif tt.typ == 0:
                t = self.tasks.get(tt.id)
                if t is not None and t.state == "pending":
                    task_retries.append((tt.id, t.version))

        # Phase 1: Settle promises.
        for pid in promise_settles:
            p = self.promises.get(pid)
            if p is None or p.state != "pending":
                continue
            p.state = self.timeout_state(p.tags)
            p.value = None
            p.settled_at = p.timeout_at
            self.del_p_timeout(pid)
        # Phase 2: Fulfill tasks whose own promise settled.
        for pid in promise_settles:
            self.enqueue_settle(pid)
        # Phase 3: Resume awaiters and notify subscribers.
        for pid in promise_settles:
            self.resume_awaiters(pid, now)
            self.notify_subscribers(pid)

        # Phase 4: Release expired leases.
        for tid, version in task_releases:
            t = self.tasks.get(tid)
            if t is not None and t.state == "acquired" and t.version == version:
                new_version = t.version + 1
                t.state = "pending"
                t.version = new_version
                t.pid = None
                t.ttl = None
                self.set_t_timeout(tid, 0, now + PENDING_RETRY_TTL)
                p = self.promises.get(tid)
                if p is not None:
                    addr = p.tags.get("resonate:target")
                    if addr is not None:
                        self.send_execute_message(addr, tid, new_version)

        # Phase 5: Retry pending tasks.
        for tid, _version in task_retries:
            t = self.tasks.get(tid)
            if t is not None and t.state == "pending":
                v = t.version
                self.set_t_timeout(tid, 0, now + PENDING_RETRY_TTL)
                p = self.promises.get(tid)
                if p is not None:
                    addr = p.tags.get("resonate:target")
                    if addr is not None:
                        self.send_execute_message(addr, tid, v)

    # =========================================================================
    # PROMISE OPERATIONS
    # =========================================================================

    def promise_get(self, corr_id: Any, req: Any) -> dict[str, Any]:
        promise_id = require_str(req, "id")
        p = self.promises.get(promise_id)
        if p is not None:
            return {
                "kind": "promise.get",
                "corrId": corr_id,
                "status": 200,
                "promise": p.to_record(),
            }
        return {
            "kind": "promise.get",
            "corrId": corr_id,
            "status": 404,
            "error": f"promise {promise_id} not found",
        }

    def promise_create(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        promise_id = require_str(req, "id")

        existing = self.promises.get(promise_id)
        if existing is not None:
            return {
                "kind": "promise.create",
                "corrId": corr_id,
                "status": 200,
                "promise": existing.to_record(),
            }

        timeout_at = _i64(req, "timeoutAt", I64_MAX)
        param = _get(req, "param")
        tags = extract_tags(req)

        if now >= timeout_at:
            promise = DurablePromise(
                id=promise_id,
                state=self.timeout_state(tags),
                param=param,
                value=None,
                tags=tags,
                timeout_at=timeout_at,
                created_at=timeout_at,
                settled_at=timeout_at,
                awaiters=set(),
                subscribers=set(),
            )
            record = promise.to_record()
            self.promises[promise_id] = promise
            self.enqueue_settle(promise_id)
            self.resume_awaiters(promise_id, now)
            self.notify_subscribers(promise_id)
            return {
                "kind": "promise.create",
                "corrId": corr_id,
                "status": 200,
                "promise": record,
            }

        promise = DurablePromise(
            id=promise_id,
            state="pending",
            param=param,
            value=None,
            tags=tags,
            timeout_at=timeout_at,
            created_at=now,
            settled_at=None,
            awaiters=set(),
            subscribers=set(),
        )
        record = promise.to_record()
        self.promises[promise_id] = promise
        self.set_p_timeout(promise_id, timeout_at)

        # Auto-create task and dispatch execute when target tag is present.
        address = tags.get("resonate:target")
        if address is not None:
            delay = _parse_int(tags.get("resonate:delay"))
            deferred = delay is not None and now < delay
            self.tasks[promise_id] = Task(
                id=promise_id,
                state="pending",
                version=0,
                pid=None,
                ttl=None,
                resumes=set(),
            )
            self.set_t_timeout(
                promise_id,
                0,
                delay if deferred and delay is not None else now + PENDING_RETRY_TTL,
            )
            if not deferred:
                self.send_execute_message(address, promise_id, 0)

        return {
            "kind": "promise.create",
            "corrId": corr_id,
            "status": 201,
            "promise": record,
        }

    def promise_settle(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        promise_id = require_str(req, "id")
        settle_state = _parse_promise_state(_get(req, "state"))
        value = _get(req, "value")

        p = self.promises.get(promise_id)
        if p is None:
            return {
                "kind": "promise.settle",
                "corrId": corr_id,
                "status": 404,
                "error": f"promise {promise_id} not found",
            }
        if p.state != "pending":
            return {
                "kind": "promise.settle",
                "corrId": corr_id,
                "status": 200,
                "promise": p.to_record(),
            }

        p.state = settle_state
        p.value = value
        p.settled_at = now
        record = p.to_record()
        self.del_p_timeout(promise_id)
        self.enqueue_settle(promise_id)
        self.resume_awaiters(promise_id, now)
        self.notify_subscribers(promise_id)
        return {
            "kind": "promise.settle",
            "corrId": corr_id,
            "status": 200,
            "promise": record,
        }

    def promise_register_listener(self, corr_id: Any, req: Any) -> dict[str, Any]:
        awaited = require_str(req, "awaited")
        address = require_str(req, "address")

        p = self.promises.get(awaited)
        if p is not None:
            if p.state == "pending":
                p.subscribers.add(address)
            return {
                "kind": "promise.register_listener",
                "corrId": corr_id,
                "status": 200,
                "promise": p.to_record(),
            }
        return {
            "kind": "promise.register_listener",
            "corrId": corr_id,
            "status": 404,
            "error": f"promise {awaited} not found",
        }

    # =========================================================================
    # TASK OPERATIONS
    # =========================================================================

    def task_create(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        pid = require_str(req, "pid")
        ttl = _ttl(req, "ttl", 60_000)
        promise_req = extract_action_data(_get(req, "action"))
        promise_id = require_str(promise_req, "id")

        existing_task = self.tasks.get(promise_id)
        if existing_task is not None:
            task_state = existing_task.state
            p = self.promises.get(promise_id)
            promise_record = p.to_record() if p is not None else None

            if task_state == "pending":
                preload = self.preload(promise_id)
                existing_task.state = "acquired"
                existing_task.pid = pid
                existing_task.ttl = ttl
                existing_task.resumes.clear()
                self.set_t_timeout(promise_id, 1, _saturating_add(now, ttl))
                return {
                    "kind": "task.create",
                    "corrId": corr_id,
                    "status": 200,
                    "task": existing_task.to_record(),
                    "promise": promise_record,
                    "preload": preload,
                }
            if task_state == "fulfilled":
                return {
                    "kind": "task.create",
                    "corrId": corr_id,
                    "status": 200,
                    "task": existing_task.to_record(),
                    "promise": promise_record,
                    "preload": self.preload(promise_id),
                }
            return {
                "kind": "task.create",
                "corrId": corr_id,
                "status": 409,
                "error": "Already exists",
            }

        # Promise already exists but no task?
        if promise_id in self.promises:
            return {
                "kind": "task.create",
                "corrId": corr_id,
                "status": 409,
                "error": "Already exists",
            }

        timeout_at = _i64(promise_req, "timeoutAt", I64_MAX)
        param = _get(promise_req, "param")
        tags = extract_tags(promise_req)

        # Already timed out?
        if now >= timeout_at:
            promise = DurablePromise(
                id=promise_id,
                state=self.timeout_state(tags),
                param=param,
                value=None,
                tags=tags,
                timeout_at=timeout_at,
                created_at=timeout_at,
                settled_at=timeout_at,
                awaiters=set(),
                subscribers=set(),
            )
            pr = promise.to_record()
            self.promises[promise_id] = promise
            task = Task(
                id=promise_id,
                state="fulfilled",
                version=0,
                pid=None,
                ttl=None,
                resumes=set(),
            )
            tr = task.to_record()
            self.tasks[promise_id] = task
            return {
                "kind": "task.create",
                "corrId": corr_id,
                "status": 200,
                "task": tr,
                "promise": pr,
                "preload": [],
            }

        # Create promise + task (acquired).
        promise = DurablePromise(
            id=promise_id,
            state="pending",
            param=param,
            value=None,
            tags=tags,
            timeout_at=timeout_at,
            created_at=now,
            settled_at=None,
            awaiters=set(),
            subscribers=set(),
        )
        pr = promise.to_record()
        self.promises[promise_id] = promise
        self.set_p_timeout(promise_id, timeout_at)

        task = Task(
            id=promise_id,
            state="acquired",
            version=0,
            pid=pid,
            ttl=ttl,
            resumes=set(),
        )
        tr = task.to_record()
        self.tasks[promise_id] = task
        self.set_t_timeout(promise_id, 1, _saturating_add(now, ttl))

        return {
            "kind": "task.create",
            "corrId": corr_id,
            "status": 201,
            "task": tr,
            "promise": pr,
            "preload": self.preload(promise_id),
        }

    def task_acquire(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        task_id = require_task_id(req)
        pid = _str(req, "pid")
        # Ensure TTL is always positive to prevent lease timeouts in the past.
        ttl = max(_ttl(req, "ttl", 60_000), 1)

        t = self.tasks.get(task_id)
        if t is None:
            return {
                "kind": "task.acquire",
                "corrId": corr_id,
                "status": 404,
                "error": f"task {task_id} not found",
            }
        if t.state != "pending":
            return {
                "kind": "task.acquire",
                "corrId": corr_id,
                "status": 409,
                "error": f"task not in pending state (state: {t.state.capitalize()})",
            }

        preload = self.preload(task_id)
        t.state = "acquired"
        t.pid = pid
        t.ttl = ttl
        t.resumes.clear()
        self.set_t_timeout(task_id, 1, _saturating_add(now, ttl))
        p = self.promises.get(task_id)
        promise_record = p.to_record() if p is not None else None
        return {
            "kind": "task.acquire",
            "corrId": corr_id,
            "status": 200,
            "task": t.to_record(),
            "promise": promise_record,
            "preload": preload,
        }

    def task_release(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        task_id = require_task_id(req)

        t = self.tasks.get(task_id)
        if t is None:
            return {"kind": "task.release", "corrId": corr_id, "status": 404}
        if t.state != "acquired":
            return {"kind": "task.release", "corrId": corr_id, "status": 409}

        new_version = t.version + 1
        t.state = "pending"
        t.version = new_version
        t.pid = None
        t.ttl = None
        self.set_t_timeout(task_id, 0, now + PENDING_RETRY_TTL)
        p = self.promises.get(task_id)
        if p is not None:
            addr = p.tags.get("resonate:target")
            if addr is not None:
                self.send_execute_message(addr, task_id, new_version)
        return {"kind": "task.release", "corrId": corr_id, "status": 200}

    def task_fulfill(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        task_id = require_task_id(req)

        t = self.tasks.get(task_id)
        if t is None:
            return {"kind": "task.fulfill", "corrId": corr_id, "status": 404}
        if t.state != "acquired":
            return {"kind": "task.fulfill", "corrId": corr_id, "status": 409}

        settle = extract_action_data(_get(req, "action"))
        settle_id = _get(settle, "id")
        promise_id = settle_id if isinstance(settle_id, str) else task_id
        settle_state = _parse_promise_state(_get(settle, "state"))
        value = _get(settle, "value")

        existing = self.promises.get(promise_id)
        if existing is not None and existing.state == "pending":
            existing.state = settle_state
            existing.value = value
            existing.settled_at = now
            self.del_p_timeout(promise_id)

        p = self.promises.get(promise_id)
        promise_record = p.to_record() if p is not None else None

        self.enqueue_settle(task_id)
        self.resume_awaiters(promise_id, now)
        self.notify_subscribers(promise_id)

        return {
            "kind": "task.fulfill",
            "corrId": corr_id,
            "status": 200,
            "promise": promise_record,
        }

    def task_suspend(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        task_id = require_task_id(req)

        t = self.tasks.get(task_id)
        if t is None:
            return {"kind": "task.suspend", "corrId": corr_id, "status": 404}
        if t.state != "acquired":
            return {"kind": "task.suspend", "corrId": corr_id, "status": 409}

        # If task already has resumes (dependency resolved while acquired), redirect.
        if t.resumes:
            t.resumes.clear()
            return {
                "kind": "task.suspend",
                "corrId": corr_id,
                "status": 300,
                "redirect": True,
                "preload": self.preload(task_id),
            }

        # Parse actions -- array of PromiseRegisterCallbackReq envelopes.
        callbacks: list[str] = []
        actions = _get(req, "actions")
        if isinstance(actions, list):
            for action in actions:
                awaited = _get(extract_action_data(action), "awaited")
                if isinstance(awaited, str):
                    callbacks.append(awaited)

        # Register this task as an awaiter on each awaited promise.
        # If any awaited promise is already settled, redirect immediately.
        any_settled = False
        for awaited_id in callbacks:
            p = self.promises.get(awaited_id)
            if p is None:
                continue
            if p.state == "pending":
                p.awaiters.add(task_id)
            else:
                any_settled = True

        if any_settled:
            return {
                "kind": "task.suspend",
                "corrId": corr_id,
                "status": 300,
                "redirect": True,
                "preload": self.preload(task_id),
            }

        # Actually suspend.
        t.state = "suspended"
        t.pid = None
        t.ttl = None
        t.resumes.clear()
        self.del_t_timeout(task_id)

        return {"kind": "task.suspend", "corrId": corr_id, "status": 200}

    def task_heartbeat(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        pid = require_str(req, "pid")

        tasks = _get(req, "tasks")
        if isinstance(tasks, list):
            for task_ref in tasks:
                task_ref_id = require_str(task_ref, "id")
                version = _opt_i64(_get(task_ref, "version"))
                t = self.tasks.get(task_ref_id)
                if (
                    t is not None
                    and t.state == "acquired"
                    and t.pid == pid
                    and (version is None or version == t.version)
                ):
                    ttl = t.ttl if t.ttl is not None else 30_000
                    self.set_t_timeout(task_ref_id, 1, _saturating_add(now, ttl))

        return {"kind": "task.heartbeat", "corrId": corr_id, "status": 200}

    # =========================================================================
    # SCHEDULE OPERATIONS (stubs for local mode)
    # =========================================================================

    def schedule_create(self, now: int, corr_id: Any, req: Any) -> dict[str, Any]:
        schedule_id = require_str(req, "id")
        existing = self.schedules.get(schedule_id)
        if existing is not None:
            return {
                "kind": "schedule.create",
                "corrId": corr_id,
                "status": 200,
                "schedule": existing.to_record(),
            }
        promise_tags = _get(req, "promiseTags")
        stub = ScheduleStub(
            id=schedule_id,
            cron=require_str(req, "cron"),
            promise_id=require_str(req, "promiseId"),
            promise_timeout=_i64(req, "promiseTimeout", 0),
            promise_param=_get(req, "promiseParam"),
            promise_tags=promise_tags if promise_tags is not None else {},
            created_at=now,
        )
        record = stub.to_record()
        self.schedules[schedule_id] = stub
        return {
            "kind": "schedule.create",
            "corrId": corr_id,
            "status": 201,
            "schedule": record,
        }

    def schedule_get(self, corr_id: Any, req: Any) -> dict[str, Any]:
        schedule_id = require_str(req, "id")
        stub = self.schedules.get(schedule_id)
        if stub is not None:
            return {
                "kind": "schedule.get",
                "corrId": corr_id,
                "status": 200,
                "schedule": stub.to_record(),
            }
        return {
            "kind": "schedule.get",
            "corrId": corr_id,
            "status": 404,
            "error": f"schedule {schedule_id} not found",
        }

    def schedule_delete(self, corr_id: Any, req: Any) -> dict[str, Any]:
        schedule_id = require_str(req, "id")
        if self.schedules.pop(schedule_id, None) is not None:
            return {"kind": "schedule.delete", "corrId": corr_id, "status": 200}
        return {
            "kind": "schedule.delete",
            "corrId": corr_id,
            "status": 404,
            "error": f"schedule {schedule_id} not found",
        }

    def schedule_search(self, corr_id: Any, req: Any) -> dict[str, Any]:
        schedules = [s.to_record() for s in self.schedules.values()]
        return {
            "kind": "schedule.search",
            "corrId": corr_id,
            "status": 200,
            "schedules": schedules,
            "cursor": None,
        }

    # =========================================================================
    # HELPERS
    # =========================================================================

    def try_auto_timeout(self, now: int, promise_id: str) -> None:
        """Auto-timeout a promise if it has expired."""
        p = self.promises.get(promise_id)
        if p is None or p.state != "pending" or now < p.timeout_at:
            return

        p.state = self.timeout_state(p.tags)
        p.settled_at = p.timeout_at
        self.del_p_timeout(promise_id)
        self.enqueue_settle(promise_id)
        self.resume_awaiters(promise_id, now)
        self.notify_subscribers(promise_id)

    def enqueue_settle(self, promise_id: str) -> None:
        """When a promise settles, fulfill its associated task (if any)."""
        t = self.tasks.get(promise_id)
        if t is None:
            # If promise has a target but no task, create a fulfilled task.
            p = self.promises.get(promise_id)
            if p is not None and "resonate:target" in p.tags:
                self.tasks[promise_id] = Task(
                    id=promise_id,
                    state="fulfilled",
                    version=0,
                    pid=None,
                    ttl=None,
                    resumes=set(),
                )
            return
        if t.state == "fulfilled":
            return

        # Fulfill the task and remove it as an awaiter from all promises.
        t.state = "fulfilled"
        t.pid = None
        t.ttl = None
        t.resumes.clear()
        self.del_t_timeout(promise_id)
        for p in self.promises.values():
            p.awaiters.discard(promise_id)

    def resume_awaiters(self, promise_id: str, now: int) -> None:
        """When a promise settles, resume all tasks that were awaiting it."""
        p = self.promises.get(promise_id)
        awaiter_ids = list(p.awaiters) if p is not None else []

        for awaiter_id in awaiter_ids:
            task = self.tasks.get(awaiter_id)
            if task is None:
                continue
            if task.state == "suspended":
                new_version = task.version + 1
                task.state = "pending"
                task.version = new_version
                task.resumes = {promise_id}
                self.set_t_timeout(awaiter_id, 0, now + PENDING_RETRY_TTL)
                ap = self.promises.get(awaiter_id)
                if ap is not None:
                    addr = ap.tags.get("resonate:target")
                    if addr is not None:
                        self.send_execute_message(addr, awaiter_id, new_version)
            elif task.state in ("pending", "acquired", "halted"):
                task.resumes.add(promise_id)

        p = self.promises.get(promise_id)
        if p is not None:
            p.awaiters.clear()

    def notify_subscribers(self, promise_id: str) -> None:
        """Notify all subscribers (listener addresses) of a settled promise."""
        p = self.promises.get(promise_id)
        if p is None or not p.subscribers:
            return

        subscribers = list(p.subscribers)
        record = p.to_record()
        for address in subscribers:
            self.outgoing.append(
                OutgoingMessage(
                    address=address,
                    message={"kind": "unblock", "data": {"promise": record}},
                )
            )
        p.subscribers.clear()

    def preload(self, promise_id: str) -> list[dict[str, Any]]:
        """Return all promises sharing the same branch tag as the given promise."""
        p = self.promises.get(promise_id)
        if p is None:
            return []
        branch = p.tags.get("resonate:branch")
        if branch is None:
            return []
        return [
            other.to_record()
            for other in self.promises.values()
            if other.id != promise_id and other.tags.get("resonate:branch") == branch
        ]

    def timeout_state(self, tags: dict[str, str]) -> PromiseState:
        if tags.get("resonate:timer") == "true":
            return "resolved"
        return "rejected_timedout"

    def set_p_timeout(self, promise_id: str, timeout: int) -> None:
        for pt in self.p_timeouts:
            if pt.id == promise_id:
                pt.timeout = timeout
                return
        self.p_timeouts.append(PTimeout(id=promise_id, timeout=timeout))

    def del_p_timeout(self, promise_id: str) -> None:
        self.p_timeouts = [pt for pt in self.p_timeouts if pt.id != promise_id]

    def set_t_timeout(self, task_id: str, typ: int, timeout: int) -> None:
        for tt in self.t_timeouts:
            if tt.id == task_id:
                tt.typ = typ
                tt.timeout = timeout
                return
        self.t_timeouts.append(TTimeout(id=task_id, typ=typ, timeout=timeout))

    def del_t_timeout(self, task_id: str) -> None:
        self.t_timeouts = [tt for tt in self.t_timeouts if tt.id != task_id]

    def send_execute_message(self, address: str, task_id: str, version: int) -> None:
        msg = {"kind": "execute", "data": {"task": {"id": task_id, "version": version}}}
        # Upsert: replace any existing execute message for the same task.
        for existing in self.outgoing:
            if _execute_task_id(existing.message) == task_id:
                existing.address = address
                existing.message = msg
                return
        self.outgoing.append(OutgoingMessage(address=address, message=msg))


# =============================================================================
# LOCAL NETWORK
# =============================================================================


class LocalNetwork:
    """In-process :class:`Network` backed by a :class:`ServerState` simulation.

    Useful for running workflows without a Resonate server. A background tick
    loop advances time once per second; outgoing messages are dispatched to
    callbacks registered via :meth:`recv` as asyncio tasks.
    """

    def __init__(self, pid: str | None = None, group: str | None = None) -> None:
        self.state = ServerState()

        self._pid = pid if pid is not None else "default"
        self._group = group if group is not None else "default"
        self._unicast = f"local://uni@{self._group}/{self._pid}"
        self._anycast = f"local://any@{self._group}/{self._pid}"
        self._lock = asyncio.Lock()
        self._subscribers: list[Callable[[str], None]] = []
        self._tick_handle: asyncio.Task[None] | None = None
        self._dispatch_tasks: set[asyncio.Task[None]] = set()

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    async def start(self) -> None:
        self._tick_handle = asyncio.create_task(self._tick_loop())

    async def _tick_loop(self) -> None:
        # The body runs once immediately on start and then once per second
        # thereafter.
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                now = now_ms()
                async with self._lock:
                    self.state.outgoing.clear()
                    self.state.tick(now)
                    outgoing = self.state.outgoing
                    self.state.outgoing = []
                self._dispatch_messages(outgoing)
                await asyncio.sleep(1)

    async def stop(self) -> None:
        handle = self._tick_handle
        self._tick_handle = None
        if handle is not None:
            handle.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle
        self._subscribers.clear()

    async def send(self, req: str) -> str:
        try:
            req_json = msgspec.json.decode(req)
        except msgspec.DecodeError as exc:
            msg = f"invalid JSON request: {exc}"
            raise DecodingError(msg) from exc

        # Unwrap envelope if present: extract flat request for internal processing.
        flat_req = unwrap_request_envelope(req_json)

        now = now_ms()
        async with self._lock:
            flat_response = self.state.apply(now, flat_req)
            outgoing = self.state.outgoing
            self.state.outgoing = []

        # Wrap flat response in protocol envelope.
        envelope_response = wrap_response_envelope(flat_response)
        resp_str = msgspec.json.encode(envelope_response).decode("utf-8")

        # Dispatch messages asynchronously, off the critical path.
        self._dispatch_messages(outgoing)

        return resp_str

    def recv(self, callback: Callable[[str], None]) -> None:
        self._subscribers.append(callback)

    def target_resolver(self, target: str) -> str:
        return f"local://any@{target}"

    def _dispatch_messages(self, messages: list[OutgoingMessage]) -> None:
        """Dispatch outgoing messages to all subscribers, off the critical path."""
        if not messages or not self._subscribers:
            return

        async def run() -> None:
            subs = list(self._subscribers)
            for msg in messages:
                msg_str = msgspec.json.encode(msg.message).decode("utf-8")
                for cb in subs:
                    cb(msg_str)

        task = asyncio.create_task(run())
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)


# =============================================================================
# UTILITIES
# =============================================================================


def _parse_int(value: str | None) -> int | None:
    """Parse a decimal string into an int, returning ``None`` if it is invalid."""
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def extract_tags(v: Any) -> dict[str, str]:
    """Extract the ``tags`` object as a ``str -> str`` map (non-string values dropped)."""
    tags = _get(v, "tags")
    if isinstance(tags, dict):
        return {k: val for k, val in tags.items() if isinstance(val, str)}
    return {}


# =============================================================================
# PROTOCOL ENVELOPE HELPERS
# =============================================================================


def extract_action_data(val: Any) -> Any:
    """Return the ``data`` portion of a sub-envelope, or ``val`` if it is flat.

    A value carrying both ``kind`` and ``data`` keys is treated as a
    ``{ kind, head, data }`` sub-envelope.
    """
    if isinstance(val, dict) and "kind" in val and "data" in val:
        return val["data"]
    return val


def unwrap_request_envelope(req: Any) -> Any:
    """Unwrap a protocol envelope request into the flat format ``ServerState`` expects.

    A request carrying ``head`` and ``data`` is flattened (``data`` fields plus
    ``kind`` and ``head.corrId``); an already-flat request is returned as-is.
    """
    if isinstance(req, dict) and "head" in req and "data" in req:
        data = req.get("data")
        flat: dict[str, Any] = dict(data) if isinstance(data, dict) else {}
        if "kind" in req:
            flat["kind"] = req["kind"]
        head = req.get("head")
        if isinstance(head, dict) and "corrId" in head:
            flat["corrId"] = head["corrId"]
        return flat
    return req


def wrap_response_envelope(flat: Any) -> dict[str, Any]:
    """Wrap a flat ``ServerState`` response into the protocol envelope format.

    ``kind``, ``corrId``, and ``status`` are lifted into the envelope head and
    stripped from the data.
    """
    kind = _get(flat, "kind")
    corr_id = _get(flat, "corrId")
    status = _u64(flat, "status", 200)

    data: dict[str, Any] = dict(flat) if isinstance(flat, dict) else {}
    data.pop("kind", None)
    data.pop("corrId", None)
    data.pop("status", None)

    return {
        "kind": kind,
        "head": {"corrId": corr_id, "status": status, "version": PROTOCOL_VERSION},
        "data": data,
    }


def _execute_task_id(message: Any) -> str | None:
    """Return the task id of an ``execute`` outgoing message, else ``None``."""
    if isinstance(message, dict) and message.get("kind") == "execute":
        task = _get(_get(message, "data"), "task")
        if isinstance(task, dict):
            tid = task.get("id")
            if isinstance(tid, str):
                return tid
    return None
