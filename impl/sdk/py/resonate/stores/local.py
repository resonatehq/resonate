from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, final

from resonate.encoders import Base64Encoder, ChainEncoder, JsonEncoder
from resonate.errors import ResonateStoreError
from resonate.message_sources import LocalMessageSource
from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.message import InvokeMesg, Mesg, NotifyMesg, ResumeMesg, TaskMesg
from resonate.models.task import Task
from resonate.routers import TagRouter

if TYPE_CHECKING:
    from resonate.models.clock import Clock
    from resonate.models.encoder import Encoder
    from resonate.models.message_source import MessageSource
    from resonate.models.router import Router


class LocalStore:
    def __init__(self, encoder: Encoder[Any, str | None] | None = None, clock: Clock | None = None) -> None:
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._tasks: dict[str, TaskRecord] = {}
        self._routers: list[Router] = [TagRouter()]

        self._encoder = encoder or ChainEncoder(JsonEncoder(), Base64Encoder())
        self._clock: Clock = clock or time

        self._promise_store = LocalPromiseStore(
            self,
            self._promises,
            self._tasks,
            self._routers,
            self._clock,
        )
        self._task_store = LocalTaskStore(
            self,
            self._promises,
            self._tasks,
            self._clock,
        )

    @property
    def encoder(self) -> Encoder[Any, str | None]:
        return self._encoder

    @property
    def promises(self) -> LocalPromiseStore:
        return self._promise_store

    @property
    def tasks(self) -> LocalTaskStore:
        return self._task_store

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def message_source(self, group: str, id: str) -> MessageSource:
        return LocalMessageSource(group, id, self)

    def step(self) -> list[tuple[str, Mesg]]:
        messages: list[tuple[str, Mesg]] = []

        for promise in self._promises.values():
            match promise, int(self._clock.time() * 1000) >= promise.timeout:
                case DurablePromiseRecord(state="PENDING"), True:
                    record, applied = self.promises.transition(id=promise.id, to="REJECTED_TIMEDOUT")
                    assert applied
                    for callback in record.callbacks:
                        self.tasks.transition(
                            id=str(len(self._tasks) + 1),
                            to="INIT",
                            type=callback.type,
                            recv=callback.recv,
                            root_promise_id=callback.root_promise_id,
                            leaf_promise_id=callback.promise_id,
                        )
                    record.callbacks.clear()

        for task in self._tasks.values():
            match task, self._clock.time() * 1000 >= (task.expiry or 0):
                case TaskRecord(type="invoke", state="INIT"), _:
                    _, applied = self.tasks.transition(id=task.id, to="ENQUEUED")
                    if applied:
                        messages.append((task.recv, InvokeMesg(type="invoke", task=TaskMesg(id=task.id, counter=task.counter))))
                case TaskRecord(type="resume", state="INIT"), _:
                    _, applied = self.tasks.transition(id=task.id, to="ENQUEUED")
                    if applied:
                        messages.append((task.recv, ResumeMesg(type="resume", task=TaskMesg(id=task.id, counter=task.counter))))
                case TaskRecord(type="notify"), _:
                    promise = self._promises.get(task.root_promise_id)
                    assert promise

                    _, applied = self.tasks.transition(id=task.id, to="COMPLETED")
                    if applied:
                        messages.append((task.recv, NotifyMesg(type="notify", promise=promise.to_dict())))
                case TaskRecord(state="ENQUEUED" | "CLAIMED"), True:
                    self.tasks.transition(id=task.id, to="INIT")

        return messages


class LocalPromiseStore:
    def __init__(self, store: LocalStore, promises: dict[str, DurablePromiseRecord], tasks: dict[str, TaskRecord], routers: list[Router], clock: Clock) -> None:
        self._store = store
        self._promises = promises
        self._tasks = tasks
        self._routers = routers
        self._clock = clock

    def get(self, id: str) -> DurablePromise:
        record = self._promises.get(id)
        if record is None:
            raise ResonateStoreError(message="The specified promise was not found", code=40400)
        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
        )

    def create(
        self,
        id: str,
        timeout: int,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise:
        promise, task = self._create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
        )
        assert task is None
        return promise

    def create_with_task(
        self,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]:
        return self._create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
            pid=pid,
            ttl=ttl,
        )

    def resolve(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, state="RESOLVED")

    def reject(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, state="REJECTED")

    def cancel(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, ikey=ikey, strict=strict, headers=headers, data=data, state="REJECTED_CANCELED")

    def callback(
        self,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        promise = self._promises.get(promise_id)
        if promise is None:
            raise ResonateStoreError(message="The specified promise was not found", code=40400)
        durable_promise = DurablePromise.from_dict(
            self._store,
            promise.to_dict(),
        )
        if promise.state != "PENDING":
            return durable_promise, None

        callback = CallbackRecord(id=id, type="resume", promise_id=promise_id, root_promise_id=root_promise_id, timeout=timeout, created_on=int(self._clock.time() * 1000), recv=recv)
        promise.callbacks.append(callback)
        return durable_promise, Callback(
            id=callback.id,
            promise_id=callback.promise_id,
            timeout=callback.timeout,
            created_on=callback.created_on,
        )

    def subscribe(
        self,
        id: str,
        promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        promise = self._promises.get(promise_id)
        if promise is None:
            raise ResonateStoreError(message="The specified promise was not found", code=40400)
        durable_promise = DurablePromise.from_dict(
            self._store,
            promise.to_dict(),
        )
        if promise.state != "PENDING":
            return durable_promise, None

        callback = CallbackRecord(id=id, type="notify", promise_id=promise_id, root_promise_id=promise_id, timeout=timeout, created_on=int(self._clock.time() * 1000), recv=recv)
        promise.callbacks.append(callback)
        return durable_promise, Callback(
            id=callback.id,
            promise_id=callback.promise_id,
            timeout=callback.timeout,
            created_on=callback.created_on,
        )

    def _create(
        self,
        id: str,
        timeout: int,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
        pid: str | None = None,
        ttl: int = 0,
    ) -> tuple[DurablePromise, Task | None]:
        record, applied = self.transition(
            id=id,
            to="PENDING",
            strict=strict,
            ikey=ikey,
            headers=headers,
            data=data,
            tags=tags,
            timeout=timeout,
        )

        task: Task | None = None
        if applied:
            for r in self._routers:
                if recv := r.route(record):
                    task_record, _ = self._store.tasks.transition(
                        id=str(len(self._tasks) + 1),
                        to="INIT",
                        type="invoke",
                        recv=recv,
                        root_promise_id=record.id,
                        leaf_promise_id=record.id,
                    )
                    if pid is not None:
                        task_record, _ = self._store.tasks.transition(id=task_record.id, to="CLAIMED", pid=pid, ttl=ttl, counter=1)
                        task = Task.from_dict(self._store, task_record.to_dict())
                    break
        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
        ), task

    def _complete(
        self,
        id: str,
        state: Literal["REJECTED_CANCELED", "REJECTED", "RESOLVED"],
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        record, applied = self.transition(id=id, to=state, strict=strict, headers=headers, data=data, ikey=ikey)
        if applied:
            for task in self._tasks.values():
                if (task.state in ("INIT", "ENQUEUED", "CLAIMED")) and task.root_promise_id == record.id:
                    self._store.tasks.transition(id=task.id, to="COMPLETED", force=True)

            for callback in record.callbacks:
                self._store.tasks.transition(
                    id=str(len(self._tasks) + 1),
                    to="INIT",
                    type=callback.type,
                    recv=callback.recv,
                    root_promise_id=callback.root_promise_id,
                    leaf_promise_id=callback.promise_id,
                )
            record.callbacks.clear()

        return DurablePromise.from_dict(
            self._store,
            record.to_dict(),
        )

    def transition(
        self,
        id: str,
        to: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"],
        strict: bool | None = None,
        timeout: int | None = None,
        ikey: str | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromiseRecord, bool]:
        match record := self._promises.get(id), to, strict:
            case None, "PENDING", _:
                assert timeout is not None
                assert int(self._clock.time() * 1000) is not None
                record = DurablePromiseRecord(
                    id=id,
                    state=to,
                    timeout=timeout,
                    param=DurablePromiseRecordValue(headers=headers or {}, data=self._store.encoder.encode(data)),
                    value=DurablePromiseRecordValue(headers={}, data=None),
                    created_on=int(self._clock.time() * 1000),
                    completed_on=None,
                    ikey_for_create=ikey,
                    ikey_for_complete=None,
                    tags=tags,
                    callbacks=[],
                )
                self._promises[record.id] = record
                return record, True

            case None, "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _:
                raise ResonateStoreError(message="The specified promise was not found", code=40400)

            case DurablePromiseRecord(state="PENDING"), "PENDING", _ if int(self._clock.time() * 1000) < record.timeout and ikey_match(record.ikey_for_create, ikey):
                return record, False

            case DurablePromiseRecord(state="PENDING"), "PENDING", False if int(self._clock.time() * 1000) >= record.timeout and ikey_match(record.ikey_for_create, ikey):
                return self.transition(id=id, to="REJECTED_TIMEDOUT")

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _ if int(self._clock.time() * 1000) < record.timeout:
                record = DurablePromiseRecord(
                    id=record.id,
                    state=to,
                    timeout=record.timeout,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers=headers, data=self._store.encoder.encode(data)),
                    created_on=record.created_on,
                    completed_on=int(self._clock.time() * 1000),
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    tags=record.tags,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if int(self._clock.time() * 1000) >= record.timeout:
                return self.transition(id=id, to="REJECTED_TIMEDOUT")

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if int(self._clock.time() * 1000) >= record.timeout:
                self.transition(id=id, to="REJECTED_TIMEDOUT")
                raise ResonateStoreError(message="The promise has already timedout", code=40303)

            case DurablePromiseRecord(state="PENDING"), "REJECTED_TIMEDOUT", _:
                assert int(self._clock.time() * 1000) >= record.timeout
                record = DurablePromiseRecord(
                    id=record.id,
                    state="RESOLVED" if record.tags and record.tags.get("resonate:timeout") == "true" else to,
                    timeout=record.timeout,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers={}, data=None),
                    tags=record.tags,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=None,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "PENDING", False if ikey_match(record.ikey_for_create, ikey):
                return record, False

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if ikey_match(record.ikey_for_complete, ikey):
                return record, False

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if (
                ikey_match(record.ikey_for_complete, ikey) and record.state == to
            ):
                return record, False

            case record, to, strict:
                # TODO(dfarr): match server error messages
                raise ResonateStoreError(message=f"Unexpected transition ({record.state if record else 'None'} -> {to}, strict={strict})", code=403)


class LocalTaskStore:
    def __init__(self, store: LocalStore, promises: dict[str, DurablePromiseRecord], tasks: dict[str, TaskRecord], clock: Clock) -> None:
        self._store = store
        self._promises = promises
        self._tasks = tasks
        self._clock = clock

    def claim(
        self,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        task_record, _ = self.transition(id=id, to="CLAIMED", pid=pid, ttl=ttl, counter=counter)
        assert task_record.type in ("invoke", "resume")

        match task_record.type:
            case "invoke":
                root_promise = self._promises[task_record.root_promise_id]
                return DurablePromise.from_dict(
                    self._store,
                    root_promise.to_dict(),
                ), None
            case "resume":
                root_promise = self._promises[task_record.root_promise_id]
                leaf_promise = self._promises[task_record.leaf_promise_id]
                return DurablePromise.from_dict(
                    self._store,
                    root_promise.to_dict(),
                ), DurablePromise.from_dict(
                    self._store,
                    leaf_promise.to_dict(),
                )

    def complete(self, id: str, counter: int) -> bool:
        self.transition(id=id, to="COMPLETED", counter=counter)
        return True

    def heartbeat(self, pid: str) -> int:
        affected_tasks = 0
        for record in self._tasks.values():
            if record.state != "CLAIMED" or record.pid != pid:
                continue

            assert record.ttl is not None

            _, modified = self.transition(id=record.id, to="CLAIMED", force=True)
            assert modified
            affected_tasks += 1

        return affected_tasks

    def transition(
        self,
        id: str,
        to: Literal["INIT", "ENQUEUED", "CLAIMED", "COMPLETED"],
        *,
        type: Literal["invoke", "resume", "notify"] | None = None,
        recv: str | None = None,
        root_promise_id: str | None = None,
        leaf_promise_id: str | None = None,
        counter: int | None = None,
        pid: str | None = None,
        ttl: int | None = None,
        force: bool = False,
    ) -> tuple[TaskRecord, bool]:
        """Transition a task.

        Returns the task record that was transitioned and a boolean indicating if a message should be sent.
        """
        match record := self._tasks.get(id), to:
            case None, "INIT":
                assert type is not None
                assert recv is not None
                assert root_promise_id is not None
                assert leaf_promise_id is not None
                assert int(self._clock.time() * 1000) is not None
                record = TaskRecord(
                    id=id,
                    counter=1,
                    state=to,
                    type=type,
                    recv=recv,
                    root_promise_id=root_promise_id,
                    leaf_promise_id=leaf_promise_id,
                    pid=None,
                    ttl=None,
                    expiry=None,
                    created_on=int(self._clock.time() * 1000),
                    completed_on=None,
                )
                self._tasks[record.id] = record
                return record, False

            case TaskRecord(state="INIT"), "ENQUEUED":
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=record.pid,
                    ttl=record.ttl,
                    expiry=int(self._clock.time() * 1000) + 5000,  # TODO(dfarr): make this configurable
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="INIT"), "CLAIMED" if record.counter == counter:
                assert ttl is not None
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    expiry=int(self._clock.time() * 1000) + ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="ENQUEUED"), "CLAIMED" if record.counter == counter:
                assert ttl is not None
                assert pid is not None
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=pid,
                    ttl=ttl,
                    expiry=int(self._clock.time() * 1000) + ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, False

            case TaskRecord(state="INIT" | "ENQUEUED", type="notify"), "COMPLETED":
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    expiry=None,
                    created_on=record.created_on,
                    completed_on=int(self._clock.time() * 1000),
                )
                self._tasks[record.id] = record
                return record, True
            case TaskRecord(state="ENQUEUED" | "CLAIMED"), "INIT":
                assert record.expiry is not None
                assert int(self._clock.time() * 1000) >= record.expiry
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter + 1,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    expiry=None,
                    created_on=record.created_on,
                    completed_on=None,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="CLAIMED"), "CLAIMED" if force:
                assert record.ttl is not None
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=record.state,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=record.pid,
                    ttl=record.ttl,
                    expiry=int(self._clock.time() * 1000) + record.ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="CLAIMED"), "COMPLETED" if record.counter == counter and (record.expiry is not None and record.expiry >= int(self._clock.time() * 1000)):
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    expiry=None,
                    created_on=record.created_on,
                    completed_on=int(self._clock.time() * 1000),
                )
                self._tasks[record.id] = record
                return record, False

            case TaskRecord(state="INIT" | "ENQUEUED" | "CLAIMED"), "COMPLETED" if force:
                record = TaskRecord(
                    id=record.id,
                    counter=record.counter,
                    state=to,
                    type=record.type,
                    recv=record.recv,
                    root_promise_id=record.root_promise_id,
                    leaf_promise_id=record.leaf_promise_id,
                    pid=None,
                    ttl=None,
                    expiry=None,
                    created_on=record.created_on,
                    completed_on=int(self._clock.time() * 1000),
                )
                self._tasks[record.id] = record
                return record, False

            case TaskRecord(state="COMPLETED"), "COMPLETED":
                return record, False

            case None, _:
                raise ResonateStoreError(message="The specified task was not found", code=40403)

            case _:
                raise ResonateStoreError(message="The task is already claimed, completed, or an invalid counter was provided", code=40305)


@final
@dataclass(frozen=True)
class DurablePromiseRecord:
    id: str
    state: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"]
    timeout: int
    ikey_for_create: str | None
    ikey_for_complete: str | None
    param: DurablePromiseRecordValue
    value: DurablePromiseRecordValue
    tags: dict[str, str] | None
    created_on: int
    completed_on: int | None
    callbacks: list[CallbackRecord]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state,
            "timeout": self.timeout,
            "idempotencyKeyForCreate": self.ikey_for_create,
            "idempotencyKeyForComplete": self.ikey_for_complete,
            "param": self.param.to_dict(),
            "value": self.value.to_dict(),
            "tags": self.tags or {},
            "createdOn": self.created_on,
            "completedOn": self.completed_on,
        }


@final
@dataclass(frozen=True)
class DurablePromiseRecordValue:
    headers: dict[str, str] | None
    data: str | None

    def to_dict(self) -> dict[str, Any]:
        return {"headers": self.headers, "data": self.data}


@final
@dataclass(frozen=True)
class CallbackRecord:
    id: str
    type: Literal["resume", "notify"]
    root_promise_id: str
    promise_id: str
    timeout: int
    created_on: int
    recv: str


@final
@dataclass(frozen=True)
class TaskRecord:
    id: str
    counter: int
    state: Literal["INIT", "ENQUEUED", "CLAIMED", "COMPLETED"]
    type: Literal["invoke", "resume", "notify"]
    recv: str
    root_promise_id: str
    leaf_promise_id: str
    pid: str | None
    ttl: int | None
    expiry: int | None
    created_on: int
    completed_on: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "counter": self.counter}


def ikey_match(left: str | None, right: str | None) -> bool:
    return left is not None and right is not None and left == right
