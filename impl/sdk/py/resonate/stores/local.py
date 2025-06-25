from __future__ import annotations

import contextlib
import random
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, final

from croniter import croniter

from resonate.encoders import Base64Encoder
from resonate.errors import ResonateStoreError
from resonate.message_sources import LocalMessageSource
from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.message import DurablePromiseMesg, DurablePromiseValueMesg, InvokeMesg, Mesg, NotifyMesg, ResumeMesg, TaskMesg
from resonate.models.schedules import Schedule
from resonate.models.task import Task
from resonate.routers import TagRouter
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.clock import Clock
    from resonate.models.encoder import Encoder
    from resonate.models.router import Router


class LocalStore:
    def __init__(self, encoder: Encoder[str | None, str | None] | None = None, clock: Clock | None = None) -> None:
        self._encoder = encoder or Base64Encoder()
        self._promises = LocalPromiseStore(self)
        self._tasks = LocalTaskStore(self)
        self._schedules = LocalScheduleStore(self)

        self._routers: list[Router] = [TagRouter()]
        self._targets: dict[str, str] = {"default": "local://any@default"}
        self._clock: Clock = clock or time

        self._conn = set[LocalMessageSource]()
        self._cond = threading.Condition()

        self._thread: threading.Thread | None = None
        self._stopped = False

    @property
    def encoder(self) -> Encoder[str | None, str | None]:
        return self._encoder

    @property
    def promises(self) -> LocalPromiseStore:
        return self._promises

    @property
    def tasks(self) -> LocalTaskStore:
        return self._tasks

    @property
    def schedules(self) -> LocalScheduleStore:
        return self._schedules

    @property
    def routers(self) -> list[Router]:
        return self._routers

    @property
    def targets(self) -> dict[str, str]:
        return self._targets

    @property
    def clock(self) -> Clock:
        return self._clock

    def add_router(self, router: Router) -> None:
        self._routers.append(router)

    def add_target(self, name: str, target: str) -> None:
        self._targets[name] = target

    def message_source(self, group: str, id: str) -> LocalMessageSource:
        source = LocalMessageSource(self, group, id)
        self.connect(source)

        return source

    def connect(self, source: LocalMessageSource) -> None:
        self._conn.add(source)

    def disconnect(self, source: LocalMessageSource) -> None:
        if source in self._conn:
            self._conn.remove(source)

        if not self._conn:
            self.stop()

    def start(self) -> None:
        if not self._thread or not self._thread.is_alive():
            self._stopped = False
            self._thread = threading.Thread(target=self.loop, name="store", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stopped = True
        self.notify()

        if self._thread and self._thread.is_alive():
            self._thread.join()

    def notify(self) -> None:
        with self._cond:
            self._cond.notify()

    @exit_on_exception
    def loop(self) -> None:
        # set initial timeout to zero to prime the control loop
        timeout = 0.0

        while not self._stopped:
            # wait until whichever occurs first:
            # the condition is cleared (occurs when get, create, resolve, etc are called)
            # the timeout expires
            with self._cond:
                self._cond.wait(timeout)

                step = self.step()
                next: bool | None = None
                timeout = None

                while True:
                    try:
                        targets = []

                        # grab the next message from the step generator
                        addr, mesg = step.send(next)

                        # check all connections for preferred and alternate
                        # targets
                        for message_source in self._conn:
                            preferred, alternate = message_source.match(addr)
                            assert not (preferred and alternate)

                            if preferred:
                                targets = [message_source]
                                break
                            if alternate:
                                targets.append(message_source)

                        # send to a random target, when we find a preferred
                        # match there will be a single target, finally let the
                        # step generator know if we were able to send to the
                        # target
                        if targets:
                            random.choice(targets).enqueue(mesg)
                            next = True
                        else:
                            next = False
                    except StopIteration:
                        break

                # The next timeout time is the minimum of:
                # all promise timeouts
                # all task expirations
                # all schedule next runtime
                for promise in self.promises.scan():
                    if promise.state == "PENDING":
                        timeout = promise.timeout if timeout is None else min(promise.timeout, timeout)
                for task in self.tasks.scan():
                    if task.state == ("INIT", "ENQUEUED", "CLAIMED"):
                        assert task.expiry is not None
                        timeout = task.expiry if timeout is None else min(task.expiry, timeout)

                for schedule in self.schedules.scan():
                    timeout = schedule.next_run_time if timeout is None else min(timeout, schedule.next_run_time)

                # convert to relative time in seconds while respecting max
                # timeout
                if timeout is not None:
                    timeout = min(max(0, (timeout / 1000) - self.clock.time()), threading.TIMEOUT_MAX)

    def step(self) -> Generator[tuple[str, Mesg], bool | None, None]:
        time = int(self._clock.time() * 1000)

        # create scheduled promises
        for schedule in self.schedules.scan():
            if time < schedule.next_run_time:
                continue

            with contextlib.suppress(ResonateStoreError):
                self.promises.create(
                    id=schedule.promise_id.replace("{{.timestamp}}", str(time)),
                    timeout=time + schedule.promise_timeout,
                    ikey=None,
                    strict=False,
                    headers=schedule.promise_param.headers,
                    data=schedule.promise_param.data,
                    tags=schedule.promise_tags,
                )
            # update schedule
            _, applied = self.schedules.transition(id=schedule.id, to="CREATED", updating=True)
            assert applied

        # transition promises to timedout
        for promise in self.promises.scan():
            if promise.state == "PENDING" and time >= promise.timeout:
                _, _, applied = self.promises.transition(id=promise.id, to="REJECTED_TIMEDOUT")
                assert applied

        # transition tasks to init
        for task in self.tasks.scan():
            if task.state in ("ENQUEUED", "CLAIMED"):
                assert task.expiry is not None

                if time >= task.expiry:
                    _, applied = self.tasks.transition(id=task.id, to="INIT", force=True)
                    assert applied

        # send all outstanding messages
        for task in self.tasks.scan():
            if task.state != "INIT":
                continue

            if task.type == "invoke":
                mesg = InvokeMesg(
                    type="invoke",
                    task=TaskMesg(id=task.id, counter=task.counter),
                )
            elif task.type == "resume":
                mesg = ResumeMesg(
                    type="resume",
                    task=TaskMesg(id=task.id, counter=task.counter),
                )
            else:
                assert task.type == "notify"

                # promise param and value are already decoded, therefore we
                # must re-encode below them before sending
                promise = self.promises.get(task.root_promise_id)

                mesg = NotifyMesg(
                    type="notify",
                    promise=DurablePromiseMesg(
                        id=promise.id,
                        state=promise.state,
                        timeout=promise.timeout,
                        idempotencyKeyForCreate=promise.ikey_for_create,
                        idempotencyKeyForComplete=promise.ikey_for_complete,
                        param=DurablePromiseValueMesg(headers=promise.param.headers, data=self._encoder.encode(promise.param.data)),
                        value=DurablePromiseValueMesg(headers=promise.value.headers, data=self._encoder.encode(promise.value.data)),
                        tags=promise.tags,
                        createdOn=promise.created_on,
                        completedOn=promise.completed_on,
                    ),
                )

            # yield the address and message so the driver (either the local
            # store or the simulator) can let us know if the message is
            # deliverable
            if (yield task.recv, mesg):
                # notify tasks go straight to completed, otherwise enqueued
                _, applied = self.tasks.transition(task.id, to="COMPLETED" if task.type == "notify" else "ENQUEUED")
                assert applied
            else:
                pass
                # TODO(dfarr): implement this
                # _, applied = self.tasks.transition(task.id, to="INIT", expiry=0)
                # assert applied


class LocalPromiseStore:
    def __init__(self, store: LocalStore) -> None:
        self._promises: dict[str, DurablePromiseRecord] = {}
        self._store = store

    def get(self, id: str) -> DurablePromise:
        promise = self._promises.get(id)
        if promise is None:
            raise ResonateStoreError(mesg="The specified promise was not found", code=40400)

        return DurablePromise.from_dict(self._store, promise.to_dict())

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
        promise, _ = self._create(
            id=id,
            timeout=timeout,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            tags=tags,
        )
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
            timeout=timeout,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
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
        return self._complete(id=id, state="RESOLVED", ikey=ikey, strict=strict, headers=headers, data=data)

    def reject(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, state="REJECTED", ikey=ikey, strict=strict, headers=headers, data=data)

    def cancel(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        return self._complete(id=id, state="REJECTED_CANCELED", ikey=ikey, strict=strict, headers=headers, data=data)

    def callback(
        self,
        promise_id: str,
        root_promise_id: str,
        recv: str,
        timeout: int,
    ) -> tuple[DurablePromise, Callback | None]:
        id = f"__resume:{root_promise_id}:{promise_id}"

        promise = self._promises.get(promise_id)
        if promise is None:
            raise ResonateStoreError(mesg="The specified promise was not found", code=40400)

        durable_promise = DurablePromise.from_dict(
            self._store,
            promise.to_dict(),
        )

        if promise.state != "PENDING" or id in promise.callbacks:
            return durable_promise, None

        callback = CallbackRecord(
            id=id,
            type="resume",
            promise_id=promise_id,
            root_promise_id=root_promise_id,
            recv=recv,
            timeout=timeout,
            created_on=int(self._store.clock.time() * 1000),
        )

        promise.callbacks[id] = callback
        return durable_promise, Callback.from_dict(callback.to_dict())

    def subscribe(
        self,
        id: str,
        promise_id: str,
        recv: str,
        timeout: int,
    ) -> tuple[DurablePromise, Callback | None]:
        id = f"__notify:{promise_id}:{id}"

        promise = self._promises.get(promise_id)
        if promise is None:
            raise ResonateStoreError(mesg="The specified promise was not found", code=40400)

        durable_promise = DurablePromise.from_dict(
            self._store,
            promise.to_dict(),
        )

        if promise.state != "PENDING" or id in promise.callbacks:
            return durable_promise, None

        callback = CallbackRecord(
            id=id,
            type="notify",
            promise_id=promise_id,
            root_promise_id=promise_id,
            recv=recv,
            timeout=timeout,
            created_on=int(self._store.clock.time() * 1000),
        )

        promise.callbacks[id] = callback
        return durable_promise, Callback.from_dict(callback.to_dict())

    def scan(self) -> Generator[DurablePromiseRecord]:
        yield from self._promises.values()

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
        promise, task, applied = self.transition(
            id=id,
            to="PENDING",
            strict=strict,
            ikey=ikey,
            headers=headers,
            data=data,
            tags=tags,
            timeout=timeout,
        )
        assert not applied or promise.state in ("PENDING", "REJECTED_TIMEDOUT")

        if applied and task and pid:
            task, applied_task = self._store.tasks.transition(id=task.id, to="CLAIMED", counter=1, pid=pid, ttl=ttl)
            assert applied_task

        if applied:
            # interrupt the control loop
            self._store.notify()

        return DurablePromise.from_dict(self._store, promise.to_dict()), Task.from_dict(self._store, task.to_dict()) if task else None

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
        promise, _, applied = self.transition(id=id, to=state, strict=strict, ikey=ikey, headers=headers, data=data)
        assert not applied or promise.state in (state, "REJECTED_TIMEDOUT")

        if applied:
            # interrupt the control loop
            self._store.notify()

        return DurablePromise.from_dict(self._store, promise.to_dict())

    def transition(
        self,
        id: str,
        to: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"],
        strict: bool | None = None,
        timeout: int | None = None,
        ikey: str | None = None,
        headers: dict[str, str] | None = None,
        data: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromiseRecord, TaskRecord | None, bool]:
        promise, applied = self._transition(
            id=id,
            to=to,
            strict=strict,
            timeout=timeout,
            ikey=ikey,
            headers=headers,
            data=data,
            tags=tags,
        )

        if applied and promise.state == "PENDING":
            # create invoke task
            for router in self._store.routers:
                if recv := router.route(promise):
                    task, applied = self._store.tasks.transition(
                        id=f"__invoke:{promise.id}",
                        to="INIT",
                        type="invoke",
                        recv=self._store.targets.get(recv, recv),
                        root_promise_id=promise.id,
                        leaf_promise_id=promise.id,
                    )
                    assert applied
                    return promise, task, applied

        if applied and promise.state in ("RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"):
            # create resume and notify tasks
            for callback in promise.callbacks.values():
                _, applied = self._store.tasks.transition(
                    id=callback.id,
                    to="INIT",
                    type=callback.type,
                    recv=callback.recv,
                    root_promise_id=callback.root_promise_id,
                    leaf_promise_id=callback.promise_id,
                )
                assert applied
            promise.callbacks.clear()

        return promise, None, applied

    def _transition(
        self,
        id: str,
        to: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"],
        strict: bool | None = None,
        timeout: int | None = None,
        ikey: str | None = None,
        headers: dict[str, str] | None = None,
        data: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromiseRecord, bool]:
        time = int(self._store.clock.time() * 1000)

        match record := self._promises.get(id), to, strict:
            case None, "PENDING", _:
                assert timeout is not None

                record = DurablePromiseRecord(
                    id=id,
                    state=to,
                    timeout=timeout,
                    ikey_for_create=ikey,
                    ikey_for_complete=None,
                    param=DurablePromiseRecordValue(headers=headers, data=self._store.encoder.encode(data)),
                    value=DurablePromiseRecordValue(headers=None, data=None),
                    tags=tags,
                    created_on=time,
                    completed_on=None,
                )
                self._promises[record.id] = record
                return record, True

            case None, "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _:
                raise ResonateStoreError(mesg="The specified promise was not found", code=40400)

            case DurablePromiseRecord(state="PENDING"), "PENDING", _ if time < record.timeout and ikey_match(record.ikey_for_create, ikey):
                return record, False

            case DurablePromiseRecord(state="PENDING"), "PENDING", False if time >= record.timeout and ikey_match(record.ikey_for_create, ikey):
                # in this case the caller will need to create callbacks and
                # notify the control loop
                return self._transition(id=id, to="REJECTED_TIMEDOUT")

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", _ if time < record.timeout:
                record = DurablePromiseRecord(
                    id=record.id,
                    state=to,
                    timeout=record.timeout,
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=ikey,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers=headers, data=self._store.encoder.encode(data)),
                    tags=record.tags,
                    created_on=record.created_on,
                    completed_on=time,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if time >= record.timeout:
                return self._transition(id=id, to="REJECTED_TIMEDOUT")

            case DurablePromiseRecord(state="PENDING"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if time >= record.timeout:
                # do not transition to timedout because we need the control
                # loop to do the transition
                raise ResonateStoreError(mesg="The promise has already timedout", code=40303)

            case DurablePromiseRecord(state="PENDING"), "REJECTED_TIMEDOUT", _:
                assert time >= record.timeout

                record = DurablePromiseRecord(
                    id=record.id,
                    state="RESOLVED" if record.tags and record.tags.get("resonate:timeout") == "true" else to,
                    timeout=record.timeout,
                    ikey_for_create=record.ikey_for_create,
                    ikey_for_complete=None,
                    param=record.param,
                    value=DurablePromiseRecordValue(headers=None, data=None),
                    tags=record.tags,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                    callbacks=record.callbacks,
                )
                self._promises[record.id] = record
                return record, True

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED" | "REJECTED_TIMEDOUT"), "PENDING", False if ikey_match(record.ikey_for_create, ikey):
                return record, False

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False if ikey_match(record.ikey_for_complete, ikey):
                return record, False

            case DurablePromiseRecord(state="REJECTED_TIMEDOUT"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", False:
                return record, False

            case DurablePromiseRecord(state="RESOLVED" | "REJECTED" | "REJECTED_CANCELED"), "RESOLVED" | "REJECTED" | "REJECTED_CANCELED", True if (
                ikey_match(record.ikey_for_complete, ikey) and record.state == to
            ):
                return record, False

            case record, to, strict:
                # TODO(dfarr): match server error messages
                raise ResonateStoreError(mesg=f"Unexpected transition ({record.state if record else 'None'} -> {to}, strict={strict})", code=40399)


class LocalTaskStore:
    def __init__(self, store: LocalStore) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._store = store

    def claim(
        self,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        task, applied = self.transition(id=id, to="CLAIMED", counter=counter, pid=pid, ttl=ttl)
        assert applied
        assert task.type in ("invoke", "resume")

        # interrupt the control loop
        self._store.notify()

        match task.type:
            case "invoke":
                root_promise = self._store.promises.get(task.root_promise_id)
                return root_promise, None
            case "resume":
                root_promise = self._store.promises.get(task.root_promise_id)
                leaf_promise = self._store.promises.get(task.leaf_promise_id)
                return root_promise, leaf_promise

    def complete(self, id: str, counter: int) -> bool:
        _, applied = self.transition(id=id, to="COMPLETED", counter=counter)

        if applied:
            # interrupt the control loop
            self._store.notify()

        return True

    def heartbeat(self, pid: str) -> int:
        applied = False
        affected_tasks = 0

        for task in self.scan():
            if task.state != "CLAIMED" or task.pid != pid:
                continue

            _, applied = self.transition(id=task.id, to="CLAIMED", force=True)
            assert applied
            affected_tasks += 1

        if applied:
            # interrupt the control loop
            self._store.notify()

        return affected_tasks

    def scan(self) -> Generator[TaskRecord]:
        yield from self._tasks.values()

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
        time = int(self._store.clock.time() * 1000)

        match record := self._tasks.get(id), to:
            case None, "INIT":
                assert type is not None
                assert recv is not None
                assert root_promise_id is not None
                assert leaf_promise_id is not None

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
                    created_on=time,
                    completed_on=None,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="INIT"), "ENQUEUED":
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
                    expiry=time + 5000,  # TODO(dfarr): make this configurable
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="INIT"), "CLAIMED" if record.counter == counter:
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
                    expiry=time + ttl,
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
                    expiry=time + ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

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
                    completed_on=time,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="ENQUEUED" | "CLAIMED"), "INIT":
                assert record.expiry is not None
                assert time >= record.expiry

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
                    expiry=time + record.ttl,
                    created_on=record.created_on,
                    completed_on=record.completed_on,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="CLAIMED"), "COMPLETED" if record.counter == counter and (record.expiry is not None and record.expiry >= time):
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
                    completed_on=time,
                )
                self._tasks[record.id] = record
                return record, True

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
                    completed_on=time,
                )
                self._tasks[record.id] = record
                return record, True

            case TaskRecord(state="COMPLETED"), "COMPLETED":
                return record, False

            case None, _:
                raise ResonateStoreError(mesg="The specified task was not found", code=40403)

            case _:
                raise ResonateStoreError(mesg="The task is already claimed, completed, or an invalid counter was provided", code=40305)


class LocalScheduleStore:
    def __init__(self, store: LocalStore) -> None:
        self._schedules: dict[str, ScheduleRecord] = {}
        self._store = store

    def create(
        self,
        id: str,
        cron: str,
        promise_id: str,
        promise_timeout: int,
        *,
        ikey: str | None = None,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        promise_headers: dict[str, str] | None = None,
        promise_data: str | None = None,
        promise_tags: dict[str, str] | None = None,
    ) -> Schedule:
        record, applied = self.transition(
            id=id,
            to="CREATED",
            cron=cron,
            promise_id=promise_id,
            promise_timeout=promise_timeout,
            ikey=ikey,
            description=description,
            tags=tags,
            promise_headers=promise_headers,
            promise_data=promise_data,
            promise_tags=promise_tags,
        )
        if applied:
            self._store.notify()
        return Schedule.from_dict(self._store, record.to_dict())

    def get(self, id: str) -> Schedule:
        record = self._schedules.get(id)
        if record is None:
            msg = "The specified schedule was not found"
            raise ResonateStoreError(msg, code=40403)
        return Schedule.from_dict(self._store, record.to_dict())

    def delete(self, id: str) -> None:
        _, applied = self.transition(id=id, to="DELETED")
        assert applied

    def transition(
        self,
        id: str,
        to: Literal["CREATED", "DELETED"],
        *,
        cron: str | None = None,
        promise_id: str | None = None,
        promise_timeout: int | None = None,
        ikey: str | None = None,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        promise_headers: dict[str, str] | None = None,
        promise_data: str | None = None,
        promise_tags: dict[str, str] | None = None,
        updating: bool = False,
    ) -> tuple[ScheduleRecord, bool]:
        time = int(self._store.clock.time() * 1000)

        match record := self._schedules.get(id), to:
            case None, "CREATED":
                assert cron is not None, "cron must be set"
                assert promise_id is not None, "promiseId must be set"
                assert promise_timeout is not None, "promiseTimeout must be set"
                assert promise_timeout >= 0, "promiseTimeout must be positive"

                record = ScheduleRecord(
                    id=id,
                    description=description,
                    cron=cron,
                    tags=tags,
                    promise_id=promise_id,
                    promise_timeout=promise_timeout,
                    promise_param=DurablePromiseRecordValue(
                        headers=promise_headers,
                        data=promise_data,
                    ),
                    promise_tags=promise_tags,
                    last_run_time=None,
                    next_run_time=next_runtime(cron, time),
                    idempotency_key=ikey,
                    created_on=time,
                )

                self._schedules[record.id] = record
                return record, True
            case ScheduleRecord(), "CREATED" if ikey_match(ikey, record.idempotency_key):
                return record, False
            case ScheduleRecord(), "CREATED" if updating:
                record = ScheduleRecord(
                    id=id,
                    description=record.description,
                    cron=record.cron,
                    tags=record.tags,
                    promise_id=record.promise_id,
                    promise_timeout=record.promise_timeout,
                    promise_param=record.promise_param,
                    promise_tags=record.promise_tags,
                    last_run_time=record.next_run_time,
                    next_run_time=next_runtime(record.cron, time),
                    idempotency_key=record.idempotency_key,
                    created_on=record.created_on,
                )
                self._schedules[id] = record
                return record, True
            case ScheduleRecord(), "CREATED":
                raise ResonateStoreError(mesg="Schedule already exists", code=40400)
            case None, "DELETED":
                msg = "The specified schedule was not found"
                raise ResonateStoreError(msg, code=40403)
            case ScheduleRecord(), "DELETED":
                self._schedules.pop(id)
                return record, True
            case _:
                raise ResonateStoreError(mesg=f"Unexpected transition ({'created' if record else 'None'} -> {to})", code=40399)

    def scan(self) -> Generator[ScheduleRecord]:
        yield from self._schedules.values()


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
    callbacks: dict[str, CallbackRecord] = field(default_factory=dict)

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
    promise_id: str
    root_promise_id: str
    recv: str
    timeout: int
    created_on: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "promiseId": self.promise_id,
            "timeout": self.timeout,
            "createdOn": self.created_on,
        }


@final
@dataclass(frozen=True)
class ScheduleRecord:
    id: str
    description: str | None
    cron: str
    tags: dict[str, str] | None
    promise_id: str
    promise_timeout: int
    promise_param: DurablePromiseRecordValue
    promise_tags: dict[str, str] | None
    last_run_time: int | None
    next_run_time: int
    idempotency_key: str | None
    created_on: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "description": self.description,
            "cron": self.cron,
            "tags": self.tags or {},
            "promiseId": self.promise_id,
            "promiseTimeout": self.promise_timeout,
            "promiseParam": self.promise_param.to_dict(),
            "promiseTags": self.promise_tags or {},
            "idempotencyKey": self.idempotency_key,
            "lastRunTime": self.last_run_time,
            "nextRunTime": self.next_run_time,
            "createdOn": self.created_on,
        }


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


def next_runtime(cron: str, base: int) -> int:
    # base is milliconds. cronitier works with seconds
    return int(croniter(cron, base / 1000).next() * 1000)


def ikey_match(left: str | None, right: str | None) -> bool:
    return left is not None and right is not None and left == right
