from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from functools import partial
from inspect import iscoroutinefunction, isfunction, isgeneratorfunction
from queue import Queue
from threading import Thread
from typing import TYPE_CHECKING, Any, TypeVar

import requests
from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import DI, LFC, LFI, RFC, RFI
from resonate.cmd_queue import (
    Claim,
    Command,
    CommandQ,
    Complete,
    Invoke,
    Resume,
)
from resonate.context import Context
from resonate.dataclasses import (
    SQE,
    DurablePromise,
    FinalValue,
    Invocation,
    ResonateCoro,
)
from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.processor.processor import Processor
from resonate.queue import DelayQueue
from resonate.record import Promise, Record
from resonate.result import Err, Ok, Result
from resonate.scheduler.traits import IScheduler
from resonate.stores.record import (
    DurablePromiseRecord,
    InvokeMsg,
    ResumeMsg,
    TaskRecord,
)
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.collections import FunctionRegistry
    from resonate.dependencies import Dependencies
    from resonate.record import Handle
    from resonate.stores.local import LocalStore
    from resonate.stores.record import TaskRecord
    from resonate.task_sources.traits import ITaskSource
    from resonate.typing import Data, DurableCoro, DurableFn, Headers, Tags

T = TypeVar("T")
P = ParamSpec("P")


class Scheduler(IScheduler):
    def __init__(
        self,
        deps: Dependencies,
        pid: str,
        registry: FunctionRegistry,
        store: LocalStore | RemoteStore,
        task_source: ITaskSource,
    ) -> None:
        self._deps = deps
        self._pid = pid
        self._processor = Processor()
        self._registry = registry
        self._store = store
        self._task_source = task_source

        self._awaiting_rfi: dict[str, list[str]] = {}
        self._awaiting_lfi: dict[str, list[str]] = {}

        self._encoder = JsonEncoder()
        self._recv = self._task_source.default_recv()

        self._records: dict[str, Record[Any]] = {}
        self._cmd_queue: CommandQ = Queue()
        self._delay_queue = DelayQueue()

        self._heartbeat_thread = Thread(target=self._heartbeat, daemon=True)
        self._scheduler_thread = Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        if isinstance(self._store, RemoteStore):
            # start the heartbeat thread
            self._heartbeat_thread.start()

            # start the task source
            self._task_source.set_pid(self._pid)
            self._task_source.start(self._cmd_queue)

        # start delay queue
        self._delay_queue.start(self._cmd_queue)

        # start the processor
        self._processor.start(self._cmd_queue)

        # start the scheduler
        self._scheduler_thread.start()

    def run(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]:
        # If there's already a record with this ID, dedup.
        record = self._records.get(id)
        if record is not None:
            return record.handle

        # Get function name from registry
        fn_name = self._registry.get_from_value(func)
        assert fn_name is not None, f"Function {func.__name__} must be registered"
        func_with_options = self._registry.get(fn_name)
        assert func_with_options is not None
        opts = func_with_options[-1]
        assert opts.durable, "Top level must always be durable."

        record = Record[Any](
            id=id,
            parent=None,
            invocation=RFI(Invocation(func, *args, **kwargs), opts),
            ctx=Context(self._deps),
        )
        self._records[record.id] = record

        # Create durable promise while claiming the task.
        assert self._recv
        durable_promise, task = self._store.promises.create_with_task(
            id=id,
            ikey=utils.string_to_uuid(id),
            strict=False,
            headers=None,
            data=self._encoder.encode(
                {"func": fn_name, "args": args, "kwargs": kwargs}
            ),
            timeout=sys.maxsize,
            tags={},
            pid=self._pid,
            ttl=5 * 1_000,
            recv=self._recv,
        )

        record.add_durable_promise(durable_promise)
        if task:
            record.add_task(task)

        if durable_promise.is_completed():
            assert task is None
            record.set_result(durable_promise.get_value(self._encoder), deduping=True)
        else:
            self._cmd_queue.put(Invoke(record.id))

        return record.handle

    def _heartbeat(self) -> None:
        assert isinstance(self._store, RemoteStore)
        while True:
            affected: int | None = None
            with contextlib.suppress(requests.exceptions.ConnectionError):
                affected = self._store.tasks.heartbeat(pid=self._pid)
                logger.debug("Heatbeat affected %s tasks", affected)
            time.sleep(2)

    def _loop(self) -> None:
        while True:
            cmd = self._cmd_queue.get()
            if cmd is None:
                break

            # start the next tick
            for loopback in self._step(cmd):
                self._cmd_queue.put(loopback)

            # mark task done
            self._cmd_queue.task_done()

    def _step(self, cmd: Command) -> list[Command]:
        if isinstance(cmd, Invoke):
            return self._handle_invoke(cmd)
        if isinstance(cmd, Resume):
            return self._handle_resume(cmd)
        if isinstance(cmd, Complete):
            return self._handle_complete(cmd)
        if isinstance(cmd, Claim):
            return self._handle_claim(cmd)

        assert_never(cmd)

    def _handle_resume(self, resume: Resume) -> list[Command]:
        return self._handle_continue(resume.id, resume.next_value)

    def _process_final_value(
        self, record: Record[Any], final_value: Result[Any, Exception]
    ) -> list[Command]:
        return [Complete(record.id, final_value)]

    def _handle_continue(
        self, id: str, next_value: Result[Any, Exception] | None
    ) -> list[Command]:
        record = self._records[id]
        coro = record.get_coro()
        yielded_value = coro.advance(next_value)

        if isinstance(yielded_value, LFI):
            return self._process_lfi(record, yielded_value)
        if isinstance(yielded_value, LFC):
            return self._process_lfc(record, yielded_value)
        if isinstance(yielded_value, RFI):
            return self._process_rfi(record, yielded_value)
        if isinstance(yielded_value, RFC):
            return self._process_rfc(record, yielded_value)
        if isinstance(yielded_value, Promise):
            return self._process_promise(record, yielded_value)
        if isinstance(yielded_value, FinalValue):
            return self._process_final_value(record, yielded_value.v)
        if isinstance(yielded_value, DI):
            # start execution from the top. Add current record to runnable
            raise NotImplementedError

        assert_never(yielded_value)

    def _handle_claim(self, claim: Claim) -> list[Command]:
        assert isinstance(self._store, RemoteStore)
        invoke_or_resume = self._store.tasks.claim(
            task_id=claim.record.task_id,
            counter=claim.record.counter,
            pid=self._pid,
            ttl=5 * 1000,
        )
        if isinstance(invoke_or_resume, InvokeMsg):
            return self._process_invoke_msg(invoke_or_resume, claim.record)
        if isinstance(invoke_or_resume, ResumeMsg):
            return self._process_resume_msg(invoke_or_resume, claim.record)

        assert_never(invoke_or_resume)

    def _process_invoke_msg(
        self, invoke_msg: InvokeMsg, task: TaskRecord
    ) -> list[Command]:
        logger.info(
            "Invoke message for %s received", invoke_msg.root_durable_promise.id
        )

        invoke_info = invoke_msg.root_durable_promise.invoke_info(self._encoder)
        func_name = invoke_info["func_name"]
        registered_func = self._registry.get(func_name)
        assert registered_func, f"There's no function registered under name {func_name}"
        func, opts = registered_func
        assert opts.durable

        rfi = RFI(
            Invocation(func, *invoke_info["args"], **invoke_info["kwargs"]), opts=opts
        )
        record = self._records.get(invoke_msg.root_durable_promise.id)
        if record:
            assert record.is_root
            assert isinstance(record.invocation, RFI)
            assert record.durable_promise is not None
            record.invocation = rfi
        else:
            record = Record[Any](
                id=invoke_msg.root_durable_promise.id,
                invocation=rfi,
                parent=None,
                ctx=Context(self._deps),
            )
            self._records[record.id] = record
            record.add_durable_promise(invoke_msg.root_durable_promise)

        record.add_task(task=task)
        return [Invoke(record.id)]

    def _process_resume_msg(
        self, resume_msg: ResumeMsg, task: TaskRecord
    ) -> list[Command]:
        logger.info(
            "Continue message for %s received", resume_msg.leaf_durable_promise.id
        )
        assert isinstance(self._store, RemoteStore)
        assert resume_msg.leaf_durable_promise.is_completed()

        root_record = self._records.get(resume_msg.root_durable_promise.id)
        leaf_record = self._records.get(resume_msg.leaf_durable_promise.id)
        if root_record and leaf_record:
            assert root_record.is_root
            assert leaf_record.is_root
            root_record.add_task(task)
            if not leaf_record.done():
                leaf_record.set_result(
                    resume_msg.leaf_durable_promise.get_value(self._encoder),
                    deduping=True,
                )

            return self._unblock_awaiting_remote(leaf_record.id)

        return self._process_invoke_msg(
            InvokeMsg(resume_msg.root_durable_promise), task
        )

    def _process_promise(
        self, record: Record[Any], promise: Promise[Any]
    ) -> list[Command]:
        loopbacks: list[Command] = []
        promise_record = self._records[promise.id]
        if promise_record.done():
            loopbacks.extend(
                self._handle_continue(record.id, promise_record.safe_result())
            )
        elif isinstance(promise_record.invocation, LFI):
            self._add_to_awaiting_local(promise_record.id, record.id)
        elif isinstance(promise_record.invocation, RFI):
            assert isinstance(self._store, RemoteStore)
            assert (
                promise_record.is_root
            ), "Callbacks can only be registered partition roots"

            root = record.root()
            leaf_id = promise_record.id

            assert self._recv
            assert (
                promise_record.durable_promise
            ), f"Record {promise_record.id} not backed by a promise"
            durable_promise, callback = self._store.callbacks.create(
                id=utils.string_to_uuid(record.id),
                promise_id=leaf_id,
                root_promise_id=root.id,
                timeout=sys.maxsize,
                recv=self._recv,
            )
            if callback is None:
                assert durable_promise.is_completed()
                record.set_result(
                    durable_promise.get_value(self._encoder), deduping=True
                )
            else:
                self._add_to_awaiting_remote(promise_record.id, record.id)
                if self._blocked_only_on_remote(root.id):
                    self._complete_task(root.id)

        else:
            assert_never(promise_record.invocation)

        return loopbacks

    def _unblock_awaiting_local(self, id: str) -> list[Command]:
        record = self._records[id]
        assert record.done()
        resume_cmds: list[Command] = []
        for blocked_id in self._awaiting_lfi.pop(record.id, []):
            blocked_record = self._records[blocked_id]
            assert blocked_record.blocked_on
            assert blocked_record.blocked_on.id == id
            blocked_record.blocked_on = None
            resume_cmds.append(Resume(blocked_id, record.safe_result()))
        return resume_cmds

    def _unblock_awaiting_remote(self, id: str) -> list[Command]:
        record = self._records[id]
        assert record.done()
        assert isinstance(record.invocation, RFI)
        resume_cmds: list[Command] = []
        for blocked_id in self._awaiting_rfi.pop(record.id, []):
            blocked_record = self._records[blocked_id]
            assert blocked_record.blocked_on
            assert blocked_record.blocked_on.id == id
            blocked_record.blocked_on = None
            resume_cmds.append(Resume(blocked_id, record.safe_result()))
        return resume_cmds

    def _add_to_awaiting_local(self, id: str, blocked: str) -> None:
        logger.info("Blocking %s awaiting locally on %s", blocked, id)
        record = self._records[id]
        assert isinstance(record.invocation, LFI)
        assert not record.done()
        blocked_record = self._records[blocked]
        assert not blocked_record.blocked_on
        blocked_record.blocked_on = record
        self._awaiting_lfi.setdefault(id, []).append(blocked)

    def _add_to_awaiting_remote(self, id: str, blocked: str) -> None:
        logger.info("Blocking %s awaiting remotely on %s", blocked, id)
        record = self._records[id]
        assert isinstance(record.invocation, RFI)
        assert not record.done()
        blocked_record = self._records[blocked]
        assert not blocked_record.blocked_on
        blocked_record.blocked_on = record
        self._awaiting_rfi.setdefault(id, []).append(blocked)

    def _blocked_only_on_remote(self, id: str, *, root: bool = True) -> bool:
        record = self._records[id]
        if isinstance(record.invocation, RFI) and not root:
            return True
        if not record.blocked_on:
            return False
        return all(
            self._blocked_only_on_remote(child.id, root=False)
            for child in record.children
            if not child.done()
        )

    def _handle_invoke(self, invoke: Invoke) -> list[Command]:
        logger.info("Ingesting record %s", invoke.id)
        record = self._records[invoke.id]
        assert not record.done()
        assert isinstance(record.invocation.unit, Invocation)
        assert not isinstance(record.invocation.unit.fn, str)
        fn, args, kwargs = (
            record.invocation.unit.fn,
            record.invocation.unit.args,
            record.invocation.unit.kwargs,
        )
        if isgeneratorfunction(fn):
            record.add_coro(
                ResonateCoro(
                    record,
                    fn(
                        record.ctx,
                        *args,
                        **kwargs,
                    ),
                )
            )
            return self._handle_continue(record.id, next_value=None)

        if iscoroutinefunction(fn):
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        asyncio.run,
                        fn(
                            record.ctx,
                            *args,
                            **kwargs,
                        ),
                    ),
                    id=record.id,
                )
            )

        else:
            assert isfunction(fn)
            self._processor.enqueue(
                SQE[Any](
                    thunk=partial(
                        fn,
                        record.ctx,
                        *args,
                        **kwargs,
                    ),
                    id=record.id,
                )
            )

        return []

    def _complete_task(self, id: str) -> None:
        record = self._records[id]
        assert record.has_task()

        assert isinstance(self._store, RemoteStore)
        task = record.get_task()
        self._store.tasks.complete(
            task_id=task.task_id,
            counter=task.counter,
        )
        record.remove_task()

    def _process_rfc(self, record: Record[Any], rfc: RFC) -> list[Command]:  # noqa: PLR0912
        loopbacks: list[Command] = []
        assert isinstance(self._store, RemoteStore)
        child_id: str
        next_child_name = record.next_child_name()
        if isinstance(rfc.unit, Invocation):
            child_id = rfc.opts.id if rfc.opts.id is not None else next_child_name
        elif isinstance(rfc.unit, DurablePromise):
            child_id = rfc.unit.id if rfc.unit.id is not None else next_child_name
        else:
            assert_never(rfc.unit)
        child_record = self._records.get(child_id)
        root = record.root()
        if child_record is not None:
            record.add_child(child_record)
            if child_record.done():
                loopbacks.extend(
                    self._handle_continue(record.id, child_record.safe_result())
                )
            else:
                self._add_to_awaiting_remote(child_id, record.id)
                if self._blocked_only_on_remote(root.id):
                    self._complete_task(root.id)
        else:
            child_record = record.create_child(id=child_id, invocation=rfc.to_rfi())
            self._records[child_id] = child_record
            assert rfc.opts.durable

            data, headers, tags, timeout = self._get_info_from_rfi(rfc.to_rfi())

            assert self._recv

            durable_promise = self._store.promises.create(
                id=child_id,
                ikey=utils.string_to_uuid(child_id),
                strict=False,
                headers=headers,
                data=data,
                timeout=timeout or sys.maxsize,
                tags=tags,
            )
            assert child_id in self._records
            assert not record.done()
            child_record.add_durable_promise(durable_promise)

            if durable_promise.is_completed():
                value = durable_promise.get_value(self._encoder)
                child_record.set_result(value, deduping=True)
                loopbacks.extend(
                    self._handle_continue(record.id, child_record.safe_result())
                )

            else:
                durable_promise, callback = self._store.callbacks.create(
                    id=utils.string_to_uuid(record.id),
                    promise_id=child_id,
                    root_promise_id=root.id,
                    timeout=sys.maxsize,
                    recv=self._recv,
                )
                if durable_promise.is_completed():
                    assert callback is None
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value, deduping=True)
                    loopbacks.extend(
                        self._handle_continue(record.id, child_record.safe_result())
                    )
                else:
                    assert callback is not None
                    self._add_to_awaiting_remote(child_id, record.id)
                    if self._blocked_only_on_remote(root.id):
                        self._complete_task(root.id)

        return loopbacks

    def _process_lfc(self, record: Record[Any], lfc: LFC) -> list[Command]:
        loopbacks: list[Command] = []
        child_id = lfc.opts.id if lfc.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            if child_record.done():
                loopbacks.extend(
                    self._handle_continue(record.id, child_record.safe_result())
                )
            else:
                self._add_to_awaiting_local(child_id, record.id)

        else:
            child_record = record.create_child(id=child_id, invocation=lfc.to_lfi())
            self._records[child_id] = child_record

            if lfc.opts.durable:
                durable_promise = self._store.promises.create(
                    id=child_id,
                    ikey=utils.string_to_uuid(child_id),
                    strict=False,
                    headers=None,
                    data=None,
                    timeout=sys.maxsize,
                    tags=None,
                )
                assert child_id in self._records
                assert not record.done()
                child_record.add_durable_promise(durable_promise)
                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value, deduping=True)
                    loopbacks.extend(
                        self._handle_continue(record.id, child_record.safe_result())
                    )
                else:
                    loopbacks.append(Invoke(child_id))
                    self._add_to_awaiting_local(child_id, record.id)

            else:
                loopbacks.append(Invoke(child_id))
                self._add_to_awaiting_local(child_id, record.id)
        return loopbacks

    def _process_rfi(self, record: Record[Any], rfi: RFI) -> list[Command]:
        loopbacks: list[Command] = []
        child_id: str
        next_child_name = record.next_child_name()
        if isinstance(rfi.unit, Invocation):
            child_id = rfi.opts.id if rfi.opts.id is not None else next_child_name
        elif isinstance(rfi.unit, DurablePromise):
            child_id = rfi.unit.id if rfi.unit.id is not None else next_child_name
        else:
            assert_never(rfi.unit)
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            loopbacks.extend(self._handle_continue(record.id, Ok(child_record.promise)))
        else:
            child_record = record.create_child(id=child_id, invocation=rfi)
            self._records[child_id] = child_record
            assert rfi.opts.durable

            data, headers, tags, timeout = self._get_info_from_rfi(rfi)

            durable_promise = self._store.promises.create(
                id=child_id,
                ikey=utils.string_to_uuid(child_id),
                strict=False,
                headers=headers,
                data=data,
                timeout=timeout or sys.maxsize,
                tags=tags,
            )
            assert child_id in self._records
            assert not record.done()
            child_record.add_durable_promise(durable_promise)

            if durable_promise.is_completed():
                value = durable_promise.get_value(self._encoder)
                child_record.set_result(value, deduping=True)
            loopbacks.extend(self._handle_continue(record.id, Ok(child_record.promise)))

        return loopbacks

    def _process_lfi(self, record: Record[Any], lfi: LFI) -> list[Command]:
        loopbacks: list[Command] = []
        child_id = lfi.opts.id if lfi.opts.id is not None else record.next_child_name()
        child_record = self._records.get(child_id)
        if child_record is not None:
            record.add_child(child_record)
            loopbacks.extend(self._handle_continue(record.id, Ok(child_record.promise)))
        else:
            child_record = record.create_child(id=child_id, invocation=lfi)
            self._records[child_id] = child_record

            if lfi.opts.durable:
                durable_promise = self._store.promises.create(
                    id=child_id,
                    ikey=utils.string_to_uuid(child_id),
                    strict=False,
                    headers=None,
                    data=None,
                    timeout=sys.maxsize,
                    tags=None,
                )
                assert child_id in self._records
                assert not record.done()

                child_record.add_durable_promise(durable_promise)

                if durable_promise.is_completed():
                    value = durable_promise.get_value(self._encoder)
                    child_record.set_result(value, deduping=True)
                else:
                    loopbacks.append(Invoke(child_id))
                loopbacks.extend(
                    self._handle_continue(record.id, Ok(child_record.promise))
                )
            else:
                loopbacks.append(Invoke(child_id))
                loopbacks.extend(
                    self._handle_continue(record.id, Ok(child_record.promise))
                )

        return loopbacks

    def _handle_complete(
        self,
        complete: Complete,
    ) -> list[Command]:
        record = self._records[complete.id]
        value = complete.result
        if isinstance(record, str):
            record = self._records[record]
        if record.should_retry(value):
            record.increate_attempt()
            self._delay_queue.enqueue(Invoke(record.id), record.next_retry_delay())
            return []

        if record.invocation.opts.durable:
            durable_promise: DurablePromiseRecord
            if isinstance(value, Ok):
                durable_promise = self._store.promises.resolve(
                    id=record.id,
                    ikey=utils.string_to_uuid(record.id),
                    strict=False,
                    headers=None,
                    data=self._encoder.encode(value.unwrap()),
                )

            elif isinstance(value, Err):
                durable_promise = self._store.promises.reject(
                    id=record.id,
                    ikey=utils.string_to_uuid(record.id),
                    strict=False,
                    headers=None,
                    data=self._encoder.encode(value.err()),
                )
            else:
                assert_never(value)

            value = durable_promise.get_value(self._encoder)
            assert not record.done()

            if record.has_task():
                self._complete_task(record.id)

            record.set_result(value, deduping=False)
            resume_cmds = self._unblock_awaiting_local(record.id)

            root = record.root()
            if root != record and self._blocked_only_on_remote(root.id):
                self._complete_task(root.id)
            return resume_cmds

        record.set_result(value, deduping=False)
        return self._unblock_awaiting_local(record.id)

    def _get_info_from_rfi(self, rfi: RFI) -> tuple[Data, Headers, Tags, int | None]:
        data: Data
        tags: Tags
        headers: Headers
        timeout: int | None = None
        if isinstance(rfi.unit, DurablePromise):
            data = rfi.unit.data
            tags = rfi.unit.tags
            headers = rfi.unit.headers
            timeout = rfi.unit.timeout
        elif isinstance(rfi.unit, Invocation):
            func: str
            if isinstance(rfi.unit.fn, str):
                func = rfi.unit.fn
            else:
                assert isfunction(rfi.unit.fn)
                registered_fn_name = self._registry.get_from_value(rfi.unit.fn)
                assert registered_fn_name is not None
                func = registered_fn_name
            data = self._encoder.encode(
                {
                    "func": func,
                    "args": rfi.unit.args,
                    "kwargs": rfi.unit.kwargs,
                }
            )
            tags = {"resonate:invoke": rfi.opts.send_to or "default"}
            headers = None
        else:
            assert_never(rfi.unit)
        return (data, headers, tags, timeout)

    def _stop(self) -> None:
        self._cmd_queue.put(None)
        self._scheduler_thread.join()

    def stop(self) -> None:
        self._delay_queue.stop()
        self._stop()
