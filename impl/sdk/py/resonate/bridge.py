from __future__ import annotations

import queue
import threading
import time
from concurrent.futures import Future
from typing import TYPE_CHECKING

from resonate.conventions import Base
from resonate.delay_q import DelayQ
from resonate.errors import ResonateShutdownError
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreateSubscriptionReq,
    Delayed,
    Function,
    Invoke,
    Listen,
    Network,
    Notify,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Retry,
    Return,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Ko, Ok
from resonate.models.task import Task
from resonate.options import Options
from resonate.processor import Processor
from resonate.scheduler import Done, Info, More, Scheduler
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.convention import Convention
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store
    from resonate.registry import Registry
    from resonate.resonate import Context


class Bridge:
    def __init__(
        self,
        ctx: Callable[[str, str, Info], Context],
        pid: str,
        ttl: int,
        opts: Options,
        unicast: str,
        anycast: str,
        registry: Registry,
        store: Store,
        message_source: MessageSource,
    ) -> None:
        self._cq = queue.Queue[Command | tuple[Command, Future] | None]()
        self._promise_id_to_task: dict[str, Task] = {}

        self._ctx = ctx
        self._pid = pid
        self._ttl = ttl
        self._opts = opts
        self._unicast = unicast
        self._anycast = unicast

        self._registry = registry
        self._store = store
        self._message_source = message_source
        self._delay_q = DelayQ[Function | Retry]()

        self._scheduler = Scheduler(
            self._ctx,
            self._pid,
            self._unicast,
            self._anycast,
        )
        self._processor = Processor()

        self._bridge_thread = threading.Thread(target=self._process_cq, name="bridge", daemon=True)
        self._message_source_thread = threading.Thread(target=self._process_msgs, name="message-source", daemon=True)

        self._delay_q_thread = threading.Thread(target=self._process_delayed_events, name="delay-q", daemon=True)
        self._delay_q_condition = threading.Condition()

        self._heartbeat_thread = threading.Thread(target=self._heartbeat, name="heartbeat", daemon=True)
        self._heartbeat_active = threading.Event()

        self._shutdown = threading.Event()

    def run(self, conv: Convention, func: Callable, args: tuple, kwargs: dict, opts: Options, future: Future) -> DurablePromise:
        encoder = opts.get_encoder()

        headers, data = encoder.encode(conv.data)
        promise, task = self._store.promises.create_with_task(
            id=conv.id,
            ikey=conv.idempotency_key,
            timeout=int((time.time() + conv.timeout) * 1000),
            headers=headers,
            data=data,
            tags=conv.tags,
            pid=self._pid,
            ttl=self._ttl * 1000,
        )

        if promise.completed:
            assert not task
            match promise.result(encoder):
                case Ok(v):
                    future.set_result(v)
                case Ko(e):
                    future.set_exception(e)
        elif task is not None:
            self._promise_id_to_task[promise.id] = task
            self.start_heartbeat()
            self._cq.put_nowait((Invoke(conv.id, conv, promise.abs_timeout, func, args, kwargs, opts, promise), future))
        else:
            self._cq.put_nowait((Listen(promise.id), future))

        return promise

    def rpc(self, conv: Convention, opts: Options, future: Future) -> DurablePromise:
        encoder = opts.get_encoder()

        headers, data = encoder.encode(conv.data)
        promise = self._store.promises.create(
            id=conv.id,
            ikey=conv.idempotency_key,
            timeout=int((time.time() + conv.timeout) * 1000),
            headers=headers,
            data=data,
            tags=conv.tags,
        )

        if promise.completed:
            match promise.result(encoder):
                case Ok(v):
                    future.set_result(v)
                case Ko(e):
                    future.set_exception(e)
        else:
            self._cq.put_nowait((Listen(promise.id), future))

        return promise

    def get(self, id: str, opts: Options, future: Future) -> DurablePromise:
        promise = self._store.promises.get(id=id)

        if promise.completed:
            match promise.result(opts.get_encoder()):
                case Ok(v):
                    future.set_result(v)
                case Ko(e):
                    future.set_exception(e)
        else:
            self._cq.put_nowait((Listen(id), future))

        return promise

    def start(self) -> None:
        self._processor.start()

        if not self._message_source_thread.is_alive():
            self._message_source.start()
            self._message_source_thread.start()

        if not self._bridge_thread.is_alive():
            self._bridge_thread.start()

        if not self._heartbeat_thread.is_alive():
            self._heartbeat_thread.start()

        if not self._delay_q_thread.is_alive():
            self._delay_q_thread.start()

    def stop(self) -> None:
        """Stop internal components and threads. Intended for use only within the resonate class."""
        self._stop_no_join()
        if self._bridge_thread.is_alive():
            self._bridge_thread.join()
        if self._message_source_thread.is_alive():
            self._message_source_thread.join()
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()

    def _stop_no_join(self) -> None:
        """Stop internal components and threads. Does not join the threads, to be able to call it from the bridge itself."""
        self._processor.stop()
        self._message_source.stop()
        self._cq.put_nowait(None)
        self._heartbeat_active.clear()
        self._shutdown.set()

    @exit_on_exception
    def _process_cq(self) -> None:
        while item := self._cq.get():
            cmd, future = item if isinstance(item, tuple) else (item, None)
            match self._scheduler.step(cmd, future):
                case More(reqs):
                    for req in reqs:
                        match req:
                            case Network(id, cid, n_req):
                                try:
                                    cmd = self._handle_network_request(id, cid, n_req)
                                    self._cq.put_nowait(cmd)
                                except Exception as e:
                                    err = ResonateShutdownError(mesg="An unexpected store error has occurred, shutting down")
                                    err.__cause__ = e  # bind original error

                                    # bypass the cq and shutdown right away
                                    self._scheduler.shutdown(err)
                                    self._stop_no_join()
                                    return

                            case Function(id, cid, func):
                                self._processor.enqueue(func, lambda r, id=id, cid=cid: self._cq.put_nowait(Return(id, cid, r)))
                            case Delayed() as item:
                                self._handle_delay(item)

                case Done(reqs):
                    cid = cmd.cid
                    task = self._promise_id_to_task.get(cid, None)
                    match reqs:
                        case [Network(_, cid, CreateSubscriptionReq(id, promise_id, timeout, recv))]:
                            # Current implementation returns a single CreateSubscriptionReq in the list
                            # if we get more than one element they are all CreateCallbackReq

                            durable_promise, callback = self._store.promises.subscribe(
                                id=id,
                                promise_id=promise_id,
                                timeout=timeout,
                                recv=recv,
                            )
                            assert durable_promise.id == cid
                            assert durable_promise.completed or callback

                            if durable_promise.completed:
                                self._cq.put_nowait(Notify(cid, durable_promise))

                        case _:
                            got_resume = False
                            for req in reqs:
                                assert isinstance(req, Network)
                                assert isinstance(req.req, CreateCallbackReq)

                                res_cmd = self._handle_network_request(req.id, req.cid, req.req)
                                if isinstance(res_cmd, Resume):
                                    # if we get a resume here we can bail the rest of the callback requests
                                    # and continue with the rest of the work in the cq.
                                    self._cq.put_nowait(res_cmd)
                                    got_resume = True
                                    break

                            if got_resume:
                                continue

                            if task is not None:
                                self._store.tasks.complete(id=task.id, counter=task.counter)

    @exit_on_exception
    def _process_msgs(self) -> None:
        encoder = self._opts.get_encoder()

        def _invoke(root: DurablePromise) -> Invoke:
            data = encoder.decode(root.param.to_tuple())
            assert isinstance(data, dict)

            assert "func" in data
            assert "version" in data
            assert isinstance(data["func"], str)
            assert isinstance(data["version"], int)

            _, func, version = self._registry.get(data["func"], data["version"])
            return Invoke(
                root.id,
                Base(
                    root.id,
                    root.rel_timeout,
                    root.ikey_for_create,
                    root.param.data,
                    root.tags,
                ),
                root.abs_timeout,
                func,
                data.get("args", ()),
                data.get("kwargs", {}),
                Options(version=version),
                root,
            )

        while msg := self._message_source.next():
            match msg:
                case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                    task = Task(id, counter, self._store)
                    root, _ = self._store.tasks.claim(id=task.id, counter=task.counter, pid=self._pid, ttl=self._ttl * 1000)
                    self.start_heartbeat()
                    self._promise_id_to_task[root.id] = task
                    self._cq.put_nowait(_invoke(root))

                case {"type": "resume", "task": {"id": id, "counter": counter}}:
                    task = Task(id, counter, self._store)
                    root, leaf = self._store.tasks.claim(id=task.id, counter=task.counter, pid=self._pid, ttl=self._ttl * 1000)
                    self.start_heartbeat()
                    assert leaf is not None, "leaf must not be None"
                    cmd = Resume(
                        id=leaf.id,
                        cid=root.id,
                        promise=leaf,
                        invoke=_invoke(root),
                    )
                    self._promise_id_to_task[root.id] = task
                    self._cq.put_nowait(cmd)

                case {"type": "notify", "promise": promise}:
                    durable_promise = DurablePromise.from_dict(self._store, promise)
                    self._cq.put_nowait(Notify(durable_promise.id, durable_promise))

    @exit_on_exception
    def _process_delayed_events(self) -> None:
        while not self._shutdown.is_set():
            with self._delay_q_condition:
                while not self._delay_q.empty():
                    if self._shutdown.is_set():
                        self._delay_q_condition.release()
                        return

                    self._delay_q_condition.wait()

                now = time.time()
                events, next_time = self._delay_q.get(now)

                # Release the lock so more event can be added to the delay queue while
                # the ones just pulled get processed.
                self._delay_q_condition.release()

                for item in events:
                    match item:
                        case Function(id, cid, func):
                            self._processor.enqueue(func, lambda r, id=id, cid=cid: self._cq.put_nowait(Return(id, cid, r)))
                        case retry:
                            self._cq.put_nowait(retry)

                if self._shutdown.is_set():
                    return

                timeout = max(0.0, next_time - now)
                self._delay_q_condition.acquire()
                self._delay_q_condition.wait(timeout=timeout)

    def start_heartbeat(self) -> None:
        self._heartbeat_active.set()

    @exit_on_exception
    def _heartbeat(self) -> None:
        while not self._shutdown.is_set():
            # If this timeout don't execute the heartbeat
            if self._heartbeat_active.wait(0.3):
                heartbeated = self._store.tasks.heartbeat(pid=self._pid)
                if heartbeated == 0:
                    self._heartbeat_active.clear()
                else:
                    self._shutdown.wait(self._ttl * 0.5)

    def _handle_delay(self, delay: Delayed) -> None:
        """Add a command to the delay queue.

        Uses a threading.condition to synchronize access to the underlaying delay_q.
        """
        with self._delay_q_condition:
            self._delay_q.add(delay.item, time.time() + delay.delay)
            self._delay_q_condition.notify()

    def _handle_network_request(self, cmd_id: str, cid: str, req: CreatePromiseReq | ResolvePromiseReq | RejectPromiseReq | CancelPromiseReq | CreateCallbackReq) -> Command:
        match req:
            case CreatePromiseReq(id, timeout, ikey, strict, headers, data, tags):
                promise = self._store.promises.create(
                    id=id,
                    timeout=timeout,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                    tags=tags,
                )
                return Receive(cmd_id, cid, CreatePromiseRes(promise))

            case ResolvePromiseReq(id, ikey, strict, headers, data):
                promise = self._store.promises.resolve(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cmd_id, cid, ResolvePromiseRes(promise))

            case RejectPromiseReq(id, ikey, strict, headers, data):
                promise = self._store.promises.reject(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cmd_id, cid, RejectPromiseRes(promise))

            case CancelPromiseReq(id, ikey, strict, headers, data):
                promise = self._store.promises.cancel(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cmd_id, cid, CancelPromiseRes(promise))

            case CreateCallbackReq(promise_id, root_promise_id, timeout, recv):
                promise, callback = self._store.promises.callback(
                    promise_id=promise_id,
                    root_promise_id=root_promise_id,
                    timeout=timeout,
                    recv=recv,
                )

                if promise.completed:
                    return Resume(cmd_id, cid, promise)

                return Receive(cmd_id, cid, CreateCallbackRes(promise, callback))

            case _:
                raise NotImplementedError
