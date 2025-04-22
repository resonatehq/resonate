from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, Any

from resonate.delay_queue import DelayQ
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
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
from resonate.models.options import Options
from resonate.models.result import Ko, Ok, Result
from resonate.models.task import Task
from resonate.scheduler import Done, Info, More, Scheduler

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from resonate.models.commands import Command
    from resonate.models.message import Mesg
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store
    from resonate.registry import Registry
    from resonate.resonate import Context


class Bridge:
    def __init__(
        self,
        ctx: Callable[[str, Info], Context],
        store: Store,
        message_source: MessageSource,
        registry: Registry,
        pid: str,
        ttl: int,
        unicast: str,
        anycast: str,
    ) -> None:
        self._store = store
        self._message_src = message_source
        self._registry = registry
        self._cq: queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None] = queue.Queue()
        self._mq: queue.Queue[Mesg | None] = queue.Queue()
        self._unicast = unicast
        self._anycast = unicast

        self._pid = pid
        self._ttl = ttl
        self._scheduler = Scheduler(
            ctx=ctx,
            pid=self._pid,
            unicast=unicast,
            anycast=anycast,
        )
        self._promise_id_to_task: dict[str, Task] = {}

        self._messages_thread = threading.Thread(target=self._process_msgs, name="bridge_msg_processor", daemon=True)
        self._bridge_thread = threading.Thread(target=self._process_cq, name="bridge_main_thread", daemon=True)

        self._heartbeat_thread = threading.Thread(target=self._heartbeat, name="bridge_hearthbeat", daemon=True)
        self._heartbeat_active = threading.Event()
        self._shutdown = threading.Event()

        self._delay_queue = DelayQ[Function | Retry]()
        self._delay_condition = threading.Condition()
        self._delay_thread = threading.Thread(target=self._process_delayed_events, name="delay_thread", daemon=True)

    def invoke(self, cmd: Invoke, futures: tuple[Future, Future]) -> DurablePromise:
        assert cmd.opts.version is not None, "Version must be set"
        assert cmd.opts.version > 0, "Version must be greater than zero"

        promise: DurablePromise
        task: Task | None = None

        if cmd.func is None:
            # RPC case

            promise = self._store.promises.create(
                id=cmd.id,
                ikey=cmd.id,
                timeout=cmd.opts.timeout,
                data={"func": cmd.name, "args": cmd.args, "kwargs": cmd.kwargs, "version": cmd.opts.version},
                tags={
                    **cmd.opts.tags,
                    "resonate:invoke": cmd.opts.send_to,
                    "resonate:scope": "global",
                },
            )
        else:
            # Invocation case
            promise, task = self._store.promises.create_with_task(
                id=cmd.id,
                ikey=cmd.id,
                timeout=cmd.opts.timeout,
                pid=self._pid,
                ttl=self._ttl * 1000,
                data={"func": cmd.name, "args": cmd.args, "kwargs": cmd.kwargs, "version": cmd.opts.version},
                tags={
                    **cmd.opts.tags,
                    "resonate:invoke": cmd.opts.send_to,
                    "resonate:scope": "global",
                },
            )

        futures[0].set_result(promise)

        # TODO(avillega): What happens if promise is completed?
        if task is not None:
            self._promise_id_to_task[promise.id] = task
            self.start_heartbeat()
            self._cq.put_nowait((cmd, futures))
        else:
            self._cq.put_nowait((Listen(promise.id), futures))

        return promise

    def listen(self, cmd: Listen, futures: tuple[Future, Future]) -> None:
        self._cq.put_nowait((cmd, futures))

    def start(self) -> None:
        if not self._messages_thread.is_alive():
            self._message_src.start(MesgQueueAdapter(self._mq), self._pid)
            self._messages_thread.start()

        if not self._bridge_thread.is_alive():
            self._bridge_thread.start()

        if not self._heartbeat_thread.is_alive():
            self._heartbeat_thread.start()

        if not self._delay_thread.is_alive():
            self._delay_thread.start()

    def stop(self) -> None:
        self._message_src.stop()
        self._cq.put_nowait(None)
        self._mq.put_nowait(None)
        self._heartbeat_active.clear()
        self._shutdown.set()
        if self._bridge_thread.is_alive():
            self._bridge_thread.join()
        if self._messages_thread.is_alive():
            self._messages_thread.join()
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()

    def _process_cq(self) -> None:
        while True:
            item = self._cq.get()
            cid: str
            match item:
                case None:
                    # None signals to stop processing
                    return
                case (cmd, futures):
                    cid = cmd.cid
                    action = self._scheduler.step(cmd, futures)
                case cmd:
                    cid = cmd.cid
                    action = self._scheduler.step(cmd)

            match action:
                case More(reqs):
                    for req in reqs:
                        match req:
                            case Network(id, cid, n_req):
                                # TODO(avillega): probaly put this in a try/except and handle errors better
                                self._cq.put_nowait(self._handle_network_request(id, cid, n_req))
                            case Function(id, cid, func):
                                self._cq.put_nowait(Return(id, cid, self._handle_function(func)))
                            case Delayed() as item:
                                self._handle_delay(item)

                case Done(reqs):
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

    def _process_msgs(self) -> None:
        def _invoke(root: DurablePromise) -> Invoke:
            version = root.param.data.get("version")
            return Invoke(
                root.id,
                root.param.data["func"],
                self._registry.get(root.param.data["func"])[0],
                root.param.data["args"],
                root.param.data["kwargs"],
                Options().merge(version=version),
            )

        while True:
            msg = self._mq.get()
            match msg:
                case None:
                    # None signals to stop
                    return

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

                case {"type": "notify", "promise": _promise}:
                    durable_promise = DurablePromise.from_dict(self._store, _promise)
                    self._cq.put_nowait(Notify(durable_promise.id, durable_promise))

    def _process_delayed_events(self) -> None:
        while not self._shutdown.is_set():
            with self._delay_condition:
                while not self._delay_queue.empty():
                    if self._shutdown.is_set():
                        self._delay_condition.release()
                        return

                    self._delay_condition.wait()

                now = time.time()
                events, next_time = self._delay_queue.get(now)

                # Release the lock so more event can be added to the delay queue while
                # the ones just pulled get processed.
                self._delay_condition.release()

                for item in events:
                    match item:
                        case Function(id, cid, func):
                            self._cq.put_nowait(Return(id, cid, self._handle_function(func)))
                        case Retry() as retry:
                            self._cq.put_nowait(retry)
                        case _:
                            raise NotImplementedError  # Unreachble

                if self._shutdown.is_set():
                    return

                timeout = max(0.0, next_time - now)
                self._delay_condition.acquire()
                self._delay_condition.wait(timeout=timeout)

    def start_heartbeat(self) -> None:
        self._heartbeat_active.set()

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
        with self._delay_condition:
            self._delay_queue.add(delay.item, time.time() + delay.delay)
            self._delay_condition.notify()

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

            case CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv):
                promise, callback = self._store.promises.callback(
                    id=id,
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

    def _handle_function(self, func: Callable[[], Any]) -> Result:
        try:
            r = func()
            return Ok(r)
        except Exception as e:
            return Ko(e)


class MesgQueueAdapter:
    def __init__(self, mq: queue.Queue[Mesg | None]) -> None:
        self.mq = mq

    def enqueue(self, mesg: Mesg) -> None:
        self.mq.put(mesg)
