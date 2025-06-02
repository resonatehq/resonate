from __future__ import annotations

import logging
import urllib.parse
from typing import TYPE_CHECKING, Any

from resonate import Context
from resonate.conventions import Base
from resonate.encoders import HeaderEncoder, JsonEncoder, JsonPickleEncoder, PairEncoder
from resonate.errors import ResonateStoreError
from resonate.loggers import DSTLogger
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    ClaimTaskReq,
    ClaimTaskRes,
    Command,
    CompleteTaskReq,
    CompleteTaskRes,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    CreateSubscriptionReq,
    CreateSubscriptionRes,
    Delayed,
    Function,
    HeartbeatTasksReq,
    HeartbeatTasksRes,
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
    Return,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Ko, Ok, Result
from resonate.models.task import Task
from resonate.options import Options
from resonate.scheduler import Done, More, Scheduler
from resonate.stores import LocalStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from random import Random

    from resonate.clocks import StepClock
    from resonate.dependencies import Dependencies
    from resonate.models.clock import Clock
    from resonate.registry import Registry

UUID = 0


class Message:
    def __init__(self, send: str, recv: str, data: Any, time: float) -> None:
        global UUID  # noqa: PLW0603
        UUID += 1
        self.uuid = UUID
        self.send = send
        self.recv = recv
        self.data = data
        self.time = time

    def __repr__(self) -> str:
        return f"Message(from={self.send}, to={self.recv}, payload={self.data}, time={self.time})"


class Simulator:
    def __init__(self, r: Random, clock: StepClock, drop_rate: float = 0.15) -> None:
        self.r = r
        self.clock = clock
        self.time = 0.0
        self.drop_rate = drop_rate
        self.logs = []
        self.buffer: list[Message] = []
        self.components: list[Component] = []

    def add_component(self, component: Component) -> None:
        self.components.append(component)

    def send_msg(self, addr: str, data: Any) -> Message:
        msg = Message("E", addr, data, self.time)
        self.buffer.append(msg)
        return msg

    def step(self) -> None:
        # set the clock
        self.clock.step(self.time)

        # TODO(dfarr): determine a better way to drop components, for now keep
        # a minimum of two components, the server (which is not droppable) and
        # one worker
        for comp in self.components:
            if comp.droppable() and len(self.components) > 2 and self.r.random() < 0.0001:
                self.components.remove(comp)

        recv: list[Message] = []
        outgoing: dict[Component, list[Message]] = {c: [] for c in self.components}

        for send in self.buffer:
            # TODO(dfarr): all tasks should be droppable, but for now only drop messages from the message source
            if isinstance(send.data, dict) and send.data.get("type") in ("invoke", "resume", "notify") and self.r.random() < self.drop_rate:
                self.buffer.remove(send)
                continue

            comps = []
            for comp in self.components:
                preferred, alternate = comp.match(send.recv)
                assert not (preferred and alternate)

                if preferred:
                    comps = [comp]
                    break
                if alternate:
                    comps.append(comp)

            assert comps or send.recv.startswith("sim://uni@")

            # send to chosen component if found
            if comps:
                outgoing[self.r.choice(comps)].append(send)
                self.buffer.remove(send)

        for comp, send in outgoing.items():
            self.r.shuffle(send)

            for msg in send:
                self.logs.append({"type": "recv", "uuid": msg.uuid, "source": msg.send, "target": comp.uni, "data": msg.data})

            for msg in comp.step(self.time, send):
                self.logs.append({"type": "send", "uuid": msg.uuid, "source": comp.uni, "target": msg.recv, "data": msg.data})
                recv.append(msg)

        # Add outgoing messages to the buffer
        self.buffer.extend(recv)

        # TODO(dfarr): determine appropriate seconds per step
        self.time += 1

    def freeze(self) -> str:
        return ":".join(component.freeze() for component in self.components)


class Component:
    def __init__(self, r: Random, uni: str, any: str) -> None:
        self.r = r
        self.uni = uni
        self.any = any
        self.time = 0.0
        self.msgcount = 0
        self.outgoing = []
        self.awaiting = {}
        self.deferred = []

    def step(self, time: float, messages: list[Message]) -> list[Message]:
        self.time = time
        self.outgoing = []
        self.handle_deferred()
        self.handle_response(messages)
        self.handle_messages(messages)
        self.next()
        return self.outgoing

    def next(self) -> None:
        pass

    def droppable(self) -> bool:
        """Remove the component from the simulation."""
        return False

    def send_msg(self, addr: str, data: Any) -> Message:
        """Send a message to another component."""
        msg = Message(self.uni, addr, data, self.time)
        self.outgoing.append(msg)
        return msg

    def send_req(self, addr: str, data: Any, callback: Callable[[Message], None]) -> Message:
        """Send a request and register a callback for handling the response."""
        self.msgcount += 1
        self.awaiting[self.msgcount] = callback
        msg = Message(self.uni, addr, {"type": "req", "uuid": self.msgcount, "data": data}, self.time)
        self.outgoing.append(msg)
        return msg

    def defer(self, callback: Callable[[], None], timeout: float = 0) -> None:
        """Defers a callback to be called after a certain time."""
        self.deferred.append((callback, self.time + timeout))

    def match(self, addr: str) -> tuple[bool, bool]:
        parsed = urllib.parse.urlparse(addr)
        assert parsed.scheme == "sim"

        uni = urllib.parse.urlparse(self.uni)
        assert uni.scheme == "sim"

        any = urllib.parse.urlparse(self.any)
        assert any.scheme == "sim"

        if parsed.username == "uni" and parsed.hostname == uni.hostname and parsed.path == uni.path:
            return True, False
        if parsed.username == "any" and parsed.hostname == any.hostname and parsed.path == any.path:
            return True, False
        if parsed.username == "any" and parsed.hostname == any.hostname:
            return False, True

        return False, False

    def handle_deferred(self) -> None:
        for callback, time in self.deferred:
            if self.time >= time:
                self.deferred.remove((callback, time))
                callback()

    def handle_response(self, messages: list[Message]) -> None:
        for message in messages:
            if isinstance(message.data, dict) and message.data.get("type") == "res":
                uuid = message.data.get("uuid")
                if uuid in self.awaiting:
                    callback = self.awaiting.pop(uuid)
                    callback(message)

    def handle_messages(self, messages: list[Message]) -> None:
        for message in messages:
            if not (isinstance(message.data, dict) and message.data.get("type") == "res"):
                self.on_message(message)

    def on_message(self, msg: Message) -> None:
        raise NotImplementedError

    def freeze(self) -> str:
        raise NotImplementedError


class Server(Component):
    def __init__(self, r: Random, uni: str, any: str, clock: Clock) -> None:
        super().__init__(r, uni, any)
        self.store = LocalStore(clock=clock)
        self.store.add_target("default", "sim://any@default")

    def on_message(self, msg: Message) -> None:
        match msg.data.get("data"):
            case CreatePromiseReq(id, timeout, ikey, strict, headers, data, tags):
                promise = self.store.promises.create(
                    id=id,
                    timeout=timeout,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                    tags=tags,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": CreatePromiseRes(promise)})

            case CreatePromiseWithTaskReq(id, timeout, pid, ttl, ikey, strict, headers, data, tags):
                promise, task = self.store.promises.create_with_task(
                    id=id,
                    timeout=timeout,
                    pid=pid,
                    ttl=ttl,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                    tags=tags,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": CreatePromiseWithTaskRes(promise, task)})

            case ResolvePromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.resolve(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": ResolvePromiseRes(promise)})

            case RejectPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.reject(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": RejectPromiseRes(promise)})

            case CancelPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.cancel(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": CancelPromiseRes(promise)})

            case CreateCallbackReq(promise_id, root_promise_id, timeout, recv):
                promise, callback = self.store.promises.callback(
                    promise_id=promise_id,
                    root_promise_id=root_promise_id,
                    timeout=timeout,
                    recv=recv,
                )
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": CreateCallbackRes(promise, callback)})

            case CreateSubscriptionReq(id, promise_id, timeout, recv):
                try:
                    promise, callback = self.store.promises.subscribe(
                        id=id,
                        promise_id=promise_id,
                        timeout=timeout,
                        recv=recv,
                    )
                    result = Ok(CreateSubscriptionRes(promise, callback))
                except ResonateStoreError as e:
                    result = Ko(e)

                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": result})

            case ClaimTaskReq(id, counter, pid, ttl):
                try:
                    root, leaf = self.store.tasks.claim(
                        id=id,
                        counter=counter,
                        pid=pid,
                        ttl=ttl,
                    )

                    result = Ok(ClaimTaskRes(root, leaf, Task(id, counter, self.store)))
                except ResonateStoreError as e:
                    result = Ko(e)

                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": result})

            case CompleteTaskReq(id, counter):
                try:
                    self.store.tasks.complete(id=id, counter=counter)
                    result = Ok(CompleteTaskRes())
                except ResonateStoreError as e:
                    result = Ko(e)

                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": result})

            case HeartbeatTasksReq(pid):
                affected = self.store.tasks.heartbeat(pid=pid)
                self.send_msg(msg.send, {"type": "res", "uuid": msg.data.get("uuid"), "data": HeartbeatTasksRes(affected)})

            case _:
                raise NotImplementedError

    def next(self) -> None:
        # step the store
        step = self.store.step()
        next: bool | None = None

        while True:
            try:
                addr, mesg = step.send(next)
                self.send_msg(addr, mesg)

                # TODO(dfarr): we could randomly not send the message and
                # return false to the generator, this simulates a different
                # path than dropping messages (which we also do)
                next = True
            except StopIteration:
                break

    def freeze(self) -> str:
        return "server"


class Worker(Component):
    def __init__(self, r: Random, uni: str, any: str, clock: Clock, registry: Registry, dependencies: Dependencies, log_level: int = logging.INFO) -> None:
        super().__init__(r, uni, any)
        self.clock = clock
        self.registry = registry
        self.dependencies = dependencies
        self.log_level = log_level
        self.commands: list[tuple[Command, Task | None]] = []
        self.tasks: dict[str, Task] = {}
        self.last_heartbeat = 0.0

        self.encoder = PairEncoder(
            HeaderEncoder("resonate:format-py", JsonPickleEncoder()),
            JsonEncoder(),
        )
        self.scheduler = Scheduler(
            lambda id, cid, info: Context(id, cid, info, self.registry, self.dependencies, DSTLogger(cid, id, clock, level=log_level)),
            pid=self.uni,
            unicast=self.uni,
            anycast=self.any,
        )

    def on_message(self, msg: Message) -> None:
        match msg.data:
            case Invoke(id, conv):
                assert id == conv.id
                headers, data = self.encoder.encode(conv.data)

                self.send_req(
                    "sim://uni@server",
                    CreatePromiseWithTaskReq(
                        id=id,
                        timeout=int((self.time + conv.timeout) * 1000),
                        ikey=conv.idempotency_key,
                        headers=headers,
                        data=data,
                        tags=conv.tags,
                        pid=self.scheduler.pid,
                        ttl=self.r.randint(0, 30000),  # TODO(dfarr): make this configurable
                    ),
                    lambda res: self._create_promise_with_task(res.data.get("data")),
                )

            case Listen(id):
                self.commands.append((Listen(id), None))

            case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    "sim://uni@server",
                    ClaimTaskReq(id, counter, self.scheduler.pid, self.r.randint(0, 30000)),
                    lambda res: self._invoke_message(res.data.get("data")),
                )

            case {"type": "resume", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    "sim://uni@server",
                    ClaimTaskReq(id, counter, self.scheduler.pid, self.r.randint(0, 30000)),
                    lambda res: self._resume_message(res.data.get("data")),
                )

            case {"type": "notify", "promise": promise}:
                # store instance does not matter
                promise = DurablePromise.from_dict(LocalStore(), promise)
                assert promise.completed
                self.commands.append((Notify(promise.id, promise), None))

            case _:
                raise NotImplementedError

    def next(self) -> None:
        self.r.shuffle(self.commands)

        for c, t in self.commands:
            task = self.tasks.get(c.cid)
            assert task or t or isinstance(c, (Listen, Notify))

            # Ignore the command if we already have a task with the same id and
            # greater or equal counter.
            if task and t and task.id == t.id and task.counter >= t.counter:
                continue

            if t:
                self.tasks[c.cid] = t

            self._next(c)

        # clear commands
        self.commands.clear()

        # heartbeat every 15s, the midpoint of the random ttl interval [0, 30s]
        if self.time - self.last_heartbeat >= 15:
            self.send_req("sim://uni@server", HeartbeatTasksReq(self.scheduler.pid), lambda _: None)
            self.last_heartbeat = self.time

    def _next(self, cmd: Command) -> None:
        suspends = 0
        if (computation := self.scheduler.computations.get(cmd.cid)) and (node := computation.graph.find(lambda n: n.id == cmd.id)):
            suspends = len(node.value.func.suspends)

        match self.scheduler.step(cmd):
            case More(reqs):
                computation = self.scheduler.computations[cmd.cid]
                assert not computation.runnable()
                assert not computation.suspendable()

                # The number of actions (create promise, complete promise, run
                # a function) that can be performed is equal to the number of
                # sub computations suspended by the command plus one.
                # -> all suspended sub computations may take a step
                # -> + a new sub computation may take a step
                assert len(reqs) <= suspends + 1

                for req in reqs:
                    match req:
                        case Function(id, cid, func) | Delayed(Function(id, cid, func)):
                            try:
                                r = Ok(func())
                            except Exception as e:
                                r = Ko(e)

                            delay = req.delay if isinstance(req, Delayed) else 0
                            self.defer(lambda id=id, cid=cid, r=r: self.commands.append((Return(id, cid, r), None)), delay)

                        case Network(id, cid, req):
                            self.send_req("sim://uni@server", req, lambda res, id=id, cid=cid: self.commands.append((Receive(id, cid, res.data.get("data")), None)))

            case Done([]):
                computation = self.scheduler.computations[cmd.cid]
                assert not computation.runnable()
                assert computation.suspendable()
                assert cmd.cid in self.tasks or isinstance(cmd, (Listen, Notify))

                if not isinstance(cmd, (Listen, Notify)):
                    task = self.tasks[cmd.cid]
                    self.send_req(
                        "sim://uni@server",
                        CompleteTaskReq(task.id, task.counter),
                        lambda _: None,
                    )

            case Done(reqs):
                computation = self.scheduler.computations[cmd.cid]
                assert not computation.runnable()
                assert computation.suspendable()

                # We are relying on the closure to keep track of the number
                # of created callbacks
                count = 0

                def create_callback_callback(res: CreateCallbackRes, cmd: Command, task_on_req: Task) -> None:
                    assert cmd.cid in self.tasks
                    task_on_res = self.tasks[cmd.cid]

                    if res.promise.completed:
                        assert not res.callback
                        self.commands.append((Resume(res.promise.id, cmd.cid, res.promise), None))
                        return

                    nonlocal count
                    count += 1

                    # Only complete the task when all callbacks have been
                    # created and the task is the same, it is possible that
                    # while creating the callbacks a new task has been
                    # received.
                    if count == len(reqs) and task_on_req.id == task_on_res.id:
                        assert task_on_res.counter >= task_on_req.counter
                        self.send_req(
                            "sim://uni@server",
                            CompleteTaskReq(task_on_res.id, task_on_res.counter),
                            lambda _: None,
                        )

                def create_subscription_callback(res: Result[CreateSubscriptionRes], cmd: Command) -> None:
                    match res:
                        case Ok(CreateSubscriptionRes(promise, callback)):
                            assert promise.id == cmd.cid
                            assert promise.pending or not callback

                            if promise.completed:
                                self.commands.append((Notify(promise.id, promise), None))

                        case Ko():
                            pass

                        case _:
                            raise NotImplementedError

                for req in reqs:
                    match req:
                        case Network(id, cid, CreateCallbackReq() as r):
                            assert cmd.cid == cid
                            assert cmd.cid in self.tasks
                            self.send_req("sim://uni@server", r, lambda res, cmd=cmd, task=self.tasks[cmd.cid]: create_callback_callback(res.data.get("data"), cmd, task))

                        case Network(id, cid, CreateSubscriptionReq() as r):
                            assert cmd.cid == cid
                            assert len(reqs) == 1
                            self.send_req("sim://uni@server", r, lambda res, cmd=cmd: create_subscription_callback(res.data.get("data"), cmd))

                        case _:
                            raise NotImplementedError

    def droppable(self) -> bool:
        return True

    def _create_promise_with_task(self, res: CreatePromiseWithTaskRes) -> None:
        if not res.task:
            return

        self.commands.append((self._invoke(res.promise), res.task))

    def _invoke_message(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.root
                assert not res.leaf
                self.commands.append((self._invoke(res.root), res.task))
            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def _resume_message(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.root
                assert res.leaf
                assert res.leaf.completed
                self.commands.append((Resume(res.leaf.id, res.root.id, res.leaf, self._invoke(res.root)), res.task))
            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def _invoke(self, promise: DurablePromise) -> Invoke:
        data = self.encoder.decode(promise.param.to_tuple())
        assert isinstance(data, dict)
        assert "func" in data
        assert "args" in data
        assert "kwargs" in data
        _, func, version = self.registry.get(data["func"])

        return Invoke(
            promise.id,
            Base(
                promise.id,
                promise.rel_timeout,
                promise.ikey_for_create,
                promise.param.data,
                promise.tags,
            ),
            promise.abs_timeout,
            func,
            data["args"],
            data["kwargs"],
            Options(version=version),
            promise,
        )

    def freeze(self) -> str:
        return "worker"
