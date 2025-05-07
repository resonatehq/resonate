from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Any

from resonate import Context
from resonate.conventions import Base
from resonate.errors import ResonateStoreError
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

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.step()

    def step(self) -> None:
        # set the clock
        self.clock.step(self.time)

        # TODO(dfarr): determine a better way to drop components
        for comp in self.components:
            if comp.drop():
                self.components.remove(comp)

        recv: list[Message] = []
        outgoing: dict[Component, list[Message]] = {}
        components_by_uni: dict[str, Component] = {}
        components_by_any: dict[str, list[Component]] = {}

        for comp in self.components:
            assert comp.uni not in components_by_uni
            components_by_uni[comp.uni] = comp
            components_by_any.setdefault(comp.any, []).append(comp)

        for send in self.buffer:
            parsed = urllib.parse.urlparse(send.recv)
            assert parsed.scheme == "sim"

            # TODO(dfarr): all tasks should be droppable, but for now only drop messages from the message source
            if isinstance(send.data, dict) and send.data.get("type") in ("invoke", "resume", "notify") and self.r.random() < self.drop_rate:
                self.buffer.remove(send)
                continue

            match parsed:
                case urllib.parse.ParseResult(scheme="sim", username="uni", hostname=hostname, path=path):
                    if comp := components_by_uni.get(f"{hostname}{path}"):
                        outgoing.setdefault(comp, []).append(send)
                        self.buffer.remove(send)

                case urllib.parse.ParseResult(scheme="sim", username="any", hostname=hostname, path=""):
                    if hostname in components_by_any:
                        comp = self.r.choice(components_by_any[hostname])
                        outgoing.setdefault(comp, []).append(send)
                        self.buffer.remove(send)

                case urllib.parse.ParseResult(scheme="sim", username="any", hostname=hostname, path=path):
                    if comp := components_by_uni.get(f"{hostname}{path}"):
                        outgoing.setdefault(comp, []).append(send)
                        self.buffer.remove(send)
                    elif hostname in components_by_any:
                        comp = self.r.choice(components_by_any[hostname])
                        outgoing.setdefault(comp, []).append(send)
                        self.buffer.remove(send)

                case _:
                    raise NotImplementedError

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
        self.init()

    def init(self) -> None:
        pass

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

    def drop(self) -> bool:
        """Remove the component from the simulation."""
        return False

    def send_msg(self, addr: str, data: Any) -> Message:
        """Send a message to another component."""
        msg = Message(f"sim://uni@{self.uni}", addr, data, self.time)
        self.outgoing.append(msg)
        return msg

    def send_req(self, addr: str, data: Any, callback: Callable[[Message], None], timeout: int = 0) -> Message:
        """Send a request and register a callback for handling the response."""
        self.msgcount += 1
        self.awaiting[self.msgcount] = callback
        msg = Message(f"sim://uni@{self.uni}", addr, {"type": "req", "uuid": self.msgcount, "data": data}, self.time)
        self.outgoing.append(msg)
        return msg

    def defer(self, callback: Callable[[], None], timeout: int = 0) -> None:
        """Defers a callback to be called after a certain time."""
        self.deferred.append((callback, self.time + timeout))

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

            case CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv):
                promise, callback = self.store.promises.callback(
                    id=id,
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
        for recv, mesg in self.store.step():
            self.send_msg(recv, mesg)

    def freeze(self) -> str:
        return "server"


class Worker(Component):
    def __init__(self, r: Random, uni: str, any: str, registry: Registry, dependencies: Dependencies, drop_at: float = 0) -> None:
        super().__init__(r, uni, any)
        self.registry = registry
        self.drop_at = drop_at
        self.dependencies = dependencies
        self.commands: list[Command] = []
        self.tasks: dict[str, Task] = {}
        self.last_heartbeat = 0.0

        self.scheduler = Scheduler(
            lambda id, info: Context(id, info, self.registry, self.dependencies),
            pid=self.uni,
            unicast=f"sim://uni@{self.uni}",
            anycast=f"sim://any@{self.uni}",  # this looks silly, but this is right
        )

    def on_message(self, msg: Message) -> None:
        match msg.data:
            case Invoke(id, conv):
                assert id == conv.id

                self.send_req(
                    "sim://uni@server",
                    CreatePromiseWithTaskReq(
                        id=id,
                        timeout=int((self.time + conv.timeout) * 1000),
                        ikey=conv.idempotency_key,
                        headers=conv.headers,
                        data=conv.data,
                        tags=conv.tags,
                        pid=self.scheduler.pid,
                        ttl=self.r.randint(0, 30000),
                    ),
                    lambda res: self.__invoke(res.data.get("data")),
                )

            case Listen(id):
                self.commands.append(Listen(id))

            case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    "sim://uni@server",
                    ClaimTaskReq(id, counter, self.scheduler.pid, self.r.randint(0, 30000)),
                    lambda res: self._invoke(res.data.get("data")),
                )

            case {"type": "resume", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    "sim://uni@server",
                    ClaimTaskReq(id, counter, self.scheduler.pid, self.r.randint(0, 30000)),
                    lambda res: self._resume(res.data.get("data")),
                )

            case {"type": "notify", "promise": promise}:
                # store instance does not matter
                promise = DurablePromise.from_dict(LocalStore(), promise)
                assert promise.completed
                self.commands.append(Notify(promise.id, promise))

            case _:
                raise NotImplementedError

    def next(self) -> None:
        if not self.commands:
            return

        cmd = self.r.choice(self.commands)
        self.commands.remove(cmd)

        match self.scheduler.step(cmd):
            case More(reqs):
                for req in reqs:
                    match req:
                        case Function(id, cid, func) | Delayed(Function(id, cid, func)):
                            try:
                                r = Ok(func())
                            except Exception as e:
                                r = Ko(e)

                            delay = int(req.delay * 1000) if isinstance(req, Delayed) else 0
                            self.defer(lambda id=id, cid=cid, r=r: self.commands.append(Return(id, cid, r)), delay)

                        case Network(id, cid, req):
                            self.send_req("sim://uni@server", req, lambda res, id=id, cid=cid: self.commands.append(Receive(id, cid, res.data.get("data"))))

            case Done(reqs):
                count = 0
                task = self.tasks.get(cmd.cid)

                if not reqs and task:
                    self.send_req(
                        "sim://uni@server",
                        CompleteTaskReq(task.id, task.counter),
                        lambda _: None,
                    )

                def create_callback_callback(res: CreateCallbackRes) -> None:
                    assert task
                    nonlocal count

                    if res.promise.completed:
                        assert not res.callback
                        self.commands.append(Resume(res.promise.id, cmd.cid, res.promise))
                    else:
                        count += 1

                    if count == len(reqs):
                        self.send_req(
                            "sim://uni@server",
                            CompleteTaskReq(task.id, task.counter),
                            lambda _: None,
                        )

                def create_subscription_callback(res: Result[CreateSubscriptionRes]) -> None:
                    match res:
                        case Ok(CreateSubscriptionRes(promise, callback)):
                            assert promise.id == cmd.cid
                            assert promise.completed or callback

                            if promise.completed:
                                self.commands.append(Notify(promise.id, promise))

                        case Ko():
                            pass

                        case _:
                            raise NotImplementedError

                for req in reqs:
                    match req:
                        case Network(id, cid, CreateCallbackReq() as req):
                            assert cmd.cid in self.tasks
                            self.send_req("sim://uni@server", req, lambda res: create_callback_callback(res.data.get("data")))

                        case Network(id, cid, CreateSubscriptionReq() as req):
                            self.send_req("sim://uni@server", req, lambda res: create_subscription_callback(res.data.get("data")))

                        case _:
                            raise NotImplementedError

        # heartbeat every 15s, the midpoint of the random ttl interval [0, 30s]
        if self.time - self.last_heartbeat >= 15:
            self.send_req("sim://uni@server", HeartbeatTasksReq(self.scheduler.pid), lambda _: None)
            self.last_heartbeat = self.time

    def drop(self) -> bool:
        # Remove the worker when time exceeds the drop_at value.
        # Tasks should be re-routed to a worker with the same anycast address.
        return bool(self.drop_at and self.drop_at <= self.time)

    def __invoke(self, res: CreatePromiseWithTaskRes) -> None:
        if not res.task:
            return

        _, func, version = self.registry.get(res.promise.param.data["func"])
        self.tasks[res.promise.id] = res.task

        self.commands.append(
            Invoke(
                res.promise.id,
                Base(
                    res.promise.id,
                    res.promise.rel_timeout,
                    res.promise.ikey_for_create,
                    res.promise.param.headers,
                    res.promise.param.data,
                    res.promise.tags,
                ),
                res.promise.abs_timeout,
                func,
                res.promise.param.data["args"],
                res.promise.param.data["kwargs"],
                Options(version=version),
                res.promise,
            )
        )

    def _invoke(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.root.pending, "Root promise must be pending."
                assert isinstance(res.root.param.data, dict)
                assert "func" in res.root.param.data, "Root param must have func."
                assert "args" in res.root.param.data, "Root param must have args."
                assert "kwargs" in res.root.param.data, "Root param must have kwargs."

                _, func, version = self.registry.get(res.root.param.data["func"])
                self.tasks[res.root.id] = res.task

                self.commands.append(
                    Invoke(
                        res.root.id,
                        Base(
                            res.root.id,
                            res.root.rel_timeout,
                            res.root.ikey_for_create,
                            res.root.param.headers,
                            res.root.param.data,
                            res.root.tags,
                        ),
                        res.root.abs_timeout,
                        func,
                        res.root.param.data["args"],
                        res.root.param.data["kwargs"],
                        Options(version=version),
                        res.root,
                    )
                )

            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def _resume(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.leaf, "Leaf must be set."
                assert res.leaf.completed, "Leaf promise must be completed."
                assert isinstance(res.leaf.param.data, dict)
                assert "func" in res.leaf.param.data, "Leaf param must have func."
                assert "args" in res.leaf.param.data, "Leaf param must have args."
                assert "kwargs" in res.leaf.param.data, "Leaf param must have kwargs."

                _, func, version = self.registry.get(res.root.param.data["func"])
                self.tasks[res.root.id] = res.task

                self.commands.append(
                    Resume(
                        id=res.leaf.id,
                        cid=res.root.id,
                        promise=res.leaf,
                        invoke=Invoke(
                            res.root.id,
                            Base(
                                res.root.id,
                                res.root.rel_timeout,
                                res.root.ikey_for_create,
                                res.root.param.headers,
                                res.root.param.data,
                                res.root.tags,
                            ),
                            res.root.abs_timeout,
                            func,
                            res.root.param.data["args"],
                            res.root.param.data["kwargs"],
                            Options(version=version),
                            res.root,
                        ),
                    )
                )

            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def freeze(self) -> str:
        return "worker"
