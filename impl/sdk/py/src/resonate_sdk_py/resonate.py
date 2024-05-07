import inspect
from typing import Callable, Any, Tuple, Generator, NamedTuple, List, Union

#####################################################################
## Classes
#####################################################################


class Promise:
    def __init__(self):
        self.state = "pending"
        self.value = None
        self.error = None

    def resolve(self, value: Any):
        if self.state == "pending":
            self.state = "resolved"
            self.value = value

    def reject(self, error: Exception):
        if self.state == "pending":
            self.state = "rejected"
            self.error = error

    def is_pending(self):
        return self.state == "pending"

    def is_completed(self):
        return not self.is_pending()


class Invocation:
    """Encapsulates a function call, potentially asynchronous, with its arguments."""

    def __init__(self, func: Callable, *args: Any, **kwargs: Any):
        self.func = func
        self.args = args
        self.kwargs = kwargs


class Call(Invocation):
    pass


#####################################################################
## Types
#####################################################################

Coro = Generator[Any, Any, Any]

Yielded = Union[Promise, Invocation, Call]


class Next(NamedTuple):
    init: bool = False
    throw: Any = None
    value: Any = None


class Runnable(NamedTuple):
    coro: Coro
    next: Next
    resv: Promise


class Awaiting(NamedTuple):
    coro: Coro
    prom: Promise
    resv: Promise


#####################################################################
## Resonate
#####################################################################


class Scheduler:
    def __init__(self):
        self.runnable: List[Runnable] = []
        self.awaiting: List[Awaiting] = []

    def add(self, func: Callable, *args: Any, **kwargs: Any):
        coro = func(*args, **kwargs)
        prom = Promise()
        next = Next(init=True)
        self.runnable = [Runnable(coro, next, prom)]

    def dump(self):
        print("runnable", self.runnable)
        print("awaiting", self.awaiting)

    def run(self):
        while len(self.runnable) > 0:
            self.dump()
            input("Press enter to continue")

            (coro, next, resv) = self.runnable.pop()
            (more, retv) = advance(coro, next)

            if more:
                if isinstance(retv, Promise):
                    if retv.is_completed():
                        # If the Promise is already completed, handle it immediately
                        next = (
                            Next(value=retv.value)
                            if retv.state == "resolved"
                            else Next(throw=retv.error)
                        )
                        self.runnable.append(Runnable(coro, next, resv))
                    else:
                        self.awaiting.append(Awaiting(coro, retv, resv))
                elif isinstance(retv, Invocation):
                    # see what you get: did you get a generator, then schedule it, did you get a value, then resolve the promise
                    prom = Promise()

                    try:
                        v = retv.func(*retv.args)

                        if inspect.isgenerator(v):
                            self.runnable.append(Runnable(v, Next(init=True), prom))
                            self.runnable.append(Awaiting(coro, Next(value=prom), resv))
                        else:
                            prom.resolve(v)
                            self.runnable.append(Runnable(coro, Next(value=prom), resv))
                    except Exception as e:
                        prom.reject(e)
                        self.runnable.append(Runnable(coro, Next(value=prom), resv))
                elif isinstance(retv, Call):
                    prom = Promise()

                    try:
                        v = retv.func(*retv.args)

                        if inspect.isgenerator(v):
                            self.runnable.append(Runnable(v, Next(init=True), prom))
                            self.awaiting.append(Awaiting(coro, prom, resv))
                        else:
                            prom.resolve(v)
                            self.runnable.append(Runnable(coro, Next(value=v), resv))
                    except Exception as e:
                        prom.reject(e)
                        self.runnable.append(Runnable(coro, Next(value=e), resv))
                else:
                    assert False, "Coroutine must return Promise, Invocation, or Call."

            else:
                resv.resolve(retv)
                for coro, prom, resv in self.awaiting:
                    if prom == resv:
                        self.runnable.append(Runnable(coro, Next(value=retv), prom))
                        self.awaiting.remove(Awaiting(coro, prom, resv))

        if isinstance(retv, Promise):
            if retv.state == "resolved":
                return retv.value
            else:
                raise retv.error

        return retv


class Resonate:
    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler

    def run(self, func: Callable, *args: Any, **kwargs: Any):
        self.scheduler.add(func, *args, **kwargs)
        return self.scheduler.run()


def advance(coro: Coro, resv: Next) -> Tuple[bool, Yielded]:
    try:
        if resv.init:
            return (True, next(coro))
        elif resv.throw:
            return (True, coro.throw(resv.throw))
        else:
            return (True, coro.send(resv.value))
    except StopIteration as e:
        return (False, e.value)
