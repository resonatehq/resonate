from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic

from result import Err, Ok, Result
from typing_extensions import ParamSpec, TypeVar, assert_never

from resonate.logging import logger
from resonate.typing import PendingToRun, Runnable, WaitingForPromiseResolution

if TYPE_CHECKING:
    from resonate.scheduler.shared import (
        Promise,
    )
    from resonate.typing import Yieldable

T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True)
class FinalValue(Generic[T]):
    v: Result[T, Exception]


def iterate_coro(runnable: Runnable[T]) -> Yieldable | FinalValue[T]:
    yieldable: Yieldable
    try:
        if runnable.next_value is None:
            # next_value is None means we need to initialize the coro
            logger.debug("Initializing coro")
            yieldable = next(runnable.coro_and_promise.coro)
        elif isinstance(runnable.next_value, Ok):
            # next_value is Ok mean we can pass a value to the coro
            logger.debug(
                "Sending successfull value to coro `%s`", runnable.next_value.unwrap()
            )
            yieldable = runnable.coro_and_promise.coro.send(
                runnable.next_value.unwrap()
            )
        elif isinstance(runnable.next_value, Err):
            # next_value is Err mean we can throw an error into the coro
            logger.debug("Sending error to coro `%s`", runnable.next_value.err())
            yieldable = runnable.coro_and_promise.coro.throw(runnable.next_value.err())
        else:
            assert_never(runnable.next_value)
    except StopIteration as e:
        # if stop iteraton is raised it means we finished the coro execution
        return FinalValue(Ok(e.value))
    except Exception as e:  # noqa: BLE001
        return FinalValue(Err(e))
    return yieldable


def unblock_depands_coros(
    p: Promise[T],
    waiting: WaitingForPromiseResolution,
    runnables: PendingToRun,
) -> None:
    assert p.done(), "Promise must be done to unblock dependant coros"

    if waiting.get(p) is None:
        return

    res: Result[T, Exception]
    try:
        res = Ok(p.result())
    except Exception as e:  # noqa: BLE001
        res = Err(e)

    dependant_coros = waiting.pop(p)
    logger.debug("Unblocking `%s` pending promises", len(dependant_coros))

    new_runnables = (Runnable(c_and_p, next_value=res) for c_and_p in dependant_coros)
    runnables.extend(new_runnables)


def callback(
    p: Promise[T],
    waiting_for_promise: WaitingForPromiseResolution,
    pending_to_run: PendingToRun,
    v: Result[T, Exception],
) -> None:
    p.set_result(v)
    unblock_depands_coros(p=p, waiting=waiting_for_promise, runnables=pending_to_run)


def handle_return_promise(
    running: Runnable[T],
    prom: Promise[T],
    waiting_for_prom: WaitingForPromiseResolution,
    pending_to_run: list[Runnable[Any]],
) -> None:
    if prom.done():
        unblock_depands_coros(
            p=prom,
            waiting=waiting_for_prom,
            runnables=pending_to_run,
        )
    else:
        waiting_for_expired_prom = waiting_for_prom.pop(running.coro_and_promise.prom)
        waiting_for_prom[prom].extend(waiting_for_expired_prom)
