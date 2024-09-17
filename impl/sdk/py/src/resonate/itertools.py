from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, final

from typing_extensions import ParamSpec, TypeVar, assert_never

from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.typing import Runnable, Yieldable

T = TypeVar("T")
P = ParamSpec("P")


@final
@dataclass(frozen=True)
class FinalValue(Generic[T]):
    v: Result[T, Exception]


def iterate_coro(runnable: Runnable[T]) -> Yieldable | FinalValue[T]:
    yieldable: Yieldable
    try:
        if runnable.next_value is None:
            # `None` means to initialize the coroutine
            yieldable = next(runnable.coro_and_promise.coro)
        elif isinstance(runnable.next_value, Ok):
            # `Ok[T]` means send a value
            yieldable = runnable.coro_and_promise.coro.send(
                runnable.next_value.unwrap()
            )
        elif isinstance(runnable.next_value, Err):
            # `Err[Exception]` means throw and Exception
            yieldable = runnable.coro_and_promise.coro.throw(runnable.next_value.err())
        else:
            assert_never(runnable.next_value)
    except StopIteration as e:
        return FinalValue(Ok(e.value))
    except Exception as e:  # noqa: BLE001
        return FinalValue(Err(e))
    return yieldable
