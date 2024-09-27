from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.actions import All, AllSettled, Invocation, Race, Sleep
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.typing import Combinator

T = TypeVar("T")


@final
class Promise(Generic[T]):
    def __init__(
        self,
        promise_id: str,
        action: Invocation | Sleep | Combinator,
    ) -> None:
        self.promise_id = promise_id
        self.f = Future[T]()
        self.action = action
        if isinstance(action, (Invocation, All, AllSettled, Race)):
            self.durable = action.opts.durable
        elif isinstance(action, Sleep):
            raise NotImplementedError
        else:
            assert_never(action)

    def result(self, timeout: float | None = None) -> T:
        return self.f.result(timeout=timeout)

    def safe_result(self, timeout: float | None = None) -> Result[T, Exception]:
        res: Result[T, Exception]
        try:
            res = Ok(self.f.result(timeout=timeout))
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res

    def set_result(self, result: Result[T, Exception]) -> None:
        if isinstance(result, Ok):
            self.f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self.f.set_exception(result.err())
        else:
            assert_never(result)

    def done(self) -> bool:
        return self.f.done()

    def success(self) -> bool:
        if not self.done():
            return False
        try:
            self.result()
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    def failure(self) -> bool:
        return not self.success()


def all_promises_are_done(promises: list[Promise[Any]]) -> bool:
    return all(p.done() for p in promises)


def any_promise_is_done(promises: list[Promise[Any]]) -> bool:
    return any(p.done() for p in promises)


def get_first_error_if_any(promises: list[Promise[Any]]) -> Err[Exception] | None:
    for p in promises:
        if p.success():
            continue
        error = p.safe_result()
        assert isinstance(error, Err)
        return error
    return None
