from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.actions import Invoke, Sleep

T = TypeVar("T")


@final
class Promise(Generic[T]):
    def __init__(
        self,
        promise_id: str,
        action: Invoke | Sleep,
    ) -> None:
        self.promise_id = promise_id
        self.f = Future[T]()
        self.action = action

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

    def __str__(self) -> str:
        return f"Promise representing execution {self.promise_id}"

    def __repr__(self) -> str:
        return self.__str__()
