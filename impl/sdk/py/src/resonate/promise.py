from __future__ import annotations

from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.actions import (
    LFI,
    RFI,
    All,
    AllSettled,
    Race,
)
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.typing import PromiseActions

T = TypeVar("T")


@final
class Promise(Generic[T]):
    def __init__(
        self,
        promise_id: str,
        action: PromiseActions,
        parent_promise: Promise[Any] | None,
    ) -> None:
        self.promise_id = promise_id
        self.f = Future[T]()
        self._next_child_num = 1
        self.parent_promise = parent_promise
        self.children_promises: list[Promise[Any]] = []
        self.leaf_promises = set[Promise[Any]]()
        self._is_partition_root = self.parent_promise is None
        self._marked_as_partition_root = False

        self.action = action
        if isinstance(action, (LFI, All, AllSettled, Race)):
            self.durable = action.opts.durable
        elif isinstance(action, RFI):
            self.durable = True
        else:
            assert_never(action)

    def is_partition_root(self) -> bool:
        return self._is_partition_root or self._marked_as_partition_root

    def is_marked_as_partition_root(self) -> bool:
        return self._marked_as_partition_root

    def unmark_as_partition_root(self) -> None:
        assert (
            self._marked_as_partition_root
        ), "Promise is already marked as partition root"
        self._marked_as_partition_root = not self._marked_as_partition_root

    def mark_as_partition_root(self) -> None:
        assert not self._is_partition_root, "Promise is already a partition root"
        assert (
            not self._marked_as_partition_root
        ), "Promise is already marked as partition root"
        self._marked_as_partition_root = not self._marked_as_partition_root

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

    def parent_promise_id(self) -> str | None:
        return (
            self.parent_promise.promise_id if self.parent_promise is not None else None
        )

    def root(self) -> Promise[Any]:
        maybe_root = self
        while True:
            if maybe_root.parent_promise is None:
                return maybe_root
            maybe_root = maybe_root.parent_promise

    def partition_root(self) -> Promise[Any]:
        maybe_root = self
        while True:
            if maybe_root.is_partition_root():
                return maybe_root
            assert maybe_root.parent_promise is not None
            maybe_root = maybe_root.parent_promise

    def child_name(self) -> str:
        return f"{self.promise_id}.{self._next_child_num}"

    def child_promise(
        self,
        promise_id: str,
        action: PromiseActions,
    ) -> Promise[Any]:
        child = Promise[Any](
            promise_id=promise_id,
            action=action,
            parent_promise=self,
        )

        self.children_promises.append(child)
        self.leaf_promises.add(child)
        root_promise = self.root()
        if root_promise is not None:
            root_promise.leaf_promises.discard(self)
            root_promise.leaf_promises.add(child)

        self._next_child_num += 1
        return child


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
