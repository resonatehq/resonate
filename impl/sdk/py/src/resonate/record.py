from __future__ import annotations

from asyncio import iscoroutinefunction
from inspect import isfunction, isgeneratorfunction
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from resonate.actions import LFI, RFI
from resonate.dataclasses import Invocation
from resonate.logging import logger
from resonate.result import Ok, Result
from resonate.retry_policy import Never, exponential, never
from resonate.stores.record import DurablePromiseRecord, TaskRecord

if TYPE_CHECKING:
    from resonate import retry_policy
    from resonate.actions import LFI
    from resonate.context import Context
    from resonate.dataclasses import ResonateCoro
    from resonate.stores.record import DurablePromiseRecord, TaskRecord

T = TypeVar("T")


@final
class Record(Generic[T]):
    def __init__(
        self,
        id: str,
        invocation: LFI | RFI,
        parent: Record[Any] | None,
        ctx: Context,
    ) -> None:
        self.id: str = id
        self.parent: Record[Any] | None = parent
        self.is_root: bool = (
            True if self.parent is None else isinstance(invocation, RFI)
        )
        self._result: Result[T, Exception] | None = None
        self.children: list[Record[Any]] = []
        self.invocation: LFI | RFI = invocation
        self.retry_policy: retry_policy.RetryPolicy | None
        if not isinstance(invocation.unit, Invocation):
            self.retry_policy = None
        elif isinstance(invocation.unit.fn, str):
            self.retry_policy = invocation.opts.retry_policy or None
        elif isgeneratorfunction(invocation.unit.fn):
            self.retry_policy = invocation.opts.retry_policy or never()
        else:
            assert iscoroutinefunction(invocation.unit.fn) or isfunction(
                invocation.unit.fn
            )
            self.retry_policy = invocation.opts.retry_policy or exponential(
                base_delay=1,
                factor=2,
                max_retries=-1,
                max_delay=30,
            )

        self._attempt: int = 1
        self.durable_promise: DurablePromiseRecord | None = None
        self._task: TaskRecord | None = None
        self.ctx = ctx
        self._coro: ResonateCoro[T] | None = None
        self.blocked_on: Record[Any] | None = None
        self._num_children: int = 0
        logger.info(
            "New record %s %s created. Child of %s",
            type(self.invocation).__name__,
            self.id,
            self.parent.id if self.parent else None,
        )

    def get_coro(self) -> ResonateCoro[T]:
        assert self._coro
        return self._coro

    def has_coro(self) -> bool:
        return self._coro is not None

    def increate_attempt(self) -> None:
        self._attempt += 1
        if self._coro is not None:
            self._coro = None

    def root(self) -> Record[Any]:
        maybe_is_the_root = self
        while True:
            if maybe_is_the_root.is_root:
                return maybe_is_the_root
            assert maybe_is_the_root.parent is not None
            maybe_is_the_root = maybe_is_the_root.parent

    def add_child(self, record: Record[Any]) -> None:
        self.children.append(record)
        self._num_children += 1

    def add_coro(self, coro: ResonateCoro[T]) -> None:
        assert self._coro is None
        self._coro = coro

    def add_durable_promise(self, durable_promise: DurablePromiseRecord) -> None:
        assert self.id == durable_promise.id
        assert self.durable_promise is None
        self.durable_promise = durable_promise
        logger.info("Durable promise added to %s", self.id)

    def add_task(self, task: TaskRecord) -> None:
        assert not self.has_task()
        self._task = task
        logger.info("Task added to %s", self.id)

    def has_task(self) -> bool:
        return self._task is not None

    def remove_task(self) -> None:
        assert self.has_task()
        self._task = None
        logger.info("Task completed for %s", self.id)

    def get_task(self) -> TaskRecord:
        assert self._task
        return self._task

    def clear_coro(self) -> None:
        assert self._coro is not None
        self._coro = None

    def should_retry(self, result: Result[T, Exception]) -> bool:
        assert self.retry_policy is not None
        if isinstance(result, Ok) or isinstance(self.retry_policy, Never):
            return False
        return self.retry_policy.should_retry(self._attempt)

    def next_retry_delay(self) -> float:
        assert self.retry_policy is not None
        assert not isinstance(self.retry_policy, Never)
        return self.retry_policy.calculate_delay(self._attempt)

    def set_result(self, result: Result[T, Exception], *, deduping: bool) -> None:
        if not deduping:
            assert not self.should_retry(
                result
            ), "Result cannot be set if retry policy allows for another attempt."

        assert all(
            r.done() for r in self.children
        ), "All children record must be completed."
        assert self._result is None
        self._result = result

    def safe_result(self) -> Result[Any, Exception]:
        assert self._result is not None
        return self._result

    def done(self) -> bool:
        return self._result is not None

    def next_child_name(self) -> str:
        return f"{self.id}.{self._num_children+1}"

    def create_child(
        self,
        id: str,
        invocation: LFI | RFI,
    ) -> Record[Any]:
        child_record = Record[Any](
            id=id,
            parent=self,
            invocation=invocation,
            ctx=self.ctx,
        )
        self.add_child(child_record)
        return child_record
