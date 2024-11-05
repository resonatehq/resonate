from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.record import (
        CallbackRecord,
        DurablePromiseRecord,
        Invoke,
        Resume,
        TaskRecord,
    )
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


class ITaskStore(ABC):
    @abstractmethod
    def claim_task(
        self, *, task_id: str, counter: int, pid: str, ttl: int
    ) -> Invoke | Resume: ...

    @abstractmethod
    def complete_task(self, *, task_id: str, counter: int) -> None: ...

    @abstractmethod
    def heartbeat_tasks(self, *, pid: str) -> int: ...

    @abstractmethod
    def create_callback(
        self,
        *,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]: ...

    @abstractmethod
    def create_with_task(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
        pid: str,
        ttl: int,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]: ...

    @abstractmethod
    def create_with_callback(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: str | None,
        strict: bool,
        timeout: int,
        headers: dict[str, str] | None,
        data: str | None,
        tags: dict[str, str] | None,
        root_promise_id: str,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]: ...


class IPromiseStore(ABC):
    @abstractmethod
    def create(  # noqa: PLR0913
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def cancel(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def resolve(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        promise_id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def get(self, *, promise_id: str) -> DurablePromiseRecord: ...

    @abstractmethod
    def search(
        self, *, promise_id: str, state: State, tags: Tags, limit: int | None = None
    ) -> list[DurablePromiseRecord]: ...
