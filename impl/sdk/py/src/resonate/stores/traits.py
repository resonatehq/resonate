from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.stores.record import (
        CallbackRecord,
        DurablePromiseRecord,
        TaskRecord,
    )
    from resonate.typing import Data, Headers, IdempotencyKey, Tags


class IPromiseStore(ABC):
    @abstractmethod
    def create_with_callback(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        timeout: int,
        headers: dict[str, str] | None,
        data: str | None,
        tags: dict[str, str] | None,
        root_id: str,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]: ...
    @abstractmethod
    def create_with_task(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
        pid: str,
        ttl: int,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]: ...

    @abstractmethod
    def create(  # noqa: PLR0913
        self,
        *,
        id: str,
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
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def resolve(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def reject(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord: ...

    @abstractmethod
    def get(self, *, id: str) -> DurablePromiseRecord: ...
