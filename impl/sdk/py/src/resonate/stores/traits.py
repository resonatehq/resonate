from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.stores.record import (
        DurablePromiseRecord,
        TaskRecord,
    )
    from resonate.typing import Data, Headers, IdempotencyKey, Tags


class IPromiseStore(ABC):
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
    def create_with_task(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]: ...

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
    def get(self, *, id: str) -> DurablePromiseRecord: ...
