from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from resonate.models.callback import Callback
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.encoder import Encoder
    from resonate.models.task import Task


class Store(Protocol):
    @property
    def encoder(self) -> Encoder[Any, str | None]: ...

    @property
    def promises(self) -> PromiseStore: ...

    @property
    def tasks(self) -> TaskStore: ...


class PromiseStore(Protocol):
    def get(
        self,
        id: str,
    ) -> DurablePromise: ...

    def create(
        self,
        id: str,
        timeout: int,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise: ...

    def create_with_task(
        self,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]: ...

    def resolve(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise: ...

    def reject(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise: ...

    def cancel(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise: ...

    def callback(
        self,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]: ...

    def subscribe(
        self,
        id: str,
        promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]: ...


class TaskStore(Protocol):
    def claim(
        self,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]: ...

    def complete(
        self,
        id: str,
        counter: int,
    ) -> bool: ...

    def heartbeat(
        self,
        pid: str,
    ) -> int: ...
