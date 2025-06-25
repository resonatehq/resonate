from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from resonate.models.callback import Callback
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.encoder import Encoder
    from resonate.models.schedules import Schedule
    from resonate.models.task import Task


@runtime_checkable
class Store(Protocol):
    @property
    def encoder(self) -> Encoder[str | None, str | None]: ...

    @property
    def promises(self) -> PromiseStore: ...

    @property
    def tasks(self) -> TaskStore: ...

    @property
    def schedules(self) -> ScheduleStore: ...


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
        data: str | None = None,
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
        data: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]: ...

    def resolve(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...

    def reject(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...

    def cancel(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: str | None = None,
    ) -> DurablePromise: ...

    def callback(
        self,
        promise_id: str,
        root_promise_id: str,
        recv: str,
        timeout: int,
    ) -> tuple[DurablePromise, Callback | None]: ...

    def subscribe(
        self,
        id: str,
        promise_id: str,
        recv: str,
        timeout: int,
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


class ScheduleStore(Protocol):
    def create(
        self,
        id: str,
        cron: str,
        promise_id: str,
        promise_timeout: int,
        *,
        ikey: str | None = None,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        promise_headers: dict[str, str] | None = None,
        promise_data: str | None = None,
        promise_tags: dict[str, str] | None = None,
    ) -> Schedule: ...

    def get(self, id: str) -> Schedule: ...

    def delete(self, id: str) -> None: ...
