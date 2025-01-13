from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, final

from resonate.errors import ResonateError
from resonate.stores.record import (
    DurablePromiseRecord,
    Param,
    TaskRecord,
    Value,
)
from resonate.stores.traits import IPromiseStore
from resonate.time import now

if TYPE_CHECKING:
    from collections.abc import Hashable

    from resonate.typing import Data, Headers, State, Tags


def _timeout(promise_record: DurablePromiseRecord) -> DurablePromiseRecord:
    new_status: State = "REJECTED_TIMEDOUT"
    if promise_record.is_pending() and now() >= promise_record.timeout:
        if promise_record.tags is not None:
            resonate_timeout: Hashable | None = promise_record.tags.get(
                "resonate:timeout"
            )
            if resonate_timeout is not None and resonate_timeout == "true":
                new_status = "RESOLVED"
        return DurablePromiseRecord(
            new_status,
            id=promise_record.id,
            timeout=promise_record.timeout,
            param=promise_record.param,
            value=Value(headers=None, data=None),
            created_on=promise_record.created_on,
            completed_on=promise_record.timeout,
            idempotency_key_for_create=promise_record.idempotency_key_for_create,
            idempotency_key_for_complete=None,
            tags=promise_record.tags,
        )
    return promise_record


class IStorage(ABC):
    @abstractmethod
    def rmw(
        self,
        id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord: ...


class MemoryStorage(IStorage):
    def __init__(self) -> None:
        self._durable_promises: dict[str, DurablePromiseRecord] = {}

    def rmw(
        self,
        id: str,
        fn: Callable[[DurablePromiseRecord | None], DurablePromiseRecord],
    ) -> DurablePromiseRecord:
        promise_record = self._durable_promises.get(id)
        if promise_record is not None:
            promise_record = _timeout(promise_record)
        item = fn(promise_record)
        self._durable_promises[id] = item
        return item


@final
class LocalPromiseStore(IPromiseStore):
    def __init__(self, storage: IStorage | None = None) -> None:
        self._storage = storage or MemoryStorage()

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
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]:
        return self.create(
            id=id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
            timeout=timeout,
            tags=tags,
        ), None

    def create(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
        timeout: int,
        tags: Tags,
    ) -> DurablePromiseRecord:
        def _create(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                return DurablePromiseRecord(
                    state="PENDING",
                    id=id,
                    timeout=timeout,
                    param=Param(headers=headers, data=data),
                    value=Value(headers=None, data=None),
                    created_on=now(),
                    completed_on=None,
                    idempotency_key_for_complete=None,
                    idempotency_key_for_create=ikey,
                    tags=tags,
                )
            if strict and not promise_record.is_pending():
                msg = "Forbidden request: Durable promise previously created"
                raise ResonateError(
                    msg,
                    "STORE_FORBIDDEN",
                )
            if (
                promise_record.idempotency_key_for_create is None
                or ikey != promise_record.idempotency_key_for_create
            ):
                msg = "Forbidden request: Missing idempotency key for create"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(id=id, fn=_create)

    def reject(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        def _reject(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="REJECTED",
                    id=promise_record.id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_rejected():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(id=id, fn=_reject)

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        def _cancel(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not Found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="REJECTED_CANCELED",
                    id=promise_record.id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_canceled():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(id=id, fn=_cancel)

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        def _resolve(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                msg = "Not found"
                raise ResonateError(msg, "STORE_NOT_FOUND")
            if promise_record.is_pending():
                return DurablePromiseRecord(
                    state="RESOLVED",
                    id=promise_record.id,
                    timeout=promise_record.timeout,
                    param=promise_record.param,
                    value=Value(headers=headers, data=data),
                    created_on=promise_record.created_on,
                    completed_on=now(),
                    idempotency_key_for_create=promise_record.idempotency_key_for_create,
                    idempotency_key_for_complete=ikey,
                    tags=promise_record.tags,
                )
            if strict and not promise_record.is_resolved():
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            if not promise_record.is_timeout() and (
                promise_record.idempotency_key_for_complete is None
                or ikey != promise_record.idempotency_key_for_complete
            ):
                msg = "Forbidden request"
                raise ResonateError(msg, "STORE_FORBIDDEN")
            return promise_record

        return self._storage.rmw(id=id, fn=_resolve)

    def get(self, *, id: str) -> DurablePromiseRecord:
        def _get(
            promise_record: DurablePromiseRecord | None,
        ) -> DurablePromiseRecord:
            if promise_record is None:
                raise ResonateError(msg="Not Found", code="STORE_NOT_FOUND")

            return promise_record

        return self._storage.rmw(id=id, fn=_get)


@final
class LocalStore:
    def __init__(self, promises: LocalPromiseStore | None = None) -> None:
        self.promises = promises or LocalPromiseStore()
