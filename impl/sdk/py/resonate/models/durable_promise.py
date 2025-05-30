from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from resonate.errors import ResonateCanceledError, ResonateTimedoutError
from resonate.models.result import Ko, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Mapping

    from resonate.models.callback import Callback
    from resonate.models.encoder import Encoder
    from resonate.models.store import Store


@dataclass
class DurablePromise:
    id: str
    state: Literal["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"]
    timeout: int
    ikey_for_create: str | None
    ikey_for_complete: str | None
    param: DurablePromiseValue
    value: DurablePromiseValue
    tags: dict[str, str]
    created_on: int
    completed_on: int | None

    store: Store = field(repr=False)

    @property
    def pending(self) -> bool:
        return self.state == "PENDING"

    @property
    def completed(self) -> bool:
        return not self.pending

    @property
    def resolved(self) -> bool:
        return self.state == "RESOLVED"

    @property
    def rejected(self) -> bool:
        return self.state == "REJECTED"

    @property
    def canceled(self) -> bool:
        return self.state == "REJECTED_CANCELED"

    @property
    def timedout(self) -> bool:
        return self.state == "REJECTED_TIMEDOUT"

    @property
    def abs_timeout(self) -> float:
        return self.timeout / 1000

    @property
    def rel_timeout(self) -> float:
        return (self.timeout - self.created_on) / 1000

    def resolve(self, data: str | None, *, ikey: str | None = None, strict: bool = False, headers: dict[str, str] | None = None) -> None:
        promise = self.store.promises.resolve(
            id=self.id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
        )
        self._complete(promise)

    def reject(self, data: str | None, *, ikey: str | None = None, strict: bool = False, headers: dict[str, str] | None = None) -> None:
        promise = self.store.promises.reject(
            id=self.id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
        )
        self._complete(promise)

    def cancel(self, data: str | None, *, ikey: str | None = None, strict: bool = False, headers: dict[str, str] | None = None) -> None:
        promise = self.store.promises.cancel(
            id=self.id,
            ikey=ikey,
            strict=strict,
            headers=headers,
            data=data,
        )
        self._complete(promise)

    def callback(self, id: str, root_promise_id: str, recv: str) -> Callback | None:
        promise, callback = self.store.promises.callback(
            promise_id=self.id,
            root_promise_id=root_promise_id,
            recv=recv,
            timeout=self.timeout,
        )
        if callback is None:
            self._complete(promise)
        return callback

    def subscribe(self, id: str, recv: str) -> Callback | None:
        promise, callback = self.store.promises.subscribe(
            id=id,
            promise_id=self.id,
            recv=recv,
            timeout=self.timeout,
        )
        if callback is None:
            self._complete(promise)
        return callback

    def _complete(self, promise: DurablePromise) -> None:
        assert promise.completed
        self.state = promise.state
        self.ikey_for_complete = promise.ikey_for_complete
        self.value = promise.value
        self.completed_on = promise.completed_on

    def result(self, encoder: Encoder[Any, tuple[dict[str, str] | None, str | None]]) -> Result[Any]:
        assert self.completed, "Promise must be completed"

        if self.rejected:
            v = encoder.decode(self.value.to_tuple())

            # In python, only exceptions may be raised. Here, we are converting
            # a value that is not an exception into an exception.
            return Ko(v) if isinstance(v, BaseException) else Ko(Exception(v))
        if self.canceled:
            return Ko(ResonateCanceledError(self.id))
        if self.timedout:
            return Ko(ResonateTimedoutError(self.id, self.abs_timeout))

        return Ok(encoder.decode(self.value.to_tuple()))

    @classmethod
    def from_dict(cls, store: Store, data: Mapping[str, Any]) -> DurablePromise:
        return cls(
            id=data["id"],
            state=data["state"],
            timeout=data["timeout"],
            ikey_for_create=data.get("idempotencyKeyForCreate"),
            ikey_for_complete=data.get("idempotencyKeyForComplete"),
            param=DurablePromiseValue.from_dict(store, data.get("param", {})),
            value=DurablePromiseValue.from_dict(store, data.get("value", {})),
            tags=data.get("tags", {}),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
            store=store,
        )


@dataclass
class DurablePromiseValue:
    headers: dict[str, str] | None
    data: str | None

    def to_tuple(self) -> tuple[dict[str, str] | None, Any]:
        return self.headers, self.data

    @classmethod
    def from_dict(cls, store: Store, data: Mapping[str, Any]) -> DurablePromiseValue:
        return cls(
            headers=data.get("headers"),
            data=store.encoder.decode(data.get("data")),
        )
