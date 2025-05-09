from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from resonate.errors.errors import ResonateCanceledError, ResonateTimedoutError
from resonate.models.result import Ko, Ok, Result

if TYPE_CHECKING:
    from resonate.models.callback import Callback
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
    def params(self) -> Any:
        return self.param.data

    @property
    def result(self) -> Result:
        assert self.completed, "Promise must be completed"

        if self.rejected:
            return Ko(self.value.data)
        if self.canceled:
            return Ko(ResonateCanceledError(self.id))
        if self.timedout:
            return Ko(ResonateTimedoutError(self.id, self.abs_timeout))

        return Ok(self.value.data)

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
            id=id,
            promise_id=self.id,
            root_promise_id=root_promise_id,
            timeout=self.timeout,
            recv=recv,
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

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> DurablePromise:
        return cls(
            store=store,
            id=data["id"],
            state=data["state"],
            timeout=data["timeout"],
            ikey_for_create=data.get("idempotencyKeyForCreate"),
            ikey_for_complete=data.get("idempotencyKeyForComplete"),
            param=DurablePromiseValue(data["param"].get("headers"), store.encoder.decode(data["param"].get("data"))),
            value=DurablePromiseValue(data["value"].get("headers"), store.encoder.decode(data["value"].get("data"))),
            tags=data.get("tags", {}),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
        )


@dataclass
class DurablePromiseValue:
    headers: dict[str, str] | None
    data: Any
