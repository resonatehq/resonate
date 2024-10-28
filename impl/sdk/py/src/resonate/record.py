from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict, final

from typing_extensions import Self

from resonate.encoders import IEncoder

if TYPE_CHECKING:
    from resonate.encoders import IEncoder
    from resonate.typing import Data, Headers, IdempotencyKey, State, Tags


class _InvokeInfo(TypedDict):
    func_name: str
    args: list[Any]
    kwargs: dict[str, Any]


class Decodable(ABC):
    @classmethod
    @abstractmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self: ...


@final
@dataclass(frozen=True)
class Param:
    data: Data
    headers: Headers


@final
@dataclass(frozen=True)
class Value:
    data: Data
    headers: Headers


@final
@dataclass(frozen=True)
class CallbackRecord(Decodable):
    callback_id: str
    promise_id: str
    timeout: int
    created_on: int

    @classmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self:
        _ = encoder
        return cls(
            callback_id=data["id"],
            promise_id=data["promiseId"],
            timeout=data["timeout"],
            created_on=data["createdOn"],
        )


@final
@dataclass(frozen=True)
class DurablePromiseRecord(Decodable):
    state: State
    promise_id: str
    timeout: int
    param: Param
    value: Value
    created_on: int
    completed_on: int | None
    idempotency_key_for_create: IdempotencyKey
    idempotency_key_for_complete: IdempotencyKey
    tags: Tags

    def invoke_info(self) -> _InvokeInfo:
        assert self.param.data is not None
        data_dict = json.loads(self.param.data)
        return {
            "func_name": data_dict["func"],
            "args": data_dict["args"],
            "kwargs": data_dict["kwargs"],
        }

    def is_completed(self) -> bool:
        return not self.is_pending()

    def is_timeout(self) -> bool:
        return self.state == "REJECTED_TIMEDOUT"

    def is_canceled(self) -> bool:
        return self.state == "REJECTED_CANCELED"

    def is_rejected(self) -> bool:
        return self.state == "REJECTED"

    def is_resolved(self) -> bool:
        return self.state == "RESOLVED"

    def is_pending(self) -> bool:
        return self.state == "PENDING"

    @classmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self:
        if data["param"]:
            param = Param(
                data=encoder.decode(data["param"]["data"]),
                headers=data["param"].get("headers"),
            )
        else:
            param = Param(data=None, headers=None)

        if data["value"]:
            value = Value(
                data=encoder.decode(data["value"]["data"]),
                headers=data["value"].get("headers"),
            )
        else:
            value = Value(data=None, headers=None)

        return cls(
            promise_id=data["id"],
            state=data["state"],
            param=param,
            value=value,
            timeout=data["timeout"],
            tags=data.get("tags"),
            created_on=data["createdOn"],
            completed_on=data.get("completedOn"),
            idempotency_key_for_complete=data.get("idempotencyKeyForComplete"),
            idempotency_key_for_create=data.get("idempotencyKeyForCreate"),
        )


@final
@dataclass(frozen=True)
class Invoke(Decodable):
    root_promise_store: DurablePromiseRecord

    @classmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self:
        return cls(
            root_promise_store=DurablePromiseRecord.decode(data, encoder=encoder),
        )


@final
@dataclass(frozen=True)
class Resume(Decodable):
    root_promise_store: DurablePromiseRecord
    leaf_promise_store: DurablePromiseRecord

    @classmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self:
        return cls(
            root_promise_store=DurablePromiseRecord.decode(
                data["root"]["data"], encoder=encoder
            ),
            leaf_promise_store=DurablePromiseRecord.decode(
                data["leaf"]["data"], encoder=encoder
            ),
        )


@final
@dataclass(frozen=True)
class TaskRecord(Decodable):
    task_id: str
    counter: int

    @classmethod
    def decode(cls, data: dict[str, Any], encoder: IEncoder[str, str]) -> Self:
        _ = encoder
        return cls(task_id=data["id"], counter=data["counter"])
