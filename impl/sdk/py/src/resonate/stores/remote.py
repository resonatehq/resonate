from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, final

import requests

from resonate.encoders import Base64Encoder, IEncoder
from resonate.errors import ResonateError
from resonate.stores.record import (
    CallbackRecord,
    DurablePromiseRecord,
    Invoke,
    Resume,
    TaskRecord,
)
from resonate.stores.traits import IPromiseStore

if TYPE_CHECKING:
    from resonate.typing import Data, Headers, IdempotencyKey


@final
class RemoteCallbackStore:
    def __init__(
        self,
        url: str,
        request_timeout: int,
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._request_timeout = request_timeout
        self._encoder = encoder

    def create(
        self,
        *,
        id: str,
        root_id: str,
        timeout: int,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]:
        response = requests.post(
            url=f"{self.url}/callbacks",
            json={
                "promiseId": id,
                "rootPromiseId": root_id,
                "timeout": timeout,
                "recv": recv,
            },
            timeout=self._request_timeout,
        )

        _ensure_success(response)
        data = response.json()

        callback_data = data["callback"]
        durable_promise = DurablePromiseRecord.decode(
            data["promise"], encoder=self._encoder
        )
        if callback_data is None:
            return durable_promise, None
        return durable_promise, CallbackRecord.decode(
            callback_data, encoder=self._encoder
        )


@final
class RemotePromiseStore(IPromiseStore):
    def __init__(
        self,
        url: str,
        request_timeout: int,
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._request_timeout = request_timeout
        self._encoder = encoder

    def _initialize_headers(
        self, *, strict: bool, ikey: IdempotencyKey
    ) -> dict[str, str]:
        request_headers: dict[str, str] = {"Strict": str(strict)}
        if ikey is not None:
            request_headers["Idempotency-Key"] = ikey

        return request_headers

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
    ) -> tuple[DurablePromiseRecord, TaskRecord | None]:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)
        response = requests.post(
            url=f"{self.url}/promises/task",
            headers=request_headers,
            json={
                "promise": {
                    "id": id,
                    "param": {
                        "headers": headers,
                        "data": self._encode_data(data),
                    },
                    "timeout": timeout,
                    "tags": tags,
                },
                "task": {
                    "processId": pid,
                    "ttl": ttl,
                    "recv": recv,
                },
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        data_json = response.json()

        task_data = data_json["task"]
        durable_promise = DurablePromiseRecord.decode(
            data_json["promise"], encoder=self._encoder
        )
        if task_data is None:
            return durable_promise, None
        return durable_promise, TaskRecord.decode(task_data, encoder=self._encoder)

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
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.post(
            url=f"{self.url}/promises/callback",
            headers=request_headers,
            json={
                "promise": {
                    "id": id,
                    "timeout": timeout,
                    "param": {"headers": headers, "data": self._encode_data(data)},
                    "tags": tags,
                },
                "callback": {
                    "rootPromiseId": root_id,
                    "timeout": timeout,
                    "recv": recv,
                },
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        data_json = response.json()

        callback_data = data_json["callback"]
        durable_promise = DurablePromiseRecord.decode(
            data_json["promise"], encoder=self._encoder
        )
        if callback_data is None:
            return durable_promise, None
        return durable_promise, CallbackRecord.decode(
            callback_data, encoder=self._encoder
        )

    def create(  # noqa: PLR0913
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
        timeout: int,
        tags: dict[str, str] | None,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.post(
            url=f"{self.url}/promises",
            headers=request_headers,
            json={
                "id": id,
                "param": {
                    "headers": headers,
                    "data": self._encode_data(data),
                },
                "timeout": timeout,
                "tags": tags,
            },
            timeout=self._request_timeout,
        )

        _ensure_success(response)

        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def cancel(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)
        response = requests.patch(
            url=f"{self.url}/promises/{id}",
            headers=request_headers,
            json={
                "state": "REJECTED_CANCELED",
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: dict[str, str] | None,
        data: str | None,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.patch(
            f"{self.url}/promises/{id}",
            headers=request_headers,
            json={
                "state": "RESOLVED",
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def reject(
        self,
        *,
        id: str,
        ikey: IdempotencyKey,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        response = requests.patch(
            f"{self.url}/promises/{id}",
            headers=request_headers,
            json={
                "state": "REJECTED",
                "value": {"headers": headers, "data": self._encode_data(data)},
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def get(self, *, id: str) -> DurablePromiseRecord:
        response = requests.get(
            f"{self.url}/promises/{id}", timeout=self._request_timeout
        )
        _ensure_success(response)
        return DurablePromiseRecord.decode(data=response.json(), encoder=self._encoder)

    def _encode_data(self, data: str | None) -> str | None:
        if data is None:
            return None
        return _encode(data, self._encoder)


@final
class RemoteTaskStore:
    def __init__(
        self,
        url: str,
        request_timeout: int,
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._request_timeout = request_timeout
        self._encoder = encoder

    def claim(
        self, *, task_id: str, counter: int, pid: str, ttl: int
    ) -> Invoke | Resume:
        response = requests.post(
            url=f"{self.url}/tasks/claim",
            headers={},
            json={
                "id": task_id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)

        data = response.json()
        kind: Literal["resume", "invoke"] = data["type"]
        assert kind in ("resume", "invoke")

        promises: dict[str, Any] = data["promises"]
        if promises.get("leaf") is None:
            return Invoke.decode(data=promises["root"]["data"], encoder=self._encoder)

        return Resume.decode(data=promises, encoder=self._encoder)

    def complete(self, *, task_id: str, counter: int) -> None:
        response = requests.post(
            url=f"{self.url}/tasks/complete",
            headers={},
            json={
                "id": task_id,
                "counter": counter,
            },
            timeout=self._request_timeout,
        )
        _ensure_success(response)

    def heartbeat(self, *, pid: str) -> int:
        response = requests.post(
            url=f"{self.url}/tasks/heartbeat",
            headers={},
            json={"processId": pid},
            timeout=self._request_timeout,
        )
        _ensure_success(response)
        return response.json()["tasksAffected"]


@final
class RemoteStore:
    def __init__(
        self,
        url: str,
        request_timeout: int = 10,
    ) -> None:
        self.url = url
        self._request_timeout = request_timeout
        self._encoder = Base64Encoder()
        self.tasks = RemoteTaskStore(
            url=self.url, request_timeout=self._request_timeout, encoder=self._encoder
        )
        self.promises = RemotePromiseStore(
            url=self.url, request_timeout=self._request_timeout, encoder=self._encoder
        )
        self.callbacks = RemoteCallbackStore(
            url=self.url, request_timeout=self._request_timeout, encoder=self._encoder
        )


def _ensure_success(resp: requests.Response) -> None:
    resp.raise_for_status()


def _encode(value: str, encoder: IEncoder[str, str]) -> str:
    try:
        return encoder.encode(value)
    except Exception as e:
        msg = "Encoder error"
        raise ResonateError(msg, "STORE_ENCODER", e) from e
