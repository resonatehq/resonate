from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Literal, final

import requests

from resonate.encoders import Base64Encoder, IEncoder
from resonate.errors import ResonateError
from resonate.logging import logger
from resonate.stores.record import (
    CallbackRecord,
    DurablePromiseRecord,
    InvokeMsg,
    ResumeMsg,
    TaskRecord,
)
from resonate.stores.traits import IPromiseStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.typing import Data, Headers, IdempotencyKey, Tags


@final
class RemotePromiseStore(IPromiseStore):
    def __init__(
        self,
        url: str,
        call: Callable[[requests.Request], requests.Response],
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._call = call
        self._encoder = encoder

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
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        res = self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/promises",
                headers=request_headers,
                json={
                    "id": id,
                    "param": {"headers": headers, "data": self._encode_data(data)},
                    "timeout": timeout,
                    "tags": tags,
                },
            )
        )

        return DurablePromiseRecord.decode(data=res.json(), encoder=self._encoder)

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
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        res = self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/promises/task",
                headers=request_headers,
                json={
                    "promise": {
                        "id": id,
                        "param": {"headers": headers, "data": self._encode_data(data)},
                        "timeout": timeout,
                        "tags": tags,
                    },
                    "task": {
                        "processId": pid,
                        "ttl": ttl,
                    },
                },
            )
        )

        data_json = res.json()
        task_data = data_json["task"]
        durable_promise = DurablePromiseRecord.decode(
            data_json["promise"], encoder=self._encoder
        )
        if task_data is None:
            return durable_promise, None
        return durable_promise, TaskRecord.decode(task_data, encoder=self._encoder)

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None,
        strict: bool,
        headers: Headers,
        data: Data,
    ) -> DurablePromiseRecord:
        request_headers = self._initialize_headers(strict=strict, ikey=ikey)

        res = self._call(
            requests.Request(
                method="patch",
                url=f"{self.url}/promises/{id}",
                headers=request_headers,
                json={
                    "state": "RESOLVED",
                    "value": {"headers": headers, "data": self._encode_data(data)},
                },
            )
        )

        return DurablePromiseRecord.decode(data=res.json(), encoder=self._encoder)

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

        res = self._call(
            requests.Request(
                method="patch",
                url=f"{self.url}/promises/{id}",
                headers=request_headers,
                json={
                    "state": "REJECTED",
                    "value": {"headers": headers, "data": self._encode_data(data)},
                },
            )
        )

        return DurablePromiseRecord.decode(data=res.json(), encoder=self._encoder)

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

        res = self._call(
            requests.Request(
                method="patch",
                url=f"{self.url}/promises/{id}",
                headers=request_headers,
                json={
                    "state": "REJECTED_CANCELED",
                    "value": {"headers": headers, "data": self._encode_data(data)},
                },
            )
        )

        return DurablePromiseRecord.decode(data=res.json(), encoder=self._encoder)

    def get(self, *, id: str) -> DurablePromiseRecord:
        res = self._call(
            requests.Request(
                method="get",
                url=f"{self.url}/promises/{id}",
            )
        )

        return DurablePromiseRecord.decode(data=res.json(), encoder=self._encoder)

    def _initialize_headers(
        self, *, strict: bool, ikey: IdempotencyKey
    ) -> dict[str, str]:
        headers: dict[str, str] = {"Strict": str(strict)}
        if ikey is not None:
            headers["Idempotency-Key"] = ikey

        return headers

    def _encode_data(self, data: Data) -> str | None:
        if data is None:
            return None
        return _encode(data, self._encoder)


@final
class RemoteCallbackStore:
    def __init__(
        self,
        url: str,
        call: Callable[[requests.Request], requests.Response],
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._call = call
        self._encoder = encoder

    def create(
        self,
        *,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str | dict[str, Any],
    ) -> tuple[DurablePromiseRecord, CallbackRecord | None]:
        res = self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/callbacks",
                json={
                    "id": id,
                    "promiseId": promise_id,
                    "rootPromiseId": root_promise_id,
                    "timeout": timeout,
                    "recv": recv,
                },
            )
        )

        data = res.json()
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
class RemoteTaskStore:
    def __init__(
        self,
        url: str,
        call: Callable[[requests.Request], requests.Response],
        encoder: IEncoder[str, str],
    ) -> None:
        self.url = url
        self._call = call
        self._encoder = encoder

    def claim(
        self, *, task_id: str, counter: int, pid: str, ttl: int
    ) -> InvokeMsg | ResumeMsg:
        res = self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/tasks/claim",
                json={
                    "id": task_id,
                    "counter": counter,
                    "processId": pid,
                    "ttl": ttl,
                },
            )
        )

        data = res.json()
        kind: Literal["resume", "invoke"] = data["type"]
        assert kind in ("resume", "invoke")

        promises: dict[str, Any] = data["promises"]
        if promises.get("leaf") is None:
            return InvokeMsg.decode(
                data=promises["root"]["data"], encoder=self._encoder
            )

        return ResumeMsg.decode(data=promises, encoder=self._encoder)

    def complete(self, *, task_id: str, counter: int) -> None:
        self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/tasks/complete",
                json={
                    "id": task_id,
                    "counter": counter,
                },
            )
        )

    def heartbeat(self, *, pid: str) -> int:
        res = self._call(
            requests.Request(
                method="post",
                url=f"{self.url}/tasks/heartbeat",
                json={
                    "processId": pid,
                },
            )
        )

        return res.json()["tasksAffected"]


@final
class RemoteStore:
    def __init__(
        self,
        url: str | None = None,
        connect_timeout: int = 5,
        request_timeout: int = 5,
    ) -> None:
        self.url = (
            url
            if url is not None
            else os.getenv("RESONATE_SERVER", "http://localhost:8001")
        )
        self._encoder = Base64Encoder()
        self._session = requests.Session()
        self._timeout = (connect_timeout, request_timeout)

        self.promises = RemotePromiseStore(self.url, self._call, self._encoder)
        self.callbacks = RemoteCallbackStore(self.url, self._call, self._encoder)
        self.tasks = RemoteTaskStore(self.url, self._call, self._encoder)

    def _call(self, req: requests.Request) -> requests.Response:
        while True:
            try:
                res = self._session.send(req.prepare(), timeout=self._timeout)

                if res.ok:
                    return res

                # inspect the response and raise the corresponding error,
                # 5xx errors retry forever for now
                if res.status_code == requests.codes.bad_request:
                    msg = "Invalid request"
                    raise ResonateError(msg, "STORE_PAYLOAD", res.json())
                if res.status_code == requests.codes.unauthorized:
                    msg = "Unauthorized request"
                    raise ResonateError(msg, "STORE_UNAUTHORIZED", res.json())
                if res.status_code == requests.codes.forbidden:
                    msg = "Forbidden request"
                    raise ResonateError(msg, "STORE_FORBIDDEN", res.json())
                if res.status_code == requests.codes.not_found:
                    msg = "Not found"
                    raise ResonateError(msg, "STORE_NOT_FOUND", res.json())
                if res.status_code == requests.codes.conflict:
                    msg = "Already exists"
                    raise ResonateError(msg, "STORE_ALREADY_EXISTS", res.json())

                logger.warning(
                    "Unexpected response from remote store: %s",
                    res.json(),
                )
            except requests.RequestException as e:
                logger.warning("Unexpected error from remote store: %s", e)

            # wait for 1s and try again
            time.sleep(1)


def _encode(value: str, encoder: IEncoder[str, str]) -> str:
    try:
        return encoder.encode(value)
    except Exception as e:
        msg = "Encoder error"
        raise ResonateError(msg, "STORE_ENCODER", e) from e
