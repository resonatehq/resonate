from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

import requests
from requests import PreparedRequest, Request, Response, Session

from resonate.encoders import Base64Encoder, ChainEncoder, JsonEncoder
from resonate.errors import ResonateStoreError
from resonate.models.callback import Callback
from resonate.models.durable_promise import DurablePromise
from resonate.models.task import Task
from resonate.retry_policies import Constant

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
    from resonate.models.retry_policy import RetryPolicy


class RemoteStore:
    def __init__(
        self,
        host: str | None = None,
        port: str | None = None,
        encoder: Encoder[Any, str | None] | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.host = host or os.getenv("RESONATE_HOST_STORE", os.getenv("RESONATE_HOST", "http://localhost"))
        self.port = port or os.getenv("RESONATE_PORT_STORE", "8001")
        self.encoder = encoder or ChainEncoder(
            JsonEncoder(),
            Base64Encoder(),
        )
        self.retry_policy = retry_policy or Constant(delay=1, max_retries=3)

    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def promises(self) -> RemotePromiseStore:
        return RemotePromiseStore(self, self.encoder)

    @property
    def tasks(self) -> RemoteTaskStore:
        return RemoteTaskStore(self, self.encoder)

    def call(self, req: PreparedRequest) -> Response:
        attempt = 0
        with Session() as s:
            while True:
                try:
                    res = s.send(req, timeout=10)
                    if res.ok:
                        return res

                    try:
                        data = res.json()
                    except Exception:
                        data = res.text

                    match res.status_code:
                        case _status_codes.BAD_REQUEST:
                            msg = f"Invalid Request: {data}"
                            raise ResonateStoreError(msg, "STORE_PAYLOAD")
                        case _status_codes.UNAUTHORIZED:
                            msg = f"Unauthorized request: {data}"
                            raise ResonateStoreError(msg, "STORE_UNAUTHORIZED")
                        case _status_codes.FORBIDDEN:
                            msg = f"Forbidden request: {data}"
                            raise ResonateStoreError(msg, "STORE_FORBIDDEN")
                        case _status_codes.NOT_FOUND:
                            msg = f"Not found: {data}"
                            raise ResonateStoreError(msg, "STORE_NOT_FOUND")
                        case _status_codes.CONFLICT:
                            msg = f"Already exists: {data}"
                            raise ResonateStoreError(msg, "STORE_ALREADY_EXISTS")
                        case _status_codes.SERVER_ERROR:
                            attempt += 1

                            delay = self.retry_policy.next(attempt)
                            if delay is None:
                                msg = f"Unexpected error on the server {res.status_code} {res.reason}: {data}"
                                raise ResonateStoreError(msg, "UNKNOWN")

                            time.sleep(delay)
                            continue

                        case _:
                            msg = f"Unknow error {res.status_code} {res.reason}: {data}"
                            raise ResonateStoreError(msg, "UNKNOWN")

                except requests.exceptions.RequestException:
                    attempt += 1

                    delay = self.retry_policy.next(attempt)
                    if delay is None:
                        raise

                    time.sleep(delay)


class RemotePromiseStore:
    def __init__(self, store: RemoteStore, encoder: Encoder[Any, str | None]) -> None:
        self.store = store
        self.encoder = encoder

    def _headers(self, *, strict: bool, ikey: str | None) -> dict[str, str]:
        headers: dict[str, str] = {"strict": str(strict)}
        if ikey is not None:
            headers["idempotency-Key"] = ikey
        return headers

    def get(self, *, id: str) -> DurablePromise:
        req = Request(
            method="get",
            url=f"{self.store.url}/promises/{id}",
        )
        res = self.store.call(req.prepare()).json()
        return DurablePromise.from_dict(
            self.store,
            res,
        )

    def create(
        self,
        *,
        id: str,
        timeout: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> DurablePromise:
        req = Request(
            method="post",
            url=f"{self.store.url}/promises",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "id": id,
                "param": {
                    "headers": headers or {},
                    "data": self.encoder.encode(data),
                },
                "timeout": timeout,
                "tags": tags or {},
            },
        )

        res = self.store.call(req.prepare()).json()

        return DurablePromise.from_dict(
            self.store,
            res,
        )

    def create_with_task(
        self,
        *,
        id: str,
        timeout: int,
        pid: str,
        ttl: int,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> tuple[DurablePromise, Task | None]:
        req = Request(
            method="post",
            url=f"{self.store.url}/promises/task",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "promise": {
                    "id": id,
                    "param": {
                        "headers": headers or {},
                        "data": self.encoder.encode(data),
                    },
                    "timeout": timeout,
                    "tags": tags or {},
                },
                "task": {
                    "processId": pid,
                    "ttl": ttl,
                },
            },
        )
        res = self.store.call(req.prepare()).json()

        promise = DurablePromise.from_dict(
            self.store,
            res["promise"],
        )
        task_dict = res.get("task")
        task = Task.from_dict(self.store, task_dict) if task_dict is not None else None
        return promise, task

    def resolve(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self.store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "RESOLVED",
                "value": {
                    "headers": headers or {},
                    "data": self.encoder.encode(data),
                },
            },
        )

        res = self.store.call(req.prepare()).json()

        return DurablePromise.from_dict(
            self.store,
            res,
        )

    def reject(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self.store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED",
                "value": {
                    "headers": headers or {},
                    "data": self.encoder.encode(data),
                },
            },
        )

        res = self.store.call(req.prepare()).json()

        return DurablePromise.from_dict(
            self.store,
            res,
        )

    def cancel(
        self,
        *,
        id: str,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self.store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED_CANCELED",
                "value": {
                    "headers": headers or {},
                    "data": self.encoder.encode(data),
                },
            },
        )

        res = self.store.call(req.prepare()).json()

        return DurablePromise.from_dict(
            self.store,
            res,
        )

    def callback(
        self,
        *,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        req = Request(
            method="post",
            url=f"{self.store.url}/callbacks",
            json={
                "id": id,
                "promiseId": promise_id,
                "rootPromiseId": root_promise_id,
                "timeout": timeout,
                "recv": recv,
            },
        )
        res = self.store.call(req.prepare()).json()

        callback_dict = res.get("callback", None)

        return DurablePromise.from_dict(
            self.store,
            res["promise"],
        ), Callback.from_dict(callback_dict) if callback_dict is not None else None

    def subscribe(
        self,
        *,
        id: str,
        promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        req = Request(
            method="post",
            url=f"{self.store.url}/subscriptions",
            json={
                "id": id,
                "promiseId": promise_id,
                "timeout": timeout,
                "recv": recv,
            },
        )
        res = self.store.call(req.prepare()).json()
        callback_dict = res.get("callback", None)

        return DurablePromise.from_dict(
            self.store,
            res["promise"],
        ), Callback.from_dict(callback_dict) if callback_dict is not None else None


class RemoteTaskStore:
    def __init__(self, store: RemoteStore, encoder: Encoder[Any, str | None]) -> None:
        self.store = store
        self.encoder = encoder

    def claim(
        self,
        *,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        req = Request(
            method="post",
            url=f"{self.store.url}/tasks/claim",
            json={
                "id": id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
        )

        res = self.store.call(req.prepare()).json()

        promises_dict = res["promises"]
        root_dict = promises_dict["root"]["data"]
        root = DurablePromise.from_dict(
            self.store,
            root_dict,
        )

        leaf_dict = promises_dict.get("leaf", {}).get("data", None)
        leaf = (
            DurablePromise.from_dict(
                self.store,
                leaf_dict,
            )
            if leaf_dict
            else None
        )

        return (root, leaf)

    def complete(self, *, id: str, counter: int) -> bool:
        req = Request(
            method="post",
            url=f"{self.store.url}/tasks/complete",
            json={
                "id": id,
                "counter": counter,
            },
        )
        self.store.call(req.prepare())
        return True

    def heartbeat(
        self,
        *,
        pid: str,
    ) -> int:
        req = Request(
            method="post",
            url=f"{self.store.url}/tasks/heartbeat",
            json={
                "processId": pid,
            },
        )

        res = self.store.call(req.prepare()).json()

        return res["tasksAffected"]


class _status_codes:  # noqa: N801
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    CONFLICT = 409
    SERVER_ERROR = 500
