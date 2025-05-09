from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

import requests
from requests import PreparedRequest, Request, Session

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
        timeout: int | tuple[int, int] = 5,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._host = host or os.getenv("RESONATE_HOST_STORE", os.getenv("RESONATE_HOST", "http://localhost"))
        self._port = port or os.getenv("RESONATE_PORT_STORE", "8001")
        self._encoder = encoder or ChainEncoder(JsonEncoder(), Base64Encoder())
        self._timeout = timeout
        self._retry_policy = retry_policy or Constant(delay=1, max_retries=3)

        self._promises = RemotePromiseStore(self)
        self._tasks = RemoteTaskStore(self)

    @property
    def url(self) -> str:
        return f"{self._host}:{self._port}"

    @property
    def encoder(self) -> Encoder[Any, str | None]:
        return self._encoder

    @property
    def promises(self) -> RemotePromiseStore:
        return self._promises

    @property
    def tasks(self) -> RemoteTaskStore:
        return self._tasks

    def call(self, req: PreparedRequest) -> Any:
        attempt = 0

        with Session() as s:
            while True:
                delay = self._retry_policy.next(attempt)
                attempt += 1

                try:
                    res = s.send(req, timeout=self._timeout)
                    res.raise_for_status()
                    data = res.json()
                except requests.exceptions.HTTPError as e:
                    try:
                        error = e.response.json()["error"]
                    except Exception:
                        error = {"message": e.response.text, "code": 0}

                    # Only a 500 response code should be retried
                    if delay is None or e.response.status_code != 500:
                        raise ResonateStoreError(**error) from e
                except requests.exceptions.Timeout as e:
                    if delay is None:
                        raise ResonateStoreError(message="Request timed out", code=0) from e
                except requests.exceptions.ConnectionError as e:
                    if delay is None:
                        raise ResonateStoreError(message="Failed to connect", code=0) from e
                except Exception as e:
                    if delay is None:
                        raise ResonateStoreError(message="Unknown exception", code=0) from e
                else:
                    return data

                time.sleep(delay)


class RemotePromiseStore:
    def __init__(self, store: RemoteStore) -> None:
        self._store = store

    def _headers(self, *, strict: bool, ikey: str | None) -> dict[str, str]:
        headers: dict[str, str] = {"strict": str(strict)}
        if ikey is not None:
            headers["idempotency-Key"] = ikey
        return headers

    def get(self, id: str) -> DurablePromise:
        req = Request(
            method="get",
            url=f"{self._store.url}/promises/{id}",
        )

        res = self._store.call(req.prepare())
        return DurablePromise.from_dict(self._store, res)

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
    ) -> DurablePromise:
        req = Request(
            method="post",
            url=f"{self._store.url}/promises",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "id": id,
                "param": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
                "timeout": timeout,
                "tags": tags or {},
            },
        )
        res = self._store.call(req.prepare())
        return DurablePromise.from_dict(self._store, res)

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
    ) -> tuple[DurablePromise, Task | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/promises/task",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "promise": {
                    "id": id,
                    "param": {
                        "headers": headers or {},
                        "data": self._store.encoder.encode(data),
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

        res = self._store.call(req.prepare())
        promise = res["promise"]
        task = res.get("task")

        return (
            DurablePromise.from_dict(self._store, promise),
            Task.from_dict(self._store, task) if task else None,
        )

    def resolve(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "RESOLVED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = self._store.call(req.prepare())
        return DurablePromise.from_dict(self._store, res)

    def reject(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = self._store.call(req.prepare())
        return DurablePromise.from_dict(self._store, res)

    def cancel(
        self,
        id: str,
        *,
        ikey: str | None = None,
        strict: bool = False,
        headers: dict[str, str] | None = None,
        data: Any = None,
    ) -> DurablePromise:
        req = Request(
            method="patch",
            url=f"{self._store.url}/promises/{id}",
            headers=self._headers(strict=strict, ikey=ikey),
            json={
                "state": "REJECTED_CANCELED",
                "value": {
                    "headers": headers or {},
                    "data": self._store.encoder.encode(data),
                },
            },
        )

        res = self._store.call(req.prepare())
        return DurablePromise.from_dict(self._store, res)

    def callback(
        self,
        id: str,
        promise_id: str,
        root_promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/callbacks",
            json={
                "id": id,
                "promiseId": promise_id,
                "rootPromiseId": root_promise_id,
                "timeout": timeout,
                "recv": recv,
            },
        )

        res = self._store.call(req.prepare())
        promise = res["promise"]
        callback = res.get("callback")

        return (
            DurablePromise.from_dict(self._store, promise),
            Callback.from_dict(callback) if callback else None,
        )

    def subscribe(
        self,
        id: str,
        promise_id: str,
        timeout: int,
        recv: str,
    ) -> tuple[DurablePromise, Callback | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/subscriptions",
            json={
                "id": id,
                "promiseId": promise_id,
                "timeout": timeout,
                "recv": recv,
            },
        )

        res = self._store.call(req.prepare())
        promise = res["promise"]
        callback = res.get("callback")

        return (
            DurablePromise.from_dict(self._store, promise),
            Callback.from_dict(callback) if callback else None,
        )


class RemoteTaskStore:
    def __init__(self, store: RemoteStore) -> None:
        self._store = store

    def claim(
        self,
        id: str,
        counter: int,
        pid: str,
        ttl: int,
    ) -> tuple[DurablePromise, DurablePromise | None]:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/claim",
            json={
                "id": id,
                "counter": counter,
                "processId": pid,
                "ttl": ttl,
            },
        )

        res = self._store.call(req.prepare())
        root = res["promises"]["root"]["data"]
        leaf = res["promises"].get("leaf", {}).get("data")

        return (
            DurablePromise.from_dict(self._store, root),
            DurablePromise.from_dict(self._store, leaf) if leaf else None,
        )

    def complete(
        self,
        id: str,
        counter: int,
    ) -> bool:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/complete",
            json={
                "id": id,
                "counter": counter,
            },
        )

        self._store.call(req.prepare())
        return True

    def heartbeat(
        self,
        pid: str,
    ) -> int:
        req = Request(
            method="post",
            url=f"{self._store.url}/tasks/heartbeat",
            json={
                "processId": pid,
            },
        )

        res = self._store.call(req.prepare())
        return res["tasksAffected"]
