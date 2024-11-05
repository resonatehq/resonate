from __future__ import annotations

import os
import sys
import uuid

import pytest

from resonate.storage.resonate_server import (
    RemoteServer,
)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_0_callback_on_existing_promise() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    store.create(
        promise_id="0.0",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    promise_record, callback_record = store.create_callback(
        promise_id="0.0", root_promise_id="0.0", timeout=sys.maxsize, recv="default"
    )
    assert callback_record is not None
    assert promise_record.promise_id == callback_record.promise_id
    assert callback_record.timeout == sys.maxsize
    assert callback_record.promise_id == "0.0"


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_1_callback_on_non_existing_promise() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    with pytest.raises(Exception):  # noqa: B017, PT011
        store.create_callback(
            promise_id="1.1", root_promise_id="1.0", timeout=sys.maxsize, recv="default"
        )


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_2_callback_on_resolved_promise() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    store.create(
        promise_id="2.0",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    store.resolve(promise_id="2.0", ikey=None, strict=False, headers=None, data="1")
    promise_record, callback_record = store.create_callback(
        promise_id="2.0", root_promise_id="2.0", timeout=sys.maxsize, recv="default"
    )
    assert promise_record.is_completed()
    assert callback_record is None


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_3_create_durable_promise_with_task() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    pid = uuid.uuid4().hex
    ttl = 5 * 1000
    durable_promise, task_record = store.create_with_task(
        promise_id="3.0",
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
        pid=pid,
        ttl=ttl,
        recv={"type": "poll", "data": {"group": "default", "id": pid}},
    )
    assert durable_promise.promise_id == "3.0"
    assert task_record is not None
    assert task_record.counter == 1
    store.heartbeat_tasks(pid=pid)
    store.complete_task(task_id=task_record.task_id, counter=task_record.counter)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_3_create_durable_promise_with_callback() -> None:
    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    pid = uuid.uuid4().hex
    durable_promise, callback_record = store.create_with_callback(
        promise_id="4.0",
        ikey=None,
        strict=False,
        timeout=sys.maxsize,
        headers=None,
        data=None,
        tags=None,
        root_promise_id="4.0",
        recv={"type": "poll", "data": {"group": "default", "id": pid}},
    )
    assert callback_record is not None
    assert durable_promise.promise_id == callback_record.promise_id
    assert durable_promise.is_pending()
    assert durable_promise.timeout == callback_record.timeout
