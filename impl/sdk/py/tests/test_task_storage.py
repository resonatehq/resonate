from __future__ import annotations

import json
import os
import sys
import uuid

import pytest

from resonate.stores.remote import (
    RemoteStore,
)


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_0_callback_on_existing_promise() -> None:
    store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
    id = "0.0"
    store.promises.create(
        id=id,
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    promise_record, callback_record = store.callbacks.create(
        id=id + id,
        promise_id=id,
        root_promise_id=id,
        timeout=sys.maxsize,
        recv="default",
    )
    assert callback_record is not None
    assert promise_record.id == callback_record.id
    assert callback_record.timeout == sys.maxsize
    assert callback_record.id == id


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_1_callback_on_non_existing_promise() -> None:
    store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
    id = "1.1"
    with pytest.raises(Exception):  # noqa: B017, PT011
        store.callbacks.create(
            id=id + id,
            promise_id=id,
            root_promise_id=id,
            timeout=sys.maxsize,
            recv="default",
        )


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_2_callback_on_resolved_promise() -> None:
    store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
    id = "2.0"
    store.promises.create(
        id=id,
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags=None,
    )
    store.promises.resolve(id=id, ikey=None, strict=False, headers=None, data="1")
    promise_record, callback_record = store.callbacks.create(
        id=id + id,
        promise_id=id,
        root_promise_id=id,
        timeout=sys.maxsize,
        recv="default",
    )
    assert promise_record.is_completed()
    assert callback_record is None


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_case_3_create_durable_promise_with_task() -> None:
    store = RemoteStore(url=os.environ["RESONATE_STORE_URL"])
    pid = uuid.uuid4().hex
    ttl = 5 * 1000
    id = "3.0"
    durable_promise, task_record = store.promises.create_with_task(
        id=id,
        ikey=None,
        strict=False,
        headers=None,
        data=None,
        timeout=sys.maxsize,
        tags={
            "resonate:invoke": json.dumps(
                {"type": "poll", "data": {"group": "default", "id": pid}}
            )
        },
        pid=pid,
        ttl=ttl,
    )
    assert durable_promise.id == id
    assert task_record is not None
    assert task_record.counter == 1
    store.tasks.heartbeat(pid=pid)
    store.tasks.complete(task_id=task_record.task_id, counter=task_record.counter)
