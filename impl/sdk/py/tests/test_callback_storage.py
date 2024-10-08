from __future__ import annotations

import os
import sys
from functools import cache

import pytest

from resonate.storage import (
    IPromiseStore,
    LocalPromiseStore,
    MemoryStorage,
    RemotePromiseStore,
)


@cache
def _promise_storages() -> list[IPromiseStore]:
    stores: list[IPromiseStore] = [LocalPromiseStore(MemoryStorage())]
    if os.getenv("RESONATE_STORE_URL") is not None:
        stores.append(RemotePromiseStore(url=os.environ["RESONATE_STORE_URL"]))
    return stores


@pytest.mark.parametrize("store", _promise_storages())
def test_case_0_callback_on_existing_promise(
    store: IPromiseStore,
) -> None:
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
    assert callback_record.callback_id == "1"
    assert callback_record.timeout == sys.maxsize
    assert callback_record.promise_id == "0.0"


@pytest.mark.parametrize("store", _promise_storages())
def test_case_1_callback_on_non_existing_promise(
    store: IPromiseStore,
) -> None:
    with pytest.raises(Exception):  # noqa: B017, PT011
        store.create_callback(
            promise_id="1.1", root_promise_id="1.0", timeout=sys.maxsize, recv="default"
        )


@pytest.mark.parametrize("store", _promise_storages())
def test_case_2_callback_on_resolved_promise(
    store: IPromiseStore,
) -> None:
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
