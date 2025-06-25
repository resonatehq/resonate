from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from resonate.errors.errors import ResonateStoreError

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.store import Store


COUNTER = 0


@pytest.fixture
def sid(store: Store) -> Generator[str]:
    global COUNTER  # noqa: PLW0603

    id = f"sid{COUNTER}"
    COUNTER += 1

    yield id

    store.schedules.delete(id)


def test_create_read_delete(store: Store, sid: str) -> None:
    schedule = store.schedules.create(sid, "0 * * * *", "foo", sys.maxsize)
    assert schedule == store.schedules.get(sid)


def test_create_twice_without_ikey(store: Store, sid: str) -> None:
    store.schedules.create(sid, "* * * * *", "foo", 10)
    with pytest.raises(ResonateStoreError):
        store.schedules.create(sid, "* * * * *", "foo", 10)


def test_create_twice_with_ikey(store: Store, sid: str) -> None:
    schedule = store.schedules.create(sid, "* * * * *", "foo", 10, ikey="foo")
    store.schedules.create(sid, "0 * 2 * *", "bar", 10, ikey="foo")
    assert schedule == store.schedules.get(sid)
