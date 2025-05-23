from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

import pytest

from resonate.errors import ResonateStoreError

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.message import TaskMesg
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store

TICK_TIME = 1
COUNTER = 0


@pytest.fixture
def task(store: Store, message_source: MessageSource) -> Generator[TaskMesg]:
    global COUNTER  # noqa: PLW0603

    id = f"tid{COUNTER}"
    COUNTER += 1

    store.promises.create(
        id=id,
        timeout=sys.maxsize,
        tags={"resonate:invoke": "default"},
    )

    mesg = message_source.next()
    assert mesg
    assert mesg["type"] == "invoke"

    yield mesg["task"]
    store.promises.resolve(id=id)


def test_case_5_transition_from_enqueue_to_claimed_via_claim(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task5", ttl=sys.maxsize)


def test_case_6_transition_from_enqueue_to_enqueue_via_claim(store: Store, task: TaskMesg) -> None:
    with pytest.raises(ResonateStoreError):
        store.tasks.claim(
            id=task["id"],
            counter=task["counter"] + 1,
            pid="task6",
            ttl=sys.maxsize,
        )


def test_case_8_transition_from_enqueue_to_enqueue_via_complete(store: Store, task: TaskMesg) -> None:
    with pytest.raises(ResonateStoreError):
        store.tasks.complete(
            id=task["id"],
            counter=task["counter"],
        )


def test_case_10_transition_from_enqueue_to_enqueue_via_hearbeat(store: Store, task: TaskMesg) -> None:
    assert store.tasks.heartbeat(pid="task10") == 0


def test_case_12_transition_from_claimed_to_claimed_via_claim(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task12", ttl=sys.maxsize)
    with pytest.raises(ResonateStoreError):
        store.tasks.claim(
            id=task["id"],
            counter=task["counter"],
            pid="task12",
            ttl=sys.maxsize,
        )


def test_case_13_transition_from_claimed_to_init_via_claim(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task13", ttl=0)
    with pytest.raises(ResonateStoreError):
        store.tasks.claim(
            id=task["id"],
            counter=task["counter"],
            pid="task12",
            ttl=sys.maxsize,
        )


def test_case_14_transition_from_claimed_to_completed_via_complete(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task14", ttl=sys.maxsize)
    store.tasks.complete(id=task["id"], counter=task["counter"])


def test_case_15_transition_from_claimed_to_init_via_complete(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task15", ttl=0)
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateStoreError):
        store.tasks.complete(id=task["id"], counter=task["counter"])


def test_case_16_transition_from_claimed_to_claimed_via_complete(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task16", ttl=sys.maxsize)
    with pytest.raises(ResonateStoreError):
        store.tasks.complete(id=task["id"], counter=task["counter"] + 1)


def test_case_17_transition_from_claimed_to_init_via_complete(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task17", ttl=0)
    time.sleep(TICK_TIME)
    with pytest.raises(ResonateStoreError):
        store.tasks.complete(id=task["id"], counter=task["counter"])


def test_case_18_transition_from_claimed_to_claimed_via_heartbeat(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task18", ttl=sys.maxsize)
    assert store.tasks.heartbeat(pid="task18") == 1


def test_case_19_transition_from_claimed_to_init_via_heartbeat(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task19", ttl=0)
    assert store.tasks.heartbeat(pid="task19") == 1


def test_case_20_transition_from_completed_to_completed_via_claim(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task20", ttl=sys.maxsize)
    store.tasks.complete(id=task["id"], counter=task["counter"])
    with pytest.raises(ResonateStoreError):
        store.tasks.claim(id=task["id"], counter=task["counter"], pid="task20", ttl=0)


def test_case_21_transition_from_completed_to_completed_via_complete(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task21", ttl=sys.maxsize)
    store.tasks.complete(id=task["id"], counter=task["counter"])
    store.tasks.complete(id=task["id"], counter=task["counter"])


def test_case_22_transition_from_completed_to_completed_via_heartbeat(store: Store, task: TaskMesg) -> None:
    store.tasks.claim(id=task["id"], counter=task["counter"], pid="task22", ttl=sys.maxsize)
    store.tasks.complete(id=task["id"], counter=task["counter"])
    assert store.tasks.heartbeat(pid="task22") == 0
