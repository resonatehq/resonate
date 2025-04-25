from __future__ import annotations

import logging
import os
import random
import sys
from typing import TYPE_CHECKING

import pytest

from resonate.message_sources import Poller
from resonate.stores import LocalStore, RemoteStore

if TYPE_CHECKING:
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store


def pytest_configure() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--seed", action="store")
    parser.addoption("--steps", action="store")


# DST fixtures


@pytest.fixture
def seed(request: pytest.FixtureRequest) -> str:
    seed = request.config.getoption("--seed")

    if not isinstance(seed, str):
        return str(random.randint(0, sys.maxsize))

    return seed


@pytest.fixture
def steps(request: pytest.FixtureRequest) -> int:
    steps = request.config.getoption("--steps")

    if isinstance(steps, str):
        try:
            return int(steps)
        except ValueError:
            pass

    return 10000


# Store fixtures


@pytest.fixture(
    scope="module",
    params=[LocalStore, RemoteStore] if "RESONATE_HOST" in os.environ else [LocalStore],
)
def store(request: pytest.FixtureRequest) -> Store:
    return request.param()


@pytest.fixture(scope="module")
def message_source(store: Store) -> MessageSource:
    match store:
        case LocalStore():
            return store.message_source(group="default", id="test")
        case _:
            return Poller(group="default", id="test", timeout=3)
