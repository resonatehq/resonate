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
    from collections.abc import Generator

    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store


def pytest_configure() -> None:
    logging.basicConfig(level=logging.ERROR)  # set log levels very high for tests


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


@pytest.fixture
def log_level(request: pytest.FixtureRequest) -> int:
    level = request.config.getoption("--log-level")

    if isinstance(level, str) and level.isdigit():
        return int(level)

    match str(level).lower():
        case "critical":
            return logging.CRITICAL
        case "error":
            return logging.ERROR
        case "warning" | "warn":
            return logging.WARNING
        case "info":
            return logging.INFO
        case "debug":
            return logging.DEBUG
        case _:
            return logging.NOTSET


# Store fixtures


@pytest.fixture(
    scope="module",
    params=[LocalStore, RemoteStore] if "RESONATE_HOST" in os.environ else [LocalStore],
)
def store(request: pytest.FixtureRequest) -> Store:
    return request.param()


@pytest.fixture
def message_source(store: Store) -> Generator[MessageSource]:
    match store:
        case LocalStore():
            ms = store.message_source(group="default", id="test")
        case _:
            assert isinstance(store, RemoteStore)
            ms = Poller(group="default", id="test")

    # start the message source
    ms.start()

    yield ms

    # stop the message source
    ms.stop()
