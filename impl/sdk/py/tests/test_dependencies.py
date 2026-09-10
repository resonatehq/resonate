from __future__ import annotations

import msgspec
import pytest

from resonate.dependencies import DependencyMap


class Config(msgspec.Struct, frozen=True, kw_only=True):
    value: str


class Counter(msgspec.Struct, frozen=True, kw_only=True):
    count: int


# -- DependencyMap ------------------------------------------------------------


def test_insert_and_get() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="hello"))
    assert deps.get(Config).value == "hello"


def test_insert_overwrites_same_type() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="first"))
    deps.insert(Config(value="second"))
    assert deps.get(Config).value == "second"


def test_multiple_dependencies_keyed_by_type() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="multi"))
    deps.insert(Counter(count=42))
    assert deps.get(Config).value == "multi"
    assert deps.get(Counter).count == 42


def test_get_missing_raises() -> None:
    deps = DependencyMap()
    with pytest.raises(KeyError, match="with_dependency"):
        deps.get(Config)
