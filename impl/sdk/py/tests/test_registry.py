"""Tests for :class:`resonate.registry.Registry`.

Covers explicit-name registration backed by reflection-built
:class:`~resonate.durable.DurableFunction` entries. The lookup
*name* is supplied by the caller (so it stays stable across renames of the
Python function); the registered callable must follow the Python SDK
convention of accepting a :class:`Context` as its first argument.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from resonate.error import AlreadyRegisteredError
from resonate.registry import Registry

if TYPE_CHECKING:
    from resonate.context import Context


async def leaf(ctx: Context, x: int) -> int:
    return x


async def flow(ctx: Context, x: int) -> int:
    return x


def test_register_and_get() -> None:
    r = Registry()
    r.register("leaf", leaf)
    df = r.get("leaf")
    assert df is not None
    assert df.name == "leaf"  # wrapped in a DurableFunction that remembers the name


def test_custom_name_is_independent_of_fn_name() -> None:
    r = Registry()
    r.register("custom", leaf)
    df = r.get("custom")
    # The lookup name is the registry key; the entry still remembers its source name.
    assert df is not None
    assert df.name == "leaf"


def test_get_unknown_returns_none() -> None:
    assert Registry().get("missing") is None


def test_empty_name_rejected() -> None:
    with pytest.raises(ValueError, match="name is required"):
        Registry().register("", leaf)


def test_duplicate_name_rejected() -> None:
    r = Registry()
    r.register("dup", leaf)
    with pytest.raises(AlreadyRegisteredError, match="dup"):
        r.register("dup", flow)


# ── Versioning (registry entries are keyed on name *and* version; see
#    Registry's docstring) ──────────────────────────────────────────────


def test_default_version_is_one() -> None:
    r = Registry()
    r.register("leaf", leaf)  # version defaults to 1
    assert r.get("leaf") is not None
    assert r.get("leaf", 1) is not None


def test_same_name_different_versions_coexist() -> None:
    r = Registry()
    r.register("flow", leaf, 1)
    r.register("flow", flow, 2)
    v1 = r.get("flow", 1)
    v2 = r.get("flow", 2)
    assert v1 is not None
    assert v2 is not None
    assert v1 is not v2


def test_duplicate_name_version_rejected() -> None:
    r = Registry()
    r.register("dup", leaf, 2)
    # Same name at a different version is fine ...
    r.register("dup", flow, 3)
    # ... but the same (name, version) pair is not.
    with pytest.raises(AlreadyRegisteredError, match="version 2"):
        r.register("dup", flow, 2)


def test_unknown_version_returns_none() -> None:
    r = Registry()
    r.register("leaf", leaf, 1)
    assert r.get("leaf", 2) is None


def test_version_below_one_rejected() -> None:
    with pytest.raises(ValueError, match="version must be >= 1"):
        Registry().register("zero", leaf, 0)


# ── Reverse lookup (function object -> its registered key) ────────────────────
#    Backs the by-object forms of the durable ops: a ``run`` recovers the version
#    and an ``rpc`` recovers the dispatch name for the exact object it was given.


def test_reverse_returns_registered_key() -> None:
    r = Registry()
    r.register("custom", leaf, 2)
    # The inverse of get: the object recovers the (name, version) it was filed
    # under -- even when the registry name differs from the Python ``__name__``.
    assert r.reverse(leaf) == ("custom", 2)


def test_reverse_unknown_returns_none() -> None:
    r = Registry()
    r.register("leaf", leaf)
    assert r.reverse(flow) is None  # never registered -> caller falls back


def test_reverse_same_object_keeps_last_key() -> None:
    r = Registry()
    r.register("a", leaf, 1)
    r.register("b", leaf, 2)
    # One object filed under two keys keeps the last, matching last-writer
    # semantics for a given identity (the forward map keeps both).
    assert r.reverse(leaf) == ("b", 2)
