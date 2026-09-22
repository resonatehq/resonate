"""Compliance tests for the promise id format the server enforces.

The server (resonatehq/resonate, ``new promise id`` / PR #1127) treats a promise
id as ``<origin>:<lineage>``: the origin is everything before the first ``:``
and the lineage segments below it are ``.``-separated::

    root -> root:1 -> root:1.1 -> root:1.1.1

:func:`server_validate` is a direct port of the server's
``validate_promise_create_data``, and :func:`origin` of its ``origin()`` helper.
Every promise the SDK creates is replayed through them here, so a drift in id
minting fails locally instead of as a 400 from a real server.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from resonate.connections import LocalConnection
from resonate.error import InvalidIdError, ServerError
from resonate.ids import join_id, origin_of, validate_root_id
from resonate.resonate import Resonate
from resonate.retry import Never

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from resonate.context import Context


# ── The server's rules, ported ───────────────────────────────────────────────


def origin(id: str) -> str:
    """Return the origin, per the server's ``origin()``: text before the first ``:``."""
    return id.split(":", 1)[0]


def server_validate(id: str, tags: Mapping[str, str]) -> None:
    """Port of the server's ``validate_promise_create_data``."""
    assert "\0" not in id, "null_bytes"

    if (o := tags.get("resonate:origin")) is not None:
        assert ":" not in o, f"colon_in_origin: origin={o!r}"
        assert id == o or id.startswith(f"{o}:"), (
            f"origin_prefix: id={id!r} is not prefixed by origin={o!r}"
        )
    for key in ("resonate:branch", "resonate:parent"):
        if (ancestor := tags.get(key)) is not None:
            # A bare root joins its first lineage segment with ':'; an ancestor
            # that already carries lineage joins deeper segments with '.'.
            sep = "." if ":" in ancestor else ":"
            assert id == ancestor or id.startswith(f"{ancestor}{sep}"), (
                f"{key}_prefix: id={id!r} is not prefixed by {key}={ancestor!r}"
            )
    if (p := tags.get("resonate:prefix")) is not None:
        assert "." not in p, f"dot_in_prefix: prefix={p!r}"


# ── Workflow under test ──────────────────────────────────────────────────────


async def leaf(ctx: Context, n: int) -> int:
    return n


async def grandchild(ctx: Context, n: int) -> int:
    await ctx.run(leaf, n)
    return n


async def tail(ctx: Context, n: int) -> int:
    return n


async def detaches_again(ctx: Context, n: int) -> int:
    # A detached child that itself detaches -- the recursion-bounding case.
    await ctx.detached("tail", n)
    return n


async def mid(ctx: Context, n: int) -> int:
    await ctx.run(grandchild, n)
    # A global-scope (bare) promise: minted from the same seq as everything else.
    await ctx.sleep(timedelta(seconds=0))
    # Detached from a *nested* context: its id is minted off the origin, not
    # off this context, so its declared ancestors must be the origin too.
    await ctx.detached("detaches_again", n)
    return n


async def top(ctx: Context, n: int) -> int:
    await ctx.run(mid, n)
    await ctx.run(mid, n + 1)
    return n


@contextlib.asynccontextmanager
async def _workflow(id: str) -> AsyncIterator[dict[str, Any]]:
    """Run the workflow above and yield the local server's promise table."""
    r = Resonate(retry_policy=Never())
    for fn in (top, mid, grandchild, leaf, detaches_again, tail):
        r.register(fn)
    try:
        await r.run(id, top, 1).result()
        # Let the fire-and-forget detached children be dispatched and run.
        for _ in range(100):
            await asyncio.sleep(0)
        net = r._network
        assert isinstance(net, LocalConnection)
        yield dict(net.state.promises)
    finally:
        await r.stop()


# ── Tests ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("root", ["wf", "my.app.workflow"])
async def test_every_created_promise_passes_server_validation(root: str) -> None:
    # A dotted root is a caller's prerogative: '.' is only read below the
    # origin, so every id minted under it still validates.
    async with _workflow(root) as promises:
        assert len(promises) > 1
        for id, promise in promises.items():
            server_validate(id, promise.tags)
        assert {origin(id) for id in promises} == {root}


@pytest.mark.asyncio
async def test_whole_workflow_shares_one_origin() -> None:
    # The origin is the server's partition key and the unit both
    # ``promise.register_callback`` and ``task.suspend`` match on, so every
    # promise a workflow creates -- detached children included -- must share it.
    async with _workflow("wf") as promises:
        assert {origin(id) for id in promises} == {"wf"}
        assert {p.tags["resonate:origin"] for p in promises.values()} == {"wf"}


@pytest.mark.asyncio
async def test_child_ids_are_colon_then_dot_separated() -> None:
    async with _workflow("wf") as promises:
        assert "wf" in promises
        # First level below the root joins with ':', deeper levels with '.'.
        assert "wf:1" in promises
        assert "wf:1.1" in promises
        assert "wf:1.1.1" in promises
        # No id keeps the old all-'.' shape.
        assert not [id for id in promises if id.startswith("wf.")]


@pytest.mark.asyncio
async def test_detached_ids_stay_bounded_below_the_origin() -> None:
    # Detached ids are ``{origin}:d{16 hex}`` -- one segment past the origin no
    # matter how deep the spawning context is, or how many times a detached
    # child detaches again.
    async with _workflow("wf") as promises:
        detached = [id for id in promises if id.startswith("wf:d")]
        assert len(detached) == 4  # 2x mid, each detaching a child that detaches
        for id in detached:
            _, _, suffix = id.partition(":")
            assert len(suffix) == 17
            assert all(c in "0123456789abcdef" for c in suffix[1:])
            assert promises[id].tags["resonate:parent"] == "wf"
            assert promises[id].tags["resonate:branch"] == id


@pytest.mark.asyncio
async def test_prefix_tag_is_not_emitted() -> None:
    async with _workflow("wf") as promises:
        for promise in promises.values():
            assert "resonate:prefix" not in promise.tags


def test_join_id_matches_the_servers_separator_rule() -> None:
    assert join_id("root", "1") == "root:1"
    assert join_id("root:1", "2") == "root:1.2"
    assert join_id("root:1.2", "3") == "root:1.2.3"
    assert join_id("root", "dbeef") == "root:dbeef"


def test_origin_of_matches_the_servers_origin() -> None:
    for id in ("root", "root:1", "root:1.2", "root:dbeef"):
        assert origin_of(id) == origin(id)


@pytest.mark.parametrize("id", ["a:b", "a.b:c", "", "a\0b"])
def test_validate_root_id_rejects_reserved_separators(id: str) -> None:
    # ':' is the one reserved separator in a root id: it becomes the origin of
    # its whole lineage. See the test below for what it actually breaks.
    with pytest.raises(InvalidIdError):
        validate_root_id(id)


def test_a_dot_in_a_root_id_is_accepted() -> None:
    # '.' only separates lineage segments *below* the origin, which is read
    # after the origin has been split off at the first ':'. A dotted root is
    # therefore unambiguous, and the server takes it.
    assert validate_root_id("my.app.workflow") == "my.app.workflow"
    id = join_id("my.app.workflow", "1")
    assert id == "my.app.workflow:1"
    assert origin_of(id) == "my.app.workflow"
    server_validate(id, {"resonate:origin": "my.app.workflow"})


def test_a_colon_in_a_root_id_is_rejected_by_the_server() -> None:
    # ':' cannot create the root either: a root is its own origin, and the
    # origin is everything before an id's first ':', so an origin holding one
    # is unrepresentable -- no id could ever split back to it.
    with pytest.raises(AssertionError, match="colon_in_origin"):
        server_validate("a:b", {"resonate:origin": "a:b"})


@pytest.mark.parametrize("id", ["a", "a-b", "a_b", "a.b", "wf-1786636678653183000"])
def test_validate_root_id_accepts_bare_ids(id: str) -> None:
    assert validate_root_id(id) == id


@pytest.mark.asyncio
async def test_run_and_rpc_reject_an_invalid_root_id() -> None:
    r = Resonate(retry_policy=Never())
    r.register(top)
    try:
        # Raised at the call site, before anything reaches the server.
        with pytest.raises(InvalidIdError):
            r.run("bad:id", top, 1)
        with pytest.raises(InvalidIdError):
            r.rpc("bad:id", "top", 1)
        with pytest.raises(InvalidIdError):
            await r.schedule("bad:id", "* * * * *", "top")
        # ``get`` is a lookup, not a create: it takes any id, including a
        # child's, so it must NOT validate. A missing one 404s rather than
        # raising InvalidIdError.
        with pytest.raises(ServerError):
            await r.get("wf:1.2")
    finally:
        await r.stop()
