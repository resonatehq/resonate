"""Golden files: the wire format and the execution tree, pinned as artifacts.

Both are complex structures whose *exact* shape matters and whose hand-written
assertions would be brittle and unreadable. The protocol envelope is the more
valuable of the two: it is a contract with a server in another repository, so
an accidental change to a tag, a field name, or a nesting level is the kind of
bug that only shows up in integration. Committing the bytes turns it into a
reviewable diff.

Regenerate with ``RESONATE_UPDATE_GOLDEN=1 uv run pytest`` -- then read the
diff. An unreviewed golden proves nothing.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING

import msgspec
import pytest
from resonate_testing import StubNetwork, assert_golden, envelope, golden as golden_mod

from resonate.context import Context
from resonate.send import Sender
from resonate.testing import FAR_FUTURE, FakeClock, local_effects
from resonate.transport import Transport
from resonate.tree import NodeType, Tree
from resonate.types import (
    PromiseCreateReq,
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    Value,
)

if TYPE_CHECKING:
    from pathlib import Path


def _pretty(body: str) -> str:
    """Render a request body deterministically for diffing."""
    return json.dumps(json.loads(body), indent=2, sort_keys=True)


def _sender(net: StubNetwork) -> Sender:
    # A pinned corrId: the one field that is otherwise nondeterministic.
    return Sender(Transport(net), None, corr_id=lambda: "sr-golden")


# ═══════════════════════════════════════════════════════════════
#  Protocol envelopes
# ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_task_create_envelope() -> None:
    net = StubNetwork(
        envelope(
            "task.create",
            "sr-golden",
            {
                "task": {"id": "t1", "state": "acquired", "version": 1},
                "promise": {
                    "id": "wf-1",
                    "state": "pending",
                    "timeoutAt": FAR_FUTURE,
                    "param": {},
                    "value": {},
                    "tags": {},
                },
            },
        )
    )
    await _sender(net).task_create(
        "worker-1",
        60_000,
        PromiseCreateReq(
            id="wf-1",
            timeout_at=FAR_FUTURE,
            param=Value(data="eyJmdW5jIjoiZ3JlZXQifQ=="),
            tags={
                "resonate:origin": "wf-1",
                "resonate:branch": "wf-1",
                "resonate:parent": "wf-1",
                "resonate:scope": "global",
                "resonate:target": "poll://any@default",
            },
        ),
    )
    assert_golden("wire/task_create.json", _pretty(net.sent[0]))


@pytest.mark.asyncio
async def test_task_fence_create_envelope() -> None:
    """The lease-gated child create -- the SDK's most structural request."""
    net = StubNetwork(
        envelope(
            "task.fence",
            "sr-golden",
            {
                "action": {
                    "data": {
                        "promise": {
                            "id": "wf-1.1",
                            "state": "pending",
                            "timeoutAt": FAR_FUTURE,
                            "param": {},
                            "value": {},
                            "tags": {},
                        }
                    }
                }
            },
        )
    )
    await _sender(net).task_fence_create(
        "t1",
        3,
        PromiseCreateReq(
            id="wf-1.1",
            timeout_at=FAR_FUTURE,
            param=Value(),
            tags={
                "resonate:scope": "local",
                "resonate:branch": "wf-1",
                "resonate:parent": "wf-1",
                "resonate:origin": "wf-1",
            },
        ),
    )
    assert_golden("wire/task_fence_create.json", _pretty(net.sent[0]))


@pytest.mark.asyncio
async def test_task_fulfill_envelope() -> None:
    net = StubNetwork(
        envelope(
            "task.fulfill",
            "sr-golden",
            {
                "promise": {
                    "id": "wf-1",
                    "state": "resolved",
                    "timeoutAt": FAR_FUTURE,
                    "param": {},
                    "value": {},
                    "tags": {},
                }
            },
        )
    )
    await _sender(net).task_fulfill(
        "t1", 3, PromiseSettleReq(id="wf-1", state="resolved", value=Value(data="NDI="))
    )
    assert_golden("wire/task_fulfill.json", _pretty(net.sent[0]))


@pytest.mark.asyncio
async def test_task_suspend_envelope() -> None:
    net = StubNetwork(envelope("task.suspend", "sr-golden", {}))
    await _sender(net).task_suspend(
        "t1",
        3,
        [
            PromiseRegisterCallbackData(awaited="wf-1.1", awaiter="t1"),
            PromiseRegisterCallbackData(awaited="wf-1.2", awaiter="t1"),
        ],
    )
    assert_golden("wire/task_suspend.json", _pretty(net.sent[0]))


@pytest.mark.asyncio
async def test_auth_header_is_carried_on_every_envelope() -> None:
    net = StubNetwork(envelope("task.release", "sr-golden", {}))
    sender = Sender(Transport(net), "secret-token", corr_id=lambda: "sr-golden")
    await sender.task_release("t1", 1)
    assert_golden("wire/task_release_authed.json", _pretty(net.sent[0]))


@pytest.mark.asyncio
async def test_child_promise_tags_for_each_durable_op() -> None:
    """One golden covering the tag scheme every durable op emits.

    The tags *are* the SDK's contract with the server's id and routing model
    (origin, branch, parent, scope, timer). A change to any of them is a
    protocol change, and this is where it surfaces.
    """
    net = StubNetwork()
    clock = FakeClock(start=1_700_000_000_000)
    ctx = Context.root(
        id="wf-1",
        origin_id="wf-1",
        timeout_at=FAR_FUTURE,
        func_name="wf",
        effects=local_effects(task_id="wf-1"),
        target_resolver=lambda t: t or "poll://any@default",
        deps=__import__(
            "resonate.dependencies", fromlist=["DependencyMap"]
        ).DependencyMap(),
        clock=clock,
    )

    requests: list[PromiseCreateReq] = [
        ctx._global_req("wf-1.1", timedelta(minutes=5), target="poll://any@w"),
        ctx._global_req("wf-1.2", timedelta(seconds=30), timer=True, target="poll://x"),
        ctx._global_req("wf-1.3", None),
        ctx._global_req("wf-1:dabc", timedelta(hours=1), parent="wf-1"),
    ]
    rendered = json.dumps(
        [msgspec.json.decode(msgspec.json.encode(r)) for r in requests],
        indent=2,
        sort_keys=True,
    )
    assert_golden("wire/child_promise_requests.json", rendered)
    assert net.sent == []


# ═══════════════════════════════════════════════════════════════
#  Execution trees
# ═══════════════════════════════════════════════════════════════


def _tree(spec: list[tuple[str, str, NodeType]], settled: tuple[str, ...] = ()) -> Tree:
    tree = Tree(root_id="wf")
    for parent, child, kind in spec:
        tree.add_child(parent, child, kind)
    for id in settled:
        tree.settle(id)
    return tree


def test_tree_fanout_shape() -> None:
    """Three local children under one root -- the ``ctx.run`` fan-out."""
    tree = _tree(
        [("wf", "wf.1", "int"), ("wf", "wf.2", "int"), ("wf", "wf.3", "int")],
        settled=("wf.1",),
    )
    assert_golden("tree/fanout.txt", tree.print())


def test_tree_mixed_kinds_shape() -> None:
    """Local, remote and detached children side by side, at two depths."""
    tree = _tree(
        [
            ("wf", "wf.1", "int"),
            ("wf.1", "wf.1.1", "ext"),
            ("wf", "wf.2", "ext"),
            ("wf", "wf:dab12", "det"),
            ("wf:dab12", "wf:dab12.1", "int"),
        ],
        settled=("wf.1.1",),
    )
    assert_golden("tree/mixed_kinds.txt", tree.print())


def test_tree_deep_chain_shape() -> None:
    """A recursive workflow: each level spawns exactly one child."""
    spec: list[tuple[str, str, NodeType]] = []
    parent = "wf"
    for depth in range(1, 6):
        child = f"{parent}.{depth}"
        spec.append((parent, child, "int"))
        parent = child
    assert_golden("tree/deep_chain.txt", _tree(spec).print())


def test_tree_frontier_of_the_mixed_shape() -> None:
    """The frontier drives suspension; pin it next to the shape it comes from."""
    tree = _tree(
        [
            ("wf", "wf.1", "int"),
            ("wf.1", "wf.1.1", "ext"),
            ("wf", "wf.2", "ext"),
            ("wf", "wf:dab12", "det"),
        ]
    )
    rendered = "\n".join(sorted(tree.frontier()))
    assert_golden("tree/mixed_kinds.frontier.txt", rendered)


# ═══════════════════════════════════════════════════════════════
#  The harness itself
# ═══════════════════════════════════════════════════════════════


def test_assert_golden_fails_loudly_on_a_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A temp dir, never the committed goldens: a meta-test that can rewrite a
    # real golden would silently launder a genuine regression into a pass.
    monkeypatch.delenv("RESONATE_UPDATE_GOLDEN", raising=False)
    monkeypatch.setattr(golden_mod, "GOLDEN_DIR", tmp_path)
    (tmp_path / "pinned.txt").write_text("the expected value\n")

    with pytest.raises(AssertionError, match="mismatch"):
        golden_mod.assert_golden("pinned.txt", "something else")


def test_assert_golden_writes_and_fails_for_a_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first run records the output and *fails*, prompting a human read."""
    monkeypatch.delenv("RESONATE_UPDATE_GOLDEN", raising=False)
    monkeypatch.setattr(golden_mod, "GOLDEN_DIR", tmp_path)

    with pytest.raises(AssertionError, match="did not exist"):
        golden_mod.assert_golden("brand_new.txt", "hello")
    assert (tmp_path / "brand_new.txt").read_text() == "hello\n"

    # Second call now passes against the recorded file.
    golden_mod.assert_golden("brand_new.txt", "hello")


def test_update_mode_rewrites_without_failing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESONATE_UPDATE_GOLDEN", "1")
    monkeypatch.setattr(golden_mod, "GOLDEN_DIR", tmp_path)

    golden_mod.assert_golden("regen.txt", "first")
    golden_mod.assert_golden("regen.txt", "second")
    assert (tmp_path / "regen.txt").read_text() == "second\n"
