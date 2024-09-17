from __future__ import annotations

from resonate.context import Context


def test_inherit_deps() -> None:
    ctx = Context(ctx_id="1", seed=10)
    ctx.deps.set(key="number", obj=1)
    child = ctx.new_child(ctx_id=None)
    assert child.parent_ctx is not None, "Child must have a parent."
    assert child.parent_ctx == ctx
    assert child.deps.get("number") == 1
    assert child.seed == child.parent_ctx.seed


def test_set_name() -> None:
    ctx = Context(ctx_id="1", seed=None)
    child_1 = ctx.new_child(ctx_id=None)
    assert child_1.ctx_id == "1.1"
    child_2 = child_1.new_child("my-name")
    assert child_2.ctx_id == "my-name"
