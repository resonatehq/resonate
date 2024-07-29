from __future__ import annotations

from resonate.context import Context


def test_inherit_deps() -> None:
    ctx = Context(ctx_id="1")
    ctx.deps.set(key="number", obj=1)
    child = ctx.new_child()
    assert child.parent_ctx is not None, "Child must have a parent."
    assert child.parent_ctx == ctx
    assert child.deps.get("number") == 1
