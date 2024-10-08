from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate.actions import LFI
from resonate.dataclasses import FnOrCoroutine
from resonate.promise import Promise

if TYPE_CHECKING:
    from resonate.context import Context


def _foo(ctx: Context) -> None: ...
def test_child_creation() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    root_promise: Promise[Any] = Promise(
        promise_id="a", action=action, parent_promise=None
    )
    child_promise = root_promise.child_promise(promise_id=None, action=action)
    assert child_promise.promise_id == "a.1"
    assert child_promise.parent_promise == root_promise
    assert child_promise.root_promise_id == root_promise.promise_id
    child_child_promise = child_promise.child_promise(None, action)
    assert child_child_promise.promise_id == "a.1.1"
    assert child_child_promise.parent_promise == child_promise
    assert child_child_promise.root_promise_id == root_promise.promise_id
