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
    child_promise = root_promise.child_promise(
        promise_id=root_promise.child_name(), action=action
    )
    assert child_promise.promise_id == "a.1"
    assert child_promise.parent_promise == root_promise
    assert child_promise.root().promise_id == root_promise.promise_id
    child_child_promise = child_promise.child_promise(
        child_promise.child_name(), action
    )
    assert child_child_promise.promise_id == "a.1.1"
    assert child_child_promise.parent_promise == child_promise
    assert child_child_promise.root().promise_id == root_promise.promise_id


def test_leaf_promises() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    root_promise: Promise[Any] = Promise(
        promise_id="a", action=action, parent_promise=None
    )
    child_promise1 = root_promise.child_promise(
        promise_id=root_promise.child_name(), action=action
    )
    child_promise2 = root_promise.child_promise(
        promise_id=root_promise.child_name(), action=action
    )

    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {child_promise1, child_promise2}
            )
        )
        == 0
    )

    leaf_promise1_1 = child_promise1.child_promise(
        promise_id=child_promise1.child_name(), action=action
    )
    leaf_promise1_2 = child_promise1.child_promise(
        promise_id=child_promise1.child_name(), action=action
    )
    leaf_promise1_3 = child_promise1.child_promise(
        promise_id=child_promise1.child_name(), action=action
    )
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {leaf_promise1_1, leaf_promise1_2, leaf_promise1_3, child_promise2}
            )
        )
        == 0
    )

    leaf_promise2_1 = child_promise2.child_promise(
        promise_id=child_promise2.child_name(), action=action
    )
    leaf_promise2_2 = child_promise2.child_promise(
        promise_id=child_promise2.child_name(), action=action
    )
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                }
            )
        )
        == 0
    )

    child_promise2_3 = child_promise2.child_promise(
        promise_id=child_promise2.child_name(), action=action
    )
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                    child_promise2_3,
                }
            )
        )
        == 0
    )
    leaf_promise2_3_1 = child_promise2_3.child_promise(
        promise_id=child_promise2_3.child_name(), action=action
    )
    leaf_promise2_3_2 = child_promise2_3.child_promise(
        promise_id=child_promise2_3.child_name(), action=action
    )
    leaf_promise2_3_3 = child_promise2_3.child_promise(
        promise_id=child_promise2_3.child_name(), action=action
    )

    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                    leaf_promise2_3_1,
                    leaf_promise2_3_2,
                    leaf_promise2_3_3,
                }
            )
        )
        == 0
    )


def test_partition_logic() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    f3: Promise[Any] = Promise("f3", action, None)
    assert f3.is_partition_root()
    assert f3.partition_root() == f3
    assert f3.root() == f3
    f2 = f3.child_promise("f2", action)
    assert not f2.is_partition_root()
    assert f2.partition_root() == f3
    assert f2.root() == f3
    f1 = f2.child_promise("f1", action)
    assert not f1.is_partition_root()
    assert f1.partition_root() == f3
    assert f1.root() == f3
    f1.mark_as_partition_root()
    assert f1.is_partition_root()
    assert f1.is_marked_as_partition_root()
    assert f1.partition_root() == f1
    assert f1.root() == f3
    f0 = f1.child_promise("f0", action)
    assert not f0.is_partition_root()
    assert f0.partition_root() == f1
    assert f1.root() == f3
    f1.unmark_as_partition_root()
    assert not f1.is_partition_root()
    assert not f1.is_marked_as_partition_root()
    assert f1.partition_root() == f3
    assert f1.root() == f3
