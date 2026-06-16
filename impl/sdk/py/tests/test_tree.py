"""Assertion-layer tests for :mod:`resonate.tree`.

The execution tree (:class:`~resonate.tree.Tree`) is a pure assertion layer: it
never drives control flow, it only *materializes* the shape an inner return
left behind so :meth:`~resonate.tree.Tree.well_formed` can check that shape
against the suspension contract (``tree.md`` §4). These tests therefore exercise
the surface specified by ``tree.md`` §12:

* the **inspection primitives** -- :meth:`~resonate.tree.Tree.root`,
  :meth:`~resonate.tree.Tree.has`, :meth:`~resonate.tree.Tree.size`,
  :meth:`~resonate.tree.Tree.ids`, :meth:`~resonate.tree.Tree.get` (defensive
  copy), :meth:`~resonate.tree.Tree.print` (deterministic ASCII diagram);
* the **mutation primitives** -- :meth:`~resonate.tree.Tree.add_child`
  (idempotent on id) and :meth:`~resonate.tree.Tree.settle` (monotonic, no-op
  on unknown);
* the **frontier walk** -- :meth:`~resonate.tree.Tree.frontier`, returning the
  ``(ext, pending)`` cell and pruning Det subtrees (§3);
* the **predicates** -- :meth:`~resonate.tree.Tree.useful` (U3 alone) and
  :meth:`~resonate.tree.Tree.well_formed` (U1/U2/U3 universal, D1 done, S1/S4
  suspended), with a *valid* and an *invalid* state for each rule.

Some invalid states (a non-Int root, an unreachable node) cannot be reached
through the public API by construction -- those are built by poking
``tree._nodes`` directly, which is exactly what an SDK bug would do to the
materialized view.
"""

from __future__ import annotations

import pytest

from resonate.tree import Node, Tree

# ── inspection primitives ────────────────────────────────────────────────────


def test_new_tree_root_is_int_pending() -> None:
    """A fresh tree is just the root, classified (int, pending) -- U1's shape."""
    t = Tree("root")
    assert t.root() == "root"
    root = t.get("root")
    assert root is not None
    assert root.type == "int"
    assert root.kind == "pending"
    assert root.children == []


def test_new_tree_has_only_root() -> None:
    """``has`` / ``size`` / ``ids`` agree on the fresh tree's single node."""
    t = Tree("root")
    assert t.has("root") is True
    assert t.has("nope") is False
    assert t.size() == 1
    assert t.ids() == ["root"]


def test_size_and_ids_grow_with_add_child() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "int")
    assert t.size() == 3
    assert set(t.ids()) == {"root", "root.1", "root.2"}
    assert t.has("root.1")
    assert t.has("root.2")


def test_get_unknown_returns_none() -> None:
    assert Tree("root").get("nope") is None


def test_get_returns_defensive_copy_not_live_reference() -> None:
    """Mutating the handle must not leak back into the tree (``tree.md`` §12)."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")

    handle = t.get("root.1")
    assert handle is not None
    # Mutate the returned copy in every way a caller could.
    handle.kind = "settled"
    handle.type = "int"
    handle.children.append("ghost")

    # The tree's own view is untouched.
    fresh = t.get("root.1")
    assert fresh is not None
    assert fresh.kind == "pending"
    assert fresh.type == "ext"
    assert fresh.children == []

    # The root's child list is not contaminated either.
    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1"]


# ── mutation: add_child ──────────────────────────────────────────────────────


def test_add_child_inserts_and_links_parent() -> None:
    t = Tree("root")
    assert t.add_child("root", "root.1", "ext") is True

    child = t.get("root.1")
    assert child is not None
    assert child.type == "ext"
    assert child.kind == "pending"

    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1"]


def test_add_child_is_idempotent_on_id() -> None:
    """A replay re-walking the same body must not duplicate nodes or re-append."""
    t = Tree("root")
    assert t.add_child("root", "root.1", "ext") is True
    # Second call -- same id, even a different type -- is a no-op returning False.
    assert t.add_child("root", "root.1", "int") is False

    child = t.get("root.1")
    assert child is not None
    assert child.type == "ext"  # type stable: first write wins

    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1"]  # not re-appended


def test_add_child_preserves_insertion_order() -> None:
    """Children are kept in call order -- the children-as-prefix replay property."""
    t = Tree("root")
    for i in (1, 2, 3):
        t.add_child("root", f"root.{i}", "ext")
    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1", "root.2", "root.3"]


def test_add_child_unknown_parent_raises() -> None:
    """Reaching ``add_child`` with an unknown parent is an SDK bug, not user input."""
    t = Tree("root")
    with pytest.raises(AssertionError):
        t.add_child("ghost", "root.1", "ext")


# ── mutation: settle ─────────────────────────────────────────────────────────


def test_settle_flips_pending_to_settled() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    child = t.get("root.1")
    assert child is not None
    assert child.kind == "settled"


def test_settle_is_idempotent_on_already_settled() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    t.settle("root.1")  # still settled, no error
    child = t.get("root.1")
    assert child is not None
    assert child.kind == "settled"


def test_settle_is_noop_on_unknown_id() -> None:
    """``tree.md`` §12: ``Settle`` is monotonic -- no-op if already settled or unknown."""
    t = Tree("root")
    t.settle("ghost")  # no raise
    assert t.size() == 1  # tree untouched


# ── frontier ─────────────────────────────────────────────────────────────────


def test_frontier_empty_for_bare_root() -> None:
    assert Tree("root").frontier() == []


def test_frontier_collects_pending_ext_leaf() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    assert t.frontier() == ["root.1"]


def test_frontier_skips_settled_ext() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    assert t.frontier() == []


def test_frontier_is_depth_first_in_insertion_order() -> None:
    """root.1, root.2, root.3 in call order -- the §9 worked-example shape."""
    t = Tree("root")
    for i in (1, 2, 3):
        t.add_child("root", f"root.{i}", "ext")
    assert t.frontier() == ["root.1", "root.2", "root.3"]


def test_frontier_descends_through_int_parent() -> None:
    """A suspended-local Int parent folds up its child's pending Ext leaf."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # ctx.run child, still in flight
    t.add_child("root.1", "root.1.1", "ext")  # the rpc it awaits
    assert t.frontier() == ["root.1.1"]


def test_frontier_prunes_det_subtree() -> None:
    """Det lives in another workflow's tree -- its whole subtree is skipped (§3)."""
    t = Tree("root")
    t.add_child("root", "d", "det")
    t.add_child("d", "d.1", "ext")  # would be a frontier leaf if not under Det
    assert t.frontier() == []


def test_frontier_pending_ext_prunes_own_subtree() -> None:
    """An (ext, pending) node is collected, then its subtree is not descended."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root.1", "root.1.1", "ext")  # below a pending Ext -> not reached
    assert t.frontier() == ["root.1"]


# ── useful() / U3 ────────────────────────────────────────────────────────────


def test_useful_passes_when_all_settled() -> None:
    """Done shape: every non-root node settled satisfies U3 via the Settled disjunct."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    t.useful()  # no raise


def test_useful_passes_with_pending_ext_descendant() -> None:
    """Suspended-local: an Int parent kept alive by a pending Ext descendant."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root.1", "root.1.1", "ext")
    t.useful()  # no raise


def test_useful_flags_dead_int_pending_leaf() -> None:
    """An Int leaf still pending with nothing below it -- U3's canonical violation."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # never settled, no children
    with pytest.raises(AssertionError):
        t.useful()


def test_useful_flags_int_pending_with_only_det_descendant() -> None:
    """A detached child keeps nothing pending in our contract -> parent is dead."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root.1", "root.1.d", "det")
    with pytest.raises(AssertionError):
        t.useful()


def test_useful_names_every_dead_branch() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root", "root.2", "int")
    with pytest.raises(AssertionError) as exc:
        t.useful()
    msg = str(exc.value)
    assert "root.1" in msg
    assert "root.2" in msg


# ── well_formed: universal rules (U1/U2/U3) ──────────────────────────────────


def test_well_formed_u1_root_must_be_int() -> None:
    t = Tree("root")
    t._nodes["root"].type = "ext"  # only an SDK bug could do this
    with pytest.raises(AssertionError, match="U1"):
        t.well_formed("done", [])


def test_well_formed_u1_root_must_be_pending() -> None:
    """The inner must never settle the root -- task.fulfill in the outer owns it."""
    t = Tree("root")
    t.settle("root")
    with pytest.raises(AssertionError, match="U1"):
        t.well_formed("done", [])


def test_well_formed_u2_flags_unreachable_node() -> None:
    t = Tree("root")
    t._nodes["orphan"] = Node(
        id="orphan", type="ext", kind="pending"
    )  # not linked under root
    with pytest.raises(AssertionError, match="U2"):
        t.well_formed("suspended", [])


def test_well_formed_u3_flags_dead_branch() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # dead pending Int leaf
    with pytest.raises(AssertionError, match="U3"):
        t.well_formed("suspended", [])


# ── well_formed: done state (D1) ─────────────────────────────────────────────


def test_well_formed_done_valid_when_frontier_empty() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.settle("root.1")
    t.well_formed("done", [])  # no raise


def test_well_formed_done_valid_for_bare_root() -> None:
    """A workflow that returns without spawning is a valid Done outcome."""
    Tree("root").well_formed("done", [])  # no raise


def test_well_formed_done_rejects_non_empty_frontier() -> None:
    """A done outcome cannot still have a live remote dependency."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")  # pending -> frontier non-empty
    with pytest.raises(AssertionError, match="D1"):
        t.well_formed("done", [])


# ── well_formed: suspended state (S1/S4) ─────────────────────────────────────


def test_well_formed_suspended_valid() -> None:
    """The canonical valid suspension: a non-empty frontier, todos within it."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.well_formed("suspended", ["root.1"])  # awaited subset of frontier


def test_well_formed_suspended_s1_rejects_empty_frontier() -> None:
    """Suspending with nothing pending remotely is a contract violation."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.settle("root.1")  # frontier now empty
    with pytest.raises(AssertionError, match="S1"):
        t.well_formed("suspended", [])


def test_well_formed_suspended_s4_rejects_todo_outside_frontier() -> None:
    """An awaited id that is not a live frontier leaf -- todos must be a subset."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.settle("root.2")  # root.2 leaves the frontier...
    with pytest.raises(AssertionError, match="S4"):
        t.well_formed("suspended", ["root.2"])  # ...but is still awaited


def test_well_formed_suspended_s4_holds_for_full_frontier_subset() -> None:
    """Todos == frontier is the trivially-valid S4 boundary."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.well_formed("suspended", ["root.1", "root.2"])


def test_well_formed_suspended_s4_holds_for_empty_todos() -> None:
    """A non-empty frontier with no explicitly awaited todo still satisfies S1+S4."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.well_formed("suspended", [])


def test_well_formed_done_s4_rejects_any_todo() -> None:
    """``todos subset frontier`` holds in both states; for Done that means todos==[]."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.settle("root.1")
    with pytest.raises(AssertionError, match="S4"):
        t.well_formed("done", ["root.1"])  # frontier empty, so any todo violates


# ── replay comparison: is_prune_of / is_extension_of / is_prune_and_extension_of
#
# is_prune_and_extension_of is the GENERAL valid-replay relation (no gate on the
# (added, removed) node-set delta, where self is the later replay); the other two
# are its one-sided specializations:
#
#   added ∅   -> is_prune_of                (pure prune, settle-only, or unchanged)
#   removed ∅ -> is_extension_of            (growth, settle-only, or unchanged)
#   any delta -> is_prune_and_extension_of  (every valid replay)
#
# They are NOT mutually exclusive: an unchanged replay (both deltas ∅) and a
# settle-only kind flip (same nodes, tree.md §9) satisfy all three -- that cell
# is the overlap of prune and extension. A pure prune satisfies is_prune_of and
# is_prune_and_extension_of; a pure extension satisfies is_extension_of and
# is_prune_and_extension_of; a mixed replay (both deltas ≠ ∅) satisfies ONLY
# is_prune_and_extension_of (see test_mixed_replay_is_prune_and_extension).
# Checked here on the canonical replay shapes of tests.test_tree_replay
# (tree.md §13): a completed Int subtree (f.1 with grandchild f.1.1) followed by
# sequential rpcs f.2 / f.3.


def _full_tree() -> Tree:
    """Build iteration 0: the full tree, suspended on rpc ``f.2``.

    ::

        f (int, pending)
        ├── f.1   (int, settled)     child -- ran to completion
        │   └── f.1.1 (int, settled) grandchild
        └── f.2   (ext, pending)     rpc "a" -- the live dependency
    """
    t = Tree("f")
    t.add_child("f", "f.1", "int")
    t.add_child("f.1", "f.1.1", "int")
    t.settle("f.1.1")
    t.settle("f.1")
    t.add_child("f", "f.2", "ext")
    return t


def _pruned_tree() -> Tree:
    """Build iteration 1 over an unchanged cache: ``f.1`` short-circuits, ``f.1.1`` gone."""
    t = Tree("f")
    t.add_child("f", "f.1", "int")
    t.settle("f.1")
    t.add_child("f", "f.2", "ext")
    return t


def _pruned_and_extended_tree() -> Tree:
    """Build iteration 1 after rpc ``f.2`` settled: prunes ``f.1.1`` AND spawns ``f.3``."""
    t = Tree("f")
    t.add_child("f", "f.1", "int")
    t.settle("f.1")
    t.add_child("f", "f.2", "ext")
    t.settle("f.2")
    t.add_child("f", "f.3", "ext")
    return t


# ── is_prune_of ── (added ∅: pure prune, settle-only, or unchanged)


def test_is_prune_of_holds_for_equal_trees() -> None:
    """Equal is a prune -- it added nothing, the one-sided gate (added ∅).

    A structurally identical replay sits in the prune/extension overlap: it
    satisfies both, since neither dropped nor added anything.
    """
    a, b = _full_tree(), _full_tree()
    assert a.is_prune_of(b)  # added ∅ -> a prune (dropped nothing is allowed)
    assert a.is_extension_of(b)


def test_is_prune_of_strict_prune() -> None:
    """The §6 replay shape: the settled Int subtree collapsed to its root.

    Exclusive: ``full.is_prune_of(pruned)`` is FALSE because ``full`` *adds*
    ``f.1.1`` relative to ``pruned`` -- that is extension, not pruning. The
    node-set gate (added must be empty) carries the distinction.
    """
    full, pruned = _full_tree(), _pruned_tree()
    assert pruned.is_prune_of(full)  # dropped f.1.1, added nothing
    assert not full.is_prune_of(pruned)  # f.1.1 is an addition, not a prune


def test_is_prune_of_false_on_pure_extension() -> None:
    """A pure §8 extension is not a prune -- ``self`` added a node, dropped none."""
    grown = _full_tree()
    grown.add_child("f", "f.3", "ext")
    assert not grown.is_prune_of(_full_tree())  # removed ∅ -> not a prune


def test_is_prune_of_false_on_mixed() -> None:
    """A mixed prune+extend is not a pure prune -- ``self`` also added a node."""
    assert not _pruned_and_extended_tree().is_prune_of(_full_tree())


def test_is_prune_of_false_on_type_change() -> None:
    """Type stability over the shared nodes, asserted on a genuine prune."""
    full = _full_tree()
    pruned = _pruned_tree()
    full._nodes["f.2"].type = "int"  # only an SDK bug could reclassify a node
    assert not pruned.is_prune_of(full)


def test_is_prune_of_false_on_kind_regression() -> None:
    """Settled in ``other``, pending in ``self`` -- the lattice never retreats."""
    regressed = _pruned_tree()
    regressed._nodes["f.1"].kind = "pending"  # settled in full, pending here
    assert not regressed.is_prune_of(_full_tree())


def test_is_prune_of_allows_kind_advance() -> None:
    """Pending in ``other``, settled in ``self`` is fine -- monotonic progress."""
    advanced = _pruned_tree()
    advanced.settle("f.2")
    assert advanced.is_prune_of(_full_tree())


def test_is_prune_of_false_on_middle_drop() -> None:
    """Pruning collapses a node's *whole* child list, never a middle child."""
    other = Tree("f")
    other.add_child("f", "f.1", "ext")
    other.settle("f.1")
    other.add_child("f", "f.2", "ext")
    dropped = Tree("f")
    dropped.add_child(
        "f", "f.2", "ext"
    )  # f.1 removed -> [f.2] not a prefix of [f.1, f.2]
    assert not dropped.is_prune_of(other)


# ── is_extension_of ── (removed ∅: pure extension, settle-only, or unchanged)


def test_is_extension_of_holds_for_structurally_equal_trees() -> None:
    """A structurally identical replay is an extension -- it drops nothing."""
    a, b = _full_tree(), _full_tree()
    assert a.is_extension_of(b)  # removed ∅ -> extension owns the unchanged cell
    assert b.is_extension_of(a)


def test_is_extension_of_settle_only() -> None:
    """A settle-only step (same nodes, a kind flip) is an extension (``tree.md`` §9).

    ``advanced`` settles ``f.2`` and adds nothing -- ``added ∅`` and
    ``removed ∅`` -- but drops nothing, which extension owns. The reverse is not
    an extension: it would regress ``f.2`` settled -> pending (kind monotonicity).
    """
    advanced = _full_tree()
    advanced.settle("f.2")
    assert advanced.is_extension_of(_full_tree())
    assert not _full_tree().is_extension_of(advanced)  # kind regression


def test_is_extension_of_strict_extension() -> None:
    """The §8 growth shape: the body ran past a settled await and appended a node.

    Exclusive: the reverse fails because ``extended`` adds ``f.3``, so ``full``
    is *missing* a node ``extended`` has (``removed ≠ ∅``) -- a prune, not an
    extension.
    """
    extended = _full_tree()
    extended.settle("f.2")
    extended.add_child("f", "f.3", "ext")
    assert extended.is_extension_of(_full_tree())
    assert not _full_tree().is_extension_of(extended)  # missing f.3 -> not extension


def test_is_extension_of_false_on_pure_prune() -> None:
    """A pure §6 prune is not an extension -- ``self`` dropped a node, added none."""
    pruned = _pruned_tree()
    assert not pruned.is_extension_of(_full_tree())  # removed ≠ ∅ -> not extension


def test_is_extension_of_false_on_mixed() -> None:
    """A mixed prune+extend is not a pure extension -- ``self`` also dropped a node."""
    assert not _pruned_and_extended_tree().is_extension_of(_full_tree())


def test_is_extension_of_false_on_type_change() -> None:
    """Type stability over the shared nodes, asserted on a genuine extension."""
    a = _full_tree()
    a.add_child("f", "f.3", "ext")
    b = _full_tree()
    b._nodes["f.2"].type = "int"  # only an SDK bug could reclassify a node
    assert not a.is_extension_of(b)


def test_is_extension_of_false_on_kind_regression() -> None:
    """Settled in ``other``, pending in ``self`` -- the lattice never retreats."""
    extended = _full_tree()
    extended.settle("f.2")
    extended.add_child("f", "f.3", "ext")
    extended._nodes["f.1"].kind = "pending"  # settled in other, pending here
    assert not extended.is_extension_of(_full_tree())


def test_is_extension_of_allows_kind_advance() -> None:
    """Pending in ``other``, settled in ``self`` is fine -- monotonic progress."""
    advanced = _full_tree()
    advanced.settle("f.2")  # pending in other, settled here
    advanced.add_child("f", "f.3", "ext")
    assert advanced.is_extension_of(_full_tree())


def test_is_extension_of_false_on_middle_divergence() -> None:
    """Extension appends to the child-list *tail*, never reorders a shared child."""
    other = Tree("f")
    other.add_child("f", "f.1", "ext")
    grown = Tree("f")
    grown.add_child("f", "f.2", "ext")  # diverges at index 0, not an append
    grown.add_child("f", "f.1", "ext")
    assert not grown.is_extension_of(other)


# ── is_prune_and_extension_of ── (general valid replay: any (added, removed) delta)


def test_mixed_replay_is_prune_and_extension() -> None:
    """The canonical §6/§8 mixed replay satisfies ONLY the mixed predicate.

    ``f.1.1`` pruned AND ``f.3`` new, so neither node set contains the other.
    The two pure atoms both reject it on their node-set gate; the mixed
    predicate projects out each side's delta and holds.
    """
    full = _full_tree()
    evolved = _pruned_and_extended_tree()
    assert evolved.is_prune_and_extension_of(full)
    assert not evolved.is_prune_of(full)  # also extends -> not pure prune
    assert not evolved.is_extension_of(full)  # also prunes -> not pure extension


def test_mixed_replay_decomposes_through_intermediate() -> None:
    """The mixed step factors as a pure prune then a pure extension.

    ``full`` --(prune f.1.1)--> ``pruned`` --(extend f.3, settle f.2)-->
    ``evolved``: the intermediate ``pruned`` is a pure prune of ``full`` and
    ``evolved`` is a pure extension of ``pruned``.
    """
    full = _full_tree()
    pruned = _pruned_tree()
    evolved = _pruned_and_extended_tree()
    assert pruned.is_prune_of(full)
    assert evolved.is_extension_of(pruned)


def test_is_prune_and_extension_holds_on_pure_prune() -> None:
    """A pure prune is a valid replay -- the general relation has no node-set gate."""
    assert _pruned_tree().is_prune_and_extension_of(_full_tree())


def test_is_prune_and_extension_holds_on_pure_extension() -> None:
    """A pure extension is a valid replay -- the general relation has no node-set gate."""
    grown = _full_tree()
    grown.add_child("f", "f.3", "ext")
    assert grown.is_prune_and_extension_of(_full_tree())


def test_is_prune_and_extension_holds_for_equal_trees() -> None:
    """Equal is a valid replay -- it neither added nor dropped a node."""
    assert _full_tree().is_prune_and_extension_of(_full_tree())


def test_is_prune_and_extension_false_on_kind_regression() -> None:
    """Kind monotonicity holds for the mixed predicate too."""
    evolved = _pruned_and_extended_tree()
    evolved._nodes["f.1"].kind = "pending"  # settled in full, pending here
    assert not evolved.is_prune_and_extension_of(_full_tree())


# ── print() -- deterministic ASCII diagram ───────────────────────────────────


def test_print_bare_root() -> None:
    assert Tree("root").print() == "root (int, pending)"


def test_print_lists_children_in_insertion_order() -> None:
    """``tree.md`` §12: children in insertion order. Last child uses └──, others ├──."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "int")
    expected = (
        "root (int, pending)\n├── root.1 (ext, pending)\n└── root.2 (int, pending)"
    )
    assert t.print() == expected


def test_print_nests_subtrees_with_continuation_bars() -> None:
    """Non-last siblings carry a │ continuation; the last is blank-padded."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root.1", "root.1.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.settle("root.2")
    expected = (
        "root (int, pending)\n"
        "├── root.1 (int, pending)\n"
        "│   └── root.1.1 (ext, pending)\n"
        "└── root.2 (ext, settled)"
    )
    assert t.print() == expected


def test_print_is_deterministic_across_calls() -> None:
    """The output depends only on tree state -- no hidden iteration order."""
    t = Tree("root")
    for i in (1, 2, 3):
        t.add_child("root", f"root.{i}", "ext")
    assert t.print() == t.print()


# ── worked example: the phased workflow under replay (tree.md §9) ────────────


def test_phased_workflow_replay_sequence() -> None:
    """Walk tree.md §9 end to end: three suspended fixed points, then done.

    Body: rpc(a); await a; rpc(b); rpc(c); await b; return await c. Each
    iteration rebuilds the tree against a richer cache and must be well-formed
    for the outcome it reports -- suspended while any leaf is pending, done
    once all settle.
    """
    # Iteration 0 -- cache empty: body creates root.1, awaits it, suspends.
    t0 = Tree("root")
    t0.add_child("root", "root.1", "ext")
    assert t0.frontier() == ["root.1"]
    t0.well_formed("suspended", ["root.1"])

    # External settles root.1. Iteration 1 -- a returns, b and c spawn, await b.
    t1 = Tree("root")
    t1.add_child("root", "root.1", "ext")
    t1.settle("root.1")
    t1.add_child("root", "root.2", "ext")
    t1.add_child("root", "root.3", "ext")
    assert t1.frontier() == ["root.2", "root.3"]
    t1.well_formed("suspended", ["root.2"])  # only b is awaited

    # External settles root.3. Iteration 2 -- still blocked on b.
    t2 = Tree("root")
    t2.add_child("root", "root.1", "ext")
    t2.settle("root.1")
    t2.add_child("root", "root.2", "ext")
    t2.add_child("root", "root.3", "ext")
    t2.settle("root.3")
    assert t2.frontier() == ["root.2"]
    t2.well_formed("suspended", ["root.2"])

    # External settles root.2. Iteration 3 -- body runs to completion, done.
    t3 = Tree("root")
    t3.add_child("root", "root.1", "ext")
    t3.add_child("root", "root.2", "ext")
    t3.add_child("root", "root.3", "ext")
    for leaf in ("root.1", "root.2", "root.3"):
        t3.settle(leaf)
    assert t3.frontier() == []
    t3.well_formed("done", [])
