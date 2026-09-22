"""The execution tree -- the in-memory model of one workflow attempt's call graph.

It is an **assertion-only** structure: the runtime never reads it to make a
control-flow decision; instead it materializes what one execution pass
produced -- the set of promises the body created, who settles each, and which
are still pending -- so that :meth:`Tree.well_formed` can assert the worker's
behavior matches the suspension contract on every return.

A node's :data:`NodeType` (who settles it) and :data:`NodeKind` (whether it
has settled) form a 2x3 product whose ``(ext, pending)`` cell is the
suspension :meth:`~Tree.frontier`.

Two implementation notes:

* **No lock.** Every writer is an asyncio task on a single event loop, and no
  public method awaits between read and write, so the node map and the
  per-node child list cannot interleave.
* **Predicates raise.** Following the repo-wide convention (codec.py /
  core.py), :meth:`Tree.useful` and :meth:`Tree.well_formed` return ``None``
  and raise :class:`AssertionError` with a multi-line message on violation.
  The tree is a pure assertion layer, so a failed check is a bug in the SDK,
  not a recoverable condition.
"""

from __future__ import annotations

from typing import Literal

import msgspec

#: Who is responsible for settling a node's durable promise. Assigned at the
#: call site and never changes:
#:
#: * ``"int"`` -- internal, created by ``ctx.run``; this worker settles it
#:   under our task lease when the local executor returns.
#: * ``"ext"`` -- external, created by ``ctx.rpc`` / ``ctx.sleep`` /
#:   ``ctx.promise``; settled by something we await (another worker, the
#:   server's timer, an external ``promise.settle`` caller).
#: * ``"det"`` -- detached, created by ``ctx.detached``; fire-and-forget,
#:   outside this workflow's contract. Det subtrees are exempt from every rule
#:   and skipped by the frontier walk.
NodeType = Literal["int", "ext", "det"]

#: Whether a node's durable promise has reached a terminal state. The
#: five-state durable lattice (``pending | resolved | rejected |
#: rejected_canceled | rejected_timedout``) collapses to this single bit -- the
#: success/failure distinction matters at ``Future.await`` time, not for the
#: structural suspension contract. Transitions monotonically:
#: ``pending`` -> ``settled`` only.
NodeKind = Literal["pending", "settled"]

#: The outcome status an execution pass can report at a tree-assertion
#: boundary. An error-fulfill (the body raised an
#: :class:`~resonate.errors.ApplicationError`) is a ``"done"`` with a rejected
#: settle state, not a separate tree-level status.
Status = Literal["done", "suspended"]


class Node(msgspec.Struct, kw_only=True):
    """One promise in the call graph.

    Mutable: ``kind`` flips ``pending`` -> ``settled`` exactly once, and
    ``children`` grows as the body spawns. ``children`` holds child IDs in
    insertion (== call) order, which is what makes the children-as-prefix
    replay property and :meth:`Tree.print` deterministic.
    """

    id: str
    type: NodeType
    kind: NodeKind
    children: list[str] = msgspec.field(default_factory=list)


class Tree:
    """The execution tree for one workflow attempt.

    Built incrementally as the body runs (:meth:`add_child` per spawn,
    :meth:`settle` as promises terminate), then asserted at each execution
    boundary via :meth:`well_formed`. The root is always ``(int, pending)`` --
    it is settled when the task is fulfilled, never during the body's own
    execution pass (invariant U1).
    """

    def __init__(self, root_id: str) -> None:
        self._root = root_id
        self._nodes: dict[str, Node] = {
            root_id: Node(id=root_id, type="int", kind="pending"),
        }

    # ── inspection ──────────────────────────────────────────────────

    def root(self) -> str:
        """Return the root node's id."""
        return self._root

    def has(self, id: str) -> bool:
        """Whether ``id`` exists in the tree."""
        return id in self._nodes

    def size(self) -> int:
        """Total node count."""
        return len(self._nodes)

    def ids(self) -> list[str]:
        """Every node id, unordered."""
        return list(self._nodes)

    def get(self, id: str) -> Node | None:
        """Return a defensive copy of the node, or ``None`` if absent.

        Returns a copy (including a fresh ``children`` list) so callers cannot
        mutate tree state through the handle.
        """
        node = self._nodes.get(id)
        return (
            Node(
                id=node.id,
                type=node.type,
                kind=node.kind,
                children=list(node.children),
            )
            if node is not None
            else None
        )

    # ── mutation ────────────────────────────────────────────────────

    def add_child(self, parent: str, id: str, type: NodeType) -> bool:
        """Attach a child of ``type`` under ``parent``; idempotent on ``id``.

        Returns ``True`` if a node was inserted, ``False`` if ``id`` already
        existed (so a replay that re-walks the same body does not duplicate
        nodes or re-append to the parent's child list). The parent must already
        be in the tree.
        """
        assert parent in self._nodes, f"unknown parent {parent!r}"
        if id in self._nodes:
            return False
        self._nodes[id] = Node(id=id, type=type, kind="pending")
        self._nodes[parent].children.append(id)
        return True

    def settle(self, id: str) -> None:
        """Mark ``id`` settled; monotonic, no-op if already settled or unknown."""
        node = self._nodes.get(id)
        if node is None:
            return
        node.kind = "settled"

    # ── frontier ────────────────────────────────────────────────────

    def frontier(self) -> list[str]:
        """Return the ``(ext, pending)`` node IDs, skipping Det subtrees.

        These are the workflow's live remote dependencies -- the promises
        whose settlement will unblock further progress. A depth-first walk in
        child insertion order: a Det node prunes its whole subtree (it lives
        in another workflow's tree); an ``(ext, pending)`` node is collected
        and its subtree pruned; everything else descends.

        Note this is a **superset** of ``outcome.todos`` (the awaited subset
        the runtime registers callbacks for): the frontier is *every* pending
        remote leaf, including ones the body created but never reached an
        ``await`` on. Invariant S4 (``todos subset frontier``) connects the
        two.
        """
        out: list[str] = []
        stack: list[str] = [self._root]

        while stack:
            curr_id = stack.pop()
            node = self._nodes[curr_id]

            if node.type == "det":
                continue

            if node.type == "ext" and node.kind == "pending":
                out.append(curr_id)
                continue

            # Extend the stack with children in reverse order to
            # maintain the depth-first walk in child insertion order.
            stack.extend(reversed(node.children))

        return out

    # ── predicates ──────────────────────────────────────────────────

    def useful(self) -> None:
        """Assert U3: no dead pending branches.

        Every non-root, non-Det node must be either ``settled`` OR have at
        least one ``(ext, pending)`` node in its subtree (itself included). A
        node failing this is *pending with no path to further progress* -- it
        should already have settled. Raises :class:`AssertionError` listing
        every dead branch.

        A suspended-local Int node is kept alive only by an Ext-pending
        descendant somewhere in its subtree.
        """
        dead = [
            id
            for id, node in self._nodes.items()
            if id != self._root
            and node.type != "det"
            and node.kind != "settled"
            and not self._has_pending_ext(id)
        ]
        lines = "\n".join(f"  - {id} (dead pending branch)" for id in dead)
        assert not dead, (
            f"U3 violated: pending node(s) with no Ext-pending descendant:\n{lines}"
        )

    def _has_pending_ext(self, id: str) -> bool:
        """Whether ``id``'s subtree contains an ``(ext, pending)`` node.

        A Det subtree never counts -- a detached child keeps nothing pending
        in our contract, so an Int parent whose only pending descendant is
        detached is still a dead branch.
        """
        node = self._nodes[id]
        if node.type == "det":
            return False
        if node.type == "ext" and node.kind == "pending":
            return True
        return any(self._has_pending_ext(c) for c in node.children)

    def well_formed(self, status: Status, todos: list[str]) -> None:
        """Assert the tree matches the reported execution outcome.

        Checked on *every* return from an execution pass. ``status`` is the
        outcome about to be reported -- ``"done"`` (the body returned,
        including a rejected fulfill) or ``"suspended"``; ``todos`` is the
        awaited subset (``Context.take_remote_todos()``), used for S4.
        Universal rules apply in both states; the status-specific rule then
        pins the frontier.

        * **U1** root is ``(int, pending)``.
        * **U2** every node reachable from the root.
        * **U3** no dead pending branches (:meth:`useful`).
        * **D1** (done) frontier empty.
        * **S1** (suspended) frontier non-empty; **S4**
          ``todos subset frontier``.

        Raises :class:`AssertionError` on the first violated rule.
        """
        # U1 -- the root is settled when the task is fulfilled, never by the
        # body's own execution pass.
        root = self._nodes[self._root]
        assert root.type == "int", (
            f"U1 violated: root {self._root!r} has type {root.type}, expected int"
        )

        assert root.kind == "pending", (
            f"U1 violated: root {self._root!r} has kind {root.kind}, expected pending"
        )

        # U2 -- every node must be reachable from the root.
        reachable: set[str] = set()
        stack: list[str] = [self._root]

        while stack:
            curr_id = stack.pop()
            if curr_id not in reachable:
                reachable.add(curr_id)
                stack.extend(self._nodes[curr_id].children)

        assert reachable == set(self._nodes), (
            f"U2 violated: node(s) not reachable from root: "
            f"{sorted(set(self._nodes) - reachable)}"
        )

        # U3 -- no dead pending branches.
        self.useful()

        frontier = self.frontier()

        # S1 / D1 -- the status-specific rule pins the frontier. Keyed on the
        # reported status, not on emptiness, so S4 below sees the suspended
        # (non-empty) frontier too.
        match status:
            case "suspended":
                assert frontier, "S1 violated: suspended outcome with an empty frontier"
            case "done":
                assert not frontier, (
                    f"D1 violated: done outcome with non-empty frontier {frontier}"
                )

        # S4 -- the awaited subset must lie within the full frontier. Holds in
        # both states: when done the frontier is empty, so this also pins
        # todos empty.
        extra = [t for t in todos if t not in frontier]
        assert not extra, (
            f"S4 violated: todos not subset of frontier; "
            f"todos={todos} frontier={frontier} extra={extra}"
        )

    # ── replay comparison ───────────────────────────────────────────

    def _shared_type_stable(self, other: Tree) -> bool:
        """Whether the trees share a root and every shared id keeps its ``type``.

        The direction-agnostic half of the replay contract: a node's settler is
        fixed at its call site and never changes, so a reclassified shared node
        is an SDK bug regardless of which tree is the later replay. All three of
        :meth:`is_prune_of`, :meth:`is_extension_of`, and
        :meth:`is_prune_and_extension_of` start here. Says nothing about
        containment -- a mixed replay's node sets need not nest, so the check
        ranges over the intersection only.
        """
        if self._root != other._root:
            return False
        return all(
            self._nodes[id].type == other._nodes[id].type
            for id in self._nodes.keys() & other._nodes.keys()
        )

    def _is_at_least_as_settled_as(self, earlier: Tree) -> bool:
        """Whether every node ``earlier`` had settled is still settled in ``self``.

        Kind monotonicity: the durable lattice only advances, so a later
        replay is never *less* settled than an earlier one. This is
        the one part of the replay contract whose direction is fixed rather than
        containment-symmetric -- ``self`` is always the later replay, so all
        three replay predicates call it the same way.
        """
        for id in self._nodes.keys() & earlier._nodes.keys():
            if (
                earlier._nodes[id].kind == "settled"
                and self._nodes[id].kind != "settled"
            ):
                return False
        return True

    def is_prune_of(self, other: Tree) -> bool:
        """Whether ``self`` is a *pruning* of ``other``.

        ``self`` is a *later* replay of the same body than ``other`` that
        **added nothing** -- it may have **dropped** nodes (a completed Int
        subtree short-circuited, so the children its body would have spawned
        were never re-added -- see context.py) or dropped nothing at all (a
        structurally unchanged replay, or a settle-only kind advance). This is
        the no-extension specialization of :meth:`is_prune_and_extension_of`:
        the gate is one-sided (``added == ∅``), so it is *not* exclusive with
        :meth:`is_extension_of` -- an unchanged replay (both deltas empty)
        satisfies both, and the unchanged/settle-only cell is exactly their
        overlap. A replay that *grows* (adds a node ``other`` never had) is an
        extension only, never this.

        * **Added nothing** -- ``self`` has *no* node ``other`` lacks
          (``added == ∅``); ``self`` may drop nodes or none.
        * **Root + type stability** (:meth:`_shared_type_stable`) and **kind
          monotonicity** (:meth:`_is_at_least_as_settled_as`) -- the later
          replay is never less settled.
        * **Pruned-as-prefix** -- for every (shared) node, ``self``'s child list
          must be a prefix of ``other``'s. A pruning boundary (a settled Int
          whose body was skipped) collapses its *whole* child list to ``[]``
          (a prefix of anything); every node above it keeps its children
          verbatim. Pruning is all-or-nothing per node, never a middle drop, so
          prefix is exactly the right shape.
        """
        added = self._nodes.keys() - other._nodes.keys()
        if added:
            return False
        if not self._shared_type_stable(other):
            return False
        if not self._is_at_least_as_settled_as(other):
            return False
        # added is empty, so every self node is shared with other.
        for id, node in self._nodes.items():
            if node.children != other._nodes[id].children[: len(node.children)]:
                return False
        return True

    def is_extension_of(self, other: Tree) -> bool:
        """Whether ``self`` is an *extension* of ``other``.

        ``self`` is the *later* replay after the cache advanced and **dropped
        nothing**: it may have run past a now-unblocked await and **appended**
        nodes ``other`` never had, or merely advanced a shared node's kind (a
        frontier dependency settled, no new spawn), or be structurally
        unchanged. This is the no-prune
        specialization of :meth:`is_prune_and_extension_of`: the gate is
        one-sided (``removed == ∅``), so it is *not* exclusive with
        :meth:`is_prune_of` -- an unchanged replay (both deltas empty) satisfies
        both, and the unchanged/settle-only cell is exactly their overlap. A
        replay that *also* prunes (drops a node ``other`` had) is a prune only,
        never this.

        * **Dropped nothing** -- ``other`` has *no* node ``self`` lacks
          (``removed == ∅``). ``self`` may add new nodes or none.
        * **Root + type stability** (:meth:`_shared_type_stable`) and **kind
          monotonicity** (:meth:`_is_at_least_as_settled_as`) -- ``self`` is the
          later replay, so kind monotonicity always runs ``other`` -> ``self``
          and is not a flipped ``other.is_prune_of(self)``.
        * **Original-as-prefix** -- for every (shared) node, ``other``'s child
          list must be a prefix of ``self``'s. Extension only appends to the
          tail, so the original order is preserved and a prefix relation is
          exactly what growth (and the no-growth degenerate case) allows.
        """
        removed = other._nodes.keys() - self._nodes.keys()
        if removed:
            return False
        if not self._shared_type_stable(other):
            return False
        if not self._is_at_least_as_settled_as(other):
            return False
        # removed is empty, so every other node is shared with self.
        for id, node in other._nodes.items():
            if node.children != self._nodes[id].children[: len(node.children)]:
                return False
        return True

    def is_prune_and_extension_of(self, other: Tree) -> bool:
        """Whether ``self`` is a *valid replay* of ``other``.

        The general replay relation -- ``self`` may have **pruned** completed
        Int subtrees, **extended** past freshly-unblocked awaits, done **both**
        at once (the common step: settling a frontier dependency completes one
        subtree while unblocking another), or neither (an unchanged or
        settle-only replay). It places *no* gate on the ``(added, removed)``
        node-set delta, so it is the union under which :meth:`is_prune_of`
        (``added == ∅``) and :meth:`is_extension_of` (``removed == ∅``) are the
        one-sided specializations: every prune and every extension is also a
        valid replay, and this predicate holds across the whole settle-to-done
        walk regardless of which facets a given step exercises.

        * **No node-set gate** -- ``self`` may add nodes, drop nodes, both, or
          neither.
        * **Root + type stability** and **kind monotonicity** -- as in the
          specializations.
        * **Survivors-as-prefix, both directions** -- over the shared nodes,
          projecting out each side's delta: ``self``'s children that ``other``
          also had must be a prefix of ``other``'s list (the prune facet), and
          ``other``'s children that ``self`` still has must be a prefix of
          ``self``'s list (the extension facet). A node touched by both facets
          satisfies each under its own projection.
        """
        if not self._shared_type_stable(other):
            return False
        if not self._is_at_least_as_settled_as(other):
            return False
        for id in self._nodes.keys() & other._nodes.keys():
            retained = [c for c in self._nodes[id].children if c in other._nodes]
            if retained != other._nodes[id].children[: len(retained)]:
                return False
            survivors = [c for c in other._nodes[id].children if c in self._nodes]
            if survivors != self._nodes[id].children[: len(survivors)]:
                return False
        return True

    # ── display ─────────────────────────────────────────────────────

    def print(self) -> str:
        """Return an ASCII tree diagram, children in insertion order.

        Each line is ``<id> (<type>, <kind>)``. Children are listed in their
        insertion (== call) order, so the output is a deterministic function
        of the (body, cache) pair -- exactly the children-as-prefix replay
        property.
        """
        lines: list[str] = []
        self._print_node(self._root, line_prefix="", child_prefix="", lines=lines)
        return "\n".join(lines)

    def _print_node(
        self, id: str, *, line_prefix: str, child_prefix: str, lines: list[str]
    ) -> None:
        node = self._nodes[id]
        lines.append(f"{line_prefix}{id} ({node.type}, {node.kind})")
        n = len(node.children)
        for i, child_id in enumerate(node.children):
            last = i == n - 1
            self._print_node(
                child_id,
                line_prefix=child_prefix + ("└── " if last else "├── "),
                child_prefix=child_prefix + ("    " if last else "│   "),
                lines=lines,
            )
