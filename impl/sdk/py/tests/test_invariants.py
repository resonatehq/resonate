"""Replay invariants stated *generically*, for an arbitrary resonate function.

Where :mod:`tests.test_tree_replay` pins the exact id set a *specific* body
leaves behind at each step, this module turns the structural contract of
``tree.md`` into **invariant functions**: each receives any resonate function,
drives it through ``execute_until_blocked_inner`` (often across a whole
settle-to-done trajectory), and asserts a property of the model holds -- so the
same claim can be checked against a battery of workflow shapes. Each corpus
entry exercises one cell of the §2 type matrix.

The invariants, grouped by the section of ``tree.md`` they formalize:

* **§1 -- the tree *is* the inner's contract, rendered as data.** At every
  reachable cache state the outcome kind agrees with the frontier:
  ``_ExecFulfilled`` iff the frontier is empty (D1), ``_ExecSuspended`` iff it
  is non-empty (S1); the awaited ``todos`` lie within the frontier (S4); the
  root stays ``(int, pending)`` -- the inner never settles it (U1). Asserted not
  just at iteration 0 but across the whole trajectory
  (``test_outcome_agrees_with_tree_along_trajectory``).

* **§6 -- replay preserves Type and only advances Kind.** A node shared between
  two consecutive replays never changes Type (its settler is fixed at the call
  site) and never un-settles (the durable lattice never retreats) -- and the
  cache obeys the same lattice: a settled promise record is frozen across
  replays. Bundled into the ``is_prune_of`` / ``is_extension_of`` atoms (R1 / R2)
  and asserted directly across the trajectory, over both tree and cache
  (``test_type_stable_and_kind_monotone_across_replay``).

* **§7 -- fixed point under an unchanging world.** Replaying over an
  **unchanged** cache only prunes (``is_prune_of``; or, once nothing remains to
  prune, a structurally unchanged tree, which ``is_extension_of`` covers) and
  converges to a fixed point after the first replay
  (``inner(inner(X)) = inner(X)``) -- **R1**. Two sharper §7 facts: the frontier
  is stable from iteration 0 (``test_frontier_stable_under_unchanged_cache``)
  and the cache from iteration 1 (``test_cache_stable_after_first_run``), so the
  frontier reaches its fixed point one step ahead of the tree.

* **§8 -- frontier evolution.** Settling each successive frontier and replaying
  yields a tree that *extends* the unchanged-cache replay (``is_extension_of``),
  all the way down the settle-to-done walk -- **R2**. Every step obeys the strongest
  cross-iteration invariant,
  ``frontier_{n+1} ⊆ (frontier_n - resolved_n) | new_ext_spawns_n``
  (``test_frontier_evolution_rule_holds``).

The trajectory invariants (§1, §6, §8 evolution) drive the body for up to
``MAX_ROUNDS`` settle-and-replay rounds and assert their property at *every*
round; reaching Done is not a requirement -- §8's ``frontier_n → ∅`` termination
guarantee is observed where it happens, never enforced by the bound.

Unlike the rest of the suite, the call-under-test lives *inside* each invariant
function rather than inline per test; the workflow corpus they range over is the
parameter.
"""

from __future__ import annotations

from datetime import timedelta
from itertools import chain, combinations
from typing import TYPE_CHECKING, Any, Concatenate, Literal

import pytest

from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.core import (
    Core,
    _ExecFulfilled,
    _ExecOutcome,
    identity_target_resolver,
)
from resonate.error import ResonateError
from resonate.registry import Registry
from resonate.types import PromiseCreateReq, PromiseRecord, Value

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.context import Context

# Far-future deadline, matching tests.test_core.
FAR_FUTURE = 1 << 50
TTL = 10_000

# How many settle-and-replay rounds the trajectory invariants drive. The point
# is that the per-round invariants hold at *every* state visited, not that the
# body converges: a workflow that has not reached Done by this bound simply
# stops -- it is not a failure, it just had its invariants checked each round.
# A generous cap (every corpus body actually converges in a handful of steps)
# keeps the walk well past any body's settling depth while bounding the loop.
MAX_ROUNDS = 1_000


# ── Dict-backed mock for Effects (self-contained; this file stands alone) ────


class MockEffects:
    """A dict-backed stand-in for :class:`~resonate.effects.Effects`.

    Holds the execution state as a plain ``{id: PromiseRecord}`` map: no Sender,
    Codec, or network. ``create_promise`` records a fresh ``pending`` node or
    returns the cached one (idempotent, like the real Effects); ``settle_promise``
    flips an existing node terminal. Reusing one instance across inner calls is
    exactly the "same body, same cache" replay scenario.
    """

    def __init__(self, cache: dict[str, PromiseRecord] | None = None) -> None:
        self.cache: dict[str, PromiseRecord] = cache or {}

    def copy(self) -> MockEffects:
        return MockEffects(cache=dict(self.cache))

    async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord:
        cached = self.cache.get(req.id)
        if cached is not None:
            return cached
        record = PromiseRecord(
            id=req.id,
            state="pending",
            param=req.param,
            timeout_at=req.timeout_at,
            tags=req.tags,
        )
        self.cache[req.id] = record
        return record

    async def settle_promise(self, id: str, result: Any) -> PromiseRecord:
        cached = self.cache.get(id)
        if cached is not None and cached.state != "pending":
            return cached

        state: Literal["resolved", "rejected"]
        if isinstance(result, ResonateError):
            state = "rejected"
            value = Value(data=_encode_error(result))
        else:
            state = "resolved"
            value = Value(data=result)

        record = PromiseRecord(
            id=id,
            state=state,
            param=cached.param if cached is not None else Value(),
            value=value,
            timeout_at=cached.timeout_at if cached is not None else FAR_FUTURE,
            tags=cached.tags if cached is not None else {},
        )
        self.cache[id] = record
        return record


# ── Shared setup ─────────────────────────────────────────────────────────────


def _setup(reg: Registry) -> tuple[Core, MockEffects, PromiseRecord]:
    """Build the pieces every invariant drives the inner with.

    A Core whose inner never touches the network (sender/heartbeat unused), a
    fresh ``MockEffects`` (one shared cache across every iteration *within* a
    single invariant call), and a pending root promise whose param carries a
    ``TaskData`` naming the registered ``"func"``.
    """
    core = Core(
        sender=None,
        codec=Codec(NoopEncryptor()),
        registry=reg,
        resolver=identity_target_resolver,
        pid="invariants-test",
        ttl=TTL,
    )
    effects = MockEffects()
    root = PromiseRecord(
        id="f",
        state="pending",
        param=Value(data={"func": "func", "args": [], "kwargs": {}, "version": 1}),
        timeout_at=FAR_FUTURE,
    )
    return core, effects, root


def _settle_external(effects: MockEffects, id: str, data: Any = None) -> None:
    """Settle ``id`` in place in the shared cache, resolved with ``data``.

    Models the external world advancing between two inner runs -- a remote
    worker fulfilling an rpc child, the server's timer firing a sleep, or an
    external ``promise.settle`` -- the §7 "disturbance" that moves the system
    to a new fixed point. Asserts the promise is still pending so a settle
    cannot double-fire (Kind is monotonic).
    """
    rec = effects.cache[id]
    assert rec.state == "pending", f"{id} is already settled"
    effects.cache[id] = PromiseRecord(
        id=rec.id,
        state="resolved",
        param=rec.param,
        value=Value(data=data),
        tags=rec.tags,
        timeout_at=rec.timeout_at,
    )


# ── The workflow corpus: one entry per §2 type-matrix cell ───────────────────
#
# ``ctx.run`` takes the function object directly, so a child body need not be
# registered -- only the root ``"func"`` is. These module-level helpers are the
# children the corpus roots spawn.


def _leaf(ctx: Context) -> int:
    return 1


def _grandchild(ctx: Context) -> int:
    return 1


async def _child_runs_grandchild(ctx: Context) -> int:
    await ctx.run(_grandchild)
    return 2


async def _child_blocks_on_rpc(ctx: Context) -> int:
    await ctx.rpc("a")
    return 2


async def _child_fans_out_rpcs(ctx: Context) -> int:
    """Spawn two rpcs and await one -- the child folds *both* todos up."""
    a = ctx.rpc("a")
    ctx.rpc("b")
    await a
    return 2


async def _grandchild_blocks_on_rpc(ctx: Context) -> int:
    await ctx.rpc("a")
    return 3


async def _child_runs_blocking_grandchild(ctx: Context) -> int:
    """Run a local child whose own child is the one that blocks -- depth-3 suspension."""
    return await ctx.run(_grandchild_blocks_on_rpc)


async def _all_local(ctx: Context) -> int:
    """No remote work: fulfills on the first run -- the degenerate case."""
    a = ctx.run(_leaf)
    b = ctx.run(_leaf)
    return await a + await b


async def _nested_local_then_rpc(ctx: Context) -> None:
    """Spawn a settled local subtree (run -> run) then an rpc -- the canonical prune."""
    await ctx.run(_child_runs_grandchild)
    await ctx.rpc("a")


async def _rpc_chain(ctx: Context) -> None:
    """Sequential rpcs -- settling the first await spawns a node run 0 never had."""
    await ctx.rpc("a")
    await ctx.rpc("b")


async def _sleep_then_rpc(ctx: Context) -> None:
    """Block on a timer Ext node, then an rpc -- the timer settle is pure extension."""
    await ctx.sleep(timedelta(seconds=30))
    await ctx.rpc("a")


async def _bare_promise(ctx: Context) -> Any:
    """Block on a bare Ext node only an external settle advances, to done."""
    return await ctx.promise()


async def _fanout(ctx: Context) -> None:
    """Three rpcs spawned, one awaited -- settling each shrinks the frontier."""
    a = ctx.rpc("a")
    ctx.rpc("b")
    ctx.rpc("c")
    await a


async def _suspended_local(ctx: Context) -> int:
    """Run a local child that suspends on its own rpc -- the §4 suspended-local case."""
    v = await ctx.run(_child_blocks_on_rpc)
    return v + 1


async def _detached_then_rpc(ctx: Context) -> None:
    """Spawn a Det node -- exempt from the contract, invisible to the frontier -- then an rpc."""
    await ctx.detached("side")
    await ctx.rpc("a")


async def _phased_rpcs(ctx: Context) -> int:
    """``tree.md`` §9's phased body: settle one await, spawn two, then drain each.

    Three disturbance phases with a mid-stream Ext spawn -- the canonical shape
    that exercises *both* terms of the §8 frontier-evolution rule: settling the
    first await spawns ``b``/``c`` (the ``new_ext_spawns`` term), and with a
    one-at-a-time settle the unawaited sibling carries over (the
    ``frontier_n - resolved_n`` term).
    """
    await ctx.rpc("a")
    b = ctx.rpc("b")
    c = ctx.rpc("c")
    await b
    await c
    return 0


async def _no_durable_ops(ctx: Context) -> int:
    """Return without any durable op: the tree is just the root, done at once.

    The most degenerate shape -- never a workflow (the ``_workflow`` flag stays
    unset), an empty frontier from iteration 0, ``D1`` trivially. Pins that the
    invariants survive a bare root with no children at all.
    """
    return 42


async def _parallel_suspended_locals(ctx: Context) -> int:
    """Two local children, each suspended on its own rpc -- a frontier across siblings.

    Two ``(int, pending)`` parents, each kept alive by an ``(ext, pending)``
    descendant in a *different* subtree (U3 holds per-branch). The frontier walk
    must collect a leaf from each, and settling one settles only its branch --
    the other carries over (the §8 ``frontier_n - resolved_n`` term, here across
    parallel locals rather than awaited siblings).
    """
    a = ctx.run(_child_blocks_on_rpc)
    b = ctx.run(_child_blocks_on_rpc)
    return await a + await b


async def _local_child_fans_out(ctx: Context) -> int:
    """Run one local child holding *two* Ext descendants -- one Int, two frontier leaves.

    Where :func:`_suspended_local` keeps one Ext leaf under its Int parent, this
    folds two up (the child awaits one rpc but spawns two), so a single
    ``(int, pending)`` node sits above a two-element frontier slice. Tests the
    fold-up of multiple todos through one suspended local.
    """
    return await ctx.run(_child_fans_out_rpcs)


async def _deep_suspended_local(ctx: Context) -> int:
    """Run a depth-3 suspended-local chain -- U3 kept alive transitively up two Int levels.

    ::

        f (int, pending)
        └── f.1   (int, pending)       mid child
            └── f.1.1   (int, pending) grandchild
                └── f.1.1.1 (ext, pending) the rpc keeping the whole chain alive

    Both Int ancestors are pending yet useful: the single deep ``(ext, pending)``
    leaf is the only thing in the frontier, and U3 must thread up through both.
    Settling it collapses the chain to done in one step.
    """
    return await ctx.run(_child_runs_blocking_grandchild)


async def _detached_then_local_done(ctx: Context) -> int:
    """Detached child + a completing local -- done at iteration 0 *with* a pending Det.

    Unlike :func:`_detached_then_rpc` (which suspends on the rpc), the only other
    work here is a local child that settles immediately, so the body fulfills on
    the first run while the Det node is still pending -- the strongest D1 check:
    a non-empty tree with a live (but exempt) detached subtree still counts as
    done.
    """
    await ctx.detached("side")
    return await ctx.run(_leaf)


async def _detached_unawaited_then_rpc(ctx: Context) -> None:
    """Fire a Det node fire-and-forget -- never await its future -- then block on an rpc.

    Unlike :func:`_detached_then_rpc` (which awaits the detached future for its
    id), here the future is dropped on the floor: ``ctx.detached`` is a bare
    statement. The Det node is still recorded at the call site and its promise
    still created through the creation chain (the following rpc's deferred create
    waits on the same chain link, so flushing it forces the detached create), yet
    nothing ever joins or awaits the detached task itself. Pins that the
    fire-and-forget pattern leaves the tree exactly as well-formed as the awaited
    one -- the Det node stays exempt, so the lone live dependency is the rpc.
    """
    ctx.detached("side")
    await ctx.rpc("a")


async def _interleaved_local_remote(ctx: Context) -> int:
    """Alternating local/remote phases -- a *new* Int node spawned past a settled await.

    ::

        run(leaf); await rpc("a"); run(leaf); await rpc("b")

    Settling ``a`` unblocks a phase that spawns a fresh local child (an Int, not
    an Ext) which then settles under our lease. Exercises extension that appends
    a non-Ext node -- and pins that the §8 ``new_ext_spawns`` term correctly
    excludes that Int (it never enters the frontier) while the replay still
    extends past the await.
    """
    await ctx.run(_leaf)
    await ctx.rpc("a")
    await ctx.run(_leaf)
    await ctx.rpc("b")
    return 0


# ── Every corpus function must satisfy every invariant ───────────────────────


CORPUS = [
    _all_local,
    _nested_local_then_rpc,
    _rpc_chain,
    _sleep_then_rpc,
    _bare_promise,
    _fanout,
    _suspended_local,
    _detached_then_rpc,
    _phased_rpcs,
    _no_durable_ops,
    _parallel_suspended_locals,
    _local_child_fans_out,
    _deep_suspended_local,
    _detached_then_local_done,
    _detached_unawaited_then_rpc,
    _interleaved_local_remote,
]


@pytest.mark.asyncio
@pytest.mark.parametrize("func", CORPUS)
async def test_idempotent_replay_holds(
    func: Callable[Concatenate[Context, ...], Any],
) -> None:
    """R1: replay over an unchanged cache prunes to a fixed point, for any body.

    Recursive form. Replay repeatedly over the *same* untouched cache for up to
    ``MAX_ROUNDS`` rounds and assert the fixed-point contract at every iteration:
    each replay is a prune of the one before -- or *equal*, once there is nothing
    left to prune (unchanged cache: only pruning is possible, and a body with no
    completed Int subtree prunes nothing). From iteration 1 onward the tree is
    equal to the iteration-1 tree -- the fixed point reached after the first
    replay, held forever after (``inner(inner(X)) = inner(X)``, ``tree.md`` §7).
    """
    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    run0 = await core.execute_until_blocked_inner(root, effects)
    cache0 = dict(effects.cache)

    run1 = await core.execute_until_blocked_inner(root, effects)
    cache1 = dict(effects.cache)

    assert cache0 == cache1
    assert run1.tree.is_prune_of(run0.tree)


@pytest.mark.asyncio
@pytest.mark.parametrize("func", CORPUS)
async def test_settling_frontier_extends(
    func: Callable[Concatenate[Context, ...], Any],
) -> None:
    """R2: settling each successive frontier and replaying extends the baseline.

    Recursive form. Rather than settling the frontier in a single batch,
    branch on *each* frontier promise individually: fork the effects, settle
    exactly one promise, replay, and assert the new tree is a *valid replay* of
    the previous one -- then recurse from the new state, whose frontier may hold
    ids the previous one never had (an rpc chain's second link, the ``b``/``c`` a
    phased body spawns once ``a`` settles). Every replay along every
    settle-to-done path is one of the three mutually-exclusive replay relations:
    a pure extension (growth past a freshly-unblocked await, or a settle-only
    kind flip), a pure prune (a completed Int subtree at the terminal step), or
    both at once (``tree.md`` §6/§8). Bounded by ``MAX_ROUNDS``; a path that has
    not converged simply stops, having had R2 checked at every state.
    """
    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    run = await core.execute_until_blocked_inner(root, effects)

    async def walk(run: _ExecOutcome, effects: MockEffects, depth: int) -> None:
        if isinstance(run, _ExecFulfilled) or depth == MAX_ROUNDS:
            return

        assert run.todos == run.tree.frontier()

        for batch in chain.from_iterable(
            combinations(run.todos, r) for r in range(1, len(run.todos) + 1)
        ):
            forked = effects.copy()
            for blocking in batch:
                _settle_external(forked, blocking)

            new_run = await core.execute_until_blocked_inner(root, forked)
            assert new_run.tree.is_prune_and_extension_of(run.tree)

            await walk(new_run, forked, depth + 1)

    await walk(run, effects, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("func", CORPUS)
async def test_type_stable_and_kind_monotone_across_replay(
    func: Callable[Concatenate[Context, ...], Any],
) -> None:
    """§6: Type is stable and Kind monotone across replay -- in both tree *and* cache.

    The replay-preservation rows of ``tree.md`` §6, asserted directly rather than
    bundled inside ``is_prune_of`` / ``is_extension_of`` (R1 / R2), and against
    *both* layers that the durable lattice governs:

    * **Tree** -- a node id shared by two consecutive trees keeps its ``type``
      (its settler is fixed at the call site) and never un-settles (a ``settled``
      node stays ``settled``).
    * **Cache** -- the underlying promise store advances the same way: a record
      that is already terminal (non-``pending``) is frozen, byte-for-byte
      identical on the next replay -- the durable store never rewrites a settled
      promise.

    Both checks range over the id intersection of each consecutive pair, for up
    to ``MAX_ROUNDS`` settle-and-replay rounds (settling the whole frontier
    between runs); the body need not converge for the property to hold at every
    round.
    """
    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    tree_prev = (await core.execute_until_blocked_inner(root, effects)).tree
    cache_prev = dict(effects.cache)
    for _ in range(MAX_ROUNDS):
        for blocking in tree_prev.frontier():
            _settle_external(effects, blocking)

        tree_next = (await core.execute_until_blocked_inner(root, effects)).tree
        cache_next = dict(effects.cache)

        for id in set(tree_prev.ids()) & set(tree_next.ids()):
            prev_node = tree_prev.get(id)
            next_node = tree_next.get(id)
            assert prev_node is not None
            assert next_node is not None
            assert prev_node.type == next_node.type  # Type stability
            if prev_node.kind == "settled":
                assert next_node.kind == "settled"  # Kind monotonicity

        for id in cache_prev.keys() & cache_next.keys():
            if cache_prev[id].state != "pending":  # a settled record is frozen
                assert cache_next[id] == cache_prev[id]

        if not tree_next.frontier():
            break

        tree_prev, cache_prev = tree_next, cache_next
