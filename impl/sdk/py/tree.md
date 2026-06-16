# The Execution Tree

The execution tree is the in-memory model of one workflow attempt's call graph. It exists in worker memory only, mirrors the durable promise records the workflow created, and is used to **assert** that the worker's behavior matches the suspension contract. The runtime itself does not read the tree.

This document is the consolidated specification of the model: types, invariants, replay semantics, fixed-point properties, frontier evolution rule, concurrency story, and implementation index.

---

## 1. Where the tree lives in the spec's architecture

The spec splits a worker into two layers:

- **`execute_until_blocked_outer`** — the task-lifecycle driver: claim, heartbeat, fulfill / suspend / release. Knows nothing about the workflow body.
- **`execute_until_blocked_inner`** — runs the user function once and reports an outcome: `Done { state, value }` or `Suspended { awaited: Promise[] }`.

The tree is observable at the **boundary between these two layers**. It captures what the inner produced — the set of promises the workflow body created, who is responsible for settling each, and which ones are still pending. The outer does not need it (it gets `awaited[]` directly from the inner's outcome). Tests, debuggers, and predicates use it.

The tree's contract is exactly the inner's contract, rendered as structured data instead of an enum return.

---

## 2. Node classification: Type and Kind

Each node has two orthogonal attributes.

### Type — who settles this node?

| Type | Created by | Settled by |
|---|---|---|
| **Int** (internal) | `ctx.Run` | this worker, under our task lease (Run's goroutine settles the durable promise on return) |
| **Ext** (external) | `ctx.RPC`, `ctx.Sleep`, `ctx.Promise` | something we await — another worker, the server's timer thread, an external `promise.settle` caller |
| **Det** (detached) | `ctx.Detached` | outside this workflow's contract — fire-and-forget; we don't await it |

Type is assigned at the call site and never changes.

### Kind — has this node settled?

| Kind | Meaning |
|---|---|
| **Pending** | the durable promise is still in the `pending` state |
| **Settled** | the durable promise has reached a terminal state |

The five-state lattice of the durable promise (`pending | resolved | rejected | rejected_canceled | rejected_timedout`) collapses to a single bit here. The success/failure distinction matters at `Future.Await` time (decode the value vs. build an error) but not for the structural suspension contract.

Kind transitions monotonically: Pending → Settled only.

### The 2×3 product

|         | Pending | Settled |
|---|---|---|
| **Int** | local executor still running, OR it has suspended awaiting an Ext descendant | local executor completed (success or failure) |
| **Ext** | in the suspension **frontier** | settled by something we await |
| **Det** | detached child still in flight — irrelevant to our contract | detached child completed externally — irrelevant to our contract |

The frontier is exactly the **(Ext, Pending)** cell. Det rows are exempt from every rule.

---

## 3. The frontier

The **frontier** is the set of node IDs that are simultaneously:

- `Type == Ext`
- `Kind == Pending`
- not in a Det subtree

These are the IDs the outer registers callbacks for in `task.suspend`. They are the workflow's remote dependencies — the promises whose settlement will eventually unblock further progress.

Computed by `Tree.Frontier()`:

```
Frontier(root):
  out = []
  for each id reachable from root (depth-first):
    if tree[id].Type == Det:    skip the entire subtree
    elif tree[id].Type == Ext ∧ tree[id].Kind == Pending:
      out.append(id); skip the subtree
    else:
      descend into children
  return out
```

The walk stops at Det because Det subtrees live in another workflow's tree by definition.

### The frontier and `outcome.todos` are not equal

A subtle and important distinction:

- `tree.Frontier()` = **all** (Ext, Pending) nodes — the entire pending remote-leaf set
- `outcome.todos` (from `Context.drainSpawnedRemote()`) = the subset of (Ext, Pending) nodes that user code explicitly **awaited** via `Future.Await`

Example:

```go
fa := ctx.RPC("a")     // tree: root.1 (Ext, Pending)
fb := ctx.RPC("b")     // tree: root.2 (Ext, Pending)
fc := ctx.RPC("c")     // tree: root.3 (Ext, Pending)
fa.Await(nil)          // appendRemoteTodo("root.1"); panic — fb/fc unreached
```

After the body suspends:
- `tree.Frontier()` = `[root.1, root.2, root.3]`
- `outcome.todos` = `[root.1]`

Both are valid views. The runtime registers callbacks only on the awaited subset — it's an efficiency win (sibling settlements wouldn't unblock current code anyway). The relationship is captured by **invariant S4**: `outcome.todos ⊆ tree.Frontier()`.

---

## 4. Well-formedness — what the tree must always satisfy at inner return

Seven rules, grouped by scope.

### Universal (every inner return)

- **U1** — Root has (`Type=Int`, `Kind=Pending`). The root is settled by `task.fulfill` in the outer; the inner never settles it.
- **U2** — Every node is reachable from the root.
- **U3** — Every non-root, non-Det node is **useful**: either `Kind=Settled`, OR has at least one (Ext, Pending) node in its subtree (including itself). A node failing U3 is a "dead pending branch" — pending with no path to further progress.

### Done (when inner returns `Done`)

- **D1** — `Frontier()` is empty. Combined with U3, this is equivalent to "every non-root, non-Det node has `Kind=Settled`."

### Suspended (when inner returns `Suspended`)

- **S1** — `Frontier()` is non-empty.
- **S4** — `outcome.todos ⊆ Frontier()` (when `todos` is non-nil).

### Why U3 is the unified rule

U3 subsumes two older rules:

- **S2** (no Int-Pending leaves) — a leaf has no descendants, so it cannot have an (Ext, Pending) descendant; a Pending leaf must be Settled to satisfy U3 → violation.
- **S3** (every Int-Pending has an Ext-Pending descendant) — same statement, generalized.

The intuition: **the tree has no dead pending branches**. A Pending node only exists because something further down keeps it pending. If nothing pending exists below, the node should have already settled.

In Done state, no (Ext, Pending) exists anywhere → every node falls back to the "Settled" disjunct → equivalent to D1.

### What an Int-Pending non-root node represents

An Int node whose `Run` goroutine is in flight is `(Int, Pending)`. By U3, it must either eventually settle (its executor completes) or have at least one (Ext, Pending) descendant.

The latter is the *suspended local* case: the goroutine ran the user function, panicked on a `Future.Await` on a pending Ext, and `executeLocal` recovered the panic and folded the child's remote-todos up into the parent's `spawnedRemote`. The durable promise stays Pending; the tree shape shows the structural relationship.

---

## 5. The tree as a function of cache

For one inner invocation, the tree's shape is fully determined by:

1. The workflow body (deterministic by assumption — same inputs produce same calls in same order)
2. The cache state at invocation time (which promise IDs exist, with what States)

Formally, write `inner(body, args, cache) → (tree, frontier, cache')` where `cache'` is `cache` plus whatever the body created. The function is deterministic in `(body, args, cache)`.

The tree carries no extra information beyond what's derivable from the body + cache. It is the *materialized view* of one inner invocation's structural state.

---

## 6. Replay — same body, richer cache

Replay is the mechanism by which durable execution survives crashes. When a worker dies mid-workflow, another worker re-runs the body against the same durable store. The new run sees the existing promises in the store and *skips* the work that already completed durably.

The tree expresses what "skipping" means:

### Pruning rule

When `ctx.Run(fn)` finds an already-Settled durable promise (because the previous attempt completed this local child):

```go
rec, _ := c.effects.CreatePromise(...)
if rec.State != PromiseStatePending {
    c.tree.Settle(childID)
    return &Future{... record: &rec}, nil   // short-circuit: no goroutine
}
```

The local function body **never executes** — so any children it would have spawned are never added to the tree. The replay tree is **pruned wherever an Int subtree completed in a prior run**.

For Ext, the behavior is symmetric: `ctx.RPC(name)` finds the existing record, marks Settled if non-Pending, returns a future. No re-execution of remote work (it never ran on this worker anyway).

### What's preserved across replay

Even with pruning, the following hold between consecutive trees `tree_n` and `tree_{n+1}` produced from a stable workflow body against a monotonically-growing cache:

| Property | Statement |
|---|---|
| Type stability | for any id in both trees: `tree_n[id].Type == tree_{n+1}[id].Type` |
| Kind monotonicity | for any id in both trees: `tree_n[id].Kind = Settled` ⇒ `tree_{n+1}[id].Kind = Settled` |
| Children-as-prefix | for any id in both trees: `tree_n[id].Children` is a prefix of `tree_{n+1}[id].Children` |

These are the "is-a-replay-of" properties. They are not codified as a single predicate in the SDK (yet), but they are *checkable* between any two snapshots.

### What's lost

- Nodes pruned from `tree_n` to `tree_{n+1}` (descendants of a now-Settled Int) may not be in `tree_{n+1}`. The pruning is *not a violation* of replay — it's the desired behavior. Tree_{n+1} ⊃ tree_n is NOT an invariant.

---

## 7. Fixed-point properties

When the durable store doesn't change between iterations, the system stabilizes quickly.

Let `inner_n` = invocation of the inner with `cache_n` as preload, producing `(tree_n, frontier_n, cache_{n+1})`.

### Stability times under unchanging external state

| Quantity | Stable from iteration | Why |
|---|---|---|
| `frontier_n` | n = 0 | Determined by which Ext promises exist and their state — body is deterministic, cache is monotonic |
| `cache_n` | n = 1 | First attempt creates every promise the workflow will ever create. Subsequent attempts find them via idempotent server creates — no new records |
| `tree_n` | n = 1 | Pruning is idempotent on its own output: once pruned, re-pruning produces the same shape |

So:

```
inner_0 (cache=∅)            → (tree_0 full,    frontier_0, cache_1)
inner_1 (cache=cache_1)      → (tree_1 pruned,  frontier_1=frontier_0, cache_2=cache_1)
inner_2 (cache=cache_2)      → (tree_2=tree_1,  frontier_2=frontier_1, cache_3=cache_2)
...
```

### Fixed-point statement

When the external world does not change between calls:

```
inner(inner(X)) = inner(X)         where X = (body, args, cache)
```

One iteration is enough to converge. From iteration 1 onward, the tuple `(tree, frontier, cache)` is invariant.

### Disturbance

When the world advances — e.g., an external `promise.settle` between iterations:

```
inner_n: (tree_n, frontier_n, cache_{n+1})
[external: settle some p ∈ frontier_n]
inner_{n+1}: (tree_{n+1}, frontier_{n+1}, cache_{n+2})
```

The system re-stabilizes at a *new* fixed point. One iteration absorbs the change.

---

## 8. Frontier evolution rule

The strongest cross-iteration invariant. Between any two consecutive inner invocations:

```
frontier_{n+1} ⊆ (frontier_n \ resolved_n) ∪ new_ext_spawns_n
```

where:

- `resolved_n` = the set of Ext promise IDs that became Settled in the cache between iter n and iter n+1 (external settlement, OR an Int's goroutine settling its descendant)
- `new_ext_spawns_n` = the set of (Ext, *) IDs in `tree_{n+1}` whose IDs do not appear in `tree_n`

In words: every element of the new frontier must be either:
1. A previously-pending frontier element that hasn't been resolved yet, or
2. A new Ext promise the workflow body created after progressing past an await.

There is no third case — anything else in the new frontier indicates a bug.

### What violations would indicate

| Violation | Underlying bug |
|---|---|
| New frontier contains an id not in `tree_n` and not Ext-typed in `tree_{n+1}` | `Frontier()` walk is leaking non-Ext nodes |
| New frontier contains an id from `tree_n` that wasn't in `prev_frontier` | The previous `Frontier()` walk was incomplete |
| New frontier contains a previously-resolved id | `tree.Settle` failed, or `Frontier()` reads stale data |
| Empty new frontier with no Done outcome AND non-resolved-set | Frontier collapsed without progress — bug somewhere in the unblock path |

### The complete cross-iteration contract

```
∀ inner invocation n:
   WellFormed(tree_{n+1}, status_{n+1})                            ← per-iteration
   settled(cache_{n+1}) ⊇ settled(cache_n)                          ← monotone
   frontier_{n+1} ⊆ (frontier_n \ resolved_n) ∪ new_ext_spawns_n   ← evolution

   eventually: frontier_n → ∅       (workflow terminates)
```

Three predicates plus a termination guarantee. They form the entire externally-observable structural contract of replay.

---

## 9. Worked example: a phased workflow under replay

Body:
```go
func(c *Context) (int, error) {
    fa, _ := c.RPC("a", nil)
    fa.Await(nil)              // suspends until a settles
    fb, _ := c.RPC("b", nil)
    fc, _ := c.RPC("c", nil)
    fb.Await(nil)              // suspends until b settles
    return 0, fc.Await(nil)    // suspends until c settles
}
```

### Iteration 0 — cache empty

Body runs, creates `root.1` (Pending), panics on `Await(root.1)`.

```
tree_0:
  root (Int, Pending)
  └── root.1 (Ext, Pending)
frontier_0 = [root.1]
cache_1 = {root.1: Pending}
```

### External: settle root.1

### Iteration 1 — preload {root.1 Settled}

Body re-runs. `RPC("a")` finds Settled root.1 → tree settles it. `Await(root.1)` returns. `RPC("b")` creates root.2 Pending. `RPC("c")` creates root.3 Pending. `Await(root.2)` panics.

```
tree_1:
  root (Int, Pending)
  ├── root.1 (Ext, Settled)
  ├── root.2 (Ext, Pending)
  └── root.3 (Ext, Pending)
frontier_1 = [root.2, root.3]
cache_2 = {root.1: Settled, root.2: Pending, root.3: Pending}
```

Frontier evolution check:
- `resolved_0 = {root.1}`
- `frontier_0 \ resolved_0 = ∅`
- `new_ext_spawns_0 = {root.2, root.3}`
- `frontier_1 = [root.2, root.3] ⊆ ∅ ∪ {root.2, root.3}` ✓

### External: settle root.3

### Iteration 2 — preload {root.1 Settled, root.2 Pending, root.3 Settled}

Body re-runs to `Await(root.2)`, still Pending, panics.

```
tree_2:
  root (Int, Pending)
  ├── root.1 (Ext, Settled)
  ├── root.2 (Ext, Pending)
  └── root.3 (Ext, Settled)
frontier_2 = [root.2]
```

Frontier evolution check:
- `resolved_1 = {root.3}`
- `frontier_1 \ resolved_1 = {root.2}`
- `new_ext_spawns_1 = ∅`
- `frontier_2 = [root.2] ⊆ {root.2} ∪ ∅` ✓

### External: settle root.2

### Iteration 3 — all settled

Body runs to completion. Returns `(0, nil)`.

```
tree_3:
  root (Int, Pending)
  ├── root.1 (Ext, Settled)
  ├── root.2 (Ext, Settled)
  └── root.3 (Ext, Settled)
frontier_3 = []
```

`WellFormed(tree_3, Done)` holds: D1 (frontier empty), U3 (every node useful — all Settled).

The workflow has terminated. The outer can now `task.fulfill`.

---

## 10. Concurrency

The tree is safe for concurrent use across the parent's goroutine and any number of `ctx.Run`-spawned child goroutines.

### Synchronization

A single `sync.Mutex` guards `t.nodes` (the ID-keyed map) and the per-node `Children` slice. Every public method acquires the lock; internal `*Locked` helpers assume it's held.

### Writers

- **Parent body** (main workflow goroutine): calls `AddChild` for each spawn.
- **Spawned local goroutine**: calls `AddChild` for its own spawns + `Settle` when it completes successfully.

Writers do not contend on shared sub-state: each goroutine appends only to its own context's child list (different parent IDs). The mutex serializes the map and the slice append.

### Readers

`Frontier()`, `WellFormed()`, `Print()` are called at quiescent points — after `flushLocalWork()` has joined every spawned goroutine. At that moment the tree is stable.

### Verified under chaos

The race detector clears every tree-related test under randomized goroutine timings:

- 25 seeds × 4 chaos scenarios (settle, suspend, mixed, nested-concurrent-subtrees)
- 200 iterations of a 3-Run workflow with random jitters
- 25 seeds × 100 concurrent `AddChild` calls on the same id (idempotence stress)

No tree-level race ever surfaces.

---

## 11. What the tree is not

- **Not durable.** The tree is per-attempt; a worker restart builds a fresh one. The Resonate server's durable promise records are the canonical source of truth.
- **Not consumed by the runtime.** No control-flow decision in `execute_until_blocked_inner` reads the tree. The outer drains `Context.spawnedRemote` for its todos list. The tree is a parallel, assertion-only view.
- **Not a trace.** A trace is event-sourced (records every action over time). The tree is a snapshot (state at one moment). They serve different purposes; both are useful.
- **Not a replacement for the spec.** The spec defines `execute_until_blocked_inner` and the durable contract. The tree is a tool for checking that an implementation matches the spec — it does not extend or replace it.

---

## 12. Implementation index

### Types and constants (`tree.go`)

| Symbol | Meaning |
|---|---|
| `NodeType` | enum: `Int`, `Ext`, `Det` |
| `NodeKind` | enum: `Pending`, `Settled` |
| `Node` | `{ ID, Type, Kind, Children []string }` |
| `Tree` | `{ root string, nodes map[string]*Node, mu sync.Mutex }` |

### Constructors

| Function | Purpose |
|---|---|
| `NewTree(rootID) *Tree` | Builds a tree with the root as (Int, Pending). |

### Mutation

| Function | Semantics |
|---|---|
| `(t).AddChild(parent, id, type) bool` | Idempotent on ID. Returns true if inserted, false if already present. |
| `(t).Settle(id)` | Monotonic — no-op if already Settled or unknown. |

### Inspection

| Function | Returns |
|---|---|
| `(t).Root() string` | Root ID. |
| `(t).Get(id) *Node` | A defensive copy of the node, or nil. |
| `(t).Has(id) bool` | Whether the ID exists in the tree. |
| `(t).Size() int` | Total node count. |
| `(t).IDs() []string` | Every node ID, unordered. |
| `(t).Frontier() []string` | (Ext, Pending) IDs, skipping Det subtrees. |
| `(t).Print() string` | ASCII tree diagram, children in insertion order. |

### Predicates

| Function | Checks |
|---|---|
| `(t).Useful() error` | U3 alone — returns a multi-line error listing dead pending branches. |
| `(t).WellFormed(status, todos) error` | U1 ∧ U2 ∧ U3 ∧ status-specific (D1 / S1 / S4) — returns a multi-line error or nil. |

### Wiring (`context.go`)

| Location | What happens |
|---|---|
| `Context.tree *Tree` | Tree pointer shared across the workflow attempt. |
| `NewRootContext(...)` | Calls `NewTree(rootID)`. |
| `Context.child(...)` | Copies the tree pointer into the new context. |
| `Context.Tree() *Tree` | Public accessor (used by tests). |
| `ctx.Run` | `tree.AddChild(c.id, childID, Int)` before create. On already-settled (idempotent replay), `tree.Settle(childID)` immediately. After `executeLocal` settle: `tree.Settle(childID)`. |
| `ctx.RPC` / `ctx.Sleep` / `ctx.Promise` | `tree.AddChild(c.id, childID, Ext)` before create. `tree.Settle(childID)` if create returned a non-Pending record. |
| `ctx.Detached` | `tree.AddChild(c.id, childID, Det)`. `tree.Settle(childID)` if create returned non-Pending. |

---

## 13. Test inventory

| File | Coverage |
|---|---|
| `tree_test.go` | Tree primitive unit tests; `Useful()` positive/negative; `WellFormed` negative tests (U1/U2/U3/D1/S1/S4 violations) |
| `core_tree_test.go` | 28 workflow-driven shape tests via `Core.ExecuteUntilBlocked`: bare RPC, fan-out, nested Runs, mixed kinds, ordering variants, preloaded promises, determinism, detached |
| `core_tree_chaos_test.go` | 6 concurrency chaos scenarios across 25 seeds each: settle race, suspend race, mixed, nested-concurrent, stress (200 iters), concurrent AddChild idempotence |
| `tree_replay_test.go` | 8 pruning scenarios; fixed-point with/without external changes; random frontier progression across 3 workflow shapes × 25 seeds; exact-settlement-count variant across 50 seeds; frontier evolution rule asserted at every replay step |

Total: ~70 top-level tests producing 500+ subtest invocations.

Combined `-race` run completes in approximately 4 seconds.

---

## 14. Open questions / future work

These were considered but not implemented:

- **`Tree.IsReplayOf(prev *Tree) error`** — a single named predicate bundling the replay-time invariants (containment, type stability, kind monotonicity, children-as-prefix). The pieces exist in `assertFrontierEvolution`; promoting them to a method on Tree would let any test add this as a one-line check.
- **Strict frontier registration semantics**. The Go SDK registers callbacks only on `outcome.todos` (the awaited subset), not the full `Frontier()`. This is an efficiency optimization. Whether the spec should mandate "full frontier registration" is an open design question with implications for spurious-wakeup behavior under fan-out.
- **Det subtree visibility**. Det nodes are currently leaf records (no children tracked). If the SDK ever wanted to expose a detached child's *own* tree (cross-graph reference), additional bookkeeping would be needed.

These do not block correctness — the existing model is complete relative to the contract as we've established it. They are extension points if the model grows.

---

## 15. The one-line summary

> The execution tree is the structured contract that `execute_until_blocked_inner` produces. Its shape at any one return must satisfy `WellFormed(status)`; its evolution across iterations must satisfy the frontier evolution rule. Together with monotone cache growth, those are the structural invariants of replay-safe durable execution.
