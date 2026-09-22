package io.resonatehq.resonate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * The execution tree — the in-memory model of one workflow attempt's call graph, mirroring {@code
 * resonate.tree} from the Python SDK.
 *
 * <p>It is an <b>assertion-only</b> structure: the runtime never reads it to make a control-flow
 * decision; instead it materializes what one execution pass produced — the set of promises the body
 * created, who settles each, and which are still pending — so that {@link #wellFormed} can assert
 * the worker's behavior matches the suspension contract on every return.
 *
 * <p>A node's {@code type} (who settles it) and {@code kind} (whether it has settled) form a 2×3
 * product whose {@code (ext, pending)} cell is the suspension {@link #frontier}.
 *
 * <p>Two implementation notes:
 *
 * <ul>
 *   <li><b>One lock.</b> In Python every writer is an asyncio task on a single event loop, so the
 *       structure needs no lock. This runtime executes logically-concurrent child bodies on a real
 *       thread pool (sibling {@code ctx.run} continuations run in parallel), so every public method
 *       is {@code synchronized} on the instance monitor: the node map and the per-node child lists are
 *       guarded as a unit, and a whole-tree read ({@link #wellFormed} / {@link #frontier} /
 *       {@link #useful}) can never observe a half-applied {@link #addChild}. The lock is coarse but
 *       cheap — no public method blocks on I/O (the network create/settle happens outside the tree),
 *       so contention is negligible and there is no deadlock risk.
 *   <li><b>Predicates raise.</b> Following the repo-wide convention ({@link Codec} / core), {@link
 *       #useful} and {@link #wellFormed} return {@code void} and raise {@link AssertionError} with a
 *       multi-line message on violation. The tree is a pure assertion layer, so a failed check is a
 *       bug in the SDK, not a recoverable condition.
 * </ul>
 *
 * <p><b>Strings, not enums.</b> Python types {@code type} / {@code kind} / {@code status} as {@code
 * Literal[...]}; as in {@link Types}, we keep them as {@link String} constants (no runtime enum) for
 * cross-SDK parity.
 */
public final class Tree {

    // ── NodeType: who is responsible for settling a node's durable promise ──────────
    // Assigned at the call site and never changes:
    //
    //   "int" — internal, created by ctx.run; this worker settles it under our task lease when the
    //           local executor returns.
    //   "ext" — external, created by ctx.rpc / ctx.sleep / ctx.promise; settled by something we await
    //           (another worker, the server's timer, an external promise.settle caller).
    //   "det" — detached, created by ctx.detached; fire-and-forget, outside this workflow's contract.
    //           Det subtrees are exempt from every rule and skipped by the frontier walk.

    /** Internal: created by {@code ctx.run}, settled by this worker. */
    public static final String INT = "int";

    /** External: created by {@code ctx.rpc} / {@code ctx.sleep} / {@code ctx.promise}, settled elsewhere. */
    public static final String EXT = "ext";

    /** Detached: created by {@code ctx.detached}, fire-and-forget, exempt from every rule. */
    public static final String DET = "det";

    // ── NodeKind: whether a node's durable promise has reached a terminal state ─────
    // The five-state durable lattice (pending | resolved | rejected | rejected_canceled |
    // rejected_timedout) collapses to this single bit — the success/failure distinction matters at
    // Future.await time, not for the structural suspension contract. Transitions monotonically:
    // pending -> settled only.

    /** The node's durable promise is not yet in a terminal state. */
    public static final String PENDING = "pending";

    /** The node's durable promise has reached a terminal state. */
    public static final String SETTLED = "settled";

    // ── Status: the outcome an execution pass can report at a tree-assertion boundary ─
    // An error-fulfill (the body raised an ApplicationError) is a "done" with a rejected settle
    // state, not a separate tree-level status.

    /** The body returned (including a rejected fulfill). */
    public static final String DONE = "done";

    /** The body suspended, waiting on a remote dependency. */
    public static final String SUSPENDED = "suspended";

    /**
     * One promise in the call graph.
     *
     * <p>Mutable: {@code kind} flips {@code pending} -> {@code settled} exactly once, and {@code
     * children} grows as the body spawns. {@code children} holds child IDs in insertion (== call)
     * order, which is what makes the children-as-prefix replay property and {@link Tree#print}
     * deterministic.
     */
    public static final class Node {
        private final String id;
        private String type;
        private String kind;
        private final List<String> children;

        public Node(String id, String type, String kind) {
            this(id, type, kind, new ArrayList<>());
        }

        public Node(String id, String type, String kind, List<String> children) {
            this.id = id;
            this.type = type;
            this.kind = kind;
            this.children = new ArrayList<>(children);
        }

        public String id() {
            return id;
        }

        public String type() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String kind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        /** The live child-id list (insertion order). Mutating a defensive copy from {@link Tree#get} is safe. */
        public List<String> children() {
            return children;
        }
    }

    private final String root;

    // Package-private (not private) so the assertion-layer tests can poke invalid states that the
    // public API cannot reach by construction — a non-Int root, an unreachable node, a reclassified
    // shared node — exactly as the Python tests reach into `tree._nodes`. LinkedHashMap preserves
    // insertion order, matching Python's dict, so ids() and useful()'s dead-branch listing are
    // deterministic.
    final Map<String, Node> nodes = new LinkedHashMap<>();

    public Tree(String rootId) {
        this.root = rootId;
        nodes.put(rootId, new Node(rootId, INT, PENDING));
    }

    // ── inspection ──────────────────────────────────────────────────

    /** Return the root node's id. */
    public String root() {
        return root;
    }

    /** Whether {@code id} exists in the tree. */
    public synchronized boolean has(String id) {
        return nodes.containsKey(id);
    }

    /** Total node count. */
    public synchronized int size() {
        return nodes.size();
    }

    /** Every node id, in insertion order. */
    public synchronized List<String> ids() {
        return new ArrayList<>(nodes.keySet());
    }

    /**
     * Return a defensive copy of the node, or {@code null} if absent.
     *
     * <p>Returns a copy (including a fresh {@code children} list) so callers cannot mutate tree state
     * through the handle.
     */
    public synchronized Node get(String id) {
        Node node = nodes.get(id);
        return node == null ? null : new Node(node.id, node.type, node.kind, node.children);
    }

    // ── mutation ────────────────────────────────────────────────────

    /**
     * Attach a child of {@code type} under {@code parent}; idempotent on {@code id}.
     *
     * <p>Returns {@code true} if a node was inserted, {@code false} if {@code id} already existed (so a
     * replay that re-walks the same body does not duplicate nodes or re-append to the parent's child
     * list). The parent must already be in the tree.
     */
    public synchronized boolean addChild(String parent, String id, String type) {
        if (!nodes.containsKey(parent)) {
            throw new AssertionError("unknown parent '" + parent + "'");
        }
        if (nodes.containsKey(id)) {
            return false;
        }
        nodes.put(id, new Node(id, type, PENDING));
        nodes.get(parent).children().add(id);
        return true;
    }

    /** Mark {@code id} settled; monotonic, no-op if already settled or unknown. */
    public synchronized void settle(String id) {
        Node node = nodes.get(id);
        if (node == null) {
            return;
        }
        node.setKind(SETTLED);
    }

    // ── frontier ────────────────────────────────────────────────────

    /**
     * Return the {@code (ext, pending)} node IDs, skipping Det subtrees.
     *
     * <p>These are the workflow's live remote dependencies — the promises whose settlement will
     * unblock further progress. A depth-first walk in child insertion order: a Det node prunes its
     * whole subtree (it lives in another workflow's tree); an {@code (ext, pending)} node is collected
     * and its subtree pruned; everything else descends.
     *
     * <p>Note this is a <b>superset</b> of {@code outcome.todos} (the awaited subset the runtime
     * registers callbacks for): the frontier is <em>every</em> pending remote leaf, including ones the
     * body created but never reached an {@code await} on. Invariant S4 ({@code todos subset frontier})
     * connects the two.
     */
    public synchronized List<String> frontier() {
        List<String> out = new ArrayList<>();
        Deque<String> stack = new ArrayDeque<>();
        stack.push(root);

        while (!stack.isEmpty()) {
            String currId = stack.pop();
            Node node = nodes.get(currId);

            if (DET.equals(node.type())) {
                continue;
            }

            if (EXT.equals(node.type()) && PENDING.equals(node.kind())) {
                out.add(currId);
                continue;
            }

            // Push children in reverse so the first child is popped first — a depth-first walk in
            // child insertion order.
            List<String> children = node.children();
            for (int i = children.size() - 1; i >= 0; i--) {
                stack.push(children.get(i));
            }
        }

        return out;
    }

    // ── predicates ──────────────────────────────────────────────────

    /**
     * Assert U3: no dead pending branches.
     *
     * <p>Every non-root, non-Det node must be either {@code settled} OR have at least one {@code (ext,
     * pending)} node in its subtree (itself included). A node failing this is <em>pending with no path
     * to further progress</em> — it should already have settled. Raises {@link AssertionError} listing
     * every dead branch.
     *
     * <p>A suspended-local Int node is kept alive only by an Ext-pending descendant somewhere in its
     * subtree.
     */
    public synchronized void useful() {
        List<String> dead = new ArrayList<>();
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String id = entry.getKey();
            Node node = entry.getValue();
            if (!id.equals(root) && !DET.equals(node.type()) && !SETTLED.equals(node.kind()) && !hasPendingExt(id)) {
                dead.add(id);
            }
        }
        if (!dead.isEmpty()) {
            String lines = dead.stream()
                    .map(id -> "  - " + id + " (dead pending branch)")
                    .collect(Collectors.joining("\n"));
            throw new AssertionError("U3 violated: pending node(s) with no Ext-pending descendant:\n" + lines);
        }
    }

    /**
     * Whether {@code id}'s subtree contains an {@code (ext, pending)} node.
     *
     * <p>A Det subtree never counts — a detached child keeps nothing pending in our contract, so an Int
     * parent whose only pending descendant is detached is still a dead branch.
     */
    private boolean hasPendingExt(String id) {
        Node node = nodes.get(id);
        if (DET.equals(node.type())) {
            return false;
        }
        if (EXT.equals(node.type()) && PENDING.equals(node.kind())) {
            return true;
        }
        for (String child : node.children()) {
            if (hasPendingExt(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Assert the tree matches the reported execution outcome.
     *
     * <p>Checked on <em>every</em> return from an execution pass. {@code status} is the outcome about
     * to be reported — {@link #DONE} (the body returned, including a rejected fulfill) or {@link
     * #SUSPENDED}; {@code todos} is the awaited subset ({@code Context.takeRemoteTodos()}), used for
     * S4. Universal rules apply in both states; the status-specific rule then pins the frontier.
     *
     * <ul>
     *   <li><b>U1</b> root is {@code (int, pending)}.
     *   <li><b>U2</b> every node reachable from the root.
     *   <li><b>U3</b> no dead pending branches ({@link #useful}).
     *   <li><b>D1</b> (done) frontier empty.
     *   <li><b>S1</b> (suspended) frontier non-empty; <b>S4</b> {@code todos subset frontier}.
     * </ul>
     *
     * <p>Raises {@link AssertionError} on the first violated rule.
     */
    public synchronized void wellFormed(String status, List<String> todos) {
        // U1 — the root is settled when the task is fulfilled, never by the body's own execution pass.
        Node rootNode = nodes.get(root);
        if (!INT.equals(rootNode.type())) {
            throw new AssertionError("U1 violated: root '" + root + "' has type " + rootNode.type() + ", expected int");
        }
        if (!PENDING.equals(rootNode.kind())) {
            throw new AssertionError(
                    "U1 violated: root '" + root + "' has kind " + rootNode.kind() + ", expected pending");
        }

        // U2 — every node must be reachable from the root.
        Set<String> reachable = new java.util.HashSet<>();
        Deque<String> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            String currId = stack.pop();
            if (reachable.add(currId)) {
                stack.addAll(nodes.get(currId).children());
            }
        }
        if (!reachable.equals(nodes.keySet())) {
            Set<String> unreachable = new TreeSet<>(nodes.keySet());
            unreachable.removeAll(reachable);
            throw new AssertionError("U2 violated: node(s) not reachable from root: " + new ArrayList<>(unreachable));
        }

        // U3 — no dead pending branches.
        useful();

        List<String> frontier = frontier();

        // S1 / D1 — the status-specific rule pins the frontier. Keyed on the reported status, not on
        // emptiness, so S4 below sees the suspended (non-empty) frontier too.
        if (SUSPENDED.equals(status)) {
            if (frontier.isEmpty()) {
                throw new AssertionError("S1 violated: suspended outcome with an empty frontier");
            }
        } else if (DONE.equals(status)) {
            if (!frontier.isEmpty()) {
                throw new AssertionError("D1 violated: done outcome with non-empty frontier " + frontier);
            }
        }

        // S4 — the awaited subset must lie within the full frontier. Holds in both states: when done
        // the frontier is empty, so this also pins todos empty.
        List<String> extra = todos.stream().filter(t -> !frontier.contains(t)).collect(Collectors.toList());
        if (!extra.isEmpty()) {
            throw new AssertionError("S4 violated: todos not subset of frontier; todos="
                    + todos
                    + " frontier="
                    + frontier
                    + " extra="
                    + extra);
        }
    }

    // ── replay comparison ───────────────────────────────────────────

    /**
     * Whether the trees share a root and every shared id keeps its {@code type}.
     *
     * <p>The direction-agnostic half of the replay contract: a node's settler is fixed at its call
     * site and never changes, so a reclassified shared node is an SDK bug regardless of which tree is
     * the later replay. All three of {@link #isPruneOf}, {@link #isExtensionOf}, and {@link
     * #isPruneAndExtensionOf} start here. Says nothing about containment — a mixed replay's node sets
     * need not nest, so the check ranges over the intersection only.
     */
    private boolean sharedTypeStable(Tree other) {
        if (!root.equals(other.root)) {
            return false;
        }
        for (String id : nodes.keySet()) {
            if (other.nodes.containsKey(id)
                    && !nodes.get(id).type().equals(other.nodes.get(id).type())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether every node {@code earlier} had settled is still settled in {@code this}.
     *
     * <p>Kind monotonicity: the durable lattice only advances, so a later replay is never <em>less</em>
     * settled than an earlier one. This is the one part of the replay contract whose direction is
     * fixed rather than containment-symmetric — {@code this} is always the later replay, so all three
     * replay predicates call it the same way.
     */
    private boolean isAtLeastAsSettledAs(Tree earlier) {
        for (String id : nodes.keySet()) {
            Node mine = nodes.get(id);
            Node theirs = earlier.nodes.get(id);
            if (theirs != null && SETTLED.equals(theirs.kind()) && !SETTLED.equals(mine.kind())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code this} is a <em>pruning</em> of {@code other}.
     *
     * <p>{@code this} is a <em>later</em> replay of the same body than {@code other} that <b>added
     * nothing</b> — it may have <b>dropped</b> nodes (a completed Int subtree short-circuited, so the
     * children its body would have spawned were never re-added) or dropped nothing at all (a
     * structurally unchanged replay, or a settle-only kind advance). This is the no-extension
     * specialization of {@link #isPruneAndExtensionOf}: the gate is one-sided ({@code added == ∅}), so
     * it is <em>not</em> exclusive with {@link #isExtensionOf} — an unchanged replay (both deltas
     * empty) satisfies both, and the unchanged/settle-only cell is exactly their overlap. A replay
     * that <em>grows</em> (adds a node {@code other} never had) is an extension only, never this.
     *
     * <ul>
     *   <li><b>Added nothing</b> — {@code this} has <em>no</em> node {@code other} lacks ({@code added
     *       == ∅}); {@code this} may drop nodes or none.
     *   <li><b>Root + type stability</b> ({@link #sharedTypeStable}) and <b>kind monotonicity</b>
     *       ({@link #isAtLeastAsSettledAs}) — the later replay is never less settled.
     *   <li><b>Pruned-as-prefix</b> — for every (shared) node, {@code this}'s child list must be a
     *       prefix of {@code other}'s. A pruning boundary (a settled Int whose body was skipped)
     *       collapses its <em>whole</em> child list to {@code []} (a prefix of anything); every node
     *       above it keeps its children verbatim. Pruning is all-or-nothing per node, never a middle
     *       drop, so prefix is exactly the right shape.
     * </ul>
     */
    public synchronized boolean isPruneOf(Tree other) {
        for (String id : nodes.keySet()) {
            if (!other.nodes.containsKey(id)) {
                return false; // added a node other lacks
            }
        }
        if (!sharedTypeStable(other)) {
            return false;
        }
        if (!isAtLeastAsSettledAs(other)) {
            return false;
        }
        // added is empty, so every self node is shared with other.
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            List<String> mine = entry.getValue().children();
            List<String> theirs = other.nodes.get(entry.getKey()).children();
            if (!mine.equals(prefix(theirs, mine.size()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code this} is an <em>extension</em> of {@code other}.
     *
     * <p>{@code this} is the <em>later</em> replay after the cache advanced and <b>dropped nothing</b>:
     * it may have run past a now-unblocked await and <b>appended</b> nodes {@code other} never had, or
     * merely advanced a shared node's kind (a frontier dependency settled, no new spawn), or be
     * structurally unchanged. This is the no-prune specialization of {@link #isPruneAndExtensionOf}:
     * the gate is one-sided ({@code removed == ∅}), so it is <em>not</em> exclusive with {@link
     * #isPruneOf} — an unchanged replay (both deltas empty) satisfies both, and the
     * unchanged/settle-only cell is exactly their overlap. A replay that <em>also</em> prunes (drops a
     * node {@code other} had) is a prune only, never this.
     *
     * <ul>
     *   <li><b>Dropped nothing</b> — {@code other} has <em>no</em> node {@code this} lacks ({@code
     *       removed == ∅}). {@code this} may add new nodes or none.
     *   <li><b>Root + type stability</b> ({@link #sharedTypeStable}) and <b>kind monotonicity</b>
     *       ({@link #isAtLeastAsSettledAs}) — {@code this} is the later replay, so kind monotonicity
     *       always runs {@code other} -> {@code this}.
     *   <li><b>Original-as-prefix</b> — for every (shared) node, {@code other}'s child list must be a
     *       prefix of {@code this}'s. Extension only appends to the tail, so the original order is
     *       preserved and a prefix relation is exactly what growth (and the no-growth degenerate case)
     *       allows.
     * </ul>
     */
    public synchronized boolean isExtensionOf(Tree other) {
        for (String id : other.nodes.keySet()) {
            if (!nodes.containsKey(id)) {
                return false; // other has a node self lacks -> removed != ∅
            }
        }
        if (!sharedTypeStable(other)) {
            return false;
        }
        if (!isAtLeastAsSettledAs(other)) {
            return false;
        }
        // removed is empty, so every other node is shared with self.
        for (Map.Entry<String, Node> entry : other.nodes.entrySet()) {
            List<String> theirs = entry.getValue().children();
            List<String> mine = nodes.get(entry.getKey()).children();
            if (!theirs.equals(prefix(mine, theirs.size()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code this} is a <em>valid replay</em> of {@code other}.
     *
     * <p>The general replay relation — {@code this} may have <b>pruned</b> completed Int subtrees,
     * <b>extended</b> past freshly-unblocked awaits, done <b>both</b> at once (the common step:
     * settling a frontier dependency completes one subtree while unblocking another), or neither (an
     * unchanged or settle-only replay). It places <em>no</em> gate on the {@code (added, removed)}
     * node-set delta, so it is the union under which {@link #isPruneOf} ({@code added == ∅}) and {@link
     * #isExtensionOf} ({@code removed == ∅}) are the one-sided specializations: every prune and every
     * extension is also a valid replay, and this predicate holds across the whole settle-to-done walk
     * regardless of which facets a given step exercises.
     *
     * <ul>
     *   <li><b>No node-set gate</b> — {@code this} may add nodes, drop nodes, both, or neither.
     *   <li><b>Root + type stability</b> and <b>kind monotonicity</b> — as in the specializations.
     *   <li><b>Survivors-as-prefix, both directions</b> — over the shared nodes, projecting out each
     *       side's delta: {@code this}'s children that {@code other} also had must be a prefix of
     *       {@code other}'s list (the prune facet), and {@code other}'s children that {@code this}
     *       still has must be a prefix of {@code this}'s list (the extension facet). A node touched by
     *       both facets satisfies each under its own projection.
     * </ul>
     */
    public synchronized boolean isPruneAndExtensionOf(Tree other) {
        if (!sharedTypeStable(other)) {
            return false;
        }
        if (!isAtLeastAsSettledAs(other)) {
            return false;
        }
        for (String id : nodes.keySet()) {
            if (!other.nodes.containsKey(id)) {
                continue;
            }
            List<String> mineChildren = nodes.get(id).children();
            List<String> otherChildren = other.nodes.get(id).children();

            List<String> retained =
                    mineChildren.stream().filter(other.nodes::containsKey).collect(Collectors.toList());
            if (!retained.equals(prefix(otherChildren, retained.size()))) {
                return false;
            }
            List<String> survivors =
                    otherChildren.stream().filter(nodes::containsKey).collect(Collectors.toList());
            if (!survivors.equals(prefix(mineChildren, survivors.size()))) {
                return false;
            }
        }
        return true;
    }

    /** The first {@code n} elements of {@code list} (the Python slice {@code list[:n]}). */
    private static List<String> prefix(List<String> list, int n) {
        return list.subList(0, Math.min(n, list.size()));
    }

    // ── display ─────────────────────────────────────────────────────

    /**
     * Return an ASCII tree diagram, children in insertion order.
     *
     * <p>Each line is {@code <id> (<type>, <kind>)}. Children are listed in their insertion (== call)
     * order, so the output is a deterministic function of the (body, cache) pair — exactly the
     * children-as-prefix replay property.
     */
    public synchronized String print() {
        List<String> lines = new ArrayList<>();
        printNode(root, "", "", lines);
        return String.join("\n", lines);
    }

    private void printNode(String id, String linePrefix, String childPrefix, List<String> lines) {
        Node node = nodes.get(id);
        lines.add(linePrefix + id + " (" + node.type() + ", " + node.kind() + ")");
        List<String> children = node.children();
        int n = children.size();
        for (int i = 0; i < n; i++) {
            boolean last = i == n - 1;
            printNode(
                    children.get(i),
                    childPrefix + (last ? "└── " : "├── "),
                    childPrefix + (last ? "    " : "│   "),
                    lines);
        }
    }
}
