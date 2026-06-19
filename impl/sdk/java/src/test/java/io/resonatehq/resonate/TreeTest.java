package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Tree.Node;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_tree.py}.
 *
 * <p>The execution tree ({@link Tree}) is a pure assertion layer: it never drives control flow, it
 * only <em>materializes</em> the shape an inner return left behind so {@link Tree#wellFormed} can
 * check that shape against the suspension contract. These tests exercise the inspection primitives,
 * the mutation primitives ({@link Tree#addChild} idempotent on id, {@link Tree#settle} monotonic),
 * the frontier walk, the predicates ({@link Tree#useful} / {@link Tree#wellFormed}), the three
 * replay-comparison relations, and the deterministic {@link Tree#print} diagram.
 *
 * <p>Some invalid states (a non-Int root, an unreachable node, a reclassified node) cannot be
 * reached through the public API by construction — those are built by poking {@code tree.nodes}
 * directly (package-private), which is exactly what an SDK bug would do to the materialized view,
 * mirroring the Python tests reaching into {@code tree._nodes}.
 */
class TreeTest {

    // ── inspection primitives ────────────────────────────────────────────────

    @Test
    void newTreeRootIsIntPending() {
        Tree t = new Tree("root");
        assertEquals("root", t.root());
        Node root = t.get("root");
        assertNotNull(root);
        assertEquals(Tree.INT, root.type());
        assertEquals(Tree.PENDING, root.kind());
        assertEquals(List.of(), root.children());
    }

    @Test
    void newTreeHasOnlyRoot() {
        Tree t = new Tree("root");
        assertTrue(t.has("root"));
        assertFalse(t.has("nope"));
        assertEquals(1, t.size());
        assertEquals(List.of("root"), t.ids());
    }

    @Test
    void sizeAndIdsGrowWithAddChild() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.INT);
        assertEquals(3, t.size());
        assertEquals(Set.of("root", "root.1", "root.2"), Set.copyOf(t.ids()));
        assertTrue(t.has("root.1"));
        assertTrue(t.has("root.2"));
    }

    @Test
    void getUnknownReturnsNull() {
        assertNull(new Tree("root").get("nope"));
    }

    @Test
    void getReturnsDefensiveCopyNotLiveReference() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);

        Node handle = t.get("root.1");
        assertNotNull(handle);
        // Mutate the returned copy in every way a caller could.
        handle.setKind(Tree.SETTLED);
        handle.setType(Tree.INT);
        handle.children().add("ghost");

        // The tree's own view is untouched.
        Node fresh = t.get("root.1");
        assertNotNull(fresh);
        assertEquals(Tree.PENDING, fresh.kind());
        assertEquals(Tree.EXT, fresh.type());
        assertEquals(List.of(), fresh.children());

        // The root's child list is not contaminated either.
        Node root = t.get("root");
        assertNotNull(root);
        assertEquals(List.of("root.1"), root.children());
    }

    // ── mutation: addChild ────────────────────────────────────────────────────

    @Test
    void addChildInsertsAndLinksParent() {
        Tree t = new Tree("root");
        assertTrue(t.addChild("root", "root.1", Tree.EXT));

        Node child = t.get("root.1");
        assertNotNull(child);
        assertEquals(Tree.EXT, child.type());
        assertEquals(Tree.PENDING, child.kind());

        Node root = t.get("root");
        assertNotNull(root);
        assertEquals(List.of("root.1"), root.children());
    }

    @Test
    void addChildIsIdempotentOnId() {
        Tree t = new Tree("root");
        assertTrue(t.addChild("root", "root.1", Tree.EXT));
        // Second call — same id, even a different type — is a no-op returning false.
        assertFalse(t.addChild("root", "root.1", Tree.INT));

        Node child = t.get("root.1");
        assertNotNull(child);
        assertEquals(Tree.EXT, child.type()); // type stable: first write wins

        Node root = t.get("root");
        assertNotNull(root);
        assertEquals(List.of("root.1"), root.children()); // not re-appended
    }

    @Test
    void addChildPreservesInsertionOrder() {
        Tree t = new Tree("root");
        for (int i = 1; i <= 3; i++) {
            t.addChild("root", "root." + i, Tree.EXT);
        }
        Node root = t.get("root");
        assertNotNull(root);
        assertEquals(List.of("root.1", "root.2", "root.3"), root.children());
    }

    @Test
    void addChildUnknownParentRaises() {
        Tree t = new Tree("root");
        assertThrows(AssertionError.class, () -> t.addChild("ghost", "root.1", Tree.EXT));
    }

    // ── mutation: settle ──────────────────────────────────────────────────────

    @Test
    void settleFlipsPendingToSettled() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.settle("root.1");
        Node child = t.get("root.1");
        assertNotNull(child);
        assertEquals(Tree.SETTLED, child.kind());
    }

    @Test
    void settleIsIdempotentOnAlreadySettled() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.settle("root.1");
        t.settle("root.1"); // still settled, no error
        Node child = t.get("root.1");
        assertNotNull(child);
        assertEquals(Tree.SETTLED, child.kind());
    }

    @Test
    void settleIsNoopOnUnknownId() {
        Tree t = new Tree("root");
        t.settle("ghost"); // no raise
        assertEquals(1, t.size()); // tree untouched
    }

    // ── frontier ──────────────────────────────────────────────────────────────

    @Test
    void frontierEmptyForBareRoot() {
        assertEquals(List.of(), new Tree("root").frontier());
    }

    @Test
    void frontierCollectsPendingExtLeaf() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        assertEquals(List.of("root.1"), t.frontier());
    }

    @Test
    void frontierSkipsSettledExt() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.settle("root.1");
        assertEquals(List.of(), t.frontier());
    }

    @Test
    void frontierIsDepthFirstInInsertionOrder() {
        Tree t = new Tree("root");
        for (int i = 1; i <= 3; i++) {
            t.addChild("root", "root." + i, Tree.EXT);
        }
        assertEquals(List.of("root.1", "root.2", "root.3"), t.frontier());
    }

    @Test
    void frontierDescendsThroughIntParent() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT); // ctx.run child, still in flight
        t.addChild("root.1", "root.1.1", Tree.EXT); // the rpc it awaits
        assertEquals(List.of("root.1.1"), t.frontier());
    }

    @Test
    void frontierPrunesDetSubtree() {
        Tree t = new Tree("root");
        t.addChild("root", "d", Tree.DET);
        t.addChild("d", "d.1", Tree.EXT); // would be a frontier leaf if not under Det
        assertEquals(List.of(), t.frontier());
    }

    @Test
    void frontierPendingExtPrunesOwnSubtree() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root.1", "root.1.1", Tree.EXT); // below a pending Ext -> not reached
        assertEquals(List.of("root.1"), t.frontier());
    }

    // ── useful() / U3 ───────────────────────────────────────────────────────────

    @Test
    void usefulPassesWhenAllSettled() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.settle("root.1");
        t.useful(); // no raise
    }

    @Test
    void usefulPassesWithPendingExtDescendant() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.addChild("root.1", "root.1.1", Tree.EXT);
        t.useful(); // no raise
    }

    @Test
    void usefulFlagsDeadIntPendingLeaf() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT); // never settled, no children
        assertThrows(AssertionError.class, t::useful);
    }

    @Test
    void usefulFlagsIntPendingWithOnlyDetDescendant() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.addChild("root.1", "root.1.d", Tree.DET);
        assertThrows(AssertionError.class, t::useful);
    }

    @Test
    void usefulNamesEveryDeadBranch() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.addChild("root", "root.2", Tree.INT);
        AssertionError exc = assertThrows(AssertionError.class, t::useful);
        String msg = exc.getMessage();
        assertTrue(msg.contains("root.1"));
        assertTrue(msg.contains("root.2"));
    }

    // ── wellFormed: universal rules (U1/U2/U3) ──────────────────────────────────

    @Test
    void wellFormedU1RootMustBeInt() {
        Tree t = new Tree("root");
        t.nodes.get("root").setType(Tree.EXT); // only an SDK bug could do this
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.DONE, List.of()));
        assertTrue(exc.getMessage().contains("U1"));
    }

    @Test
    void wellFormedU1RootMustBePending() {
        Tree t = new Tree("root");
        t.settle("root");
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.DONE, List.of()));
        assertTrue(exc.getMessage().contains("U1"));
    }

    @Test
    void wellFormedU2FlagsUnreachableNode() {
        Tree t = new Tree("root");
        t.nodes.put("orphan", new Node("orphan", Tree.EXT, Tree.PENDING)); // not linked under root
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.SUSPENDED, List.of()));
        assertTrue(exc.getMessage().contains("U2"));
    }

    @Test
    void wellFormedU3FlagsDeadBranch() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT); // dead pending Int leaf
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.SUSPENDED, List.of()));
        assertTrue(exc.getMessage().contains("U3"));
    }

    // ── wellFormed: done state (D1) ──────────────────────────────────────────────

    @Test
    void wellFormedDoneValidWhenFrontierEmpty() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.settle("root.1");
        t.wellFormed(Tree.DONE, List.of()); // no raise
    }

    @Test
    void wellFormedDoneValidForBareRoot() {
        new Tree("root").wellFormed(Tree.DONE, List.of()); // no raise
    }

    @Test
    void wellFormedDoneRejectsNonEmptyFrontier() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT); // pending -> frontier non-empty
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.DONE, List.of()));
        assertTrue(exc.getMessage().contains("D1"));
    }

    // ── wellFormed: suspended state (S1/S4) ──────────────────────────────────────

    @Test
    void wellFormedSuspendedValid() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.EXT);
        t.wellFormed(Tree.SUSPENDED, List.of("root.1")); // awaited subset of frontier
    }

    @Test
    void wellFormedSuspendedS1RejectsEmptyFrontier() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.settle("root.1"); // frontier now empty
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.SUSPENDED, List.of()));
        assertTrue(exc.getMessage().contains("S1"));
    }

    @Test
    void wellFormedSuspendedS4RejectsTodoOutsideFrontier() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.EXT);
        t.settle("root.2"); // root.2 leaves the frontier...
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.SUSPENDED, List.of("root.2")));
        assertTrue(exc.getMessage().contains("S4")); // ...but is still awaited
    }

    @Test
    void wellFormedSuspendedS4HoldsForFullFrontierSubset() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.EXT);
        t.wellFormed(Tree.SUSPENDED, List.of("root.1", "root.2"));
    }

    @Test
    void wellFormedSuspendedS4HoldsForEmptyTodos() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.wellFormed(Tree.SUSPENDED, List.of());
    }

    @Test
    void wellFormedDoneS4RejectsAnyTodo() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.settle("root.1");
        AssertionError exc = assertThrows(AssertionError.class, () -> t.wellFormed(Tree.DONE, List.of("root.1")));
        assertTrue(exc.getMessage().contains("S4")); // frontier empty, so any todo violates
    }

    // ── replay comparison fixtures (tree.md §13) ─────────────────────────────────

    /** Iteration 0: the full tree, suspended on rpc {@code f.2}. */
    private static Tree fullTree() {
        Tree t = new Tree("f");
        t.addChild("f", "f.1", Tree.INT);
        t.addChild("f.1", "f.1.1", Tree.INT);
        t.settle("f.1.1");
        t.settle("f.1");
        t.addChild("f", "f.2", Tree.EXT);
        return t;
    }

    /** Iteration 1 over an unchanged cache: {@code f.1} short-circuits, {@code f.1.1} gone. */
    private static Tree prunedTree() {
        Tree t = new Tree("f");
        t.addChild("f", "f.1", Tree.INT);
        t.settle("f.1");
        t.addChild("f", "f.2", Tree.EXT);
        return t;
    }

    /** Iteration 1 after rpc {@code f.2} settled: prunes {@code f.1.1} AND spawns {@code f.3}. */
    private static Tree prunedAndExtendedTree() {
        Tree t = new Tree("f");
        t.addChild("f", "f.1", Tree.INT);
        t.settle("f.1");
        t.addChild("f", "f.2", Tree.EXT);
        t.settle("f.2");
        t.addChild("f", "f.3", Tree.EXT);
        return t;
    }

    // ── isPruneOf ── (added ∅: pure prune, settle-only, or unchanged)

    @Test
    void isPruneOfHoldsForEqualTrees() {
        Tree a = fullTree();
        Tree b = fullTree();
        assertTrue(a.isPruneOf(b)); // added ∅ -> a prune (dropped nothing is allowed)
        assertTrue(a.isExtensionOf(b));
    }

    @Test
    void isPruneOfStrictPrune() {
        Tree full = fullTree();
        Tree pruned = prunedTree();
        assertTrue(pruned.isPruneOf(full)); // dropped f.1.1, added nothing
        assertFalse(full.isPruneOf(pruned)); // f.1.1 is an addition, not a prune
    }

    @Test
    void isPruneOfFalseOnPureExtension() {
        Tree grown = fullTree();
        grown.addChild("f", "f.3", Tree.EXT);
        assertFalse(grown.isPruneOf(fullTree())); // removed ∅ -> not a prune
    }

    @Test
    void isPruneOfFalseOnMixed() {
        assertFalse(prunedAndExtendedTree().isPruneOf(fullTree()));
    }

    @Test
    void isPruneOfFalseOnTypeChange() {
        Tree full = fullTree();
        Tree pruned = prunedTree();
        full.nodes.get("f.2").setType(Tree.INT); // only an SDK bug could reclassify a node
        assertFalse(pruned.isPruneOf(full));
    }

    @Test
    void isPruneOfFalseOnKindRegression() {
        Tree regressed = prunedTree();
        regressed.nodes.get("f.1").setKind(Tree.PENDING); // settled in full, pending here
        assertFalse(regressed.isPruneOf(fullTree()));
    }

    @Test
    void isPruneOfAllowsKindAdvance() {
        Tree advanced = prunedTree();
        advanced.settle("f.2");
        assertTrue(advanced.isPruneOf(fullTree()));
    }

    @Test
    void isPruneOfFalseOnMiddleDrop() {
        Tree other = new Tree("f");
        other.addChild("f", "f.1", Tree.EXT);
        other.settle("f.1");
        other.addChild("f", "f.2", Tree.EXT);
        Tree dropped = new Tree("f");
        dropped.addChild("f", "f.2", Tree.EXT); // f.1 removed -> [f.2] not a prefix of [f.1, f.2]
        assertFalse(dropped.isPruneOf(other));
    }

    // ── isExtensionOf ── (removed ∅: pure extension, settle-only, or unchanged)

    @Test
    void isExtensionOfHoldsForStructurallyEqualTrees() {
        Tree a = fullTree();
        Tree b = fullTree();
        assertTrue(a.isExtensionOf(b)); // removed ∅ -> extension owns the unchanged cell
        assertTrue(b.isExtensionOf(a));
    }

    @Test
    void isExtensionOfSettleOnly() {
        Tree advanced = fullTree();
        advanced.settle("f.2");
        assertTrue(advanced.isExtensionOf(fullTree()));
        assertFalse(fullTree().isExtensionOf(advanced)); // kind regression
    }

    @Test
    void isExtensionOfStrictExtension() {
        Tree extended = fullTree();
        extended.settle("f.2");
        extended.addChild("f", "f.3", Tree.EXT);
        assertTrue(extended.isExtensionOf(fullTree()));
        assertFalse(fullTree().isExtensionOf(extended)); // missing f.3 -> not extension
    }

    @Test
    void isExtensionOfFalseOnPurePrune() {
        Tree pruned = prunedTree();
        assertFalse(pruned.isExtensionOf(fullTree())); // removed != ∅ -> not extension
    }

    @Test
    void isExtensionOfFalseOnMixed() {
        assertFalse(prunedAndExtendedTree().isExtensionOf(fullTree()));
    }

    @Test
    void isExtensionOfFalseOnTypeChange() {
        Tree a = fullTree();
        a.addChild("f", "f.3", Tree.EXT);
        Tree b = fullTree();
        b.nodes.get("f.2").setType(Tree.INT); // only an SDK bug could reclassify a node
        assertFalse(a.isExtensionOf(b));
    }

    @Test
    void isExtensionOfFalseOnKindRegression() {
        Tree extended = fullTree();
        extended.settle("f.2");
        extended.addChild("f", "f.3", Tree.EXT);
        extended.nodes.get("f.1").setKind(Tree.PENDING); // settled in other, pending here
        assertFalse(extended.isExtensionOf(fullTree()));
    }

    @Test
    void isExtensionOfAllowsKindAdvance() {
        Tree advanced = fullTree();
        advanced.settle("f.2"); // pending in other, settled here
        advanced.addChild("f", "f.3", Tree.EXT);
        assertTrue(advanced.isExtensionOf(fullTree()));
    }

    @Test
    void isExtensionOfFalseOnMiddleDivergence() {
        Tree other = new Tree("f");
        other.addChild("f", "f.1", Tree.EXT);
        Tree grown = new Tree("f");
        grown.addChild("f", "f.2", Tree.EXT); // diverges at index 0, not an append
        grown.addChild("f", "f.1", Tree.EXT);
        assertFalse(grown.isExtensionOf(other));
    }

    // ── isPruneAndExtensionOf ── (general valid replay: any (added, removed) delta)

    @Test
    void mixedReplayIsPruneAndExtension() {
        Tree full = fullTree();
        Tree evolved = prunedAndExtendedTree();
        assertTrue(evolved.isPruneAndExtensionOf(full));
        assertFalse(evolved.isPruneOf(full)); // also extends -> not pure prune
        assertFalse(evolved.isExtensionOf(full)); // also prunes -> not pure extension
    }

    @Test
    void mixedReplayDecomposesThroughIntermediate() {
        Tree full = fullTree();
        Tree pruned = prunedTree();
        Tree evolved = prunedAndExtendedTree();
        assertTrue(pruned.isPruneOf(full));
        assertTrue(evolved.isExtensionOf(pruned));
    }

    @Test
    void isPruneAndExtensionHoldsOnPurePrune() {
        assertTrue(prunedTree().isPruneAndExtensionOf(fullTree()));
    }

    @Test
    void isPruneAndExtensionHoldsOnPureExtension() {
        Tree grown = fullTree();
        grown.addChild("f", "f.3", Tree.EXT);
        assertTrue(grown.isPruneAndExtensionOf(fullTree()));
    }

    @Test
    void isPruneAndExtensionHoldsForEqualTrees() {
        assertTrue(fullTree().isPruneAndExtensionOf(fullTree()));
    }

    @Test
    void isPruneAndExtensionFalseOnKindRegression() {
        Tree evolved = prunedAndExtendedTree();
        evolved.nodes.get("f.1").setKind(Tree.PENDING); // settled in full, pending here
        assertFalse(evolved.isPruneAndExtensionOf(fullTree()));
    }

    // ── print() — deterministic ASCII diagram ────────────────────────────────────

    @Test
    void printBareRoot() {
        assertEquals("root (int, pending)", new Tree("root").print());
    }

    @Test
    void printListsChildrenInInsertionOrder() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.INT);
        String expected = "root (int, pending)\n├── root.1 (ext, pending)\n└── root.2 (int, pending)";
        assertEquals(expected, t.print());
    }

    @Test
    void printNestsSubtreesWithContinuationBars() {
        Tree t = new Tree("root");
        t.addChild("root", "root.1", Tree.INT);
        t.addChild("root.1", "root.1.1", Tree.EXT);
        t.addChild("root", "root.2", Tree.EXT);
        t.settle("root.2");
        String expected = "root (int, pending)\n"
                + "├── root.1 (int, pending)\n"
                + "│   └── root.1.1 (ext, pending)\n"
                + "└── root.2 (ext, settled)";
        assertEquals(expected, t.print());
    }

    @Test
    void printIsDeterministicAcrossCalls() {
        Tree t = new Tree("root");
        for (int i = 1; i <= 3; i++) {
            t.addChild("root", "root." + i, Tree.EXT);
        }
        assertEquals(t.print(), t.print());
    }

    // ── worked example: the phased workflow under replay (tree.md §9) ─────────────

    @Test
    void phasedWorkflowReplaySequence() {
        // Iteration 0 — cache empty: body creates root.1, awaits it, suspends.
        Tree t0 = new Tree("root");
        t0.addChild("root", "root.1", Tree.EXT);
        assertEquals(List.of("root.1"), t0.frontier());
        t0.wellFormed(Tree.SUSPENDED, List.of("root.1"));

        // External settles root.1. Iteration 1 — a returns, b and c spawn, await b.
        Tree t1 = new Tree("root");
        t1.addChild("root", "root.1", Tree.EXT);
        t1.settle("root.1");
        t1.addChild("root", "root.2", Tree.EXT);
        t1.addChild("root", "root.3", Tree.EXT);
        assertEquals(List.of("root.2", "root.3"), t1.frontier());
        t1.wellFormed(Tree.SUSPENDED, List.of("root.2")); // only b is awaited

        // External settles root.3. Iteration 2 — still blocked on b.
        Tree t2 = new Tree("root");
        t2.addChild("root", "root.1", Tree.EXT);
        t2.settle("root.1");
        t2.addChild("root", "root.2", Tree.EXT);
        t2.addChild("root", "root.3", Tree.EXT);
        t2.settle("root.3");
        assertEquals(List.of("root.2"), t2.frontier());
        t2.wellFormed(Tree.SUSPENDED, List.of("root.2"));

        // External settles root.2. Iteration 3 — body runs to completion, done.
        Tree t3 = new Tree("root");
        t3.addChild("root", "root.1", Tree.EXT);
        t3.addChild("root", "root.2", Tree.EXT);
        t3.addChild("root", "root.3", Tree.EXT);
        for (String leaf : List.of("root.1", "root.2", "root.3")) {
            t3.settle(leaf);
        }
        assertEquals(List.of(), t3.frontier());
        t3.wellFormed(Tree.DONE, List.of());
    }

    // ── concurrency ────────────────────────────────────────────────────────────

    /**
     * Sibling {@code ctx.run} children run on real pool threads, all mutating one shared tree. Without
     * the instance lock the backing {@code LinkedHashMap} corrupts under concurrent {@code put}: a
     * whole-tree read sees a half-applied {@code addChild} and throws (NPE, {@code
     * ConcurrentModificationException}, or a bogus {@code U2}/{@code D1} {@link AssertionError}). With
     * the lock every read observes a consistent snapshot. Hammer it and assert nothing throws.
     */
    @Test
    void concurrentMutationAndReadsAreConsistent() throws InterruptedException {
        Tree t = new Tree("root");
        int writers = 8;
        int perWriter = 500;
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers + 1);

        for (int w = 0; w < writers; w++) {
            final int id = w;
            new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < perWriter; i++) {
                                String child = "root." + id + "." + i;
                                t.addChild("root", child, Tree.EXT);
                                t.settle(child); // settled ext leaf keeps the tree well-formed (frontier empty)
                            }
                        } catch (Throwable e) {
                            failures.add(e);
                        } finally {
                            done.countDown();
                        }
                    })
                    .start();
        }

        // Reader races the writers: whole-tree walks must never observe a torn map.
        new Thread(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 2000; i++) {
                            // These never throw on a *valid* tree, even mid-add (a pending ext leaf is a
                            // legal transient). Corruption (torn map) surfaces here as NPE/CME/AssertionError.
                            t.frontier();
                            t.ids();
                            t.size();
                            t.useful();
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    } finally {
                        done.countDown();
                    }
                })
                .start();

        start.countDown();
        done.await();

        assertTrue(failures.isEmpty(), () -> "concurrent access threw: " + failures);
        assertEquals(1 + writers * perWriter, t.size());
        t.wellFormed(Tree.DONE, List.of());
    }
}
