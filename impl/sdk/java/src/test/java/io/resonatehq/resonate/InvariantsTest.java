package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Core.ExecFulfilled;
import io.resonatehq.resonate.Core.ExecOutcome;
import io.resonatehq.resonate.Core.ExecSuspended;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_invariants.py}: replay invariants stated <em>generically</em>
 * for an arbitrary resonate function. Where {@link TreeTest} pins the exact id set a specific body
 * leaves behind, this module turns the structural contract of {@code tree.md} into invariants over a
 * whole workflow corpus -- one entry per §2 type-matrix cell -- and drives each through {@code
 * Core.executeUntilBlockedInner} along a settle-to-done trajectory.
 *
 * <p>The three concrete invariants:
 *
 * <ul>
 *   <li><b>R1</b> ({@code idempotentReplayHolds}): replay over an unchanged cache prunes to a fixed
 *       point -- the second tree is a prune of the first and the cache is unchanged.
 *   <li><b>R2</b> ({@code settlingFrontierExtends}): settling each successive frontier subset and
 *       replaying yields a tree that is a valid replay ({@code isPruneAndExtensionOf}) of the
 *       previous one, all the way down every settle-to-done path.
 *   <li><b>§6</b> ({@code typeStableAndKindMonotoneAcrossReplay}): Type is stable and Kind monotone
 *       across replay, in both the tree and the cache -- a shared node never changes type or
 *       un-settles, and a settled cache record is frozen byte-for-byte.
 * </ul>
 *
 * <p><b>Self-contained backing store.</b> Python uses a dict-backed {@code MockEffects}; Java's
 * {@link Effects} is final and speaks to a {@link Sender}, so the in-memory {@link StubNetwork} (a
 * minimal create/settle promise store, as in {@link EffectsTest}) plays the server. Forking the
 * effects copies its decoded cache via the package-private {@link Effects#cache()}; the shared
 * network is safe because every op it sees -- create, local-child settle -- is idempotent and
 * deterministic, and external settles ({@link #settleExternal}) touch only the cache.
 */
class InvariantsTest {

    // Far-future deadline, matching CoreTest.
    private static final long FAR_FUTURE = 1L << 50;
    private static final int TTL = 10_000;

    // How many settle-and-replay rounds the trajectory invariants drive. Every corpus body converges
    // in a handful of steps; a generous cap keeps the walk well past any body's settling depth while
    // bounding the loop.
    private static final int MAX_ROUNDS = 1_000;

    // ── Self-contained in-memory promise store (create / settle only) ────────

    /** An in-memory promise store mimicking the server; handles only {@code promise.create} / {@code promise.settle}. */
    static final class StubNetwork implements Network {
        final Map<String, Map<String, Object>> promises = new LinkedHashMap<>();

        @Override
        public String pid() {
            return "invariants-pid";
        }

        @Override
        public String group() {
            return "invariants-group";
        }

        @Override
        public String unicast() {
            return "invariants-unicast";
        }

        @Override
        public String anycast() {
            return "invariants-anycast";
        }

        @Override
        public CompletableFuture<Void> start() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stop() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void recv(Consumer<String> callback) {}

        @Override
        public String targetResolver(String target) {
            return target;
        }

        @Override
        public CompletableFuture<String> send(String req) {
            try {
                JsonNode reqJson = MAPPER.readTree(req);
                String kind = reqJson.path("kind").asText("");
                String corrId = reqJson.path("head").path("corrId").asText(null);
                JsonNode data = reqJson.path("data");

                int status;
                Object respData;
                switch (kind) {
                    case "promise.create" -> {
                        status = 200;
                        respData = handleCreate(data);
                    }
                    case "promise.settle" -> {
                        status = 200;
                        respData = handleSettle(data);
                    }
                    default -> {
                        status = 400;
                        respData = "unknown request kind: " + kind;
                    }
                }

                Map<String, Object> head = new LinkedHashMap<>();
                head.put("corrId", corrId);
                head.put("status", status);
                head.put("version", "2025-01-15");
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("kind", kind);
                resp.put("head", head);
                resp.put("data", respData);
                return CompletableFuture.completedFuture(MAPPER.writeValueAsString(resp));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Map<String, Object> handleCreate(JsonNode data) {
            String id = data.path("id").asText("");
            Map<String, Object> existing = promises.get(id);
            if (existing != null) {
                return Map.of("promise", existing);
            }
            long timeoutAt = data.has("timeoutAt") ? data.get("timeoutAt").asLong() : I64_MAX;
            Map<String, Object> tags = data.has("tags") && data.get("tags").isObject()
                    ? MAPPER.convertValue(data.get("tags"), new TypeReference<Map<String, Object>>() {})
                    : Map.of();
            Map<String, Object> record =
                    promiseJson(id, "pending", timeoutAt, valueFromWire(data.get("param")), emptyValue(), tags, null);
            promises.put(id, record);
            return Map.of("promise", record);
        }

        private Map<String, Object> handleSettle(JsonNode data) {
            String id = data.path("id").asText("");
            String stateStr = data.hasNonNull("state") ? data.get("state").asText() : "resolved";
            String promiseState = "resolved".equals(stateStr) ? "resolved" : "rejected";
            Map<String, Object> value = valueFromWire(data.get("value"));

            Map<String, Object> existing = promises.get(id);
            if (existing != null) {
                if (!"pending".equals(existing.get("state"))) {
                    return Map.of("promise", existing);
                }
                Map<String, Object> updated = new LinkedHashMap<>(existing);
                updated.put("state", promiseState);
                updated.put("value", value);
                updated.put("settledAt", 1);
                promises.put(id, updated);
                return Map.of("promise", updated);
            }
            Map<String, Object> record = promiseJson(id, promiseState, I64_MAX, emptyValue(), value, Map.of(), 1L);
            promises.put(id, record);
            return Map.of("promise", record);
        }

        private static Map<String, Object> valueFromWire(JsonNode raw) {
            Map<String, Object> v = new HashMap<>();
            if (raw == null || raw.isNull() || !raw.isObject()) {
                v.put("headers", null);
                v.put("data", null);
                return v;
            }
            JsonNode headers = raw.get("headers");
            JsonNode dataNode = raw.get("data");
            v.put("headers", (headers == null || headers.isNull()) ? null : MAPPER.convertValue(headers, Object.class));
            v.put("data", (dataNode == null || dataNode.isNull()) ? null : MAPPER.convertValue(dataNode, Object.class));
            return v;
        }

        private static Map<String, Object> emptyValue() {
            Map<String, Object> v = new HashMap<>();
            v.put("headers", null);
            v.put("data", null);
            return v;
        }

        private static Map<String, Object> promiseJson(
                String id, String state, long timeoutAt, Object param, Object value, Map<String, ?> tags, Long sa) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("id", id);
            p.put("state", state);
            p.put("param", param);
            p.put("value", value);
            p.put("tags", tags);
            p.put("timeoutAt", timeoutAt);
            p.put("createdAt", 0);
            p.put("settledAt", sa);
            return p;
        }
    }

    // ── Shared setup ─────────────────────────────────────────────────────────

    /** The pieces every invariant drives the inner with: a Core, a fresh {@link Effects}, and a pending root. */
    static final class Fixture {
        final Codec codec = new Codec(new NoopEncryptor());
        final StubNetwork net = new StubNetwork();
        final Sender sender = new Sender(new Transport(net), null);
        final Registry reg = new Registry();
        // sender=null in Core: the inner never touches task lifecycle APIs; Effects owns its own sender.
        final Core core = new Core(null, codec, reg, null, null, "invariants-test", TTL, null, null);

        Effects newEffects() {
            return new Effects(sender, codec, List.of());
        }

        /** Fork: a new Effects over the same network, seeded with a copy of the current decoded cache. */
        Effects fork(Effects effects) {
            Effects copy = new Effects(sender, codec, List.of());
            copy.cache().putAll(effects.cache());
            return copy;
        }

        /** A pending root promise whose param carries a {@link TaskData} naming the registered {@code "func"}. */
        PromiseRecord root() {
            return new PromiseRecord(
                    "f",
                    "pending",
                    new Value(null, new TaskData(List.of(), Map.of(), "func", 1)),
                    new Value(),
                    Map.of(),
                    FAR_FUTURE,
                    0L,
                    null);
        }
    }

    /**
     * Settle {@code id} in place in the shared cache, resolved with no data -- models the external
     * world advancing between two inner runs (a remote worker, the server's timer, an external
     * settle). Asserts the promise is still pending so a settle cannot double-fire.
     */
    private static void settleExternal(Effects effects, String id) {
        PromiseRecord rec = effects.cache().get(id);
        assertEquals("pending", rec.state(), id + " is already settled");
        effects.cache()
                .put(
                        id,
                        new PromiseRecord(
                                rec.id(),
                                "resolved",
                                rec.param(),
                                new Value(),
                                rec.tags(),
                                rec.timeoutAt(),
                                rec.createdAt(),
                                rec.settledAt()));
    }

    /** The tree on either outcome variant ({@link ExecOutcome} does not declare the accessor). */
    private static Tree treeOf(ExecOutcome outcome) {
        return outcome instanceof ExecFulfilled f ? f.tree() : ((ExecSuspended) outcome).tree();
    }

    /** Every non-empty subset of {@code items}, preserving element order (the analogue of itertools.combinations). */
    private static List<List<String>> nonEmptySubsets(List<String> items) {
        List<List<String>> out = new ArrayList<>();
        int n = items.size();
        for (int mask = 1; mask < (1 << n); mask++) {
            List<String> sub = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if ((mask & (1 << i)) != 0) {
                    sub.add(items.get(i));
                }
            }
            out.add(sub);
        }
        return out;
    }

    // ── The workflow corpus: one entry per §2 type-matrix cell ───────────────
    //
    // ctx.run takes the function reference directly, so a child body need not be registered -- only
    // the root "func" is. These children are what the corpus roots spawn.

    static int leaf(Context ctx) {
        return 1;
    }

    static int grandchild(Context ctx) {
        return 1;
    }

    static int childRunsGrandchild(Context ctx) {
        ctx.run(InvariantsTest::grandchild).await();
        return 2;
    }

    static int childBlocksOnRpc(Context ctx) {
        ctx.rpc("a").await();
        return 2;
    }

    /** Spawn two rpcs and await one -- the child folds both todos up. */
    static int childFansOutRpcs(Context ctx) {
        ResonateFuture<Object> a = ctx.rpc("a");
        ctx.rpc("b");
        a.await();
        return 2;
    }

    static int grandchildBlocksOnRpc(Context ctx) {
        ctx.rpc("a").await();
        return 3;
    }

    /** Run a local child whose own child is the one that blocks -- depth-3 suspension. */
    static int childRunsBlockingGrandchild(Context ctx) {
        return ctx.run(InvariantsTest::grandchildBlocksOnRpc).await();
    }

    /** No remote work: fulfills on the first run -- the degenerate case. */
    static Object allLocal(Context ctx) {
        ResonateFuture<Integer> a = ctx.run(InvariantsTest::leaf);
        ResonateFuture<Integer> b = ctx.run(InvariantsTest::leaf);
        return a.await() + b.await();
    }

    /** Spawn a settled local subtree (run -> run) then an rpc -- the canonical prune. */
    static Object nestedLocalThenRpc(Context ctx) {
        ctx.run(InvariantsTest::childRunsGrandchild).await();
        ctx.rpc("a").await();
        return null;
    }

    /** Sequential rpcs -- settling the first await spawns a node run 0 never had. */
    static Object rpcChain(Context ctx) {
        ctx.rpc("a").await();
        ctx.rpc("b").await();
        return null;
    }

    /** Block on a timer Ext node, then an rpc -- the timer settle is pure extension. */
    static Object sleepThenRpc(Context ctx) {
        ctx.sleep(Duration.ofSeconds(30)).await();
        ctx.rpc("a").await();
        return null;
    }

    /** Block on a bare Ext node only an external settle advances, to done. */
    static Object barePromise(Context ctx) {
        return ctx.promise(Duration.ofSeconds(30)).await();
    }

    /** Three rpcs spawned, one awaited -- settling each shrinks the frontier. */
    static Object fanout(Context ctx) {
        ResonateFuture<Object> a = ctx.rpc("a");
        ctx.rpc("b");
        ctx.rpc("c");
        a.await();
        return null;
    }

    /** Run a local child that suspends on its own rpc -- the §4 suspended-local case. */
    static Object suspendedLocal(Context ctx) {
        int v = ctx.run(InvariantsTest::childBlocksOnRpc).await();
        return v + 1;
    }

    /** Spawn a Det node -- exempt from the contract, invisible to the frontier -- then an rpc. */
    static Object detachedThenRpc(Context ctx) {
        ctx.detached("side").await();
        ctx.rpc("a").await();
        return null;
    }

    /** tree.md §9's phased body: settle one await, spawn two, then drain each. */
    static Object phasedRpcs(Context ctx) {
        ctx.rpc("a").await();
        ResonateFuture<Object> b = ctx.rpc("b");
        ResonateFuture<Object> c = ctx.rpc("c");
        b.await();
        c.await();
        return 0;
    }

    /** Return without any durable op: the tree is just the root, done at once. */
    static Object noDurableOps(Context ctx) {
        return 42;
    }

    /** Two local children, each suspended on its own rpc -- a frontier across siblings. */
    static Object parallelSuspendedLocals(Context ctx) {
        ResonateFuture<Integer> a = ctx.run(InvariantsTest::childBlocksOnRpc);
        ResonateFuture<Integer> b = ctx.run(InvariantsTest::childBlocksOnRpc);
        return a.await() + b.await();
    }

    /** Run one local child holding two Ext descendants -- one Int, two frontier leaves. */
    static Object localChildFansOut(Context ctx) {
        return ctx.run(InvariantsTest::childFansOutRpcs).await();
    }

    /** Run a depth-3 suspended-local chain -- U3 kept alive transitively up two Int levels. */
    static Object deepSuspendedLocal(Context ctx) {
        return ctx.run(InvariantsTest::childRunsBlockingGrandchild).await();
    }

    /** Detached child + a completing local -- done at iteration 0 with a pending Det. */
    static Object detachedThenLocalDone(Context ctx) {
        ctx.detached("side").await();
        return ctx.run(InvariantsTest::leaf).await();
    }

    /** Fire a Det node fire-and-forget -- never await its future -- then block on an rpc. */
    static Object detachedUnawaitedThenRpc(Context ctx) {
        ctx.detached("side");
        ctx.rpc("a").await();
        return null;
    }

    /** Alternating local/remote phases -- a new Int node spawned past a settled await. */
    static Object interleavedLocalRemote(Context ctx) {
        ctx.run(InvariantsTest::leaf).await();
        ctx.rpc("a").await();
        ctx.run(InvariantsTest::leaf).await();
        ctx.rpc("b").await();
        return 0;
    }

    /** Every corpus function must satisfy every invariant. */
    static Stream<Arguments> corpus() {
        return Stream.of(
                arguments("allLocal", (Fn.F0<Object>) InvariantsTest::allLocal),
                arguments("nestedLocalThenRpc", (Fn.F0<Object>) InvariantsTest::nestedLocalThenRpc),
                arguments("rpcChain", (Fn.F0<Object>) InvariantsTest::rpcChain),
                arguments("sleepThenRpc", (Fn.F0<Object>) InvariantsTest::sleepThenRpc),
                arguments("barePromise", (Fn.F0<Object>) InvariantsTest::barePromise),
                arguments("fanout", (Fn.F0<Object>) InvariantsTest::fanout),
                arguments("suspendedLocal", (Fn.F0<Object>) InvariantsTest::suspendedLocal),
                arguments("detachedThenRpc", (Fn.F0<Object>) InvariantsTest::detachedThenRpc),
                arguments("phasedRpcs", (Fn.F0<Object>) InvariantsTest::phasedRpcs),
                arguments("noDurableOps", (Fn.F0<Object>) InvariantsTest::noDurableOps),
                arguments("parallelSuspendedLocals", (Fn.F0<Object>) InvariantsTest::parallelSuspendedLocals),
                arguments("localChildFansOut", (Fn.F0<Object>) InvariantsTest::localChildFansOut),
                arguments("deepSuspendedLocal", (Fn.F0<Object>) InvariantsTest::deepSuspendedLocal),
                arguments("detachedThenLocalDone", (Fn.F0<Object>) InvariantsTest::detachedThenLocalDone),
                arguments("detachedUnawaitedThenRpc", (Fn.F0<Object>) InvariantsTest::detachedUnawaitedThenRpc),
                arguments("interleavedLocalRemote", (Fn.F0<Object>) InvariantsTest::interleavedLocalRemote));
    }

    // ── R1: replay over an unchanged cache prunes to a fixed point ───────────

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void idempotentReplayHolds(String name, Fn.F0<Object> func) {
        Fixture fix = new Fixture();
        fix.reg.register("func", func);
        Effects effects = fix.newEffects();
        PromiseRecord root = fix.root();

        ExecOutcome run0 = fix.core.executeUntilBlockedInner(root, effects);
        Map<String, PromiseRecord> cache0 = new HashMap<>(effects.cache());

        ExecOutcome run1 = fix.core.executeUntilBlockedInner(root, effects);
        Map<String, PromiseRecord> cache1 = new HashMap<>(effects.cache());

        assertEquals(cache0, cache1);
        assertTrue(treeOf(run1).isPruneOf(treeOf(run0)));
    }

    // ── R2: settling each successive frontier subset and replaying extends ───

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void settlingFrontierExtends(String name, Fn.F0<Object> func) {
        Fixture fix = new Fixture();
        fix.reg.register("func", func);
        Effects effects = fix.newEffects();
        PromiseRecord root = fix.root();

        ExecOutcome run = fix.core.executeUntilBlockedInner(root, effects);
        walk(fix, root, run, effects, 0);
    }

    private void walk(Fixture fix, PromiseRecord root, ExecOutcome run, Effects effects, int depth) {
        if (run instanceof ExecFulfilled || depth == MAX_ROUNDS) {
            return;
        }
        ExecSuspended suspended = (ExecSuspended) run;
        assertEquals(suspended.todos(), treeOf(run).frontier());

        for (List<String> batch : nonEmptySubsets(suspended.todos())) {
            Effects forked = fix.fork(effects);
            for (String blocking : batch) {
                settleExternal(forked, blocking);
            }

            ExecOutcome newRun = fix.core.executeUntilBlockedInner(root, forked);
            assertTrue(treeOf(newRun).isPruneAndExtensionOf(treeOf(run)));

            walk(fix, root, newRun, forked, depth + 1);
        }
    }

    // ── §6: Type is stable and Kind monotone across replay (tree and cache) ──

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void typeStableAndKindMonotoneAcrossReplay(String name, Fn.F0<Object> func) {
        Fixture fix = new Fixture();
        fix.reg.register("func", func);
        Effects effects = fix.newEffects();
        PromiseRecord root = fix.root();

        Tree treePrev = treeOf(fix.core.executeUntilBlockedInner(root, effects));
        Map<String, PromiseRecord> cachePrev = new HashMap<>(effects.cache());

        for (int round = 0; round < MAX_ROUNDS; round++) {
            for (String blocking : treePrev.frontier()) {
                settleExternal(effects, blocking);
            }

            Tree treeNext = treeOf(fix.core.executeUntilBlockedInner(root, effects));
            Map<String, PromiseRecord> cacheNext = new HashMap<>(effects.cache());

            Set<String> sharedNodes = new HashSet<>(treePrev.ids());
            sharedNodes.retainAll(treeNext.ids());
            for (String id : sharedNodes) {
                Tree.Node prev = treePrev.get(id);
                Tree.Node next = treeNext.get(id);
                assertEquals(prev.type(), next.type()); // Type stability
                if (Tree.SETTLED.equals(prev.kind())) {
                    assertEquals(Tree.SETTLED, next.kind()); // Kind monotonicity
                }
            }

            Set<String> sharedRecords = new HashSet<>(cachePrev.keySet());
            sharedRecords.retainAll(cacheNext.keySet());
            for (String id : sharedRecords) {
                if (!"pending".equals(cachePrev.get(id).state())) { // a settled record is frozen
                    assertEquals(cachePrev.get(id), cacheNext.get(id));
                }
            }

            if (treeNext.frontier().isEmpty()) {
                break;
            }

            treePrev = treeNext;
            cachePrev = cacheNext;
        }
    }
}
