package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.Suspended;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Retry.Constant;
import io.resonatehq.resonate.Retry.Never;
import io.resonatehq.resonate.Retry.RetryPolicy;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_context.py}, focused on {@link Context}'s durable ops
 * ({@code run} / {@code rpc} / {@code sleep} / {@code promise} / {@code detached}) over a real {@link
 * LocalNetwork}, so create/settle exercise the actual durability boundary.
 *
 * <p><b>Async-model note.</b> Python's tests are {@code async} and drive an event loop; here a durable
 * op's background body is a {@link java.util.concurrent.CompletableFuture} composition that, against
 * the synchronous {@code LocalNetwork}, runs inline on the calling thread, and {@code await} maps to
 * {@link ResonateFuture#await()}. The asyncio-plumbing tests (gating {@code create_promise} on an
 * event, asserting a task is "not done" while a create is in flight, mock-driven create-order and
 * chain-failure propagation) test the event-loop mechanism rather than SDK behaviour and have no
 * faithful Java analogue without a mocking framework; they are omitted. The {@link Chain} ordering
 * those rest on is covered by {@code ChainTest}.
 */
class ContextTest {

    private static final long I64_MAX = Long.MAX_VALUE;

    // =========================================================================
    // Harness
    // =========================================================================

    private static Codec codec() {
        return new Codec(new NoopEncryptor());
    }

    private static Context root() {
        return root(List.of(), I64_MAX, new Dependencies(), null, null);
    }

    private static Context root(List<PromiseRecord> preload) {
        return root(preload, I64_MAX, new Dependencies(), null, null);
    }

    private static Context rootTimeout(long timeoutAt) {
        return root(List.of(), timeoutAt, new Dependencies(), null, null);
    }

    private static Context root(
            List<PromiseRecord> preload, long timeoutAt, Dependencies deps, RetryPolicy policy, Registry registry) {
        Sender sender = new Sender(new Transport(new LocalNetwork()), null);
        Effects effects = new Effects(sender, codec(), preload);
        return Context.root(
                "root",
                "root",
                "root",
                timeoutAt,
                "root",
                effects,
                target -> target == null ? "" : target,
                deps,
                policy,
                registry);
    }

    /** A pre-settled resolved record, wire-encoded for the preload cache. */
    private static PromiseRecord resolved(String id, Object value) {
        return new PromiseRecord(id, "resolved", new Value(), codec().encode(value), Map.of(), I64_MAX, 0, 1L);
    }

    /** A pre-settled rejected record carrying an encoded error payload. */
    private static PromiseRecord rejected(String id, String message) {
        return new PromiseRecord(
                id, "rejected", new Value(), codec().encode(new ApplicationError(message)), Map.of(), I64_MAX, 0, 1L);
    }

    private static TaskData taskData(Object paramData) {
        return codec().convert(paramData, TaskData.class);
    }

    private static String detachedId(String prefix, String raw) {
        return prefix + ".d" + Context.hashId(raw);
    }

    // =========================================================================
    // Durable functions under test (resolved reflectively below)
    // =========================================================================

    static final class BookingError extends RuntimeException {
        BookingError(String message) {
            super(message);
        }
    }

    record Point(int x, int y) {}

    static final class Config {
        final String value;

        Config(String value) {
            this.value = value;
        }
    }

    /** A value Jackson cannot serialize (private field, no getter), standing in for a live handle. */
    static final class Resource {
        private final String label;

        Resource(String label) {
            this.label = label;
        }

        String label() {
            return label;
        }
    }

    static int doubleFn(Context ctx, int x) {
        return x * 2;
    }

    static String beat(Context ctx) {
        return "ok";
    }

    static int sumPoint(Context ctx, Point p) {
        return p.x() + p.y();
    }

    static int failing(Context ctx) {
        throw new ApplicationError("denied");
    }

    static int failingPlain(Context ctx) {
        throw new BookingError("card declined");
    }

    static String useResource(Context ctx, Resource r) {
        return r.label();
    }

    static double makeFloat(Context ctx, int x) {
        return x;
    }

    static Point makePoint(Context ctx, int x, int y) {
        return new Point(x, y);
    }

    static int parentWorkflow(Context ctx, int x) {
        int a = ctx.run(ContextTest::doubleFn, x).await();
        int b = ctx.run(ContextTest::doubleFn, a).await();
        return a + b;
    }

    static int blocksOnRemote(Context ctx) {
        ctx.spawnedRemote().add("remote-dep");
        throw new Suspended();
    }

    static int fireAndForget(Context ctx) {
        ctx.spawnedRemote().add("ff-dep");
        return 7;
    }

    static int deepInner(Context ctx) {
        ctx.spawnedRemote().add("deep-dep");
        throw new Suspended();
    }

    static int deepMiddle(Context ctx) {
        return ctx.run(ContextTest::deepInner).await();
    }

    static int deepTop(Context ctx) {
        return ctx.run(ContextTest::deepMiddle).await();
    }

    static int completesThenSuspends(Context ctx) {
        int a = ctx.run(ContextTest::doubleFn, 21).await();
        int b = ctx.run(ContextTest::blocksOnRemote).await();
        return a + b;
    }

    static int multiRemote(Context ctx) {
        ctx.spawnedRemote().add("dep-a");
        ctx.spawnedRemote().add("dep-b");
        ctx.spawnedRemote().add("dep-c");
        throw new Suspended();
    }

    static int parentWithFireAndForget(Context ctx) {
        ctx.run(ContextTest::blocksOnRemote);
        return 99;
    }

    static int quietChild(Context ctx) {
        return 42;
    }

    static int parentDoesNotAwaitChild(Context ctx) {
        ctx.run(ContextTest::quietChild);
        return 1;
    }

    static String dispatchesDetached(Context ctx) {
        ctx.detached("remote_fn", 1).await();
        return "done";
    }

    private static Method m(String name, Class<?>... params) {
        try {
            return ContextTest.class.getDeclaredMethod(name, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    // =========================================================================
    // Context plumbing
    // =========================================================================

    @Test
    void nextIdSequential() {
        Context ctx = root();
        assertEquals("root.1", ctx.nextId());
        assertEquals("root.2", ctx.nextId());
        assertEquals("root.3", ctx.nextId());
    }

    @Test
    void childParentIsCurrentId() {
        Context child = root().child("root.1", "fn", I64_MAX);
        assertEquals("root", child.info().parentId());
        assertEquals("root", child.info().originId());
        assertEquals("root.1", child.info().branchId());
    }

    @Test
    void childTimeoutCapsToParent() {
        long cap = Send.nowMs() + 1_000;
        Context ctx = rootTimeout(cap);
        assertEquals(cap, ctx.childTimeout(Duration.ofDays(1)));
        assertTrue(ctx.childTimeout(Duration.ofMillis(500)) <= cap);
    }

    // =========================================================================
    // get_dependency
    // =========================================================================

    @Test
    void getDependencyReturnsStoredValue() {
        Dependencies deps = new Dependencies();
        Config cfg = new Config("hello");
        deps.insert(cfg);
        Context ctx = root(List.of(), I64_MAX, deps, null, null);
        assertSame(cfg, ctx.getDependency(Config.class));
        assertEquals("hello", ctx.getDependency(Config.class).value);
    }

    @Test
    void getDependencyMissingThrows() {
        Context ctx = root();
        assertThrows(NoSuchElementException.class, () -> ctx.getDependency(Config.class));
    }

    @Test
    void getDependencySharedWithChildContext() {
        Dependencies deps = new Dependencies();
        Config cfg = new Config("shared");
        deps.insert(cfg);
        Context child = root(List.of(), I64_MAX, deps, null, null).child("root.1", "fn", I64_MAX);
        assertSame(cfg, child.getDependency(Config.class));
    }

    // =========================================================================
    // run: leaves
    // =========================================================================

    @Test
    void runLeafReturnsAndSettlesResolved() {
        Context ctx = root();
        assertEquals(42, ctx.run(ContextTest::doubleFn, 21).await());
        PromiseRecord record = ctx.effects().cache().get("root.1");
        assertEquals("resolved", record.state());
        assertEquals(42, record.value().data());
    }

    @Test
    void runCtxOnlyFunction() {
        Context ctx = root();
        assertEquals("ok", ctx.run(ContextTest::beat).await());
        assertEquals("resolved", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void runPassesLiveStructArg() {
        Context ctx = root();
        assertEquals(7, ctx.run(ContextTest::sumPoint, new Point(3, 4)).await());
    }

    @Test
    void runSequentialChildIds() {
        Context ctx = root();
        assertEquals(4, ctx.run(ContextTest::doubleFn, 2).await());
        assertEquals(6, ctx.run(ContextTest::doubleFn, 3).await());
        assertEquals(4, ctx.effects().cache().get("root.1").value().data());
        assertEquals(6, ctx.effects().cache().get("root.2").value().data());
    }

    // =========================================================================
    // run: non-serializable arguments
    // =========================================================================

    @Test
    void runAcceptsNonSerializableArg() {
        Context ctx = root();
        assertEquals("db", ctx.run(ContextTest::useResource, new Resource("db")).await());
    }

    @Test
    void runLocalChildParamIsEmpty() {
        Context ctx = root();
        ctx.run(ContextTest::doubleFn, 21).await();
        assertEquals(new Value(), ctx.effects().cache().get("root.1").param());
    }

    @Test
    void rpcStillRejectsNonSerializableArg() {
        Context ctx = root();
        PlatformError exc = assertThrows(PlatformError.class, () -> ctx.rpc("remote_fn", new Resource("db"))
                .await());
        assertInstanceOf(SerializationError.class, exc.getCause());
    }

    // =========================================================================
    // run: error handling
    // =========================================================================

    @Test
    void runFunctionErrorPropagatesAndSettlesRejected() {
        Context ctx = root();
        ApplicationError exc = assertThrows(
                ApplicationError.class, () -> ctx.run(ContextTest::failing).await());
        assertEquals("denied", exc.getMessage());
        assertEquals("rejected", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void runPlainExceptionPreservesTypeAndSettlesRejected() {
        Context ctx = root();
        BookingError exc = assertThrows(
                BookingError.class, () -> ctx.run(ContextTest::failingPlain).await());
        assertEquals("card declined", exc.getMessage());
        assertEquals("rejected", ctx.effects().cache().get("root.1").state());
    }

    // =========================================================================
    // run: idempotent recovery
    // =========================================================================

    @Test
    void runPresettledResolvedSkipsExecution() {
        Context ctx = root(List.of(resolved("root.1", 99)));
        assertEquals(99, ctx.run(ContextTest::doubleFn, 1).await());
    }

    @Test
    void runPresettledRejectedRaisesWithoutExecution() {
        Context ctx = root(List.of(rejected("root.1", "stored failure")));
        ApplicationError exc = assertThrows(
                ApplicationError.class, () -> ctx.run(ContextTest::doubleFn, 1).await());
        assertEquals("stored failure", exc.getMessage());
    }

    @Test
    void runRecoveryCoercesReturnToStruct() {
        Context ctx = root(List.of(resolved("root.1", new Point(3, 4))));
        Object result = ctx.run(ContextTest::makePoint, 3, 4).await();
        assertEquals(new Point(3, 4), result);
        assertInstanceOf(Point.class, result);
    }

    // =========================================================================
    // run: the live path coerces the return too (symmetric with recovery)
    // =========================================================================

    @Test
    void runLivePathCoercesReturnToDeclaredType() {
        Context ctx = root();
        Object result = ctx.run(ContextTest::makeFloat, 3).await();
        assertEquals(3.0, result);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void runLivePathCoercesReturnToStruct() {
        Context ctx = root();
        Object result = ctx.run(ContextTest::makePoint, 3, 4).await();
        assertEquals(new Point(3, 4), result);
        assertInstanceOf(Point.class, result);
    }

    // =========================================================================
    // run: nested workflow + suspension
    // =========================================================================

    @Test
    void workflowRunsNestedLeaves() {
        Context ctx = root();
        assertEquals(30, ctx.run(ContextTest::parentWorkflow, 5).await());
        assertEquals(List.of(), ctx.spawnedRemote());
        assertEquals("resolved", ctx.effects().cache().get("root.1").state());
        assertEquals(10, ctx.effects().cache().get("root.1.1").value().data());
        assertEquals(20, ctx.effects().cache().get("root.1.2").value().data());
    }

    @Test
    void runSuspendsWhenChildBlocksOnRemote() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::blocksOnRemote).await());
        assertEquals(List.of("remote-dep"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void runSuspendsWhenChildCompletesWithPendingRemote() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::fireAndForget).await());
        assertEquals(List.of("ff-dep"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void runSuspensionPropagatesThroughIntermediateWorkflow() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::deepMiddle).await());
        assertEquals(List.of("deep-dep"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
        assertEquals("pending", ctx.effects().cache().get("root.1.1").state());
    }

    @Test
    void runSuspensionPropagatesThroughThreeLevels() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::deepTop).await());
        assertEquals(List.of("deep-dep"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
        assertEquals("pending", ctx.effects().cache().get("root.1.1").state());
        assertEquals("pending", ctx.effects().cache().get("root.1.1.1").state());
    }

    @Test
    void runCompletedSiblingSettlesButParentStillSuspends() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::completesThenSuspends)
                .await());
        assertEquals(List.of("remote-dep"), ctx.spawnedRemote());
        assertEquals("resolved", ctx.effects().cache().get("root.1.1").state());
        assertEquals(42, ctx.effects().cache().get("root.1.1").value().data());
        assertEquals("pending", ctx.effects().cache().get("root.1.2").state());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void runMergesMultipleTodosFromSingleChild() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::multiRemote).await());
        assertEquals(List.of("dep-a", "dep-b", "dep-c"), ctx.spawnedRemote());
    }

    @Test
    void runFireAndForgetChildSuspensionPropagates() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.run(ContextTest::parentWithFireAndForget)
                .await());
        assertEquals(List.of("remote-dep"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
        assertEquals("pending", ctx.effects().cache().get("root.1.1").state());
    }

    @Test
    void runUnawaitedChildSettlesBeforeParent() {
        // ``flush_local_work`` joins the unawaited child before the parent settles, so both reach a
        // terminal state with the child's value observed. (The strict settle-ordering assertion in the
        // Python test relies on patching ``settle_promise``; here the join ordering is structural.)
        Context ctx = root();
        assertEquals(1, ctx.run(ContextTest::parentDoesNotAwaitChild).await());
        assertEquals("resolved", ctx.effects().cache().get("root.1.1").state());
        assertEquals(42, ctx.effects().cache().get("root.1.1").value().data());
        assertEquals("resolved", ctx.effects().cache().get("root.1").state());
        assertEquals(1, ctx.effects().cache().get("root.1").value().data());
    }

    // =========================================================================
    // run: options scoping
    // =========================================================================

    @Test
    void runWithOptionsTimeoutSetsChildDeadline() {
        Context ctx = root();
        long before = Send.nowMs();
        assertEquals(
                10,
                ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)))
                        .run(ContextTest::doubleFn, 5)
                        .await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        assertTrue(before + 30_000 <= timeoutAt && timeoutAt <= after + 30_000);
    }

    @Test
    void runWithOptionsTimeoutCappedToParent() {
        long cap = Send.nowMs() + 5_000;
        Context ctx = rootTimeout(cap);
        ctx.options(new Opts().withTimeout(Duration.ofDays(365)))
                .run(ContextTest::doubleFn, 1)
                .await();
        assertEquals(cap, ctx.effects().cache().get("root.1").timeoutAt());
    }

    @Test
    void runOptionsDoNotLeakToBaseContext() {
        Context ctx = root();
        ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)))
                .run(ContextTest::doubleFn, 1)
                .await();
        long shortDeadline = ctx.effects().cache().get("root.1").timeoutAt();
        ctx.run(ContextTest::doubleFn, 1).await();
        assertTrue(ctx.effects().cache().get("root.2").timeoutAt() > shortDeadline);
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void optionsReturnsIndependentHandleSharingState() {
        Context ctx = root();
        Context scoped = ctx.options(new Opts().withTimeout(Duration.ofSeconds(5)));
        assertNotSame(scoped, ctx);
        assertEquals(new Opts(Duration.ofSeconds(5), null, 1, null), scoped.opts());
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void optionsBuildsFreshOptsNotMergedFromBase() {
        Context ctx = root();
        // Each options() call takes a complete Opts; a fresh Opts only sets version, so target stays default.
        Context scoped = ctx.options(new Opts().withTarget("worker-1")).options(new Opts().withVersion(2));
        assertEquals(new Opts(null, null, 2, null), scoped.opts());
        assertEquals(null, scoped.opts().target());
    }

    @Test
    void optionsNoArgsYieldsDefaultOptsOnNewHandle() {
        Context ctx = root();
        Context scoped = ctx.options();
        assertNotSame(scoped, ctx);
        assertEquals(new Opts(), scoped.opts());
    }

    @Test
    void optionsCarriesEveryFieldIndependently() {
        Context ctx = root();
        RetryPolicy policy = new Constant(4, 0);
        Opts opts = new Opts(Duration.ofSeconds(7), "w", 3, policy);
        Context scoped = ctx.options(opts);
        assertEquals(opts, scoped.opts());
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void optionsSiblingHandlesAreMutuallyIsolated() {
        Context ctx = root();
        Context a = ctx.options(new Opts().withTimeout(Duration.ofSeconds(5)));
        Context b = ctx.options(new Opts().withTarget("worker-2"));
        assertEquals(new Opts(Duration.ofSeconds(5), null, 1, null), a.opts());
        assertEquals(new Opts(null, "worker-2", 1, null), b.opts());
        assertNotEquals(a.opts(), b.opts());
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void optionsHandlesShareIdSequence() {
        Context ctx = root();
        assertEquals("root.1", ctx.options(new Opts().withTarget("x")).nextId());
        assertEquals("root.2", ctx.options(new Opts().withVersion(2)).nextId());
        assertEquals("root.3", ctx.nextId());
    }

    @Test
    void optionsOpFlipsSharedWorkflowFlag() {
        Context ctx = root();
        assertEquals(false, ctx.workflow());
        assertThrows(
                Suspended.class,
                () -> ctx.options(new Opts().withTarget("x")).rpc("fn").await());
        assertEquals(true, ctx.workflow());
    }

    @Test
    void optionsHandlesShareSpawnedState() {
        Context ctx = root();
        ResonateFuture<Object> f1 = ctx.options(new Opts().withTarget("a")).rpc("fn");
        ResonateFuture<Object> f2 = ctx.options(new Opts().withTarget("b")).rpc("fn");
        assertThrows(Suspended.class, f1::await);
        assertThrows(Suspended.class, f2::await);
        assertEquals(List.of("root.1", "root.2"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
        assertEquals("pending", ctx.effects().cache().get("root.2").state());
    }

    // =========================================================================
    // ResonateFuture.id()
    // =========================================================================

    @Test
    void futureIdReturnsIdAfterCreate() {
        Context ctx = root();
        ResonateFuture<Object> fut = ctx.run(ContextTest::doubleFn, 21);
        assertEquals("root.1", fut.id());
        assertEquals(42, fut.await());
    }

    // =========================================================================
    // rpc
    // =========================================================================

    @Test
    void rpcPendingRegistersTodoAndSuspends() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.rpc("remote_fn", 1, 2).await());
        assertEquals(List.of("root.1"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void rpcPresettledResolvedReturnsValue() {
        Context ctx = root(List.of(resolved("root.1", "remote-result")));
        assertEquals("remote-result", ctx.rpc("remote_fn").await());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void rpcPresettledRejectedRaises() {
        Context ctx = root(List.of(rejected("root.1", "remote failure")));
        ApplicationError exc =
                assertThrows(ApplicationError.class, () -> ctx.rpc("remote_fn").await());
        assertEquals("remote failure", exc.getMessage());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void rpcRequestTagsAndParam() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.rpc("remote_fn", 1, 2).await());
        PromiseRecord record = ctx.effects().cache().get("root.1");
        assertEquals(
                Map.of(
                        "resonate:scope", "global",
                        "resonate:target", "",
                        "resonate:branch", "root.1",
                        "resonate:parent", "root",
                        "resonate:origin", "root",
                        "resonate:prefix", "root"),
                record.tags());
        assertEquals(
                new TaskData(List.of(1, 2), Map.of(), "remote_fn", 1),
                taskData(record.param().data()));
    }

    @Test
    void rpcNoArgsParamIsEmpty() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.rpc("remote_fn").await());
        assertEquals(
                new TaskData(List.of(), Map.of(), "remote_fn", 1),
                taskData(ctx.effects().cache().get("root.1").param().data()));
    }

    @Test
    void rpcSequentialChildIds() {
        Context ctx = root(List.of(resolved("root.1", "a"), resolved("root.2", "b")));
        assertEquals("a", ctx.rpc("fn").await());
        assertEquals("b", ctx.rpc("fn").await());
    }

    @Test
    void rpcWithOptionsTargetSetsTag() {
        Context ctx = root();
        assertThrows(
                Suspended.class,
                () -> ctx.options(new Opts().withTarget("worker-1")).rpc("fn").await());
        assertEquals("worker-1", ctx.effects().cache().get("root.1").tags().get("resonate:target"));
    }

    @Test
    void rpcWithOptionsTimeoutSetsChildDeadline() {
        Context ctx = root();
        long before = Send.nowMs();
        assertThrows(Suspended.class, () -> ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)))
                .rpc("fn")
                .await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        assertTrue(before + 30_000 <= timeoutAt && timeoutAt <= after + 30_000);
    }

    @Test
    void rpcWithOptionsTimeoutCappedToParent() {
        long cap = Send.nowMs() + 5_000;
        Context ctx = rootTimeout(cap);
        assertThrows(Suspended.class, () -> ctx.options(new Opts().withTimeout(Duration.ofDays(365)))
                .rpc("fn")
                .await());
        assertEquals(cap, ctx.effects().cache().get("root.1").timeoutAt());
    }

    @Test
    void rpcOptionsDoNotLeakToBaseContext() {
        Context ctx = root(List.of(resolved("root.1", "a")));
        ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)).withTarget("x"))
                .rpc("fn")
                .await();
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void rpcOnBaseContextIgnoresDiscardedOptionsCall() {
        Context ctx = root();
        ctx.options(new Opts().withTimeout(Duration.ofSeconds(5)).withTarget("x"));
        assertThrows(Suspended.class, () -> ctx.rpc("fn").await());
        assertEquals("", ctx.effects().cache().get("root.1").tags().get("resonate:target"));
    }

    // =========================================================================
    // sleep
    // =========================================================================

    @Test
    void sleepPendingRegistersTodoAndSuspends() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.sleep(Duration.ofSeconds(30)).await());
        assertEquals(List.of("root.1"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void sleepPresettledResolvedReturnsNull() {
        Context ctx = root(List.of(resolved("root.1", null)));
        assertEquals(null, ctx.sleep(Duration.ofSeconds(1)).await());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void sleepRequestTagsAndTimerFlag() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.sleep(Duration.ofSeconds(30)).await());
        PromiseRecord record = ctx.effects().cache().get("root.1");
        assertEquals(
                Map.of(
                        "resonate:scope", "global",
                        "resonate:branch", "root.1",
                        "resonate:parent", "root",
                        "resonate:origin", "root",
                        "resonate:prefix", "root",
                        "resonate:timer", "true"),
                record.tags());
        assertEquals(null, record.param().data());
    }

    @Test
    void sleepTimeoutAtIsNowPlusDuration() {
        Context ctx = root();
        long before = Send.nowMs();
        assertThrows(Suspended.class, () -> ctx.sleep(Duration.ofSeconds(30)).await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        assertTrue(before + 30_000 <= timeoutAt && timeoutAt <= after + 30_000);
    }

    @Test
    void sleepDurationCappedToParentTimeout() {
        long cap = Send.nowMs() + 5_000;
        Context ctx = rootTimeout(cap);
        assertThrows(Suspended.class, () -> ctx.sleep(Duration.ofDays(365)).await());
        assertEquals(cap, ctx.effects().cache().get("root.1").timeoutAt());
    }

    @Test
    void sleepIgnoresOptsTimeoutForDuration() {
        Context ctx = root();
        long before = Send.nowMs();
        assertThrows(Suspended.class, () -> ctx.options(new Opts().withTimeout(Duration.ofSeconds(5)))
                .sleep(Duration.ofSeconds(30))
                .await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        assertTrue(before + 30_000 <= timeoutAt && timeoutAt <= after + 30_000);
    }

    @Test
    void sleepSequentialChildIds() {
        Context ctx = root(List.of(resolved("root.1", null), resolved("root.2", null)));
        assertEquals(null, ctx.sleep(Duration.ofSeconds(1)).await());
        assertEquals(null, ctx.sleep(Duration.ofSeconds(1)).await());
    }

    // =========================================================================
    // promise
    // =========================================================================

    @Test
    void promisePendingRegistersTodoAndSuspends() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.promise(Duration.ofSeconds(30)).await());
        assertEquals(List.of("root.1"), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get("root.1").state());
    }

    @Test
    void promisePresettledResolvedReturnsValue() {
        Context ctx = root(List.of(resolved("root.1", "external-result")));
        assertEquals("external-result", ctx.promise(Duration.ofSeconds(1)).await());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void promisePresettledRejectedRaises() {
        Context ctx = root(List.of(rejected("root.1", "external failure")));
        ApplicationError exc = assertThrows(
                ApplicationError.class, () -> ctx.promise(Duration.ofSeconds(1)).await());
        assertEquals("external failure", exc.getMessage());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void promiseRequestTagsAndEmptyParam() {
        Context ctx = root();
        assertThrows(Suspended.class, () -> ctx.promise(Duration.ofSeconds(30)).await());
        PromiseRecord record = ctx.effects().cache().get("root.1");
        assertEquals(
                Map.of(
                        "resonate:scope", "global",
                        "resonate:branch", "root.1",
                        "resonate:parent", "root",
                        "resonate:origin", "root",
                        "resonate:prefix", "root"),
                record.tags());
        assertEquals(null, record.param().data());
    }

    @Test
    void promiseTimeoutAtIsNowPlusTimeout() {
        Context ctx = root();
        long before = Send.nowMs();
        assertThrows(Suspended.class, () -> ctx.promise(Duration.ofSeconds(30)).await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        assertTrue(before + 30_000 <= timeoutAt && timeoutAt <= after + 30_000);
    }

    @Test
    void promiseNoneTimeoutUsesDefault() {
        Context ctx = root();
        long before = Send.nowMs();
        assertThrows(Suspended.class, () -> ctx.promise(null).await());
        long after = Send.nowMs();
        long timeoutAt = ctx.effects().cache().get("root.1").timeoutAt();
        long dayMs = 24L * 60 * 60 * 1000;
        assertTrue(before + dayMs <= timeoutAt && timeoutAt <= after + dayMs);
    }

    @Test
    void promiseTimeoutCappedToParent() {
        long cap = Send.nowMs() + 5_000;
        Context ctx = rootTimeout(cap);
        assertThrows(Suspended.class, () -> ctx.promise(Duration.ofDays(365)).await());
        assertEquals(cap, ctx.effects().cache().get("root.1").timeoutAt());
    }

    @Test
    void promiseSequentialChildIds() {
        Context ctx = root(List.of(resolved("root.1", "a"), resolved("root.2", "b")));
        assertEquals("a", ctx.promise(Duration.ofSeconds(1)).await());
        assertEquals("b", ctx.promise(Duration.ofSeconds(1)).await());
    }

    // =========================================================================
    // detached
    // =========================================================================

    @Test
    void detachedReturnsIdWithoutSuspending() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        assertEquals(childId, ctx.detached("remote_fn", 1, 2).await());
        assertEquals(List.of(), ctx.spawnedRemote());
        assertEquals("pending", ctx.effects().cache().get(childId).state());
    }

    @Test
    void detachedIdIsPrefixRootedHash() {
        Context ctx = root();
        String childId = (String) ctx.detached("remote_fn").await();
        assertEquals(detachedId("root", "root.1"), childId);
        assertNotEquals("root.1", childId);
        String suffix = childId.substring(childId.indexOf('.') + 1);
        assertEquals('d', suffix.charAt(0));
        assertEquals(17, suffix.length());
        assertTrue(suffix.substring(1).chars().allMatch(c -> "0123456789abcdef".indexOf(c) >= 0));
    }

    @Test
    void detachedConsumesSeqAndYieldsDistinctIds() {
        Context ctx = root();
        String id1 = (String) ctx.detached("fn").await();
        String id2 = (String) ctx.detached("fn").await();
        assertEquals(detachedId("root", "root.1"), id1);
        assertEquals(detachedId("root", "root.2"), id2);
        assertNotEquals(id1, id2);
    }

    @Test
    void detachedRequestTagsAndParam() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        ctx.detached("remote_fn", 1, 2).await();
        PromiseRecord record = ctx.effects().cache().get(childId);
        assertEquals(
                Map.of(
                        "resonate:scope", "global",
                        "resonate:target", "",
                        "resonate:branch", childId,
                        "resonate:parent", "root",
                        "resonate:origin", childId,
                        "resonate:prefix", "root"),
                record.tags());
        assertEquals(
                new TaskData(List.of(1, 2), Map.of(), "remote_fn", 1),
                taskData(record.param().data()));
    }

    @Test
    void detachedNoArgsParamIsEmpty() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        ctx.detached("remote_fn").await();
        assertEquals(
                new TaskData(List.of(), Map.of(), "remote_fn", 1),
                taskData(ctx.effects().cache().get(childId).param().data()));
    }

    @Test
    void detachedIdempotentOnPreloadedRecord() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        ctx.effects().cache().put(childId, codec().decodePromise(resolved(childId, "external-result")));
        assertEquals(childId, ctx.detached("fn").await());
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void detachedWithOptionsTargetAndTimeout() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        long before = Send.nowMs();
        ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)).withTarget("worker-1"))
                .detached("fn")
                .await();
        long after = Send.nowMs();
        PromiseRecord record = ctx.effects().cache().get(childId);
        assertEquals("worker-1", record.tags().get("resonate:target"));
        assertTrue(before + 30_000 <= record.timeoutAt() && record.timeoutAt() <= after + 30_000);
    }

    @Test
    void detachedOptionsDoNotLeakToBaseContext() {
        Context ctx = root();
        ctx.options(new Opts().withTimeout(Duration.ofSeconds(30)).withTarget("x"))
                .detached("fn")
                .await();
        assertEquals(new Opts(), ctx.opts());
    }

    @Test
    void detachedTimeoutCappedToParent() {
        long cap = Send.nowMs() + 5_000;
        Context ctx = rootTimeout(cap);
        String childId = detachedId("root", "root.1");
        ctx.options(new Opts().withTimeout(Duration.ofDays(365))).detached("fn").await();
        assertEquals(cap, ctx.effects().cache().get(childId).timeoutAt());
    }

    @Test
    void detachedDoesNotForceParentToSuspend() {
        Context ctx = root();
        assertEquals("done", ctx.run(ContextTest::dispatchesDetached).await());
        assertEquals(List.of(), ctx.spawnedRemote());
        assertEquals("resolved", ctx.effects().cache().get("root.1").state());
        assertEquals("done", ctx.effects().cache().get("root.1").value().data());
        String detached = detachedId("root", "root.1.1");
        assertEquals("pending", ctx.effects().cache().get(detached).state());
    }

    @Test
    void detachedBgTaskRegisteredForFlush() {
        Context ctx = root();
        ResonateFuture<String> fut = ctx.detached("fn");
        assertEquals(1, ctx.spawnedRemoteTasks().size());
        assertSame(fut.task(), ctx.spawnedRemoteTasks().get(0));
        ctx.flushLocalWork().join();
        assertEquals(List.of(), ctx.spawnedRemoteTasks());
    }

    @Test
    void detachedCreatePromiseCompletesByFlushWhenUnawaited() {
        Context ctx = root();
        String childId = detachedId("root", "root.1");
        ResonateFuture<String> fut = ctx.detached("remote_fn", 1, 2);
        // The create is driven through the chain; join the flush to guarantee it completed.
        ctx.flushLocalWork().join();
        assertEquals("pending", ctx.effects().cache().get(childId).state());
        assertEquals(List.of(), ctx.takeRemoteTodos());
        assertEquals(childId, fut.id());
    }

    // =========================================================================
    // invoke_with_retry
    // =========================================================================

    @Test
    void invokeWithRetryReturnsOnFirstSuccess() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafSucceeds", Context.class));
        Object out = ctx.invokeWithRetry(leaf, leaf.packArgs(), new Constant(3, 0), true);
        assertEquals(7, out);
        assertEquals(1, Counter.calls);
    }

    @Test
    void invokeWithRetryRetriesPureLeafUntilSuccess() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafFlaky", Context.class));
        Object out = ctx.invokeWithRetry(leaf, leaf.packArgs(), new Constant(5, 0), true);
        assertEquals(42, out);
        assertEquals(3, Counter.calls);
    }

    @Test
    void invokeWithRetryExhaustsThenRaises() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafAlwaysFails", Context.class));
        assertThrows(
                RuntimeException.class, () -> ctx.invokeWithRetry(leaf, leaf.packArgs(), new Constant(2, 0), true));
        assertEquals(3, Counter.calls);
    }

    @Test
    void invokeWithRetryNeverPolicyDoesNotRetry() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafAlwaysFails", Context.class));
        assertThrows(RuntimeException.class, () -> ctx.invokeWithRetry(leaf, leaf.packArgs(), new Never(), true));
        assertEquals(1, Counter.calls);
    }

    @Test
    void invokeWithRetryWorkflowNeverRetries() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafWorkflowThenFails", Context.class));
        assertThrows(
                RuntimeException.class, () -> ctx.invokeWithRetry(leaf, leaf.packArgs(), new Constant(9, 0), true));
        assertTrue(ctx.workflow());
        assertEquals(1, Counter.calls);
    }

    @Test
    void invokeWithRetryPropagatesSuspendedWithoutRetry() {
        Context ctx = root();
        Counter.calls = 0;
        Durable leaf = new Durable(m("leafSuspends", Context.class));
        assertThrows(Suspended.class, () -> ctx.invokeWithRetry(leaf, leaf.packArgs(), new Constant(9, 0), true));
        assertEquals(1, Counter.calls);
    }

    @Test
    void ctxRunChildInheritsContextDefaultRetryPolicy() {
        Counter.calls = 0;
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), new Constant(2, 0), null);
        assertThrows(
                ApplicationError.class, () -> ctx.run(ContextTest::leafFlakyApp).await());
        assertEquals(3, Counter.calls);
    }

    @Test
    void ctxRunWithOptsRetryPolicyOverridesContextDefault() {
        Counter.calls = 0;
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), new Constant(9, 0), null);
        assertThrows(ApplicationError.class, () -> ctx.options(new Opts().withRetryPolicy(new Never()))
                .run(ContextTest::leafFlakyApp)
                .await());
        assertEquals(1, Counter.calls);
    }

    /** Side-effect counter shared by the retry leaves (the analogue of the tests' ``nonlocal calls``). */
    static final class Counter {
        static int calls;
    }

    static int leafSucceeds(Context ctx) {
        Counter.calls++;
        return 7;
    }

    static int leafFlaky(Context ctx) {
        Counter.calls++;
        if (Counter.calls < 3) {
            throw new RuntimeException("transient");
        }
        return 42;
    }

    static int leafAlwaysFails(Context ctx) {
        Counter.calls++;
        throw new RuntimeException("denied");
    }

    static int leafWorkflowThenFails(Context ctx) {
        Counter.calls++;
        ctx.setWorkflow(true);
        throw new RuntimeException("boom after durable op");
    }

    static int leafSuspends(Context ctx) {
        Counter.calls++;
        throw new Suspended();
    }

    static int leafFlakyApp(Context ctx) {
        Counter.calls++;
        throw new ApplicationError("boom");
    }

    // =========================================================================
    // run: by-name dispatch
    // =========================================================================

    @Test
    void runByNameResolvesAndExecutesFromRegistry() {
        Registry registry = new Registry();
        registry.register("double", ContextTest::doubleFn);
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), null, registry);
        assertEquals(42, ctx.run("double", 21).await());
    }

    @Test
    void runByNameDispatchesOptsVersion() {
        Registry registry = new Registry();
        registry.register("impl", ContextTest::v1, 1);
        registry.register("impl", ContextTest::v2, 2);
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), null, registry);
        assertEquals("one", ctx.run("impl").await());
        assertEquals("two", ctx.options(new Opts().withVersion(2)).run("impl").await());
    }

    @Test
    void runByNameUnregisteredRaisesFunctionNotFound() {
        Context ctx = root();
        PlatformError exc = assertThrows(PlatformError.class, () -> ctx.run("missing"));
        assertInstanceOf(FunctionNotFoundError.class, exc.causes().get(0));
    }

    static String v1(Context ctx) {
        return "one";
    }

    static String v2(Context ctx) {
        return "two";
    }

    // =========================================================================
    // rpc: by-object dispatch
    // =========================================================================

    @Test
    void rpcByObjectDispatchesRegisteredNameAndVersion() {
        Registry registry = new Registry();
        registry.register("remote_impl", ContextTest::doubleFn, 3);
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), null, registry);
        assertThrows(Suspended.class, () -> ctx.rpc(ContextTest::doubleFn, 5).await());
        assertEquals(
                new TaskData(List.of(5), Map.of(), "remote_impl", 3),
                taskData(ctx.effects().cache().get("root.1").param().data()));
    }

    @Test
    void rpcByObjectVersionFromIdentityNotOpts() {
        Registry registry = new Registry();
        registry.register("remote_impl", ContextTest::doubleFn, 2);
        Context ctx = root(List.of(), I64_MAX, new Dependencies(), null, registry);
        assertThrows(Suspended.class, () -> ctx.options(new Opts().withVersion(9))
                .rpc(ContextTest::doubleFn, 5)
                .await());
        assertEquals(
                2, taskData(ctx.effects().cache().get("root.1").param().data()).version());
    }

    @Test
    void rpcByObjectUnregisteredRaises() {
        Context ctx = root();
        PlatformError exc = assertThrows(PlatformError.class, () -> ctx.rpc(ContextTest::beat));
        assertInstanceOf(FunctionNotFoundError.class, exc.causes().get(0));
        assertEquals(List.of(), ctx.spawnedRemote());
    }

    @Test
    void rpcByObjectRecoveryCoercesReturnToStruct() {
        Registry registry = new Registry();
        registry.register("make_point", ContextTest::makePoint, 1);
        Context ctx = root(List.of(resolved("root.1", new Point(3, 4))), I64_MAX, new Dependencies(), null, registry);
        Object result = ctx.rpc(ContextTest::makePoint, 3, 4).await();
        assertInstanceOf(Point.class, result);
        assertEquals(new Point(3, 4), result);
    }

    @Test
    void rpcByNameRecoveryStaysRawBuiltins() {
        Context ctx = root(List.of(resolved("root.1", new Point(3, 4))));
        Object result = ctx.rpc("make_point", 3, 4).await();
        assertInstanceOf(Map.class, result);
        assertEquals(Map.of("x", 3, "y", 4), result);
    }
}
