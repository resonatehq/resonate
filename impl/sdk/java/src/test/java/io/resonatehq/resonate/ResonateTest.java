package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Errors.AlreadyRegisteredError;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Heartbeat.Async;
import io.resonatehq.resonate.Heartbeat.Noop;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Resonate.Builder;
import io.resonatehq.resonate.Resonate.ResonateSchedule;
import io.resonatehq.resonate.Retry.Never;
import io.resonatehq.resonate.Types.PromiseRecord;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_resonate.py}: behaviour tests for {@link Resonate} run
 * against the real in-process {@link LocalNetwork} driven through the real {@link Send.Sender} /
 * {@link Transport} -- "real server, real wire", no mocks.
 *
 * <p><b>kwargs divergence.</b> Java has no keyword arguments (see {@code Durable}), so Python's
 * kwargs-carrying tests pass their values positionally; the round-tripped {@code kwargs} slot is
 * therefore always empty. The asserted outcomes are otherwise unchanged.
 *
 * <p><b>Omitted (no faithful Java analogue, mirroring the omissions in {@code CoreTest} /
 * {@code ContextTest}).</b> The three "promise-gone settlement" tests
 * ({@code test_handle_settles_with_error_when_listener_register_returns_404} and friends) drive a
 * forced 404 by monkeypatching {@code sender.promise_register_listener}; this suite uses no mocking
 * framework, so they are dropped. The {@code return_type} forward-reference tests
 * ({@code ignores_unresolvable_param}, {@code non_string_annotation_passthrough}) are Python-specific
 * ({@code from __future__ import annotations}); Java methods always carry concrete reflected types.
 */
class ResonateTest {

    // ── Harness ──────────────────────────────────────────────────────────────

    private final List<Resonate> created = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (Resonate r : created) {
            r.stop().join();
        }
        created.clear();
    }

    /**
     * A local-mode Resonate, stopped on teardown. Pins {@link Never} so a failing pure leaf settles
     * immediately (the SDK default is an effectively-unbounded Exponential that would retry forever).
     */
    private Resonate local() {
        return track(Resonate.builder().retryPolicy(new Never()));
    }

    private Resonate track(Builder b) {
        Resonate r = b.build();
        created.add(r);
        return r;
    }

    /** Run a future to completion, unwrapping {@link CompletionException} to its cause. */
    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    /** Poll until the durable promise {@code id} exists (for fire-and-forget creates). */
    private static PromiseRecord waitForPromise(Resonate r, String id) {
        for (int i = 0; i < 2000; i++) {
            try {
                return await(r.promises.get(id));
            } catch (ServerError e) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ie);
                }
            }
        }
        throw new AssertionError("promise " + id + " was never created");
    }

    /** Minimal non-Local {@link Network} for the heartbeat-selection tests. Mirrors Python's {@code _FakeNetwork}. */
    static final class FakeNetwork implements Network {
        @Override
        public String pid() {
            return "fake";
        }

        @Override
        public String group() {
            return "g";
        }

        @Override
        public String unicast() {
            return "fake://uni@g/fake";
        }

        @Override
        public String anycast() {
            return "fake://any@g/fake";
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
        public CompletableFuture<String> send(String req) {
            return CompletableFuture.completedFuture("{}");
        }

        @Override
        public void recv(Consumer<String> callback) {}

        @Override
        public String targetResolver(String target) {
            return "fake://any@" + target;
        }
    }

    // ── Workflow library ───────────────────────────────────────────────────

    record Point(int x, int y) {}

    record Vec(int dx, int dy) {}

    static final class Config {
        final String value;

        Config(String value) {
            this.value = value;
        }
    }

    static final class Counter {
        final int count;

        Counter(int count) {
            this.count = count;
        }
    }

    // Per-test signals for the timing tests; set at the start of the test that uses them.
    static volatile CountDownLatch STARTED;
    static volatile CountDownLatch GATE;

    static Object noop(Context ctx) {
        return null;
    }

    static int add(Context ctx, int x, int y) {
        return x + y;
    }

    static int boom(Context ctx) {
        throw new ApplicationError("deliberate failure");
    }

    static Point makePoint(Context ctx, int x, int y) {
        return new Point(x, y);
    }

    static Vec makeVec(Context ctx, int dx, int dy) {
        return new Vec(dx, dy);
    }

    static List<Integer> makeList(Context ctx) {
        return List.of(1, 2, 3);
    }

    /** Unannotated analogue: Object params pass through, Object return decodes as Any. */
    static Object bareAdd(Context ctx, Object x, Object y) {
        return ((Number) x).intValue() + ((Number) y).intValue();
    }

    static int addViaChild(Context ctx, int x, int y) {
        return ctx.run(ResonateTest::add, x, y).await();
    }

    static int slow(Context ctx) {
        STARTED.countDown();
        return 1;
    }

    static int waits(Context ctx) {
        try {
            GATE.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return 7;
    }

    static String readConfig(Context ctx) {
        return ctx.getDependency(Config.class).value;
    }

    static String readTwoDeps(Context ctx) {
        return ctx.getDependency(Config.class).value + ":" + ctx.getDependency(Counter.class).count;
    }

    static String implOne(Context ctx) {
        return "one";
    }

    static String implTwo(Context ctx) {
        return "two";
    }

    static Object versioned(Context ctx) {
        return null;
    }

    // `add` is overloaded (add(Context,int,int) and add(Context)), so a bare `ResonateTest::add`
    // reference is ambiguous; these two keep an explicit Fn type to disambiguate. Every other
    // function has a unique name and is passed inline as `ResonateTest::fn`.

    // Reflection helper for the white-box Durable tests below, which construct a Durable from a raw
    // Method (an internal type that takes no Fn reference).
    private static Method m(String name, Class<?>... params) {
        try {
            return ResonateTest.class.getDeclaredMethod(name, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Constructor / configuration
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void localConstructorSetsDefaults() {
        Resonate r = local();
        assertEquals("default", r.pid);
        assertEquals("", r.idPrefix);
        assertEquals(Resonate.DEFAULT_TTL, r.ttl);
        assertInstanceOf(LocalNetwork.class, r.network);
    }

    @Test
    void configWithCustomPidAndGroup() {
        Resonate r = track(
                Resonate.builder().retryPolicy(new Never()).pid("worker-1").group("workers"));
        assertEquals("worker-1", r.pid);
        assertTrue(r.network.unicast().contains("worker-1"));
        assertTrue(r.network.unicast().contains("workers"));
    }

    @Test
    void configWithPrefix() {
        Resonate r = track(
                Resonate.builder().retryPolicy(new Never()).prefix("myapp").ttl(Duration.ofSeconds(30)));
        assertEquals("myapp:", r.idPrefix);
        assertEquals(Duration.ofSeconds(30), r.ttl);
    }

    @Test
    void configWithEmptyPrefix() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).prefix(""));
        assertEquals("", r.idPrefix);
    }

    @Test
    void defaultTtlIsOneMinute() {
        assertEquals(Duration.ofMinutes(1), local().ttl);
    }

    @Test
    void networkIdentityLocalMode() {
        Resonate r = local();
        assertTrue(r.network.unicast().startsWith("local://uni@"));
        assertTrue(r.network.anycast().startsWith("local://any@"));
        assertEquals("default", r.network.group());
        assertEquals("default", r.network.pid());
    }

    @Test
    void targetResolverReturnsLocalAnycast() {
        assertEquals("local://any@my-target", local().network.targetResolver("my-target"));
    }

    @Test
    void localModeUsesNoopHeartbeat() {
        assertInstanceOf(Noop.class, local().heartbeat);
    }

    @Test
    void remoteNetworkUsesAsyncHeartbeat() {
        Resonate r = track(Resonate.builder().network(new FakeNetwork()));
        assertInstanceOf(Async.class, r.heartbeat);
    }

    @Test
    void explicitHeartbeatOverrideWins() {
        Noop hb = new Noop();
        Resonate r = track(Resonate.builder().network(new FakeNetwork()).heartbeat(hb));
        assertSame(hb, r.heartbeat);
    }

    @Test
    void heartbeatIntervalIsHalfTheTtl() {
        Resonate r = track(Resonate.builder().network(new FakeNetwork()).ttl(Duration.ofSeconds(60)));
        Async hb = assertInstanceOf(Async.class, r.heartbeat);
        assertEquals(60_000 / Resonate.HEARTBEAT_INTERVAL_DIVISOR, hb.intervalMs());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  register
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void registerThenRunSucceeds() {
        Resonate r = local();
        r.register(ResonateTest::add);
        assertEquals(3, r.run("t", ResonateTest::add, 1, 2).result());
    }

    @Test
    void registerWithCustomName() {
        Resonate r = local();
        r.register(ResonateTest::add, "sum", 1);
        // run takes the function object; its registered name ("sum", not "add") is recovered by identity.
        assertEquals(9, r.run("t", ResonateTest::add, 4, 5).result());
    }

    @Test
    void registerDuplicateRaises() {
        Resonate r = local();
        r.register(ResonateTest::noop);
        assertThrows(AlreadyRegisteredError.class, () -> r.register(ResonateTest::noop));
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  run
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void runIsSynchronousAndReturnsHandle() {
        Resonate r = local();
        r.register(ResonateTest::noop);
        ResonateHandle<Object> h = r.run("greet-1", ResonateTest::noop);
        assertInstanceOf(ResonateHandle.class, h);
        assertEquals("greet-1", await(h.id()));
    }

    @Test
    void runStartsExecutionImmediately() throws InterruptedException {
        Resonate r = local();
        STARTED = new CountDownLatch(1);
        r.register(ResonateTest::slow);
        var h = r.run("s", ResonateTest::slow);
        assertTrue(STARTED.await(1, TimeUnit.SECONDS));
        assertEquals(1, h.result());
    }

    @Test
    void runResolvesResult() {
        Resonate r = local();
        r.register(ResonateTest::add);
        assertEquals(5, r.run("a", ResonateTest::add, 2, 3).result());
    }

    @Test
    void runDecodesStructResult() {
        Resonate r = local();
        r.register(ResonateTest::makePoint);
        assertEquals(new Point(1, 2), r.run("pt", ResonateTest::makePoint, 1, 2).result());
    }

    @Test
    void runDecodesRecordResult() {
        Resonate r = local();
        r.register(ResonateTest::makeVec);
        Object result = r.run("vec", ResonateTest::makeVec, 3, 4).result();
        assertEquals(new Vec(3, 4), result);
        assertInstanceOf(Vec.class, result);
    }

    @Test
    void runRejectedWorkflowRaises() {
        Resonate r = local();
        r.register(ResonateTest::boom);
        ApplicationError exc = assertThrows(
                ApplicationError.class, () -> r.run("b", ResonateTest::boom).result());
        assertTrue(exc.getMessage().contains("deliberate failure"));
    }

    @Test
    void runWithPrefixPrependsId() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).prefix("app"));
        r.register(ResonateTest::noop);
        assertEquals("app:my-id", await(r.run("my-id", ResonateTest::noop).id()));
    }

    @Test
    void runUnregisteredRaisesSynchronously() {
        Resonate r = local();
        // Refused at the call site: an unregistered object's registry name is not its method name.
        assertThrows(FunctionNotFoundError.class, () -> r.run("x", ResonateTest::add));
    }

    @Test
    void runIdempotentSameId() {
        Resonate r = local();
        r.register(ResonateTest::add);
        assertEquals(2, r.run("dup", ResonateTest::add, 1, 1).result());
        // Second run with the same id observes the existing settled promise.
        assertEquals(2, r.run("dup", ResonateTest::add, 1, 1).result());
    }

    @Test
    void runUnannotatedFunctionResolves() {
        Resonate r = local();
        r.register(ResonateTest::bareAdd);
        assertEquals(5, r.run("bare", ResonateTest::bareAdd, 2, 3).result());
    }

    @Test
    void runHandleIdResolvesToCreatedId() {
        Resonate r = local();
        r.register(ResonateTest::add);
        var h = r.run("rid", ResonateTest::add, 1, 1);
        assertEquals("rid", await(h.id()));
        h.result();
        assertEquals("rid", await(h.id()));
    }

    @Test
    void runDoneFalseUntilSettled() {
        Resonate r = local();
        GATE = new CountDownLatch(1);
        r.register(ResonateTest::waits);
        var h = r.run("rd", ResonateTest::waits);
        assertFalse(h.done());
        GATE.countDown();
        assertEquals(7, h.result());
        assertTrue(h.done());
    }

    @Test
    void runReturnsNoneResult() {
        Resonate r = local();
        r.register(ResonateTest::noop);
        assertEquals(null, r.run("rn", ResonateTest::noop).result());
    }

    @Test
    void runMultistepWorkflowResolves() {
        Resonate r = local();
        r.register(ResonateTest::addViaChild);
        assertEquals(9, r.run("wf", ResonateTest::addViaChild, 4, 5).result());
    }

    @Test
    void runDefaultTargetUsesNetworkResolver() {
        Resonate r = local();
        r.register(ResonateTest::noop);
        r.run("rt", ResonateTest::noop).result();
        PromiseRecord record = await(r.promises.get("rt"));
        assertEquals("local://any@default", record.tags().get("resonate:target"));
        assertEquals("global", record.tags().get("resonate:scope"));
    }

    // ── run by name (registry lookup) ───────────────────────────────────────

    @Test
    void runByNameResolvesFromRegistry() {
        Resonate r = local();
        r.register(ResonateTest::add);
        assertEquals(5, r.run("a", "add", 2, 3).result());
    }

    @Test
    void runByNameDecodesStructResult() {
        Resonate r = local();
        r.register(ResonateTest::makePoint);
        assertEquals(new Point(1, 2), r.run("pt", "makePoint", 1, 2).result());
    }

    @Test
    void runByNameUsesOptsVersion() {
        Resonate r = local();
        r.register(ResonateTest::implOne, "impl", 1);
        r.register(ResonateTest::implTwo, "impl", 2);
        assertEquals("one", r.run("x1", "impl").result()); // default version 1
        assertEquals(
                "two", r.options(new Opts().withVersion(2)).run("x2", "impl").result());
    }

    @Test
    void runByNameUnregisteredRaisesSynchronously() {
        Resonate r = local();
        FunctionNotFoundError exc = assertThrows(FunctionNotFoundError.class, () -> r.run("x", "ghost"));
        assertTrue(exc.getMessage().contains("ghost"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  rpc
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void rpcIsSynchronousAndReturnsHandle() {
        Resonate r = local();
        ResonateHandle<Object> h = r.rpc("rpc-1", "remote_fn", 1);
        assertInstanceOf(ResonateHandle.class, h);
        assertEquals("rpc-1", await(h.id()));
    }

    @Test
    void rpcDoesNotRequireRegistration() {
        Resonate r = local();
        r.rpc("rpc-1", "remote_fn", 1);
        // The promise is created even though no function is registered locally.
        assertEquals("pending", waitForPromise(r, "rpc-1").state());
    }

    @Test
    void rpcWithPrefix() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).prefix("svc"));
        ResonateHandle<Object> h = r.rpc("rpc-2", "remote");
        assertEquals("svc:rpc-2", await(h.id()));
        waitForPromise(r, "svc:rpc-2");
    }

    @Test
    void rpcArgsRoundTripIntoParam() {
        // kwargs divergence: Java passes flag positionally; the kwargs slot stays empty.
        Resonate r = local();
        r.rpc("rpc-args", "remote", 1, 2, true);
        PromiseRecord record = waitForPromise(r, "rpc-args");
        assertEquals(
                Map.of("func", "remote", "args", List.of(1, 2, true), "kwargs", Map.of(), "version", 1),
                record.param().data());
    }

    @Test
    void rpcNoArgsHasEmptyArgs() {
        Resonate r = local();
        r.rpc("rpc-empty", "remote");
        PromiseRecord record = waitForPromise(r, "rpc-empty");
        assertEquals(
                Map.of("func", "remote", "args", List.of(), "kwargs", Map.of(), "version", 1),
                record.param().data());
    }

    @Test
    void rpcDefaultTarget() {
        Resonate r = local();
        r.rpc("rpc-dt", "remote");
        assertEquals("local://any@default", waitForPromise(r, "rpc-dt").tags().get("resonate:target"));
    }

    @Test
    void rpcDoneFalseWhilePending() {
        Resonate r = local();
        ResonateHandle<Object> h = r.rpc("rpc-pending", "remote");
        waitForPromise(r, "rpc-pending");
        assertFalse(h.done());
    }

    @Test
    void rpcIdempotentSameId() {
        Resonate r = local();
        ResonateHandle<Object> h1 = r.rpc("rpc-dup", "remote", 1);
        ResonateHandle<Object> h2 = r.rpc("rpc-dup", "remote", 1);
        assertEquals("rpc-dup", await(h1.id()));
        assertEquals("rpc-dup", await(h2.id()));
        PromiseRecord record = waitForPromise(r, "rpc-dup");
        assertEquals("remote", ((Map<?, ?>) record.param().data()).get("func"));
    }

    @Test
    void rpcHandleIdResolves() {
        Resonate r = local();
        ResonateHandle<Object> h = r.rpc("rpc-id", "remote");
        assertEquals("rpc-id", await(h.id()));
        assertFalse(h.done());
    }

    // ── rpc by object (reverse registry lookup) ─────────────────────────────

    @Test
    void rpcByObjectDispatchesRegisteredName() {
        Resonate r = local();
        r.register(ResonateTest::add, "sum", 1);
        r.rpc("rpc-obj", ResonateTest::add, 1, 2);
        PromiseRecord record = waitForPromise(r, "rpc-obj");
        Map<?, ?> data = (Map<?, ?>) record.param().data();
        assertEquals("sum", data.get("func"));
        assertEquals(1, data.get("version"));
        assertEquals(List.of(1, 2), data.get("args"));
    }

    @Test
    void rpcByObjectVersionFromIdentity() {
        Resonate r = local();
        r.register(ResonateTest::versioned, "impl", 4);
        // The object carries its own version, so options(version=9) is ignored.
        r.options(new Opts().withVersion(9)).rpc("rpc-ver", ResonateTest::versioned);
        PromiseRecord record = waitForPromise(r, "rpc-ver");
        Map<?, ?> data = (Map<?, ?>) record.param().data();
        assertEquals("impl", data.get("func"));
        assertEquals(4, data.get("version"));
    }

    @Test
    void rpcByObjectUnregisteredRaises() {
        Resonate r = local();
        // An unregistered object cannot be dispatched (registry name != method name); refuse, not guess.
        FunctionNotFoundError exc =
                assertThrows(FunctionNotFoundError.class, () -> r.rpc("rpc-stranger", ResonateTest::add));
        assertTrue(exc.getMessage().contains("add"));
    }

    @Test
    void rpcByObjectHandleIsTypedByNameIsAny() {
        Resonate r = local();
        r.register(ResonateTest::makePoint);
        // White-box: a by-object dispatch carries the registered return type; by-name has no local
        // function to read one from, so it is Object (Any).
        var typed = r.rpc("rpc-typed", ResonateTest::makePoint, 1, 2);
        ResonateHandle<Object> untyped = r.rpc("rpc-untyped", "makePoint", 1, 2);
        waitForPromise(r, "rpc-typed");
        waitForPromise(r, "rpc-untyped");
        assertEquals(Point.class, typed.type());
        assertEquals(Object.class, untyped.type());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  withDependency (DI)
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void withDependencyReturnsSelfForChaining() {
        Resonate r = local();
        assertSame(r, r.withDependency(new Config("x")));
    }

    @Test
    void workflowReadsDependencyViaContext() {
        Resonate r = local();
        r.withDependency(new Config("hello-from-di"));
        r.register(ResonateTest::readConfig);
        assertEquals("hello-from-di", r.run("di-ctx", ResonateTest::readConfig).result());
    }

    @Test
    void multipleDependencies() {
        Resonate r = local();
        r.withDependency(new Config("multi")).withDependency(new Counter(42));
        r.register(ResonateTest::readTwoDeps);
        assertEquals("multi:42", r.run("di-multi", ResonateTest::readTwoDeps).result());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  options
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void optionsMintsNewHandleSharingState() {
        Resonate r = local();
        Resonate scoped = r.options(new Opts().withTimeout(Duration.ofSeconds(1)));
        assertNotSame(r, scoped);
        // The clone shares everything by reference: the rebound-state container and the wiring alike.
        assertSame(r.runtime, scoped.runtime);
        assertSame(r.promises, scoped.promises);
        assertSame(r.schedules, scoped.schedules);
        assertEquals(new Opts(Duration.ofSeconds(1), null, 1, null), scoped.opts());
        assertEquals(new Opts(), r.opts());
    }

    @Test
    void optionsHandlesAreHoldableAndReusable() {
        Resonate r = local();
        Resonate a = r.options(new Opts().withTarget("worker-a"));
        Resonate b = r.options(new Opts().withTarget("worker-b"));
        a.rpc("held-a", "remote");
        b.rpc("held-b", "remote");
        a.rpc("held-a2", "remote"); // a still carries worker-a after b was created and used
        for (Map.Entry<String, String> e : Map.of("held-a", "worker-a", "held-b", "worker-b", "held-a2", "worker-a")
                .entrySet()) {
            PromiseRecord record = waitForPromise(r, e.getKey());
            assertEquals("local://any@" + e.getValue(), record.tags().get("resonate:target"));
        }
    }

    @Test
    void optionsBareNameTargetRewritten() {
        Resonate r = local();
        r.options(new Opts().withTarget("my-worker")).rpc("t-bare", "remote");
        assertEquals("local://any@my-worker", waitForPromise(r, "t-bare").tags().get("resonate:target"));
    }

    @Test
    void optionsUrlTargetPassesThrough() {
        Resonate r = local();
        String url = "https://remote:9000/workers/hello";
        r.options(new Opts().withTarget(url)).rpc("t-url", "remote");
        assertEquals(url, waitForPromise(r, "t-url").tags().get("resonate:target"));
    }

    @Test
    void runVersionComesFromRegistration() {
        Resonate r = local();
        r.register(ResonateTest::noop, "noop", 99);
        r.run("t-tags", ResonateTest::noop).result();
        PromiseRecord record = await(r.promises.get("t-tags"));
        assertEquals(99, ((Map<?, ?>) record.param().data()).get("version"));
        assertEquals("global", record.tags().get("resonate:scope"));
    }

    @Test
    void rpcVersionComesFromOpts() {
        Resonate r = local();
        r.options(new Opts().withVersion(7)).rpc("t-rpc-ver", "remote");
        PromiseRecord record = waitForPromise(r, "t-rpc-ver");
        assertEquals(7, ((Map<?, ?>) record.param().data()).get("version"));
    }

    @Test
    void optionsAppliesToRunTarget() {
        Resonate r = local();
        r.register(ResonateTest::noop);
        r.options(new Opts().withTarget("my-target"))
                .run("rt2", ResonateTest::noop)
                .result();
        PromiseRecord record = await(r.promises.get("rt2"));
        assertEquals("local://any@my-target", record.tags().get("resonate:target"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  get
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void getNonexistentRaises404() {
        Resonate r = local();
        ServerError exc = assertThrows(ServerError.class, () -> await(r.get("nonexistent")));
        assertEquals(404, exc.code());
    }

    @Test
    void getExistingReturnsHandle() {
        Resonate r = local();
        r.register(ResonateTest::add);
        r.run("g1", ResonateTest::add, 1, 2).result();
        ResonateHandle<Object> handle = await(r.get("g1"));
        assertEquals("g1", await(handle.id()));
        assertEquals(3, handle.result());
    }

    @Test
    void getWithPrefixPrepends() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).prefix("ns"));
        r.rpc("p1", "remote");
        waitForPromise(r, "ns:p1");
        ResonateHandle<Object> handle = await(r.get("p1"));
        assertEquals("ns:p1", await(handle.id()));
    }

    @Test
    void getPendingPromiseReturnsUnsettledHandle() {
        Resonate r = local();
        r.rpc("g-pending", "remote");
        waitForPromise(r, "g-pending");
        ResonateHandle<Object> handle = await(r.get("g-pending"));
        assertFalse(handle.done());
    }

    @Test
    void getRejectedPromiseRaisesOnResult() {
        Resonate r = local();
        r.register(ResonateTest::boom);
        try {
            r.run("g-boom", ResonateTest::boom).result();
        } catch (ApplicationError ignored) {
            // expected
        }
        ResonateHandle<Object> handle = await(r.get("g-boom"));
        ApplicationError exc = assertThrows(ApplicationError.class, () -> handle.result());
        assertTrue(exc.getMessage().contains("deliberate failure"));
    }

    @Test
    void getDecodesResultAsAny() {
        Resonate r = local();
        r.register(ResonateTest::makePoint);
        r.run("g-pt", ResonateTest::makePoint, 1, 2).result();
        ResonateHandle<Object> handle = await(r.get("g-pt"));
        assertEquals(Map.of("x", 1, "y", 2), handle.result());
    }

    @Test
    void getTwiceSharesSubscription() {
        Resonate r = local();
        r.register(ResonateTest::add);
        r.run("g-share", ResonateTest::add, 1, 2).result();
        ResonateHandle<Object> h1 = await(r.get("g-share"));
        ResonateHandle<Object> h2 = await(r.get("g-share"));
        assertEquals(3, h1.result());
        assertEquals(3, h2.result());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Multiple handles to the same id
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void multipleHandlesSameIdAllResolve() {
        Resonate r = local();
        r.register(ResonateTest::add);
        var h1 = r.run("multi", ResonateTest::add, 2, 3);
        ResonateHandle<Object> h2 = await(r.get("multi"));
        assertEquals(5, h1.result());
        assertEquals(5, h2.result());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  id prefix consistency
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void prefixAppliedConsistentlyToRunRpcGet() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).prefix("p"));
        r.register(ResonateTest::add);
        var h1 = r.run("id1", ResonateTest::add, 1, 1);
        assertEquals("p:id1", await(h1.id()));
        ResonateHandle<Object> h2 = r.rpc("id2", "remote");
        assertEquals("p:id2", await(h2.id()));
        waitForPromise(r, "p:id2");
        ResonateHandle<Object> h3 = await(r.get("id2"));
        assertEquals("p:id2", await(h3.id()));
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  schedule
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void scheduleCreatesAndDeletes() {
        Resonate r = local();
        ResonateSchedule schedule =
                await(r.schedule("my-schedule", "*/5 * * * *", "my-func", List.of(), Map.of(), null, 1));
        assertEquals("my-schedule", schedule.name());
        await(schedule.delete()); // does not raise
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  stop
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void stopIsCleanAndIdempotent() {
        Resonate r = new Resonate();
        r.stop().join();
        r.stop().join(); // second stop is a no-op
    }

    @Test
    void stopCancelsRefreshTask() {
        Resonate r = new Resonate();
        Future<?> handle = r.runtime.refreshHandle;
        assertTrue(handle != null && !handle.isDone());
        r.stop().join();
        assertEquals(null, r.runtime.refreshHandle);
        assertTrue(handle.isCancelled());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Bounded execution concurrency
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void boundedExecuteCapsConcurrentExecutions() {
        Resonate r = track(Resonate.builder().retryPolicy(new Never()).maxConcurrentTasks(2));
        AtomicInteger live = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);

        Runnable work = () -> {
            int now = live.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            live.decrementAndGet();
        };

        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(CompletableFuture.runAsync(() -> r.boundedExecute(work)));
        }
        // Let everything that can start, start; only the ceiling should be live.
        sleep(100);
        assertTrue(peak.get() <= 2, "expected at most 2 concurrent executions, saw " + peak.get());
        assertTrue(live.get() <= 2);

        gate.countDown();
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        assertEquals(0, live.get());
        assertTrue(peak.get() <= 2);
    }

    @Test
    void defaultConcurrencyCeilingApplied() {
        assertEquals(
                Resonate.DEFAULT_MAX_CONCURRENT_TASKS,
                local().runtime.executeSema.availablePermits());
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Durable.returnType resolution (mirrors the return_type tests)
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    void returnTypeBuiltinScalar() {
        assertEquals(int.class, new Durable(m("add", Context.class, int.class, int.class)).returnType());
    }

    @Test
    void returnTypeBuiltinContainer() {
        java.lang.reflect.Type t = new Durable(m("makeList", Context.class)).returnType();
        ParameterizedType pt = assertInstanceOf(ParameterizedType.class, t);
        assertEquals(List.class, pt.getRawType());
        assertEquals(Integer.class, pt.getActualTypeArguments()[0]);
    }

    @Test
    void returnTypeRecordStruct() {
        assertEquals(Point.class, new Durable(m("makePoint", Context.class, int.class, int.class)).returnType());
    }

    @Test
    void returnTypeNoAnnotationIsAny() {
        // bareAdd declares an Object return -> pass-through (the Any analogue).
        assertEquals(Object.class, new Durable(m("bareAdd", Context.class, Object.class, Object.class)).returnType());
    }

    @Test
    void returnTypeVoidIsAny() {
        // A void return is pass-through, collapsing to Object (mirrors Python's ``-> None`` -> Any).
        assertEquals(Object.class, new Durable(m("versioned", Context.class)).returnType());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e);
        }
    }
}
