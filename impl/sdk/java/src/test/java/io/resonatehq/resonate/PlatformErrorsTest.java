package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Codec.Encryptor;
import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.HttpError;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Retry.Constant;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.TaskAcquireResult;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_platform_errors.py}: behaviour tests for platform-error
 * handling inside durable executions.
 *
 * <p>Once the root durable promise exists, a server failure on a durable op ({@code ctx.run} /
 * {@code ctx.rpc} / {@code ctx.sleep} / ...) must surface as a {@link PlatformError} — a {@link
 * java.lang.Error} that user {@code catch (Exception)} cannot swallow — and the task must be
 * <b>released</b> (so another worker can resume it), never fulfilled. {@link
 * Core#executeUntilBlockedOuter} is the boundary: after releasing it unwraps the {@code
 * PlatformError} and rethrows its <em>original</em> {@link ResonateError} cause. Before the root
 * promise exists (top-level {@code run} / {@code rpc}), failures stay plain {@link ResonateError}.
 *
 * <p>Like {@code CoreTest}, these run against the in-process {@link LocalNetwork} through the real
 * {@link Sender} / {@link Transport}. Platform failures are injected by {@link FailingNetwork}, a
 * thin {@link Network} decorator that fails specific request {@code kind}s on demand — the Java
 * analogue of Python's {@code FailingSender} (the SDK's {@link Sender} is {@code final}, so the seam
 * is one layer lower, at the {@link Network}).
 *
 * <p><b>Divergences from the Python suite, all forced by the language model:</b>
 *
 * <ul>
 *   <li><b>No {@code __cause__} assertion.</b> Python does {@code raise original from PlatformError},
 *       so {@code excinfo.value.__cause__ is PlatformError}. Java's {@link Core} rethrows {@code
 *       platform.cause()} directly (no {@code initCause}), so the surfaced error has no PlatformError
 *       chained. The tests assert the surfaced <em>type</em> (and identity, where an instance is
 *       injected) plus the release contract instead.
 *   <li><b>Coercion failure → encode failure.</b> Java cannot "lie" about a return type the way
 *       Python's {@code cast} does, so {@code returnCoercionFailureReleasesTask} returns an
 *       unserializable value: the settle-side encode failure is the symmetric platform failure the
 *       Python test references ("symmetric with the encode side").
 *   <li><b>Decode-failure injection.</b> Python patches {@code codec.decode_promise}; {@link Codec}
 *       is {@code final}, so {@link ToggleEncryptor} corrupts {@code decrypt} after setup to make the
 *       same {@code decodePromise} throw.
 *   <li><b>{@code spawn_and_stop_tolerate_base_exception} omitted.</b> It pins asyncio
 *       background-task plumbing ({@code task.exception()} + {@code gather(return_exceptions=True)} +
 *       caplog) that has no analogue in Java's thread/{@code CompletableFuture} model — the same kind
 *       of faithful omission CoreTest/ResonateTest already make for Python-only mechanics.
 * </ul>
 */
class PlatformErrorsTest {

    private static final long FAR_FUTURE = 1L << 50;
    private static final int TTL = 10_000;

    // ── FailingNetwork: arm specific request kinds to fail ───────────────────

    /**
     * A {@link Network} decorator that can be armed to fail specific durable-op request kinds. The
     * armed kinds complete the send with {@link #error} (the analogue of Python's {@code FailingSender}
     * raising); everything else passes through to the wrapped {@link LocalNetwork}.
     */
    static final class FailingNetwork implements Network {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private final Network inner;

        ResonateError error = new ServerError(503, "server unavailable");
        volatile boolean failPromiseCreate = false;
        volatile boolean failPromiseSettle = false;
        volatile boolean failTaskFulfill = false;
        volatile boolean failTaskSuspend = false;
        volatile boolean failTaskRelease = false;
        final AtomicInteger releaseAttempts = new AtomicInteger();
        final AtomicInteger createAttempts = new AtomicInteger();

        FailingNetwork(Network inner) {
            this.inner = inner;
        }

        private static String kindOf(String req) {
            try {
                JsonNode node = MAPPER.readTree(req);
                JsonNode kind = node.get("kind");
                return kind != null && kind.isTextual() ? kind.asText() : "";
            } catch (Exception e) {
                return "";
            }
        }

        @Override
        public CompletableFuture<String> send(String req) {
            switch (kindOf(req)) {
                case "promise.create" -> {
                    createAttempts.incrementAndGet();
                    if (failPromiseCreate) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                case "promise.settle" -> {
                    if (failPromiseSettle) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                case "task.fulfill" -> {
                    if (failTaskFulfill) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                case "task.suspend" -> {
                    if (failTaskSuspend) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                case "task.release" -> {
                    releaseAttempts.incrementAndGet();
                    if (failTaskRelease) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                default -> {
                    /* pass through */
                }
            }
            return inner.send(req);
        }

        @Override
        public String pid() {
            return inner.pid();
        }

        @Override
        public String group() {
            return inner.group();
        }

        @Override
        public String unicast() {
            return inner.unicast();
        }

        @Override
        public String anycast() {
            return inner.anycast();
        }

        @Override
        public CompletableFuture<Void> start() {
            return inner.start();
        }

        @Override
        public CompletableFuture<Void> stop() {
            return inner.stop();
        }

        @Override
        public void recv(Consumer<String> callback) {
            inner.recv(callback);
        }

        @Override
        public String targetResolver(String target) {
            return inner.targetResolver(target);
        }
    }

    /**
     * An {@link Encryptor} whose {@code decrypt} can be flipped to corrupt the JSON bytes, so a later
     * {@code codec.decodePromise} throws a {@link SerializationError}. Used to inject a root-promise
     * decode failure without patching the {@code final} {@link Codec}.
     */
    static final class ToggleEncryptor implements Encryptor {
        volatile boolean corrupt = false;

        @Override
        public byte[] encrypt(byte[] data) {
            return data;
        }

        @Override
        public byte[] decrypt(byte[] data) {
            // A single non-JSON byte makes ObjectMapper.readValue fail -> SerializationError.
            return corrupt ? new byte[] {0x00} : data;
        }
    }

    // ── Fixture: LocalNetwork + FailingNetwork + Sender + Codec + Core ───────

    record RootTask(int version, PromiseRecord promise, List<PromiseRecord> preload) {}

    static final class PlatformFixture {
        final String pid = "platform-test-pid";
        final FailingNetwork net = new FailingNetwork(new LocalNetwork(pid, null));
        final Sender sender = new Sender(new Transport(net), null);
        final Codec codec;
        final Registry reg = new Registry();
        final Core core;

        PlatformFixture() {
            this(new NoopEncryptor());
        }

        PlatformFixture(Encryptor encryptor) {
            this.codec = new Codec(encryptor);
            this.core = new Core(sender, codec, reg, Core.IDENTITY_TARGET_RESOLVER, null, pid, TTL, null, null);
        }

        RootTask createRootTask(String id, String funcName, Object... args) {
            Value param = codec.encode(new TaskData(List.of(args), Map.of(), funcName, 1));
            TaskAcquireResult res = await(sender.taskCreate(
                    pid,
                    TTL,
                    new PromiseCreateReq(
                            id, FAR_FUTURE, param, Map.of("resonate:branch", id, "resonate:target", "any"))));
            PromiseRecord decoded = codec.decodePromise(res.promise());
            return new RootTask(res.task().version(), decoded, res.preload());
        }

        PromiseRecord promiseGetRaw(String id) {
            return await(sender.promiseGet(id));
        }

        /** The platform-error contract: root still pending, task re-acquirable by another worker. */
        void assertReleasedRootPending(String rootId) {
            assertEquals("pending", promiseGetRaw(rootId).state());
            TaskAcquireResult result = await(sender.taskAcquire(rootId, 0, "other-pid", 1000));
            assertEquals("acquired", result.task().state());
        }
    }

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

    // ── Workflow library ─────────────────────────────────────────────────────

    static int leaf(Context ctx) {
        return 1;
    }

    static Object stranger(Context ctx) {
        return 1;
    }

    static Object wfAwaitRpcChild(Context ctx) {
        return ctx.rpc("child").await();
    }

    static int wfRunLeaf(Context ctx) {
        return ctx.run(PlatformErrorsTest::leaf).await();
    }

    static Object wfReturnUnserializable(Context ctx) {
        return ctx.run(PlatformErrorsTest::returnsUnserializable).await();
    }

    static Object returnsUnserializable(Context ctx) {
        return new Object(); // codec.encode fails (FAIL_ON_EMPTY_BEANS) -> settle-side platform failure
    }

    static String wfSwallowWithExcept(Context ctx) {
        try {
            ctx.rpc("child").await();
        } catch (Exception e) {
            return "swallowed"; // PlatformError is an Error, so this catch must NOT fire
        }
        return "unreachable";
    }

    static int wfFireAndForgetLeaf(Context ctx) {
        ctx.run(PlatformErrorsTest::leaf); // fire-and-forget; settle of child fails
        return 0;
    }

    static int wfTwoFireAndForget(Context ctx) {
        ctx.run(PlatformErrorsTest::leaf);
        ctx.run(PlatformErrorsTest::leaf);
        return 0;
    }

    /** A body that catches even the platform error itself; a follow-up durable op must re-raise. */
    static volatile boolean reachedAfterSwallow;

    static String wfSwallowThenContinue(Context ctx) {
        try {
            ctx.rpc("child").await();
        } catch (Throwable t) {
            // deliberate worst-case swallow
        }
        reachedAfterSwallow = true;
        ctx.rpc("child2").await();
        return "unreachable";
    }

    static Object wfRunGhost(Context ctx) {
        return ctx.run("ghost").await(); // not registered anywhere
    }

    static Object wfRpcStranger(Context ctx) {
        return ctx.rpc(PlatformErrorsTest::stranger).await(); // object never registered here
    }

    static int wfReturnZero(Context ctx) {
        return 0;
    }

    static final AtomicInteger FLAKY_ATTEMPTS = new AtomicInteger();

    static int flaky(Context ctx) {
        if (FLAKY_ATTEMPTS.incrementAndGet() < 3) {
            throw new RuntimeException("flaky");
        }
        return 42;
    }

    // ── 1. create_promise failure → released task, original error raised ─────

    private void rpcCreateFailureReleasesTask(ResonateError error) {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfAwaitRpcChild);
        RootTask rt = fix.createRootTask("pe-rpc", "wf");

        fix.net.error = error;
        fix.net.failPromiseCreate = true;

        // Outer unwraps the PlatformError: the caller sees the *original* ResonateError instance.
        ResonateError thrown = assertThrows(
                ResonateError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-rpc", rt.version(), rt.promise(), rt.preload()));
        assertSame(error, thrown);
        fix.assertReleasedRootPending("pe-rpc");
    }

    @Test
    void rpcCreateFailureReleasesTaskServerError() {
        rpcCreateFailureReleasesTask(new ServerError(503, "server unavailable"));
    }

    @Test
    void rpcCreateFailureReleasesTaskHttpError() {
        rpcCreateFailureReleasesTask(new HttpError(new java.net.ConnectException("refused")));
    }

    @Test
    void runCreateFailureReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfRunLeaf);
        RootTask rt = fix.createRootTask("pe-run", "wf");

        fix.net.failPromiseCreate = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-run", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-run");
    }

    @Test
    void returnCoercionFailureReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfReturnUnserializable);
        RootTask rt = fix.createRootTask("pe-coerce", "wf");

        // No sender failure armed: the settle-side encode fails to serialize the value, which must
        // release (platform failure) rather than store a rejection.
        assertThrows(
                SerializationError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-coerce", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-coerce");
    }

    // ── 2. user code cannot swallow a platform error ─────────────────────────

    @Test
    void exceptCatchDoesNotSwallowPlatformError() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfSwallowWithExcept);
        RootTask rt = fix.createRootTask("pe-swallow", "wf");

        fix.net.failPromiseCreate = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-swallow", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-swallow");
    }

    // ── 3. settle failure on a fire-and-forget child surfaces via flush ──────

    @Test
    void fireAndForgetSettleFailureReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfFireAndForgetLeaf);
        RootTask rt = fix.createRootTask("pe-ff", "wf");

        fix.net.failPromiseSettle = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-ff", rt.version(), rt.promise(), rt.preload()));
        // The child's settle never landed.
        assertEquals("pending", fix.promiseGetRaw("pe-ff.1").state());
        fix.assertReleasedRootPending("pe-ff");
    }

    @Test
    void firstPlatformErrorWinsWithMultipleFailures() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfTwoFireAndForget);
        RootTask rt = fix.createRootTask("pe-multi", "wf");

        fix.net.failPromiseSettle = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-multi", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-multi");
    }

    // ── 3b. abort gate: no further durable work after the first failure ──────

    @Test
    void firstPlatformErrorStopsFurtherDurableWork() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfTwoFireAndForget);
        RootTask rt = fix.createRootTask("pe-stop", "wf");

        // Fail every create; the breaker must short-circuit the second child *before* it reaches the
        // server, so exactly one create attempt is observed.
        fix.net.failPromiseCreate = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-stop", rt.version(), rt.promise(), rt.preload()));
        assertEquals(1, fix.net.createAttempts.get(), "the second child's create must be short-circuited");
        fix.assertReleasedRootPending("pe-stop");
    }

    @Test
    void baseExceptionSwallowThenContinueStillReleases() {
        PlatformFixture fix = new PlatformFixture();
        reachedAfterSwallow = false;
        fix.reg.register("wf", PlatformErrorsTest::wfSwallowThenContinue);
        RootTask rt = fix.createRootTask("pe-swallow-base", "wf");

        fix.net.failPromiseCreate = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-swallow-base", rt.version(), rt.promise(), rt.preload()));
        assertTrue(reachedAfterSwallow, "body did swallow the first error and continue");
        fix.assertReleasedRootPending("pe-swallow-base");
    }

    // ── 3c. in-execution registry miss releases (not a permanent rejection) ──

    @Test
    void runByNameUnregisteredChildReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfRunGhost);
        RootTask rt = fix.createRootTask("pe-nofn-run", "wf");

        assertThrows(
                FunctionNotFoundError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-nofn-run", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-nofn-run");
    }

    @Test
    void rpcByObjectUnregisteredChildReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfRpcStranger);
        RootTask rt = fix.createRootTask("pe-nofn-rpc", "wf");

        assertThrows(
                FunctionNotFoundError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-nofn-rpc", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-nofn-rpc");
    }

    // ── 3d. task-lifecycle failures (fulfill / suspend / release) ────────────

    @Test
    void taskFulfillFailureReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfReturnZero);
        RootTask rt = fix.createRootTask("pe-fulfill", "wf");

        fix.net.failTaskFulfill = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-fulfill", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-fulfill");
    }

    @Test
    void taskSuspendFailureReleasesTask() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfAwaitRpcChild); // child stays pending -> suspend
        RootTask rt = fix.createRootTask("pe-suspend", "wf");

        fix.net.failTaskSuspend = true;

        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-suspend", rt.version(), rt.promise(), rt.preload()));
        fix.assertReleasedRootPending("pe-suspend");
    }

    @Test
    void taskReleaseFailureDoesNotMaskOriginalError() {
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register("wf", PlatformErrorsTest::wfAwaitRpcChild);
        RootTask rt = fix.createRootTask("pe-relfail", "wf");

        fix.net.failPromiseCreate = true;
        fix.net.failTaskRelease = true;

        // The original create failure surfaces; the release failure is swallowed (logged), not raised.
        assertThrows(
                ServerError.class,
                () -> fix.core.executeUntilBlockedOuter("pe-relfail", rt.version(), rt.promise(), rt.preload()));
        assertEquals(1, fix.net.releaseAttempts.get());
    }

    @Test
    void onMessageRootDecodeFailureReleasesTask() {
        ToggleEncryptor enc = new ToggleEncryptor();
        PlatformFixture fix = new PlatformFixture(enc);
        fix.reg.register("wf", PlatformErrorsTest::wfReturnZero);
        RootTask rt = fix.createRootTask("pe-decode", "wf");
        // Release so on_message can re-acquire under its own lease, then reset the release counter.
        await(fix.sender.taskRelease("pe-decode", rt.version()));
        fix.net.releaseAttempts.set(0);

        // Arm the corrupt decrypt: on_message's decodePromise now throws before the release boundary.
        enc.corrupt = true;

        assertThrows(SerializationError.class, () -> fix.core.onMessage("pe-decode", rt.version()));
        assertEquals(1, fix.net.releaseAttempts.get());

        // The lease was released: a fresh acquire under a different pid succeeds (decrypt still corrupt,
        // but acquire/get do not decode through the codec).
        enc.corrupt = false;
        fix.assertReleasedRootPending("pe-decode");
    }

    // ── 4. creation-chain integrity: no deadlock past a failed link ──────────

    @Test
    void chainFailureRejectsCreatedSoSuccessorsDoNotDeadlock() {
        FailingNetwork net = new FailingNetwork(new LocalNetwork("chain-pid", null));
        net.failPromiseCreate = true;
        Sender sender = new Sender(new Transport(net), null);
        Effects effects = new Effects(sender, new Codec(new NoopEncryptor()), List.of());
        Context ctx = Context.root(
                "r", "r", "r", FAR_FUTURE, "f", effects, Core.IDENTITY_TARGET_RESOLVER, new Dependencies(), null, null);

        ResonateFuture<Object> fut1 = ctx.rpc("a");
        ResonateFuture<Object> fut2 = ctx.rpc("b");

        // Link 1's failure settles its own `created` AND propagates down the chain, so link 2's id()
        // resolves (with the error) instead of hanging.
        assertThrows(PlatformError.class, fut1::await);
        assertThrows(PlatformError.class, fut2::id);

        // The flush surfaces the platform error and aggregates both link failures into one error.
        PlatformError pe = assertThrows(PlatformError.class, () -> await(ctx.flushLocalWork()));
        assertEquals(2, pe.causes().size());
        assertTrue(pe.causes().stream().allMatch(c -> c instanceof ResonateError));
        assertSame(pe.causes().get(0), pe.cause());
    }

    // ── 5. PlatformError invariants ──────────────────────────────────────────

    @Test
    void platformErrorCauseIsFirstOfMany() {
        ServerError first = new ServerError(503, "first");
        HttpError second = new HttpError(new java.net.ConnectException("second"));
        PlatformError err = new PlatformError(List.of(first, second));
        assertSame(first, err.cause());
        assertEquals(List.of(first, second), err.causes());
    }

    @Test
    void platformErrorRejectsEmptyCauses() {
        // Java raises IllegalArgumentException (the analogue of Python's ValueError).
        IllegalArgumentException exc = assertThrows(IllegalArgumentException.class, () -> new PlatformError(List.of()));
        assertTrue(exc.getMessage().contains("at least one cause"));
    }

    // ── 6. pre-durable-world failures stay regular ResonateErrors ────────────

    @Test
    void topLevelCreateFailureStaysPlainResonateError() {
        FailingNetwork net = new FailingNetwork(new LocalNetwork());
        Resonate res = Resonate.builder().network(net).build();
        try {
            net.failPromiseCreate = true;
            var handle = res.rpc("pe-top", "somefn");
            // Stays a plain ResonateError. (In Java a PlatformError is a java.lang.Error, not a
            // ResonateError subtype, so "not a PlatformError" is structural — asserting the concrete
            // ServerError type is the meaningful check.)
            ResonateError thrown = assertThrows(ResonateError.class, () -> await(handle.id()));
            assertInstanceOf(ServerError.class, thrown);
        } finally {
            res.stop().join();
        }
    }

    @Test
    void topLevelUnserializableParamStaysPlainResonateError() {
        Resonate res = Resonate.builder().build();
        try {
            res.register((io.resonatehq.resonate.Fn.F1<Object, Integer>) PlatformErrorsTest::myfn, "myfn", 1);
            var handle = res.run(
                    "pe-ser", (io.resonatehq.resonate.Fn.F1<Object, Integer>) PlatformErrorsTest::myfn, new Object());
            assertThrows(SerializationError.class, () -> await(handle.id()));
        } finally {
            res.stop().join();
        }
    }

    static int myfn(Context ctx, Object x) {
        return 1;
    }

    // ── 7. retry interaction ─────────────────────────────────────────────────

    @Test
    void platformErrorNeverFedToRetryPolicy() {
        // A counting policy that never retries; a platform failure must not consult it.
        CountingPolicy policy = new CountingPolicy();
        PlatformFixture fix = new PlatformFixture();
        Core core = new Core(
                fix.sender, fix.codec, fix.reg, Core.IDENTITY_TARGET_RESOLVER, null, fix.pid, TTL, null, policy);
        fix.reg.register("wf", PlatformErrorsTest::wfAwaitRpcChild);
        RootTask rt = fix.createRootTask("pe-retry", "wf");

        fix.net.failPromiseCreate = true;

        assertThrows(
                ServerError.class,
                () -> core.executeUntilBlockedOuter("pe-retry", rt.version(), rt.promise(), rt.preload()));
        assertEquals(0, policy.calls);
    }

    /** A RetryPolicy that records every consultation and never retries. */
    static final class CountingPolicy implements Retry.RetryPolicy {
        int calls = 0;

        @Override
        public Long next(int attempt) {
            calls++;
            return null;
        }
    }

    @Test
    void pureLeafUserFailureStillRetries() {
        FLAKY_ATTEMPTS.set(0);
        PlatformFixture fix = new PlatformFixture();
        fix.reg.register(
                "flaky",
                io.resonatehq.resonate.Fn.methodOf((io.resonatehq.resonate.Fn.F0<Integer>) PlatformErrorsTest::flaky),
                1,
                new Constant(5, 0));
        RootTask rt = fix.createRootTask("pe-leaf", "flaky");

        String status = fix.core.executeUntilBlockedOuter("pe-leaf", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);
        assertEquals(3, FLAKY_ATTEMPTS.get());
        assertEquals("resolved", fix.promiseGetRaw("pe-leaf").state());
    }
}
