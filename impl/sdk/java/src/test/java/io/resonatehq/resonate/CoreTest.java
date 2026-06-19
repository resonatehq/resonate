package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.Suspended;
import io.resonatehq.resonate.Heartbeat.Hb;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.TaskAcquireResult;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_core.py}: behaviour tests for {@link Core} run against a
 * real server simulation -- the in-process {@link LocalNetwork} driven through the real {@link Sender}
 * / {@link Transport} -- so the contract is "real server, real wire".
 *
 * <p><b>Async-model note.</b> Python's tests are {@code async}; {@code Core} here is blocking (see its
 * class doc), so {@code core.executeUntilBlockedOuter} / {@code core.onMessage} are called directly
 * and the harness's own {@link Sender} calls are run through {@link #await(CompletableFuture)}, which
 * unwraps the {@link CompletionException} a failed durable op surfaces.
 *
 * <p><b>kwargs divergence.</b> Python's {@code wf_add} is dispatched with keyword args ({@code a=3,
 * b=4}); Java has no keyword binding (see {@code Durable}), so the analogue passes the same values
 * positionally. The asserted outcome (7) is unchanged.
 *
 * <p>Two groups are intentionally omitted, exactly as in the Python suite: the <b>short-circuit</b>
 * branch (settling a root promise on LocalNetwork auto-fulfills its task, so "acquired task +
 * already-settled root promise" can't be constructed) and the <b>redirect loop</b> (needs an awaited
 * promise settled at the instant {@code task.suspend} lands -- a race LocalNetwork can't produce
 * deterministically). The redirect response itself is covered by {@code NetworkTest}.
 */
class CoreTest {

    // Far-future deadline.
    private static final long FAR_FUTURE = 1L << 50;
    private static final int TTL = 10_000;

    // ── Test harness ────────────────────────────────────────────────────────

    /** Counts {@code start}/{@code stop} calls for Core's heartbeat hook. */
    static final class TrackingHeartbeat implements Hb {
        int started = 0;
        int stopped = 0;

        @Override
        public void start(String taskId, int taskVersion) {
            started++;
        }

        @Override
        public void stop(String taskId) {
            stopped++;
        }

        @Override
        public void shutdown() {}
    }

    /** What {@code create_root_task} returns: the acquired version, decoded root promise, and preload. */
    record RootTask(int version, PromiseRecord promise, List<PromiseRecord> preload) {}

    /** Wires a {@link LocalNetwork} + {@link Sender} + {@link Codec} + {@link Registry} + {@link Core}. */
    static final class CoreFixture {
        final String pid = "core-test-pid";
        final LocalNetwork net = new LocalNetwork(pid, null);
        final Sender sender = new Sender(new Transport(net), null);
        final Codec codec = new Codec(new NoopEncryptor());
        final Registry reg = new Registry();
        final TrackingHeartbeat hb = new TrackingHeartbeat();
        final Core core = new Core(sender, codec, reg, Core.IDENTITY_TARGET_RESOLVER, hb, pid, TTL, null, null);

        /**
         * Create a root durable promise + task atomically, acquired by us. {@code funcName} and {@code
         * args} go into the promise param as {@link TaskData} (positional args; Java has no kwargs).
         */
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

        /** Fetch a promise and decode it through the codec (use for root promises). */
        PromiseRecord promiseGet(String id) {
            return codec.decodePromise(await(sender.promiseGet(id)));
        }

        /** Fetch a promise without codec decoding (state/tags only). */
        PromiseRecord promiseGetRaw(String id) {
            return await(sender.promiseGet(id));
        }
    }

    /** Run a future to completion, unwrapping {@link CompletionException} to its cause. */
    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    // ── Workflow library used across tests ──────────────────────────────────

    static int wfReturnSeven(Context ctx, int n) {
        return n;
    }

    static Map<String, Object> wfReturnObj(Context ctx, int n) {
        return Map.of("x", n);
    }

    static int wfFail(Context ctx) {
        throw new ApplicationError("deliberate failure");
    }

    static int wfAdd(Context ctx, int a, int b) {
        return a + b;
    }

    /** Await a remote child created in this run; {@code ctx.rpc} creates it pending, the await suspends. */
    static int wfSuspendOnPending(Context ctx) {
        ctx.rpc("childA").await();
        return 0;
    }

    /** Create two pending remote children, then await -- suspends on the first. */
    static int wfSuspendOnTwo(Context ctx, int n) {
        ResonateFuture<Object> fut1 = ctx.rpc("childA");
        ResonateFuture<Object> fut2 = ctx.rpc("childB");
        fut1.await();
        fut2.await();
        return n;
    }

    /** Read a remote child the test pre-resolved; {@code ctx.rpc} returns it resolved, the await resolves inline. */
    static int wfReadPreloaded(Context ctx) {
        return (int) ctx.rpc("preloaded").await();
    }

    static int wfPlainPanic(Context ctx) {
        throw new RuntimeException("something went wrong");
    }

    /** Mimic a user error that <em>mentions</em> suspension -- still classified as a plain panic. */
    static int wfUnwrapSuspend(Context ctx) {
        throw new RuntimeException("execution suspended (simulated .unwrap() on suspended future)");
    }

    static int wfDone(Context ctx) {
        return 7;
    }

    /** A workflow that swallows the suspension but still has pending todos. */
    static int wfSwallow(Context ctx) {
        try {
            ctx.rpc("childZ").await(); // raises Suspended, suppressed
        } catch (Suspended ignored) {
            // swallowed
        }
        return 0;
    }

    static int wfChildThatSuspends(Context ctx) {
        return (int) ctx.rpc("childW").await();
    }

    /** A fire-and-forget local child that suspends propagates as parent suspension. */
    static int wfFireForgetParent(Context ctx) {
        ctx.run(CoreTest::wfChildThatSuspends);
        return 0;
    }

    // ── Error handling: a ctx.run child's rejection is recovered with its original type ──────
    //
    // The local-dispatch analogue of tests/test_error_handling.py: a step rejects its promise with
    // the encoded error (message + best-effort __java_serialized), and the parent's ctx.run await
    // recovers the original exception -- so the orchestrator can catch it by its concrete class.

    /** A custom domain exception: module-scope and serializable, so it survives the boundary. */
    @SuppressWarnings("serial")
    static class PaymentDeclinedError extends RuntimeException {
        PaymentDeclinedError(String message) {
            super(message);
        }
    }

    /** Subclass of a custom exception: its base relationship survives too. */
    @SuppressWarnings("serial")
    static class FraudSuspectedError extends PaymentDeclinedError {
        FraudSuspectedError(String message) {
            super(message);
        }
    }

    static String stepRaisesDomainError(Context ctx) {
        throw new PaymentDeclinedError("card declined for $42");
    }

    static String stepRaisesDomainSubclass(Context ctx) {
        throw new FraudSuspectedError("suspected fraud on card 4242");
    }

    static String stepRaisesTaggedApplicationError(Context ctx) {
        // The cross-SDK-safe discrimination idiom: a parseable prefix in the message text.
        throw new ApplicationError("E_NOT_FOUND: user 42 does not exist");
    }

    static String stepRaisesPlain(Context ctx) {
        throw new RuntimeException("bad input from step");
    }

    static int stepReturnsOk(Context ctx, int x) {
        return x * 2;
    }

    /** Catches the reconstructed custom class directly; returns "class|message". */
    static String orchestratorCatchesDomainClass(Context ctx) {
        try {
            ctx.run(CoreTest::stepRaisesDomainError).await();
        } catch (PaymentDeclinedError exc) {
            return exc.getClass().getSimpleName() + "|" + exc.getMessage();
        }
        throw new AssertionError("step unexpectedly succeeded");
    }

    /** ``except PaymentDeclined`` catches the reconstructed subclass too. */
    static String orchestratorCatchesDomainSubclass(Context ctx) {
        try {
            ctx.run(CoreTest::stepRaisesDomainSubclass).await();
        } catch (PaymentDeclinedError exc) {
            return exc.getClass().getSimpleName();
        }
        return "not caught as PaymentDeclined";
    }

    /** The tagged-message pattern: parse the code prefix out of an ApplicationError. */
    static String orchestratorParsesTaggedApplicationError(Context ctx) {
        try {
            ctx.run(CoreTest::stepRaisesTaggedApplicationError).await();
        } catch (ApplicationError exc) {
            return exc.getMessage().split(": ", 2)[0];
        }
        throw new AssertionError("step unexpectedly succeeded");
    }

    /** "By position" discrimination: one await per try block, so the block that raised names the step. */
    static String orchestratorPositionalDiscrimination(Context ctx) {
        ctx.run(CoreTest::stepReturnsOk, 1).await(); // step A: succeeds
        try {
            ctx.run(CoreTest::stepRaisesPlain).await(); // step B: fails
        } catch (RuntimeException exc) {
            return "step B failed";
        }
        return "no failure";
    }

    // ── Fulfill (success / failure / object value) ──────────────────────────

    @Test
    void fulfillResolvedViaExecuteUntilBlocked() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("add", CoreTest::wfAdd);
        RootTask rt = fix.createRootTask("p1-add", "add", 3, 4);

        String status = fix.core.executeUntilBlockedOuter("p1-add", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-add");
        assertEquals("resolved", got.state());
        assertEquals(7, got.value().data());
    }

    @Test
    void fulfillRejectedViaExecuteUntilBlocked() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("fail", CoreTest::wfFail);
        RootTask rt = fix.createRootTask("p1-fail", "fail");

        String status = fix.core.executeUntilBlockedOuter("p1-fail", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-fail");
        assertEquals("rejected", got.state());
    }

    @Test
    void fulfillObjectValueRoundTripsCodec() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("obj", CoreTest::wfReturnObj);
        RootTask rt = fix.createRootTask("p1-obj", "obj", 1);

        fix.core.executeUntilBlockedOuter("p1-obj", rt.version(), rt.promise(), rt.preload());

        PromiseRecord got = fix.promiseGet("p1-obj");
        assertEquals(Map.of("x", 1), got.value().data());
    }

    // ── Suspend ─────────────────────────────────────────────────────────────

    @Test
    void suspendsOnPendingRemote() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("waitOne", CoreTest::wfSuspendOnPending);
        RootTask rt = fix.createRootTask("p1-wait", "waitOne");

        String status = fix.core.executeUntilBlockedOuter("p1-wait", rt.version(), rt.promise(), rt.preload());
        assertEquals("suspended", status);

        PromiseRecord child = fix.promiseGetRaw("p1-wait.1");
        assertEquals("pending", child.state());
    }

    @Test
    void suspendsRegistersAllAwaiteds() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("waitTwo", CoreTest::wfSuspendOnTwo);
        RootTask rt = fix.createRootTask("p1-two", "waitTwo", 2);

        String status = fix.core.executeUntilBlockedOuter("p1-two", rt.version(), rt.promise(), rt.preload());
        assertEquals("suspended", status);
    }

    // ── onMessage (Path 1: acquires then executes) ─────────────────────────

    @Test
    void onMessageHappyPath() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("seven", CoreTest::wfReturnSeven);
        RootTask rt = fix.createRootTask("p1-on", "seven", 1);
        // Release so onMessage can re-acquire under its own lease.
        await(fix.sender.taskRelease("p1-on", rt.version()));

        String status = fix.core.onMessage("p1-on", rt.version());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-on");
        assertEquals("resolved", got.state());
    }

    @Test
    void onMessageAcquireFailureRaises() {
        CoreFixture fix = new CoreFixture();
        assertThrows(ResonateError.class, () -> fix.core.onMessage("nonexistent-task", 0));
    }

    @Test
    void onMessageReturnsSuspended() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("waitOne", CoreTest::wfSuspendOnPending);
        RootTask rt = fix.createRootTask("p1-onsus", "waitOne");
        await(fix.sender.taskRelease("p1-onsus", rt.version()));

        String status = fix.core.onMessage("p1-onsus", rt.version());
        assertEquals("suspended", status);
    }

    // ── executeUntilBlocked specifics ─────────────────────────────────────

    @Test
    void executeUntilBlockedWithPreload() {
        CoreFixture fix = new CoreFixture();
        RootTask rt = fix.createRootTask("p1-pre", "readPre");
        fix.reg.register("readPre", CoreTest::wfReadPreloaded);

        // Pre-resolve the child the workflow will read. ctx.rpc generates the child id "p1-pre.1".
        // Children are codec-encoded on the wire just like root promises, so pre-settle with
        // codec.encode to match.
        Value encVal = fix.codec.encode(99);
        await(fix.sender.promiseCreate(new PromiseCreateReq("p1-pre.1", FAR_FUTURE, new Value(), Map.of())));
        await(fix.sender.promiseSettle(new PromiseSettleReq("p1-pre.1", "resolved", encVal)));

        // Feed the preloaded child to Effects via the preload arg too, exercising the
        // seed-at-construction path.
        PromiseRecord pre = await(fix.sender.promiseGet("p1-pre.1"));
        List<PromiseRecord> preload = List.of(pre);

        String status = fix.core.executeUntilBlockedOuter("p1-pre", rt.version(), rt.promise(), preload);
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-pre");
        assertEquals(99, got.value().data());
    }

    // ── Error path: function not found releases the task ────────────────────

    @Test
    void releasesTaskOnFunctionNotFound() {
        CoreFixture fix = new CoreFixture();
        RootTask rt = fix.createRootTask("p1-nofn", "missing");

        assertThrows(
                FunctionNotFoundError.class,
                () -> fix.core.executeUntilBlockedOuter("p1-nofn", rt.version(), rt.promise(), rt.preload()));

        // Task should be releasable: a fresh acquire under a different pid succeeds.
        TaskAcquireResult result = await(fix.sender.taskAcquire("p1-nofn", 0, "other-pid", 1000));
        assertEquals("acquired", result.task().state());
    }

    // ── Heartbeat: started and stopped on every code path ──────────────────

    @Test
    void heartbeatStartedAndStoppedOnSuccess() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("seven2", CoreTest::wfReturnSeven);
        RootTask rt = fix.createRootTask("p1-hb-ok", "seven2");

        fix.core.executeUntilBlockedOuter("p1-hb-ok", rt.version(), rt.promise(), rt.preload());
        assertEquals(1, fix.hb.started);
        assertEquals(1, fix.hb.stopped);
    }

    @Test
    void heartbeatStoppedOnError() {
        CoreFixture fix = new CoreFixture();
        RootTask rt = fix.createRootTask("p1-hb-err", "missing");

        assertThrows(
                FunctionNotFoundError.class,
                () -> fix.core.executeUntilBlockedOuter("p1-hb-err", rt.version(), rt.promise(), rt.preload()));
        assertEquals(1, fix.hb.started);
        assertEquals(1, fix.hb.stopped); // stopped even after error
    }

    // ── Plain exceptions: caught, settle the promise ``rejected`` ──────────

    @Test
    void plainExceptionRejectsPromiseAndFulfillsTask() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("boom", CoreTest::wfPlainPanic);
        RootTask rt = fix.createRootTask("p1-boom", "boom");

        String status = fix.core.executeUntilBlockedOuter("p1-boom", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-boom");
        assertEquals("rejected", got.state());
        Object data = got.value().data();
        assertInstanceOf(Map.class, data);
        assertTrue(((Map<?, ?>) data).get("message").toString().contains("something went wrong"));

        // Task is settled along the fulfill path; another worker cannot acquire it.
        assertThrows(ResonateError.class, () -> await(fix.sender.taskAcquire("p1-boom", 0, "other-pid", 1000)));
    }

    @Test
    void exceptionMentioningSuspendStillRejectsPromise() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("unwrap", CoreTest::wfUnwrapSuspend);
        RootTask rt = fix.createRootTask("p1-unwrap", "unwrap");

        String status = fix.core.executeUntilBlockedOuter("p1-unwrap", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-unwrap");
        assertEquals("rejected", got.state());
    }

    @Test
    void heartbeatStoppedAfterUserException() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("boomHb", CoreTest::wfPlainPanic);
        RootTask rt = fix.createRootTask("p1-hb-boom", "boomHb");

        String status = fix.core.executeUntilBlockedOuter("p1-hb-boom", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);
        assertEquals(1, fix.hb.started);
        assertEquals(1, fix.hb.stopped); // stopped along the normal fulfill path
    }

    // ── NoopHeartbeat sanity ───────────────────────────────────────────────

    @Test
    void noopHeartbeatDoesNotInterfere() {
        String pid = "noop-pid";
        LocalNetwork net = new LocalNetwork(pid, null);
        Sender sender = new Sender(new Transport(net), null);
        Codec codec = new Codec(new NoopEncryptor());
        Registry reg = new Registry();
        reg.register("seven3", CoreTest::wfReturnSeven);
        // heartbeat=null -> Noop.
        Core core = new Core(sender, codec, reg, null, null, pid, TTL, null, null);

        Value param = codec.encode(new TaskData(List.of(), Map.of(), "seven3", 1));
        TaskAcquireResult res = await(sender.taskCreate(
                pid,
                TTL,
                new PromiseCreateReq(
                        "p1-noophb",
                        FAR_FUTURE,
                        param,
                        Map.of("resonate:branch", "p1-noophb", "resonate:target", "any"))));
        PromiseRecord decoded = codec.decodePromise(res.promise());
        String status = core.executeUntilBlockedOuter("p1-noophb", res.task().version(), decoded, res.preload());
        assertEquals("done", status);
    }

    // ── Run-workflow boundary tests ─────────────────────────────────────────

    @Test
    void doneOnReturn() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("done", CoreTest::wfDone);
        RootTask rt = fix.createRootTask("p1-done", "done");

        String status = fix.core.executeUntilBlockedOuter("p1-done", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-done");
        assertEquals("resolved", got.state());
    }

    @Test
    void swallowedSuspendStillSuspends() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("swallow", CoreTest::wfSwallow);
        RootTask rt = fix.createRootTask("p1-swallow", "swallow");

        String status = fix.core.executeUntilBlockedOuter("p1-swallow", rt.version(), rt.promise(), rt.preload());
        assertEquals("suspended", status);
    }

    // ── Error handling: ctx.run rejection recovery + discrimination ─────────

    @Test
    void ctxRunDomainExceptionPreservesType() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("orchDomain", CoreTest::orchestratorCatchesDomainClass);
        RootTask rt = fix.createRootTask("p1-domain", "orchDomain");

        String status = fix.core.executeUntilBlockedOuter("p1-domain", rt.version(), rt.promise(), rt.preload());
        assertEquals("done", status);

        PromiseRecord got = fix.promiseGet("p1-domain");
        assertEquals("resolved", got.state());
        // The custom PaymentDeclinedError raised by the step is reconstructed for the orchestrator.
        assertEquals("PaymentDeclinedError|card declined for $42", got.value().data());
    }

    @Test
    void ctxRunDomainSubclassCaughtAsBase() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("orchSubclass", CoreTest::orchestratorCatchesDomainSubclass);
        RootTask rt = fix.createRootTask("p1-subclass", "orchSubclass");

        fix.core.executeUntilBlockedOuter("p1-subclass", rt.version(), rt.promise(), rt.preload());

        PromiseRecord got = fix.promiseGet("p1-subclass");
        assertEquals("resolved", got.state());
        // FraudSuspected is a PaymentDeclined; the whole class survives, so ``except base`` catches it.
        assertEquals("FraudSuspectedError", got.value().data());
    }

    @Test
    void ctxRunTaggedApplicationErrorParsed() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("orchTagged", CoreTest::orchestratorParsesTaggedApplicationError);
        RootTask rt = fix.createRootTask("p1-tagged", "orchTagged");

        fix.core.executeUntilBlockedOuter("p1-tagged", rt.version(), rt.promise(), rt.preload());

        PromiseRecord got = fix.promiseGet("p1-tagged");
        assertEquals("resolved", got.state());
        assertEquals("E_NOT_FOUND", got.value().data());
    }

    @Test
    void ctxRunPositionalDiscriminationIdentifiesFailingStep() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("orchPos", CoreTest::orchestratorPositionalDiscrimination);
        RootTask rt = fix.createRootTask("p1-pos", "orchPos");

        fix.core.executeUntilBlockedOuter("p1-pos", rt.version(), rt.promise(), rt.preload());

        PromiseRecord got = fix.promiseGet("p1-pos");
        assertEquals("resolved", got.state());
        assertEquals("step B failed", got.value().data());
    }

    @Test
    void rejectedPromiseRecoversApplicationErrorType() {
        // Top-level boundary (handle.result analogue): an orchestrator that raises ApplicationError
        // settles the root rejected, and the stored value decodes back to the original ApplicationError.
        CoreFixture fix = new CoreFixture();
        fix.reg.register("failRecover", CoreTest::wfFail);
        RootTask rt = fix.createRootTask("p1-recover", "failRecover");

        fix.core.executeUntilBlockedOuter("p1-recover", rt.version(), rt.promise(), rt.preload());

        PromiseRecord got = fix.promiseGet("p1-recover");
        assertEquals("rejected", got.state());
        Throwable recovered = Codec.deserializeError(got.value().data());
        assertInstanceOf(ApplicationError.class, recovered);
        assertEquals("deliberate failure", recovered.getMessage());
    }

    @Test
    void fireAndForgetLocalSuspension() {
        CoreFixture fix = new CoreFixture();
        fix.reg.register("ffparent", CoreTest::wfFireForgetParent);
        RootTask rt = fix.createRootTask("p1-ff", "ffparent");

        String status = fix.core.executeUntilBlockedOuter("p1-ff", rt.version(), rt.promise(), rt.preload());
        assertEquals("suspended", status);
    }
}
