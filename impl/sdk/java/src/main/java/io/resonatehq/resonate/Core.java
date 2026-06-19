package io.resonatehq.resonate;

import io.resonatehq.resonate.Context.TargetResolver;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.Suspended;
import io.resonatehq.resonate.Heartbeat.Hb;
import io.resonatehq.resonate.Heartbeat.Noop;
import io.resonatehq.resonate.Retry.Never;
import io.resonatehq.resonate.Retry.RetryPolicy;
import io.resonatehq.resonate.Send.Redirect;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.SuspendResult;
import io.resonatehq.resonate.Send.TaskAcquireResult;
import io.resonatehq.resonate.Types.Args;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseRegisterCallbackData;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Orchestrates the full lifecycle of one task, mirroring {@code resonate.core.Core} from the Python
 * SDK.
 *
 * <p>A {@code Core} acquires a task (via {@link #onMessage}) or takes an already-acquired one (via
 * {@link #executeUntilBlockedOuter}), executes the registered function, then fulfills on done,
 * suspends on remote work, or releases on error.
 *
 * <p>Methods raise a {@link ResonateError} on failure and return the success status ({@code "done"} /
 * {@code "suspended"}) otherwise. On error the task is released before the exception is re-raised.
 *
 * <p><b>Async model.</b> Python's {@code Core} is {@code async}, driven on a single asyncio event
 * loop. Java has no coroutines and the inner execution is inherently blocking — {@link
 * Context#invokeWithRetry} runs user code synchronously, blocking on each child {@link
 * Context.ResonateFuture#await()} — so {@code Core} runs blocking on the worker thread that owns the
 * task. The durable {@link Sender} operations return a {@link CompletableFuture}; {@link
 * #join(CompletableFuture)} blocks on each and surfaces its {@link ResonateError} (or {@link
 * PlatformError}) cause, the analogue of Python's {@code await} re-raising. Concurrency across tasks
 * is achieved with multiple worker threads, each blocking here, rather than one loop multiplexing
 * {@code async} tasks.
 */
public final class Core {

    private static final Logger LOGGER = System.getLogger(Core.class.getName());

    /**
     * The fallback target resolver used when a {@code Core} is built without one: returns the
     * override unchanged, or the empty string when there is none. Mirrors Python's {@code
     * identity_target_resolver}.
     */
    public static final TargetResolver IDENTITY_TARGET_RESOLVER = override -> override != null ? override : "";

    // ═══════════════════════════════════════════════════════════════
    //  Execution outcome -- what the inner reports back to the lifecycle loop
    // ═══════════════════════════════════════════════════════════════

    /**
     * Outcome reported by {@link #executeUntilBlockedInner}. Mirrors Python's {@code _ExecOutcome =
     * _ExecFulfilled | _ExecSuspended}; package-private (the analogue of Python's underscore-prefixed
     * module-private types), exposed for the behaviour tests.
     */
    sealed interface ExecOutcome permits ExecFulfilled, ExecSuspended {}

    /**
     * The workflow finished; the task should be fulfilled with these args.
     *
     * <p>{@code value} is already codec-encoded, so the caller has a single uniform fulfill path.
     */
    record ExecFulfilled(String state, Value value, Tree tree) implements ExecOutcome {}

    /** The workflow has remote dependencies; suspend on these awaiteds. */
    record ExecSuspended(List<String> todos, Tree tree) implements ExecOutcome {}

    // ═══════════════════════════════════════════════════════════════
    //  Fields
    // ═══════════════════════════════════════════════════════════════

    private final Sender sender; // may be null in tests that never touch the network
    private final Codec codec;
    private final Registry registry;
    private final TargetResolver resolver;
    private final Hb heartbeat;
    private final String pid;
    private final int ttl;

    /**
     * The dependency map threaded into the root context, making registered dependencies available to
     * user functions.
     */
    private final Dependencies deps;

    /**
     * SDK-wide default retry policy for a root function with no per-function override registered.
     * Threaded in from {@code Resonate}; defaults to {@link Never} so a directly-constructed {@code
     * Core} (tests) does not retry.
     */
    private final RetryPolicy retryPolicy;

    /**
     * Build a {@code Core}. {@code resolver} defaults to {@link #IDENTITY_TARGET_RESOLVER}, {@code
     * heartbeat} to {@link Noop}, {@code deps} to an empty {@link Dependencies}, and {@code
     * retryPolicy} to {@link Never} when {@code null} (the analogue of Python's struct field
     * defaults).
     */
    public Core(
            Sender sender,
            Codec codec,
            Registry registry,
            TargetResolver resolver,
            Hb heartbeat,
            String pid,
            int ttl,
            Dependencies deps,
            RetryPolicy retryPolicy) {
        this.sender = sender;
        this.codec = codec;
        this.registry = registry;
        this.resolver = resolver != null ? resolver : IDENTITY_TARGET_RESOLVER;
        this.heartbeat = heartbeat != null ? heartbeat : new Noop();
        this.pid = pid;
        this.ttl = ttl;
        this.deps = deps != null ? deps : new Dependencies();
        this.retryPolicy = retryPolicy != null ? retryPolicy : new Never();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 1: onMessage -- acquire then execute
    // ═══════════════════════════════════════════════════════════════

    /**
     * Handle an execute push message.
     *
     * <p>Acquires the task, decodes the root promise, and runs {@link #executeUntilBlockedOuter}.
     */
    public String onMessage(String taskId, int version) {
        assert sender != null : "sender must be set";
        TaskAcquireResult res = join(sender.taskAcquire(taskId, version, pid, ttl));
        LOGGER.log(Level.DEBUG, "core: task acquired task_id={0}", taskId);

        // The lease is now held, but the root-promise decode happens before
        // executeUntilBlockedOuter's release boundary -- a corrupt promise (Base64DecodeError /
        // SerializationError) would otherwise leak the lease until TTL expiry. Release immediately so
        // re-delivery retries at once, mirroring the symmetric TaskData decode inside the inner loop.
        PromiseRecord promise;
        try {
            promise = codec.decodePromise(res.promise());
        } catch (ResonateError exc) {
            LOGGER.log(Level.ERROR, "core: failed to decode root promise, releasing task task_id=" + taskId, exc);
            try {
                join(sender.taskRelease(taskId, res.task().version()));
            } catch (ResonateError releaseExc) {
                LOGGER.log(
                        Level.ERROR, "core: failed to release task after decode error task_id=" + taskId, releaseExc);
            }
            throw exc;
        }

        return executeUntilBlockedOuter(taskId, res.task().version(), promise, res.preload());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 2: executeUntilBlocked -- task already acquired
    // ═══════════════════════════════════════════════════════════════

    /**
     * Run an already-acquired task to completion or suspension.
     *
     * <p>The caller owns the acquire step and must pass a {@code promise} whose {@code param} /
     * {@code value} have been run through {@link Codec#decodePromise}.
     *
     * <p>Owns the task lifecycle: builds {@link Effects}, drives the redirect loop, fulfills or
     * suspends on the inner's outcome, and releases on error.
     */
    public String executeUntilBlockedOuter(
            String taskId, int taskVersion, PromiseRecord promise, List<PromiseRecord> preload) {
        heartbeat.start(taskId, taskVersion);
        assert sender != null : "sender must be set";
        try {
            try {
                LOGGER.log(Level.DEBUG, "core: starting execution task_id={0} promise_id={1}", taskId, promise.id());
                List<PromiseRecord> currentPreload = preload;
                while (true) {
                    Effects effects = new Effects(sender, codec, currentPreload);
                    ExecOutcome outcome = executeUntilBlockedInner(promise, effects);

                    if (outcome instanceof ExecFulfilled fulfilled) {
                        // The fulfill is a durable server interaction like any other durable op; a
                        // failure here surfaces uniformly as a PlatformError, released (and unwrapped
                        // back to its ResonateError cause) by the outer handler below.
                        try {
                            join(sender.taskFulfill(
                                    taskId,
                                    taskVersion,
                                    new PromiseSettleReq(promise.id(), fulfilled.state(), fulfilled.value())));
                        } catch (ResonateError exc) {
                            throw new PlatformError(List.of(exc));
                        }
                        LOGGER.log(
                                Level.DEBUG, "core: task fulfilled task_id={0} promise_id={1}", taskId, promise.id());
                        return "done";
                    }

                    ExecSuspended suspended = (ExecSuspended) outcome;
                    List<String> todos = suspended.todos();
                    LOGGER.log(
                            Level.DEBUG,
                            "core: attempting to suspend task task_id={0} remote_deps={1}",
                            taskId,
                            todos.size());
                    // Suspend is a durable server interaction too -- wrap a failure uniformly as
                    // PlatformError (see the fulfill case above).
                    List<PromiseRegisterCallbackData> actions = new ArrayList<>();
                    for (String awaited : todos) {
                        actions.add(new PromiseRegisterCallbackData(awaited, taskId));
                    }
                    SuspendResult sr;
                    try {
                        sr = join(sender.taskSuspend(taskId, taskVersion, actions));
                    } catch (ResonateError exc) {
                        throw new PlatformError(List.of(exc));
                    }

                    if (!(sr instanceof Redirect redirect)) {
                        LOGGER.log(Level.DEBUG, "core: task suspended task_id={0}", taskId);
                        return "suspended";
                    }
                    LOGGER.log(
                            Level.DEBUG,
                            "core: suspend returned redirect, re-executing task task_id={0} preload={1}",
                            taskId,
                            redirect.preload().size());
                    currentPreload = redirect.preload();
                }
            }
            // PlatformError is java.lang.Error-derived, so it reaches here past user code untouched;
            // this catch is the single place the release-on-platform-error guarantee lives. Its
            // Error-ness has done its job once it arrives (there is no user code above outer), so after
            // the release it is unwrapped: callers always see the original ResonateError, with the
            // PlatformError -- whose traceback records which durable op failed -- chained as cause.
            catch (ResonateError | PlatformError exc) {
                LOGGER.log(
                        Level.ERROR,
                        "core: execution failed, releasing task task_id=" + taskId + " promise_id=" + promise.id(),
                        exc);
                try {
                    join(sender.taskRelease(taskId, taskVersion));
                } catch (ResonateError releaseExc) {
                    LOGGER.log(Level.ERROR, "core: failed to release task after error task_id=" + taskId, releaseExc);
                }
                if (exc instanceof PlatformError platform) {
                    throw platform.cause();
                }
                throw exc;
            }
        } finally {
            heartbeat.stop(taskId);
        }
    }

    /**
     * Run the workflow body once and report the outcome.
     *
     * <p>Does not touch task lifecycle APIs (fulfill/suspend/release) -- the caller owns those.
     * Encodes return values through the codec so the caller has a single, uniform fulfill path.
     */
    ExecOutcome executeUntilBlockedInner(PromiseRecord promise, Effects effects) {
        // 1. Decode TaskData from the (already-decoded) promise param.
        TaskData taskData;
        try {
            taskData = codec.convert(promise.param().data(), TaskData.class);
        } catch (SerializationError exc) {
            throw new DecodingError("invalid task data: " + exc.getMessage());
        }

        // 2. Look up the function in the registry by (name, version). The version was persisted in
        //    TaskData at create time, so this resolves the same implementation on every replay
        //    regardless of later registrations.
        Durable df = registry.get(taskData.func(), taskData.version());
        if (df == null) {
            throw new FunctionNotFoundError(taskData.func(), taskData.version());
        }

        // Retry policy for this root function. A remote dispatch carries no policy on the wire, so
        // resolve it here: the per-function override registered with the function wins, else the
        // SDK-wide default. Only a pure-leaf root honors it -- a workflow root touches ctx, marking it
        // a workflow that never retries (see Context.invokeWithRetry).
        RetryPolicy registered = registry.getPolicy(taskData.func(), taskData.version());
        RetryPolicy policy = registered != null ? registered : retryPolicy;

        // 3. SHORT-CIRCUIT: if the root promise is already settled, report a fulfill outcome without
        //    invoking the function.
        if (!"pending".equals(promise.state())) {
            LOGGER.log(
                    Level.INFO,
                    "core: promise already settled, fulfilling task without execution promise_id={0} state={1}",
                    promise.id(),
                    promise.state());
            Tree tree = new Tree(promise.id());
            tree.settle(promise.id());
            String state = "rejected_timedout".equals(promise.state()) ? "rejected" : promise.state();
            return new ExecFulfilled(state, codec.encode(promise.value().data()), tree);
        }

        // 4. EXECUTE the workflow.
        Context rootCtx = Context.root(
                promise.id(),
                // Take the lineage origin from the promise's resonate:origin tag, which the dispatcher
                // set: a top-level run / rpc propagates its own origin, while detached resets it to the
                // child's own id (a new lineage). Falling back to promise.id when absent keeps a genuine
                // top-level root (whose tag equals its id anyway) and any tag-less promise correct.
                promise.tags().getOrDefault("resonate:origin", promise.id()),
                // The id-generation prefix from resonate:prefix. Unlike origin it is propagated
                // unchanged across detached re-roots, so every recursion level mints {prefix}.{16hex}
                // off the same fixed prefix instead of off its own grown id -- this is what bounds
                // recursive detached ids. Falls back to promise.id when absent, matching origin.
                promise.tags().getOrDefault("resonate:prefix", promise.id()),
                promise.timeoutAt(),
                taskData.func(),
                effects,
                resolver,
                deps,
                // The root's OWN retry uses policy (resolved above) via invokeWithRetry; this default is
                // what ctx.run children inherit when they don't override -- the SDK-wide default, not
                // the root function's per-function override.
                retryPolicy,
                // Share Core's registry so by-name ctx.run / by-object ctx.rpc resolve against the same
                // functions this worker registered.
                registry);

        boolean suspended = false;
        Exception runErr = null;
        Object res = null;
        try {
            res = rootCtx.invokeWithRetry(df, new Args(taskData.args(), taskData.kwargs()), policy, true);
        } catch (Suspended exc) {
            suspended = true;
        } catch (RuntimeException exc) {
            // User code reported failure by raising. Any ordinary exception (a deliberately raised
            // ApplicationError, a child rejection surfaced by ctx.rpc, or a plain domain-specific
            // subclass) settles the promise rejected. The original object crosses the boundary
            // verbatim; the codec flattens it to the wire error shape and serializes it best-effort, so
            // an awaiter recovers the original type when it can and an ApplicationError otherwise (see
            // Codec.deserializeError).
            //
            // java.lang.Error subclasses (Suspended, PlatformError, and the JVM's own) are NOT caught
            // here -- they propagate out so the task is released and the runtime can shut down. This is
            // the exact analogue of Python catching Exception but not BaseException.
            LOGGER.log(
                    Level.DEBUG,
                    "core: user function raised " + exc.getClass().getSimpleName() + " in task="
                            + rootCtx.info().id(),
                    exc);
            runErr = exc;
        }

        // Flush local work and collect remote todos. flushLocalWork joins every spawned sibling and
        // re-raises any platform failures aggregated into one PlatformError, so reaching past it
        // guarantees none occurred.
        join(rootCtx.flushLocalWork());
        List<String> todos = rootCtx.takeRemoteTodos();

        // 5. FINALIZE: fulfill when no remote todos remain and the function did not request suspension.
        if (!suspended && todos.isEmpty()) {
            // ASSERT the tree matches a Done outcome (U1/U2/U3/D1) before fulfilling. The tree is a
            // parallel assertion-only view -- wellFormed never feeds the decision below, it only
            // verifies it (see Tree).
            rootCtx.tree().wellFormed("done", todos);
            String state;
            Value encoded;
            if (runErr != null) {
                state = "rejected";
                encoded = codec.encode(runErr);
            } else {
                state = "resolved";
                encoded = codec.encode(res);
            }
            return new ExecFulfilled(state, encoded, rootCtx.tree());
        }

        // If the function returned done but there are pending todos, treat as suspended
        // (structured-concurrency rule; covers the fire-and-forget child case).
        if (!suspended && !todos.isEmpty()) {
            LOGGER.log(
                    Level.WARNING,
                    "core: workflow returned done with pending remote todos -- either a fire-and-forget"
                            + " child suspended or the function swallowed an error func={0} id={1} todos={2}",
                    taskData.func(),
                    promise.id(),
                    todos);
        }

        // ASSERT the tree matches a Suspended outcome (U1/U2/U3/S1/S4): the frontier is non-empty and
        // the awaited todos are a subset of it.
        rootCtx.tree().wellFormed("suspended", todos);
        return new ExecSuspended(todos, rootCtx.tree());
    }

    // -- helpers ------------------------------------------------------------------------------

    /**
     * Block on a durable {@link Sender} future, surfacing its underlying cause.
     *
     * <p>The analogue of Python's {@code await}: a {@link Sender} failure arrives wrapped in a {@link
     * CompletionException}; this strips one wrapper layer and rethrows the cause verbatim, so a {@link
     * ResonateError} (server / decoding) or a {@link PlatformError} (from {@link Effects}) reaches the
     * caller as its own type rather than a {@code CompletionException}.
     */
    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException exc) {
            Throwable cause = exc.getCause() != null ? exc.getCause() : exc;
            throw sneaky(cause);
        }
    }

    /** Rethrow any throwable without declaring it -- the analogue of Python re-raising verbatim. */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
        throw (E) t;
    }
}
