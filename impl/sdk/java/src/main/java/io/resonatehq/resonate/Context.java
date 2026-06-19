package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.Suspended;
import io.resonatehq.resonate.Registry.NameVersion;
import io.resonatehq.resonate.Retry.Never;
import io.resonatehq.resonate.Retry.RetryPolicy;
import io.resonatehq.resonate.Types.Args;
import io.resonatehq.resonate.Types.Info;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * A thin, per-call handle over shared execution state, mirroring {@code resonate.context.Context}
 * from the Python SDK.
 *
 * <p>A {@code Context} pairs the shared execution state with the {@link Opts} for the <em>next</em>
 * durable op. {@link #options(Opts)} mints a fresh {@code Context} over the <em>same</em> state
 * carrying overridden opts, so an {@code ctx.options(...).run(...)} chain configures only that one
 * call and leaves the originating context (and its default opts) untouched. Everything mutated during
 * execution (id sequence, spawned children, the creation chain, the workflow flag, the tree) lives on
 * the shared {@link State} and is therefore visible across every {@code Context} that shares it.
 *
 * <p><b>Async model.</b> Python's {@code asyncio} runs every durable op's background body
 * cooperatively on a single event loop; {@code await} yields. Java has no coroutines, so the
 * background body is composed from {@link CompletableFuture}s anchored on the {@link Chain} link and
 * the {@link Effects} create/settle futures, and {@link ResonateFuture#await()} blocks (and unwinds
 * exceptions, including {@link Suspended}). Against a synchronous network ({@code LocalNetwork} in
 * tests) the whole composition runs inline on the calling thread; when {@code create_promise} returns
 * an incomplete future the continuation defers to that boundary, which is exactly where Python's task
 * parks too -- so the chain ordering and "blocks until the promise is created" guarantees hold.
 */
public final class Context {

    /** Resolves an optional per-call target into a concrete routing string. Python's {@code TargetResolver}. */
    public interface TargetResolver extends Function<String, String> {}

    /**
     * Per-call options for the next durable op. Frozen value object; never serialized.
     *
     * <p>{@code retryPolicy} is the per-call override for {@code ctx.run}'s child set via {@code
     * ctx.options(retry_policy=...)}; {@code null} means "inherit the context's default".
     */
    public record Opts(Duration timeout, String target, int version, RetryPolicy retryPolicy) {
        public Opts() {
            this(null, null, 1, null);
        }

        // Builder-style overrides: change one field, inherit the rest. Mirrors Python's kwargs,
        // e.g. new Opts().withRetryPolicy(POLICY).
        public Opts withTimeout(Duration timeout) {
            return new Opts(timeout, target, version, retryPolicy);
        }

        public Opts withTarget(String target) {
            return new Opts(timeout, target, version, retryPolicy);
        }

        public Opts withVersion(int version) {
            return new Opts(timeout, target, version, retryPolicy);
        }

        public Opts withRetryPolicy(RetryPolicy retryPolicy) {
            return new Opts(timeout, target, version, retryPolicy);
        }
    }

    /** A {@code ctx.run} child spawned eagerly: its id paired with its background task. */
    public record SpawnedLocal(String id, CompletableFuture<Object> handle) {}

    /**
     * A future over a durable op's result.
     *
     * <p>Mirrors Python's {@code ResonateFuture}: {@link #await()} yields the op's value (re-raising a
     * rejection or {@link Suspended}), and {@link #id()} waits for the durable promise to be created
     * and then reports its id (surfacing a create failure).
     */
    public static final class ResonateFuture<T> {
        private final String id;
        private final CompletableFuture<Object> task;
        private final CompletableFuture<Void> created;

        ResonateFuture(String id, CompletableFuture<Object> task, CompletableFuture<Void> created) {
            this.id = id;
            this.task = task;
            this.created = created;
        }

        /** The backing task. Package-private: exposed for the assertion tests, like Python's {@code _task}. */
        CompletableFuture<Object> task() {
            return task;
        }

        /**
         * Block on the op's result. Re-raises a rejection's originating error and {@link Suspended}
         * untouched -- the Java analogue of {@code await fut}.
         */
        @SuppressWarnings("unchecked")
        public T await() {
            try {
                return (T) task.join();
            } catch (CancellationException exc) {
                throw exc;
            } catch (CompletionException exc) {
                throw sneaky(unwrap(exc));
            }
        }

        /** Wait for the durable promise to be created, then return its id (surfacing a create failure). */
        public String id() {
            try {
                created.join();
            } catch (CompletionException exc) {
                throw sneaky(unwrap(exc));
            }
            return id;
        }

        // The engine works in Object; the typed run/rpc overloads know the declared return type R.
        // Erasure makes this retype a no-op at runtime -- the await() cast is what can actually fail,
        // exactly where an explicit caller cast would have.
        @SuppressWarnings("unchecked")
        <R> ResonateFuture<R> as() {
            return (ResonateFuture<R>) this;
        }
    }

    /**
     * The shared, mutable execution state. One instance per context node (root and each {@code
     * ctx.run} child); everything spawned during a node's execution records here.
     */
    private static final class State {
        final String id;
        final String originId;
        final String prefixId;
        final String branchId;
        final String parentId;
        final String funcName;
        final long timeoutAt;
        int seq = 0;

        final Effects effects;
        final TargetResolver targetResolver;

        List<String> spawnedRemote = new ArrayList<>();
        List<SpawnedLocal> spawnedLocals = new ArrayList<>();
        List<CompletableFuture<Object>> spawnedRemoteTasks = new ArrayList<>();

        final Dependencies deps;
        final Tree tree;
        final Registry registry;
        final RetryPolicy retryPolicy;

        // One chain per state: serializes this node's create-promise calls into call order.
        final Chain chain = new Chain();

        // Whether this execution has performed any durable op -- i.e. is a workflow, not a pure leaf.
        boolean workflow = false;

        State(
                String id,
                String originId,
                String prefixId,
                String branchId,
                String parentId,
                String funcName,
                long timeoutAt,
                Effects effects,
                TargetResolver targetResolver,
                Dependencies deps,
                Tree tree,
                RetryPolicy retryPolicy,
                Registry registry) {
            this.id = id;
            this.originId = originId;
            this.prefixId = prefixId;
            this.branchId = branchId;
            this.parentId = parentId;
            this.funcName = funcName;
            this.timeoutAt = timeoutAt;
            this.effects = effects;
            this.targetResolver = targetResolver;
            this.deps = deps;
            this.tree = tree;
            this.retryPolicy = retryPolicy;
            this.registry = registry;
        }
    }

    private final State state;
    private final Opts opts;

    private Context(State state, Opts opts) {
        this.state = state;
        this.opts = opts;
    }

    /**
     * Build a root context.
     *
     * <p>{@code originId} is the lineage origin (a {@code detached} child resets it to its own id);
     * {@code prefixId} is the id-generation prefix, propagated unchanged across {@code detached}
     * re-roots. For a genuine top-level root both equal {@code id}. A {@code null} {@code retryPolicy}
     * defaults to {@link Never} and a {@code null} {@code registry} to an empty one (test paths).
     */
    public static Context root(
            String id,
            String originId,
            String prefixId,
            long timeoutAt,
            String funcName,
            Effects effects,
            TargetResolver targetResolver,
            Dependencies deps,
            RetryPolicy retryPolicy,
            Registry registry) {
        State st = new State(
                id,
                originId,
                prefixId,
                id,
                id,
                funcName,
                timeoutAt,
                effects,
                targetResolver,
                deps,
                new Tree(id),
                retryPolicy != null ? retryPolicy : new Never(),
                registry != null ? registry : new Registry());
        return new Context(st, new Opts());
    }

    // Package-private (not private) so the behaviour tests can verify child linkage directly, like
    // Python's ``ctx._child(...)``.
    Context child(String id, String funcName, long timeoutAt) {
        assert state.timeoutAt >= timeoutAt : "child timeout_at must be bounded by parents timeout_at";
        State st = new State(
                id,
                state.originId,
                state.prefixId,
                id,
                state.id,
                funcName,
                timeoutAt,
                state.effects,
                state.targetResolver,
                state.deps,
                state.tree,
                state.retryPolicy,
                state.registry);
        return new Context(st, new Opts());
    }

    /** The execution tree for this workflow attempt (shared by reference across children). */
    public Tree tree() {
        return state.tree;
    }

    /** Per-invocation metadata. */
    public Info info() {
        return new Info(
                state.id, state.parentId, state.originId, state.branchId, state.timeoutAt, state.funcName, Map.of());
    }

    /** The opts carried for this handle's next durable op. */
    public Opts opts() {
        return opts;
    }

    /** Type-keyed dependency lookup into the shared {@link Dependencies}. */
    public <T> T getDependency(Class<T> type) {
        return state.deps.get(type);
    }

    /** Mint a fresh handle over the same state carrying these options. The base handle is untouched. */
    public Context options(Opts opts) {
        return new Context(state, opts);
    }

    /** Mint a fresh handle over the same state carrying default opts. */
    public Context options() {
        return new Context(state, new Opts());
    }

    // -- test/runtime accessors (mirror Python's reachable ``ctx._state`` attributes) ----------

    Effects effects() {
        return state.effects;
    }

    List<String> spawnedRemote() {
        return state.spawnedRemote;
    }

    List<CompletableFuture<Object>> spawnedRemoteTasks() {
        return state.spawnedRemoteTasks;
    }

    boolean workflow() {
        return state.workflow;
    }

    void setWorkflow(boolean workflow) {
        state.workflow = workflow;
    }

    // -- id / timeout / request plumbing ------------------------------------------------------

    // Package-private: the behaviour tests call ``nextId`` / ``childTimeout`` directly, like Python's
    // ``ctx._next_id()`` / ``ctx._child_timeout(...)``.
    String nextId() {
        state.seq += 1;
        return state.id + "." + state.seq;
    }

    long childTimeout(Duration requested) {
        long now = Send.nowMs();
        Duration timeout = requested != null ? requested : Duration.ofDays(1);
        return Math.min(now + timeout.toMillis(), state.timeoutAt);
    }

    private String resolveTarget(String target) {
        return state.targetResolver.apply(target);
    }

    /**
     * Build a global-scope promise request. {@code data} carries a {@link TaskData} for function
     * dispatch (null otherwise); {@code target} adds the routing tag; {@code timer} adds the timer
     * tag distinguishing a sleep from a bare promise. {@code origin} overrides the lineage origin
     * (only {@code detached} does so); the prefix is always this context's.
     */
    private PromiseCreateReq globalReq(
            String id, Duration timeout, Object data, String target, boolean timer, String origin) {
        String resolvedOrigin = origin != null ? origin : state.originId;
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("resonate:scope", "global");
        if (target != null) {
            tags.put("resonate:target", target);
        }
        tags.put("resonate:branch", id);
        tags.put("resonate:parent", state.id);
        tags.put("resonate:origin", resolvedOrigin);
        tags.put("resonate:prefix", state.prefixId);
        if (timer) {
            tags.put("resonate:timer", "true");
        }
        return new PromiseCreateReq(id, childTimeout(timeout), new Value(null, data), tags);
    }

    // -- run: execute a child inline on its own context ---------------------------------------

    /** Run a registered function by name as a local child (versioned by {@code opts.version}). */
    public ResonateFuture<Object> run(String name, Object... args) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        Durable resolved = state.registry.get(name, opts.version());
        if (resolved == null) {
            FunctionNotFoundError exc = new FunctionNotFoundError(name, opts.version());
            throw new PlatformError(List.of(exc));
        }
        return runWith(resolved, link, args);
    }

    /** Internal: run the method behind a resolved reference as a local child. */
    private ResonateFuture<Object> run(Method fn, Object... args) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        return runWith(new Durable(fn), link, args);
    }

    /** Run a function passed as a method reference ({@code Owner::fn}) as a local child. */
    public <R> ResonateFuture<R> run(Fn.F0<R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    public <A, R> ResonateFuture<R> run(Fn.F1<A, R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    public <A, B, R> ResonateFuture<R> run(Fn.F2<A, B, R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, R> ResonateFuture<R> run(Fn.F3<A, B, C, R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, D, R> ResonateFuture<R> run(Fn.F4<A, B, C, D, R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, D, E, R> ResonateFuture<R> run(Fn.F5<A, B, C, D, E, R> ref, Object... args) {
        return run(Fn.methodOf(ref), args).as();
    }

    private ResonateFuture<Object> runWith(Durable df, Chain.Link link, Object[] args) {
        Args payload = df.packArgs(args);

        // A local child's param is write-only -- nothing reads it back -- so it is left empty; that is
        // what lets ctx.run accept non-serializable arguments. The return value still round-trips.
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("resonate:scope", "local");
        tags.put("resonate:branch", state.branchId);
        tags.put("resonate:parent", state.id);
        tags.put("resonate:origin", state.originId);
        tags.put("resonate:prefix", state.prefixId);
        PromiseCreateReq req = new PromiseCreateReq(nextId(), childTimeout(opts.timeout()), new Value(), tags);

        // Record the local child before its promise is created (call-order siblings). Idempotent.
        state.tree.addChild(state.id, req.id(), Tree.INT);

        Opts callOpts = this.opts;
        CompletableFuture<Object> task = link.run(() -> state.effects.createPromise(req))
                .thenCompose(record -> runAfterCreate(df, req, record, payload, callOpts));

        state.spawnedLocals.add(new SpawnedLocal(req.id(), task));
        return new ResonateFuture<>(req.id(), task, link.done());
    }

    private CompletableFuture<Object> runAfterCreate(
            Durable df, PromiseCreateReq req, PromiseRecord record, Args payload, Opts callOpts) {
        // Idempotent recovery: an already-settled promise short-circuits execution. Prune the tree.
        if (!"pending".equals(record.state())) {
            state.tree.settle(req.id());
            return CompletableFuture.completedFuture(settled(df, record));
        }

        Context child = child(req.id(), df.name(), record.timeoutAt());

        String outcome;
        Object value;
        try {
            // The override from options wins; otherwise inherit this context's default. coerceArgs is
            // false: a local child's in-memory args reach the function verbatim.
            RetryPolicy policy = callOpts.retryPolicy() != null ? callOpts.retryPolicy() : state.retryPolicy;
            value = child.invokeWithRetry(df, payload, policy, false);
            outcome = "done";
        } catch (Suspended exc) {
            outcome = "suspended";
            value = null;
        } catch (RuntimeException exc) {
            // Any ordinary exception from the local child is a rejection. PlatformError / Suspended
            // (java.lang.Error) deliberately propagate.
            value = exc;
            outcome = "error";
        }

        String finalOutcome = outcome;
        Object finalValue = value;
        // Always drain the child's sub-work before deciding.
        return child.flushLocalWork().thenCompose(ignored -> {
            List<String> childRemote = child.takeRemoteTodos();
            if ("suspended".equals(finalOutcome) || !childRemote.isEmpty()) {
                state.spawnedRemote.addAll(childRemote);
                throw new Suspended();
            }
            // Settle, then read the outcome back through the settled record -- uniformly for resolved
            // and rejected -- so the live and recovery paths are identical.
            return state.effects.settlePromise(req.id(), finalValue).thenApply(rec -> {
                state.tree.settle(req.id());
                return settled(df, rec);
            });
        });
    }

    /**
     * Execute {@code df} on this context, retrying only a <em>pure-leaf</em> failure.
     *
     * <p>The moment this context performs a durable op (which sets {@code workflow}) the execution has
     * a durable footprint and must never re-run, so its failure settles on the first attempt. {@link
     * Suspended} (a {@code java.lang.Error}) and {@link PlatformError} propagate untouched -- only an
     * ordinary exception is considered for retry.
     */
    public Object invokeWithRetry(Durable df, Args payload, RetryPolicy policy, boolean coerceArgs) {
        int attempt = 0;
        while (true) {
            try {
                return df.invoke(this, payload, coerceArgs);
            } catch (RuntimeException exc) {
                Long delay = state.workflow ? null : policy.next(attempt + 1);
                if (delay == null) {
                    throw exc;
                }
                assert state.seq == 0 : "retried pure-leaf must not have advanced seq";
                if (delay > 0) {
                    try {
                        Thread.sleep(delay * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw exc;
                    }
                }
                attempt += 1;
            }
        }
    }

    // -- rpc / sleep / promise / detached: remote dispatch ------------------------------------

    /** Dispatch a registered function by name over the wire (versioned by {@code opts.version}). */
    public ResonateFuture<Object> rpc(String name, Object... args) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        TaskData data = new TaskData(Arrays.asList(args), Map.of(), name, opts.version());
        PromiseCreateReq req = globalReq(nextId(), opts.timeout(), data, resolveTarget(opts.target()), false, null);
        return remoteFuture(req, link, null);
    }

    /**
     * Internal: dispatch the method behind a resolved reference over the wire by the name it was
     * registered under (reverse lookup), carrying its own registered version and threading its {@link
     * Durable} so the settled result is coerced to the declared return type.
     */
    private ResonateFuture<Object> rpc(Method fn, Object... args) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        NameVersion recorded = state.registry.reverse(fn);
        if (recorded == null) {
            FunctionNotFoundError exc = new FunctionNotFoundError(fn.getName());
            throw new PlatformError(List.of(exc));
        }
        Durable df = state.registry.get(recorded.name(), recorded.version());
        TaskData data = new TaskData(Arrays.asList(args), Map.of(), recorded.name(), recorded.version());
        PromiseCreateReq req = globalReq(nextId(), opts.timeout(), data, resolveTarget(opts.target()), false, null);
        return remoteFuture(req, link, df);
    }

    /** Dispatch a function passed as a method reference ({@code Owner::fn}) over the wire. */
    public <R> ResonateFuture<R> rpc(Fn.F0<R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    public <A, R> ResonateFuture<R> rpc(Fn.F1<A, R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    public <A, B, R> ResonateFuture<R> rpc(Fn.F2<A, B, R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, R> ResonateFuture<R> rpc(Fn.F3<A, B, C, R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, D, R> ResonateFuture<R> rpc(Fn.F4<A, B, C, D, R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    public <A, B, C, D, E, R> ResonateFuture<R> rpc(Fn.F5<A, B, C, D, E, R> ref, Object... args) {
        return rpc(Fn.methodOf(ref), args).as();
    }

    /** Create a timer promise that resolves after {@code duration}. */
    public ResonateFuture<Void> sleep(Duration duration) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        PromiseCreateReq req = globalReq(nextId(), duration, null, null, true, null);
        return remoteFuture(req, link, null).as();
    }

    /** Create a deferred (DI) promise resolved/rejected by an external party. */
    public ResonateFuture<Object> promise(Duration timeout) {
        state.workflow = true;
        Chain.Link link = state.chain.link();
        PromiseCreateReq req = globalReq(nextId(), timeout, null, null, false, null);
        return remoteFuture(req, link, null);
    }

    /** Create a deferred (DI) promise with the default timeout. */
    public ResonateFuture<Object> promise() {
        return promise(null);
    }

    /**
     * Fire-and-forget remote dispatch. The id is minted off the propagated prefix as {@code
     * {prefix}.d{16hex}}, the lineage origin is reset to the child's own id (a fresh lineage), and the
     * future resolves to the child id without ever suspending.
     */
    public ResonateFuture<String> detached(String fn, Object... args) {
        state.workflow = true;
        Chain.Link link = state.chain.link();

        String childId = state.prefixId + ".d" + hashId(nextId());
        TaskData data = new TaskData(Arrays.asList(args), Map.of(), fn, opts.version());
        PromiseCreateReq req = globalReq(childId, opts.timeout(), data, resolveTarget(opts.target()), false, childId);

        // Det nodes are exempt from the contract and skipped by the frontier walk.
        state.tree.addChild(state.id, req.id(), Tree.DET);

        CompletableFuture<Object> task = link.run(() -> state.effects.createPromise(req))
                .thenApply(record -> {
                    if (!"pending".equals(record.state())) {
                        state.tree.settle(req.id());
                    }
                    return (Object) req.id();
                });

        // Registered for the flush so the create completes before the parent settles, even unawaited.
        state.spawnedRemoteTasks.add(task);
        return new ResonateFuture<String>(req.id(), task, link.done());
    }

    private ResonateFuture<Object> remoteFuture(PromiseCreateReq req, Chain.Link link, Durable df) {
        // All three remote callers are external -- a pending ext node sits in the suspension frontier.
        state.tree.addChild(state.id, req.id(), Tree.EXT);
        CompletableFuture<Object> task = awaitRemote(req, link, df);
        state.spawnedRemoteTasks.add(task);
        return new ResonateFuture<>(req.id(), task, link.done());
    }

    private CompletableFuture<Object> awaitRemote(PromiseCreateReq req, Chain.Link link, Durable df) {
        return link.run(() -> state.effects.createPromise(req)).thenApply(record -> {
            if (!"pending".equals(record.state())) {
                // Already settled: leave the frontier and surface the value (coerced if by-object rpc).
                state.tree.settle(req.id());
                return settled(df, record);
            }
            // Still pending: keep the node in the frontier, register the todo, and unwind.
            state.spawnedRemote.add(req.id());
            throw new Suspended();
        });
    }

    // -- structured-concurrency join ----------------------------------------------------------

    /**
     * Wait for every eagerly spawned task on this context to finish, merging each child's remote todos
     * before the caller drains them.
     *
     * <p>Ordinary rejections and {@link Suspended} signals are swallowed -- the backing task delivers
     * the same outcome to the real awaiter. {@link PlatformError} causes are aggregated and re-raised
     * as one error so a fire-and-forget child's failure releases the task.
     */
    public CompletableFuture<Void> flushLocalWork() {
        List<SpawnedLocal> locals = state.spawnedLocals;
        List<CompletableFuture<Object>> remotes = state.spawnedRemoteTasks;
        state.spawnedLocals = new ArrayList<>();
        state.spawnedRemoteTasks = new ArrayList<>();

        List<CompletableFuture<Object>> awaitables = new ArrayList<>();
        for (SpawnedLocal local : locals) {
            awaitables.add(local.handle());
        }
        awaitables.addAll(remotes);

        List<ResonateError> causes = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<?>[] handled = awaitables.stream()
                .map(future -> future.handle((value, exc) -> {
                    if (exc != null) {
                        Throwable cause = unwrap(exc);
                        if (cause instanceof PlatformError platform) {
                            causes.addAll(platform.causes());
                        } else if (cause instanceof Exception || cause instanceof Suspended) {
                            // Swallowed: it belongs to the real awaiter.
                            return null;
                        } else {
                            throw sneaky(cause);
                        }
                    }
                    return null;
                }))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(handled).thenRun(() -> {
            if (!causes.isEmpty()) {
                throw new PlatformError(new ArrayList<>(causes));
            }
        });
    }

    /** Drain and return all remote todos accumulated on this context. */
    public List<String> takeRemoteTodos() {
        List<String> todos = state.spawnedRemote;
        state.spawnedRemote = new ArrayList<>();
        return todos;
    }

    // -- settled-value reconstruction ---------------------------------------------------------

    /**
     * Reshape a settled record to its value, raising a rejection's originating error.
     *
     * <p>{@code decodeSettled} yields the resolved builtins (or raises the reconstructed user error
     * for a rejected record before any coercion); {@code df}, when non-null, then coerces the resolved
     * value to the declared return type. A coercion failure on an already-persisted value is a
     * platform failure (release the task), not a user rejection.
     */
    private Object settled(Durable df, PromiseRecord record) {
        Object decoded;
        try {
            decoded = Codec.decodeSettled(record);
        } catch (Throwable t) {
            throw sneaky(t);
        }
        if (df == null) {
            return decoded;
        }
        try {
            return df.coerceResult(decoded);
        } catch (SerializationError exc) {
            throw new PlatformError(List.of(exc));
        }
    }

    // -- helpers ------------------------------------------------------------------------------

    /** Strip the {@link CompletionException} wrapper so callers see the original throwable. */
    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /** Rethrow any throwable without declaring it -- the analogue of Python re-raising verbatim. */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
        throw (E) t;
    }

    // -- detached id hashing ------------------------------------------------------------------

    /**
     * Lowercase-hex of the first 8 bytes of {@code s}'s SHA-256 digest.
     *
     * <p>Python's {@code _hash_id} uses BLAKE2b with an 8-byte digest, but the digest only needs to
     * be a stable, collision-resistant id segment -- the exact algorithm is irrelevant since nothing
     * cross-checks it against the Python output. SHA-256 (a {@link MessageDigest} every JVM ships)
     * truncated to 8 bytes yields the same 16-hex-char {@code {prefix}.d{16hex}} shape.
     */
    static String hashId(String s) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exc) {
            // SHA-256 is guaranteed present on every JVM, so this is unreachable.
            throw new IllegalStateException(exc);
        }
        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));

        int outlen = 8;
        StringBuilder out = new StringBuilder(outlen * 2);
        for (int i = 0; i < outlen; i++) {
            int b = hash[i] & 0xff;
            out.append(Character.forDigit(b >>> 4, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return out.toString();
    }
}
