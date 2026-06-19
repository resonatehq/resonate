package io.resonatehq.resonate;

import io.resonatehq.resonate.Codec.Encryptor;
import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Handle.PromiseResult;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Handle.Subscription;
import io.resonatehq.resonate.Heartbeat.Async;
import io.resonatehq.resonate.Heartbeat.Hb;
import io.resonatehq.resonate.Heartbeat.Noop;
import io.resonatehq.resonate.Network.HttpNetwork;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Registry.NameVersion;
import io.resonatehq.resonate.Retry.Exponential;
import io.resonatehq.resonate.Retry.RetryPolicy;
import io.resonatehq.resonate.Send.Created;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.TaskCreateOutcome;
import io.resonatehq.resonate.Transport.ExecuteMsg;
import io.resonatehq.resonate.Transport.Message;
import io.resonatehq.resonate.Transport.UnblockMsg;
import io.resonatehq.resonate.Types.Args;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.Value;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * The top-level Resonate SDK entry point, mirroring {@code resonate.resonate.Resonate} from the
 * Python SDK.
 *
 * <p>Applications construct one {@code Resonate}, register durable functions with {@code register}
 * (always by method reference, {@code Owner::fn}), start them with {@link #run} or {@link #rpc}, look
 * up existing invocations with {@link #get}, and shut down with {@link #stop}.
 *
 * <p>{@link #run} and {@link #rpc} are <em>synchronous</em> fire-and-forget triggers: they return a
 * {@link ResonateHandle} immediately while the network work runs on a background task. Per-call
 * options come from {@link #options(Duration, String, int)}, which mints a fresh handle over the same
 * shared engine carrying overridden {@link Opts}.
 *
 * <p>Internally each promise id maps to a single {@link Subscription}; every handle to that id awaits
 * the same subscription, so one settle wakes them all.
 *
 * <p><b>Async model.</b> Python drives every background body cooperatively on one asyncio event loop.
 * Java has no coroutines and {@link Core} is blocking, so each background body runs on a worker thread
 * from {@link #executor} (the analogue of {@code asyncio.create_task}); the durable {@link Sender}
 * calls inside it block via {@link #join(CompletableFuture)}. Live background jobs are retained in
 * {@link Runtime#bgTasks} so {@link #stop} can join them, exactly as Python retains its tasks.
 */
public final class Resonate {

    private static final Logger LOGGER = System.getLogger(Resonate.class.getName());

    /** Default deadline applied to a top-level {@link #run} / {@link #rpc} when the caller passes none. */
    public static final Duration DEFAULT_TOP_LEVEL_TIMEOUT = Duration.ofDays(1);

    /** Default per-task lease duration. */
    public static final Duration DEFAULT_TTL = Duration.ofMinutes(1);

    /** Ceiling on tasks executing concurrently in this process (see {@link Runtime#executeSema}). */
    public static final int DEFAULT_MAX_CONCURRENT_TASKS = 64;

    /** Divisor applied to the lease TTL to derive the heartbeat interval. */
    public static final int HEARTBEAT_INTERVAL_DIVISOR = 2;

    /** How often the background loop re-issues listener registration for still-pending subscriptions. */
    static final long SUBSCRIPTION_REFRESH_SECS = 60;

    /** Handle to a created schedule. */
    public static final class ResonateSchedule {
        private final String name;
        private final Schedules schedules;

        ResonateSchedule(String name, Schedules schedules) {
            this.name = name;
            this.schedules = schedules;
        }

        /** The schedule's name/id. */
        public String name() {
            return name;
        }

        /** Delete this schedule. */
        public CompletableFuture<Void> delete() {
            return schedules.delete(name);
        }
    }

    /**
     * The shared mutable runtime behind every {@link Resonate} handle.
     *
     * <p>{@link Resonate#options(Duration, String, int)} mints handles that share this object by
     * reference, so every handle observes the same lifecycle state (subscriptions, background tasks,
     * the stopping flag, the refresh loop). The never-rebound wiring (network, codec, core, ...) lives
     * directly on {@link Resonate}.
     */
    static final class Runtime {
        /** id -&gt; settle-once subscription, shared by every handle to that id. Guarded by {@code this}. */
        final Map<String, Subscription> subs = new HashMap<>();
        /** Live fire-and-forget jobs, retained so {@link Resonate#stop} can join them. Guarded by {@code this}. */
        final Set<CompletableFuture<Void>> bgTasks = new HashSet<>();
        /** Set by {@link Resonate#stop} to refuse new {@code execute} work so the join can drain. */
        volatile boolean stopping = false;
        /** Caps tasks executing (holding a lease) at once. */
        final Semaphore executeSema;
        /** The listener-refresh loop; cancelled by {@link Resonate#stop}. */
        volatile Future<?> refreshHandle;

        Runtime(int maxConcurrentTasks) {
            this.executeSema = new Semaphore(maxConcurrentTasks);
        }
    }

    /** Builder for the (many) optional construction parameters Python passes as keyword arguments. */
    public static final class Builder {
        private String url;
        private Network network;
        private String group;
        private String pid;
        private Duration ttl;
        private String token;
        private Encryptor encryptor;
        private Hb heartbeat;
        private String prefix;
        private Integer maxConcurrentTasks;
        private RetryPolicy retryPolicy;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder network(Network network) {
            this.network = network;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder pid(String pid) {
            this.pid = pid;
            return this;
        }

        public Builder ttl(Duration ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder encryptor(Encryptor encryptor) {
            this.encryptor = encryptor;
            return this;
        }

        public Builder heartbeat(Hb heartbeat) {
            this.heartbeat = heartbeat;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder maxConcurrentTasks(Integer maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Resonate build() {
            return new Resonate(this);
        }
    }

    // -- Construction-time wiring (set once, shared by reference across option handles) ----------

    final Duration ttl;
    final String idPrefix;
    final Network network;
    final String pid;
    final Codec codec;
    final Registry registry;
    final Sender sender;
    final Dependencies deps;
    final Hb heartbeat;
    final Core core;
    /** Public clients for direct promise/schedule manipulation. */
    public final Promises promises;

    public final Schedules schedules;

    final Runtime runtime;
    final ExecutorService executor;
    /** Listener-refresh interval in ms; package-private and mutable for tests (Python's monkeypatch). */
    volatile long refreshIntervalMs = SUBSCRIPTION_REFRESH_SECS * 1000;

    /** This handle's options, applied by its {@code run} / {@code rpc} calls. */
    final Opts opts;

    public static Builder builder() {
        return new Builder();
    }

    /** Construct with all defaults (local mode). */
    public Resonate() {
        this(new Builder());
    }

    private Resonate(Builder b) {
        Duration resolvedTtl = b.ttl != null ? b.ttl : DEFAULT_TTL;
        int safeTtl = safeTtlMs(resolvedTtl);

        RetryPolicy resolvedRetryPolicy =
                b.retryPolicy != null ? b.retryPolicy : new Exponential(1, 30, 2, Long.MAX_VALUE);

        String resolvedPrefix = b.prefix != null ? b.prefix : System.getenv("RESONATE_PREFIX");
        String auth = b.token != null ? b.token : System.getenv("RESONATE_TOKEN");

        Network net = selectNetwork(b.url, b.network, b.group, b.pid, auth);
        String netPid = net.pid();

        Transport transport = new Transport(net);
        Codec codec = new Codec(b.encryptor != null ? b.encryptor : new NoopEncryptor());
        Registry registry = new Registry();
        Sender sender = new Sender(transport, auth);
        Dependencies deps = new Dependencies();

        Hb hb;
        if (b.heartbeat != null) {
            hb = b.heartbeat;
        } else if (net instanceof LocalNetwork) {
            hb = new Noop();
        } else {
            long intervalMs = Math.max(safeTtl / HEARTBEAT_INTERVAL_DIVISOR, 1);
            hb = new Async(netPid, intervalMs, sender);
        }

        // Wiring is assigned before Core is built so the resolver -- invoked lazily at dispatch time
        // -- sees a fully-wired network (the analogue of Python handing over a bound method).
        this.ttl = resolvedTtl;
        this.idPrefix = resolvedPrefix != null && !resolvedPrefix.isEmpty() ? resolvedPrefix + ":" : "";
        this.network = net;
        this.pid = netPid;
        this.codec = codec;
        this.registry = registry;
        this.sender = sender;
        this.deps = deps;
        this.heartbeat = hb;
        this.promises = new Promises(sender, codec);
        this.schedules = new Schedules(sender, codec);
        this.runtime = new Runtime(b.maxConcurrentTasks != null ? b.maxConcurrentTasks : DEFAULT_MAX_CONCURRENT_TASKS);
        this.opts = new Opts();
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "resonate-bg");
            t.setDaemon(true);
            return t;
        });
        this.core =
                new Core(sender, codec, registry, this::resolveTarget, hb, netPid, safeTtl, deps, resolvedRetryPolicy);

        // Wire push-message dispatch BEFORE starting the network so the initial frames are not missed.
        transport.recv(this::onMessage);
        // Start the network (fire-and-forget: both implementations complete start() synchronously).
        net.start();
        this.runtime.refreshHandle = executor.submit(this::runRefresh);
    }

    /** Copy constructor used by {@link #options}: shares all wiring + runtime, carries a new {@link Opts}. */
    private Resonate(Resonate base, Opts opts) {
        this.ttl = base.ttl;
        this.idPrefix = base.idPrefix;
        this.network = base.network;
        this.pid = base.pid;
        this.codec = base.codec;
        this.registry = base.registry;
        this.sender = base.sender;
        this.deps = base.deps;
        this.heartbeat = base.heartbeat;
        this.core = base.core;
        this.promises = base.promises;
        this.schedules = base.schedules;
        this.runtime = base.runtime;
        this.executor = base.executor;
        this.refreshIntervalMs = base.refreshIntervalMs;
        this.opts = opts;
    }

    // ── Public API ──────────────────────────────────────────────────────────

    /** Store a typed application dependency, shared with every context. Add before processing starts. */
    public Resonate withDependency(Object value) {
        deps.insert(value);
        return this;
    }

    /**
     * Mint a new handle over the same engine, carrying these options.
     *
     * <p>Returns a <em>new</em> {@code Resonate} sharing wiring and {@link Runtime} by reference, whose
     * {@link #run} / {@link #rpc} calls use the given options. The originating handle is untouched, so
     * held handles never interfere. Options <em>replace</em> rather than merge. {@code version} applies
     * to the by-name form of {@code run} / {@code rpc}; the by-object form recovers its version from
     * the registration.
     */
    public Resonate options(Opts opts) {
        return new Resonate(this, opts);
    }

    /** This handle's options. */
    public Opts opts() {
        return opts;
    }

    // -- register --------------------------------------------------------------

    /**
     * Register a durable function, always passed as a method reference ({@code Owner::fn}) -- never a
     * raw {@link Method}. The {@code F1..F5} overloads select by the function's arity. The single-arg
     * form registers under the method's own name at version 1; the {@code (ref, name, version)} form
     * overrides both, and the {@code (ref, name, version, retryPolicy)} form additionally sets a
     * per-function {@link RetryPolicy} (the SDK-wide default when null). The same name may be
     * registered at several versions. Each overload returns
     * its {@code ref} unchanged (mirroring Python's {@code return fn}) so it stays usable directly with
     * {@code run}/{@code rpc} or a workflow's {@code ctx.run}.
     */
    public <R> Fn.F0<R> register(Fn.F0<R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <R> Fn.F0<R> register(Fn.F0<R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <R> Fn.F0<R> register(Fn.F0<R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    public <A, R> Fn.F1<A, R> register(Fn.F1<A, R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <A, R> Fn.F1<A, R> register(Fn.F1<A, R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <A, R> Fn.F1<A, R> register(Fn.F1<A, R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    public <A, B, R> Fn.F2<A, B, R> register(Fn.F2<A, B, R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <A, B, R> Fn.F2<A, B, R> register(Fn.F2<A, B, R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <A, B, R> Fn.F2<A, B, R> register(Fn.F2<A, B, R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    public <A, B, C, R> Fn.F3<A, B, C, R> register(Fn.F3<A, B, C, R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <A, B, C, R> Fn.F3<A, B, C, R> register(Fn.F3<A, B, C, R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <A, B, C, R> Fn.F3<A, B, C, R> register(
            Fn.F3<A, B, C, R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    public <A, B, C, D, R> Fn.F4<A, B, C, D, R> register(Fn.F4<A, B, C, D, R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <A, B, C, D, R> Fn.F4<A, B, C, D, R> register(Fn.F4<A, B, C, D, R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <A, B, C, D, R> Fn.F4<A, B, C, D, R> register(
            Fn.F4<A, B, C, D, R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    public <A, B, C, D, E, R> Fn.F5<A, B, C, D, E, R> register(Fn.F5<A, B, C, D, E, R> ref) {
        registerRef(ref, null, 1, null);
        return ref;
    }

    public <A, B, C, D, E, R> Fn.F5<A, B, C, D, E, R> register(Fn.F5<A, B, C, D, E, R> ref, String name, int version) {
        registerRef(ref, name, version, null);
        return ref;
    }

    public <A, B, C, D, E, R> Fn.F5<A, B, C, D, E, R> register(
            Fn.F5<A, B, C, D, E, R> ref, String name, int version, RetryPolicy retryPolicy) {
        registerRef(ref, name, version, retryPolicy);
        return ref;
    }

    /** Resolve a reference to its {@link Method} and register it under {@code name} (own name if null) at {@code version} with an optional per-function {@code retryPolicy}. */
    private void registerRef(java.io.Serializable ref, String name, int version, RetryPolicy retryPolicy) {
        Method fn = Fn.methodOf(ref);
        String regName = name != null ? name : fn.getName();
        if (regName.isEmpty()) {
            throw new ApplicationError("register: a name is required for an anonymous function");
        }
        registry.register(regName, fn, version, retryPolicy);
    }

    // -- run --------------------------------------------------------------------

    /**
     * Start a durable invocation of a function passed as a method reference ({@code Owner::fn}). The
     * {@code F1..F5} overloads select by arity; the function must be registered. Mirrors Python's
     * {@code r.run(id, foo, ...)} where the function object is passed directly. The handle's result
     * type {@code R} is inferred from the reference. The result is decoded against the registered
     * return type; an unregistered reference raises {@link FunctionNotFoundError} synchronously.
     */
    public <R> ResonateHandle<R> run(String id, Fn.F0<R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    public <A, R> ResonateHandle<R> run(String id, Fn.F1<A, R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    public <A, B, R> ResonateHandle<R> run(String id, Fn.F2<A, B, R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    public <A, B, C, R> ResonateHandle<R> run(String id, Fn.F3<A, B, C, R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    public <A, B, C, D, R> ResonateHandle<R> run(String id, Fn.F4<A, B, C, D, R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    public <A, B, C, D, E, R> ResonateHandle<R> run(String id, Fn.F5<A, B, C, D, E, R> ref, Object... args) {
        return runRef(id, ref, args);
    }

    private <R> ResonateHandle<R> runRef(String id, java.io.Serializable ref, Object[] args) {
        NameVersion nv = registeredKey(Fn.methodOf(ref));
        return runResolved(id, nv.name(), nv.version(), args);
    }

    /**
     * Start a durable invocation of a locally registered function by name, dispatched at
     * {@link #options(Duration, String, int)}'s version. The name must be registered locally (run
     * executes here); an unregistered name raises {@link FunctionNotFoundError} synchronously.
     */
    public ResonateHandle<Object> run(String id, String func, Object... args) {
        return runResolved(id, func, opts.version(), args);
    }

    private <T> ResonateHandle<T> runResolved(String id, String name, int version, Object[] args) {
        Opts o = this.opts;

        Durable df = registry.get(name, version);
        if (df == null) {
            throw new FunctionNotFoundError(name, version);
        }

        String prefixedId = prefixId(id);
        PromiseCreateReq req =
                buildRootPromiseCreateReq(prefixedId, name, df.packArgs(args), version, o.timeout(), o.target());

        CompletableFuture<Void> created = new CompletableFuture<>();
        Sub s = subscribe(prefixedId);
        Subscription sub = s.sub();
        boolean isNew = s.isNew();

        spawn(() -> {
            TaskCreateOutcome outcome;
            try {
                outcome = join(sender.taskCreateOrConflict(pid, safeTtlMs(ttl), encodeCreateReq(req)));
            } catch (Throwable exc) {
                created.completeExceptionally(exc);
                throw sneaky(exc);
            }
            created.complete(null);

            if (outcome instanceof Created c
                    && "acquired".equals(c.result().task().state())) {
                PromiseRecord decoded = codec.decodePromise(c.result().promise());
                String taskId = c.result().task().id();
                int taskVersion = c.result().task().version();
                List<PromiseRecord> preload = c.result().preload();
                spawn(() -> boundedExecute(() -> core.executeUntilBlockedOuter(taskId, taskVersion, decoded, preload)));
            }

            if (isNew) {
                registerAndSettle(prefixedId, sub);
            }
        });

        return new ResonateHandle<>(prefixedId, sub, codec, df.returnType(), created);
    }

    // -- rpc --------------------------------------------------------------------

    /**
     * Dispatch a function passed as a method reference ({@code Owner::fn}) remotely. The {@code
     * F1..F5} overloads select by arity; the result type {@code R} is inferred from the reference. Its
     * name and version are recovered from the registration (so the result is decoded against the
     * declared return type, like {@link #run}); an unregistered reference raises {@link
     * FunctionNotFoundError} synchronously.
     */
    public <R> ResonateHandle<R> rpc(String id, Fn.F0<R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    public <A, R> ResonateHandle<R> rpc(String id, Fn.F1<A, R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    public <A, B, R> ResonateHandle<R> rpc(String id, Fn.F2<A, B, R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    public <A, B, C, R> ResonateHandle<R> rpc(String id, Fn.F3<A, B, C, R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    public <A, B, C, D, R> ResonateHandle<R> rpc(String id, Fn.F4<A, B, C, D, R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    public <A, B, C, D, E, R> ResonateHandle<R> rpc(String id, Fn.F5<A, B, C, D, E, R> ref, Object... args) {
        return rpcRef(id, ref, args);
    }

    private <R> ResonateHandle<R> rpcRef(String id, java.io.Serializable ref, Object[] args) {
        NameVersion nv = registeredKey(Fn.methodOf(ref));
        Durable df = registry.get(nv.name(), nv.version());
        Type returnType = df != null ? df.returnType() : Object.class;
        return rpcResolved(id, nv.name(), nv.version(), returnType, args);
    }

    /**
     * Dispatch a function remotely by name, at {@link #options(Duration, String, int)}'s version. The
     * target need not be registered locally; the result is untyped ({@code Object}) since there is no
     * local return annotation to read.
     */
    public ResonateHandle<Object> rpc(String id, String fn, Object... args) {
        return rpcResolved(id, fn, opts.version(), Object.class, args);
    }

    private <T> ResonateHandle<T> rpcResolved(String id, String name, int version, Type returnType, Object[] args) {
        Opts o = this.opts;
        String prefixedId = prefixId(id);

        // Unlike run, the dispatched function may not run here, so args are packed raw (no local
        // pack/validate); Java has no kwargs, so the kwargs slot is always empty.
        PromiseCreateReq req = buildRootPromiseCreateReq(
                prefixedId, name, new Args(Arrays.asList(args), Map.of()), version, o.timeout(), o.target());

        CompletableFuture<Void> created = new CompletableFuture<>();
        Sub s = subscribe(prefixedId);
        Subscription sub = s.sub();
        boolean isNew = s.isNew();

        spawn(() -> {
            try {
                join(sender.promiseCreate(encodeCreateReq(req)));
            } catch (Throwable exc) {
                created.completeExceptionally(exc);
                throw sneaky(exc);
            }
            created.complete(null);

            if (isNew) {
                registerAndSettle(prefixedId, sub);
            }
        });

        return new ResonateHandle<>(prefixedId, sub, codec, returnType, created);
    }

    // -- get / schedule / stop --------------------------------------------------

    /**
     * Return a handle for an existing promise.
     *
     * <p>Asynchronous: the listener registration is what surfaces a {@link ServerError} (404) when the
     * promise does not exist. The result is untyped ({@code Object}, the analogue of Python's {@code
     * Any}) -- there is no local function to read a return annotation from.
     */
    public CompletableFuture<ResonateHandle<Object>> get(String id) {
        String pid = prefixId(id);
        Sub s = subscribe(pid);
        Subscription sub = s.sub();

        if (!s.isNew()) {
            return CompletableFuture.completedFuture(handleFor(pid, sub));
        }

        return sender.promiseRegisterListener(pid, network.unicast()).handle((record, exc) -> {
            if (exc != null) {
                Throwable cause = unwrap(exc);
                if (cause instanceof ResonateError) {
                    synchronized (runtime.subs) {
                        if (runtime.subs.get(pid) == sub && !sub.settled()) {
                            runtime.subs.remove(pid);
                        }
                    }
                }
                throw sneaky(cause);
            }
            if (!"pending".equals(record.state())) {
                settleAndCleanup(pid, sub, record.state(), record.value());
            }
            return handleFor(pid, sub);
        });
    }

    private ResonateHandle<Object> handleFor(String id, Subscription sub) {
        return new ResonateHandle<>(id, sub, codec, Object.class, CompletableFuture.completedFuture(null));
    }

    /** Create a schedule for periodic function execution. */
    public CompletableFuture<ResonateSchedule> schedule(
            String id,
            String cron,
            String funcName,
            List<Object> args,
            Map<String, Object> kwargs,
            Duration promiseTimeout,
            int version) {
        Duration pt = promiseTimeout != null ? promiseTimeout : DEFAULT_TOP_LEVEL_TIMEOUT;
        Value param = new Value(null, new TaskData(args, kwargs, funcName, version));
        return schedules
                .create(id, cron, idPrefix + "{{.id}}.{{.timestamp}}", timeoutMs(pt), param)
                .thenApply(record -> new ResonateSchedule(id, schedules));
    }

    /**
     * Tear down background jobs and the network. Idempotent.
     *
     * <p>The network is stopped first (clearing subscribers, so a re-dispatched job finds nobody
     * listening and the spawn chain terminates), then every remaining background job is joined, then
     * the refresh loop is cancelled and the heartbeat shut down.
     */
    public CompletableFuture<Void> stop() {
        runtime.stopping = true;

        Future<?> handle = runtime.refreshHandle;
        runtime.refreshHandle = null;
        if (handle != null) {
            handle.cancel(true);
        }

        try {
            network.stop().join();
        } catch (CompletionException exc) {
            // Suppress a ResonateError from network teardown (mirrors Python's contextlib.suppress).
            if (!(unwrap(exc) instanceof ResonateError)) {
                throw exc;
            }
        }

        while (true) {
            List<CompletableFuture<Void>> snapshot;
            synchronized (runtime.bgTasks) {
                snapshot = new ArrayList<>(runtime.bgTasks);
            }
            if (snapshot.isEmpty()) {
                break;
            }
            try {
                CompletableFuture.allOf(snapshot.toArray(new CompletableFuture[0]))
                        .join();
            } catch (CompletionException | CancellationException ignored) {
                // join surfaces the first failure; we only care that the jobs have settled.
            }
            // Drop settled jobs so a job that spawned a child after the snapshot is re-observed and the
            // loop still terminates once everything is done.
            synchronized (runtime.bgTasks) {
                runtime.bgTasks.removeIf(CompletableFuture::isDone);
            }
        }

        heartbeat.shutdown();
        executor.shutdownNow();
        return CompletableFuture.completedFuture(null);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private String prefixId(String id) {
        return idPrefix.isEmpty() ? id : idPrefix + id;
    }

    /** Recover the {@code (name, version)} a function object was registered under, raising if unregistered. */
    private NameVersion registeredKey(Method fn) {
        NameVersion recorded = registry.reverse(fn);
        if (recorded == null) {
            throw new FunctionNotFoundError(fn.getName());
        }
        return recorded;
    }

    /** Resolve a routing target: empty falls back to the group, a URL passes through, a name resolves. */
    String resolveTarget(String target) {
        String resolved = target != null ? target : network.group();
        if (resolved.contains("://")) {
            return resolved;
        }
        return network.targetResolver(resolved);
    }

    private PromiseCreateReq buildRootPromiseCreateReq(
            String prefixedId, String funcName, Args packed, int version, Duration timeout, String target) {
        Duration t = timeout != null ? timeout : DEFAULT_TOP_LEVEL_TIMEOUT;
        TaskData data = new TaskData(packed.args(), packed.kwargs(), funcName, version);
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("resonate:origin", prefixedId);
        // A genuine top-level root is its own lineage origin and id-generation prefix.
        tags.put("resonate:prefix", prefixedId);
        tags.put("resonate:branch", prefixedId);
        tags.put("resonate:parent", prefixedId);
        tags.put("resonate:scope", "global");
        tags.put("resonate:target", resolveTarget(target));
        return new PromiseCreateReq(prefixedId, Send.nowMs() + timeoutMs(t), new Value(null, data), tags);
    }

    /** Encode a plaintext root req's {@code param} through the codec for the wire. */
    private PromiseCreateReq encodeCreateReq(PromiseCreateReq req) {
        return new PromiseCreateReq(
                req.id(), req.timeoutAt(), codec.encode(req.param().data()), req.tags());
    }

    // -- subscription / handle path --------------------------------------------

    /** {@code (sub, isNew)} returned by {@link #subscribe}. */
    private record Sub(Subscription sub, boolean isNew) {}

    private Sub subscribe(String id) {
        synchronized (runtime.subs) {
            Subscription existing = runtime.subs.get(id);
            if (existing != null) {
                return new Sub(existing, false);
            }
            Subscription sub = new Subscription();
            runtime.subs.put(id, sub);
            return new Sub(sub, true);
        }
    }

    /**
     * Register a unicast listener for {@code id}; settle now if already done. Runs in a background job,
     * so failures are logged, not raised -- except a 404, which means the durable promise is gone, so
     * the subscription is settled with a synthetic rejection rather than left to hang.
     */
    private void registerAndSettle(String id, Subscription sub) {
        PromiseRecord record;
        try {
            record = join(sender.promiseRegisterListener(id, network.unicast()));
        } catch (ServerError exc) {
            if (exc.code() == 404) {
                settleSubscriptionGone(id, sub, exc.message());
                return;
            }
            LOGGER.log(Level.WARNING, "listener registration failed id=" + id, exc);
            return;
        } catch (ResonateError exc) {
            LOGGER.log(Level.WARNING, "listener registration failed id=" + id, exc);
            return;
        }
        if (!"pending".equals(record.state())) {
            settleAndCleanup(id, sub, record.state(), record.value());
        }
    }

    /** Settle {@code sub} rejected when the server reports the promise gone (404). */
    private void settleSubscriptionGone(String id, Subscription sub, String reason) {
        Value encoded = codec.encode(new ApplicationError(reason));
        settleAndCleanup(id, sub, "rejected", encoded);
    }

    /** Settle {@code sub} and remove {@code id} from the map. {@code value} is still in wire form. */
    private void settleAndCleanup(String id, Subscription sub, String state, Value value) {
        sub.settle(new PromiseResult(state, value));
        synchronized (runtime.subs) {
            if (runtime.subs.get(id) == sub) {
                runtime.subs.remove(id);
            }
        }
    }

    // -- message dispatch -------------------------------------------------------

    private void onMessage(Message msg) {
        if (msg instanceof ExecuteMsg em) {
            if (!runtime.stopping) {
                spawn(() -> boundedExecute(() -> core.onMessage(em.taskId(), em.version())));
            }
        } else if (msg instanceof UnblockMsg um) {
            PromiseRecord promise = um.promise();
            Subscription sub;
            synchronized (runtime.subs) {
                sub = runtime.subs.get(promise.id());
            }
            if (sub == null) {
                // No one waiting (already settled+cleaned, or a duplicate push).
                return;
            }
            settleAndCleanup(promise.id(), sub, promise.state(), promise.value());
        }
    }

    // -- background tasks -------------------------------------------------------

    /**
     * Run a core execution under the concurrency semaphore, holding a permit for its whole lifetime so
     * the number of leases held at once never exceeds the ceiling. The permit is acquired on the
     * background thread, so surplus jobs wait here before touching the network.
     */
    void boundedExecute(Runnable coro) {
        try {
            runtime.executeSema.acquire();
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            coro.run();
        } finally {
            runtime.executeSema.release();
        }
    }

    /** Fire-and-forget a background body on {@link #executor}, retaining it for {@link #stop} and logging failures. */
    private void spawn(Runnable body) {
        CompletableFuture<Void> task = CompletableFuture.runAsync(body, executor);
        synchronized (runtime.bgTasks) {
            runtime.bgTasks.add(task);
        }
        task.whenComplete((value, exc) -> {
            synchronized (runtime.bgTasks) {
                runtime.bgTasks.remove(task);
            }
            if (exc != null && !(unwrap(exc) instanceof CancellationException)) {
                // Message-only, matching Python's ``logger.error("background task failed: %s", exc)``.
                LOGGER.log(Level.ERROR, "background task failed: {0}", unwrap(exc));
            }
        });
    }

    /** Re-register listeners for pending subscriptions every interval; defends against a dropped SSE connection. */
    private void runRefresh() {
        try {
            while (true) {
                Thread.sleep(refreshIntervalMs);
                String addr = network.unicast();

                List<Map.Entry<String, Subscription>> entries;
                synchronized (runtime.subs) {
                    entries = new ArrayList<>(runtime.subs.entrySet());
                }
                for (Map.Entry<String, Subscription> e : entries) {
                    String id = e.getKey();
                    Subscription sub = e.getValue();
                    if (sub.settled()) {
                        continue;
                    }
                    PromiseRecord record;
                    try {
                        record = join(sender.promiseRegisterListener(id, addr));
                    } catch (ServerError exc) {
                        if (exc.code() == 404) {
                            settleSubscriptionGone(id, sub, exc.message());
                            continue;
                        }
                        LOGGER.log(Level.WARNING, "subscription refresh failed id=" + id, exc);
                        continue;
                    } catch (ResonateError exc) {
                        LOGGER.log(Level.WARNING, "subscription refresh failed id=" + id, exc);
                        continue;
                    }
                    if (!"pending".equals(record.state())) {
                        settleAndCleanup(id, sub, record.state(), record.value());
                    }
                }
            }
        } catch (InterruptedException exc) {
            // Cancelled by stop().
            Thread.currentThread().interrupt();
        }
    }

    // -- module-level helpers ---------------------------------------------------

    private static int timeoutMs(Duration timeout) {
        return (int) timeout.toMillis();
    }

    private static int safeTtlMs(Duration ttl) {
        long ms = ttl.toMillis();
        return ms > 0 ? (int) Math.min(ms, Integer.MAX_VALUE) : (1 << 30);
    }

    private static Network selectNetwork(String url, Network network, String group, String pid, String auth) {
        if (url != null) {
            return new HttpNetwork(url, pid, group, auth, null);
        }
        if (network != null) {
            return network;
        }
        String envUrl = resolveEnvUrl();
        if (envUrl != null) {
            return new HttpNetwork(envUrl, pid, group, auth, null);
        }
        return new LocalNetwork(pid, group);
    }

    private static String resolveEnvUrl() {
        String envUrl = System.getenv("RESONATE_URL");
        if (envUrl != null && !envUrl.isEmpty()) {
            return envUrl;
        }
        String host = System.getenv("RESONATE_HOST");
        if (host == null || host.isEmpty()) {
            return null;
        }
        String scheme = System.getenv("RESONATE_SCHEME");
        if (scheme == null) {
            scheme = "http";
        }
        String port = System.getenv("RESONATE_PORT");
        if (port == null) {
            port = "8001";
        }
        return scheme + "://" + host + ":" + port;
    }

    /** Block on a durable {@link Sender} future, surfacing its underlying cause (the analogue of {@code await}). */
    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException exc) {
            throw sneaky(unwrap(exc));
        }
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
        throw (E) t;
    }
}
