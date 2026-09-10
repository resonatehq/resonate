package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.AlreadyRegisteredError;
import io.resonatehq.resonate.Retry.RetryPolicy;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps a {@code (name, version)} pair to a validated {@link Durable}, mirroring {@code
 * resonate.registry.Registry} from the Python SDK.
 *
 * <p>The version is explicit -- never "latest" -- so a lookup is deterministic regardless of what is
 * registered afterwards: a task records its version at create time and resolves the same
 * implementation on every replay.
 *
 * <p>Names are passed at register time rather than derived from the Java method, so they stay stable
 * across renames. The callable is wrapped in a {@link Durable}, which validates the ctx-first
 * convention and handles serializing/deserializing its arguments and return value.
 */
public final class Registry {

    /** A registered {@code (name, version)} pair, the analogue of Python's returned tuple. */
    public record NameVersion(String name, int version) {}

    private record Key(String name, int version) {}

    private final Map<Key, Durable> byKey = new HashMap<>();
    // Per-function retry policy override, looked up by (name, version) when a root task is
    // dispatched (a remote dispatch carries no policy on the wire). null (or absent) means "no
    // override" -- the SDK-wide default applies.
    private final Map<Key, RetryPolicy> policyByKey = new HashMap<>();
    // Reverse of byKey: function object -> its registered (name, version). Lets a caller holding the
    // function object recover what to dispatch by. Registering the same object under several keys
    // keeps the last one.
    private final Map<Method, NameVersion> byFn = new HashMap<>();

    // Raw-Method registration is internal plumbing: the ref-based overloads below resolve a method
    // reference ({@code Owner::fn}) to its Method and funnel through here. Package-private so the
    // public SDK surface is uniformly ref-based, never a raw Method.

    /** {@link #register(String, Method, int, RetryPolicy)} with {@code version=1}, no policy. */
    void register(String name, Method fn) {
        register(name, fn, 1, null);
    }

    /** {@link #register(String, Method, int, RetryPolicy)} with no policy override. */
    void register(String name, Method fn, int version) {
        register(name, fn, version, null);
    }

    /**
     * Validate {@code fn} and store it under {@code (name, version)}.
     *
     * <p>{@code retryPolicy} is a per-function override, applied when this function fails as a pure
     * (leaf) function running as a root task. {@code null} (the default) means no override -- the
     * SDK-wide default applies. A workflow body never retries regardless of the policy.
     */
    void register(String name, Method fn, int version, RetryPolicy retryPolicy) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        Key key = new Key(name, version);
        if (byKey.containsKey(key)) {
            throw new AlreadyRegisteredError(name, version);
        }
        byKey.put(key, new Durable(fn));
        policyByKey.put(key, retryPolicy);
        byFn.put(fn, new NameVersion(name, version));
    }

    // -- ref-based registration --------------------------------------------------
    // Mirrors the Python SDK passing the function object itself: a durable function is registered
    // by method reference ({@code Owner::fn}), never a raw Method. The {@code F0..F5} overloads
    // select by arity; each resolves the reference to its Method and stores it like the Method form.
    // Mirroring Python's {@code register(name, fn, version=1, retry_policy=None)}, {@code version}
    // defaults to 1 and {@code retryPolicy} defaults to null (no override).

    public <R> void register(String name, Fn.F0<R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <R> void register(String name, Fn.F0<R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <R> void register(String name, Fn.F0<R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    public <A, R> void register(String name, Fn.F1<A, R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <A, R> void register(String name, Fn.F1<A, R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <A, R> void register(String name, Fn.F1<A, R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    public <A, B, R> void register(String name, Fn.F2<A, B, R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <A, B, R> void register(String name, Fn.F2<A, B, R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <A, B, R> void register(String name, Fn.F2<A, B, R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    public <A, B, C, R> void register(String name, Fn.F3<A, B, C, R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <A, B, C, R> void register(String name, Fn.F3<A, B, C, R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <A, B, C, R> void register(String name, Fn.F3<A, B, C, R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    public <A, B, C, D, R> void register(String name, Fn.F4<A, B, C, D, R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <A, B, C, D, R> void register(String name, Fn.F4<A, B, C, D, R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <A, B, C, D, R> void register(String name, Fn.F4<A, B, C, D, R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    public <A, B, C, D, E, R> void register(String name, Fn.F5<A, B, C, D, E, R> ref) {
        register(name, Fn.methodOf(ref), 1, null);
    }

    public <A, B, C, D, E, R> void register(String name, Fn.F5<A, B, C, D, E, R> ref, int version) {
        register(name, Fn.methodOf(ref), version, null);
    }

    public <A, B, C, D, E, R> void register(
            String name, Fn.F5<A, B, C, D, E, R> ref, int version, RetryPolicy retryPolicy) {
        register(name, Fn.methodOf(ref), version, retryPolicy);
    }

    /** {@link #get(String, int)} with {@code version=1}. */
    public Durable get(String name) {
        return get(name, 1);
    }

    /** Return the {@link Durable} registered under {@code (name, version)}, or {@code null}. */
    public Durable get(String name, int version) {
        return byKey.get(new Key(name, version));
    }

    /**
     * Return the {@code (name, version)} {@code fn} was registered under, or {@code null}.
     *
     * <p>The inverse of {@link #get}: a caller holding the function object rather than its name uses
     * this to recover what to dispatch by. {@code null} means the object was never registered.
     */
    NameVersion reverse(Method fn) {
        return byFn.get(fn);
    }

    // -- ref-based reverse lookup ------------------------------------------------
    // Public analogue of Python's reverse(fn): callers pass the function object as a method
    // reference ({@code Owner::fn}) rather than a raw Method, keeping the public surface ref-based.
    // The F0..F5 overloads select by arity and funnel through the package-private Method form.

    public <R> NameVersion reverse(Fn.F0<R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    public <A, R> NameVersion reverse(Fn.F1<A, R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    public <A, B, R> NameVersion reverse(Fn.F2<A, B, R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    public <A, B, C, R> NameVersion reverse(Fn.F3<A, B, C, R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    public <A, B, C, D, R> NameVersion reverse(Fn.F4<A, B, C, D, R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    public <A, B, C, D, E, R> NameVersion reverse(Fn.F5<A, B, C, D, E, R> ref) {
        return reverse(Fn.methodOf(ref));
    }

    /** {@link #getPolicy(String, int)} with {@code version=1}. */
    public RetryPolicy getPolicy(String name) {
        return getPolicy(name, 1);
    }

    /** Return the per-function policy override, or {@code null} if there is none. */
    public RetryPolicy getPolicy(String name, int version) {
        return policyByKey.get(new Key(name, version));
    }
}
