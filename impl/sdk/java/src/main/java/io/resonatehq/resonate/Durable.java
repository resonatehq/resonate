package io.resonatehq.resonate;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Types.Args;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runtime representation of a user function registered as durable, mirroring {@code
 * resonate.durable.DurableFunction} from the Python SDK.
 *
 * <p><b>The {@code inspect} → reflection mapping.</b> Python introspects a callable with {@code
 * inspect.signature}: parameter kinds, per-parameter type annotations, and the return annotation
 * drive both the ctx-first convention and the symmetric serialize/deserialize coercion. Java's
 * analogue is {@link java.lang.reflect.Method} — a {@code Method} (unlike a bare lambda) exposes its
 * parameter <em>types</em>, generic element types, varargs-ness, and return type at runtime, so a
 * durable function here is supplied as a reflected {@code Method} (plus an optional target instance
 * for instance methods).
 *
 * <p><b>Convention.</b> Every durable function takes a {@code Context} as its first positional
 * argument; the runtime strips that slot by position (it never inspects the slot's declared type)
 * and injects the context on each call. Only the <em>user</em> parameters round-trip through the
 * durable promise's {@code param} field as an {@link Args} slot.
 *
 * <p><b>Coercion.</b> On the dispatched/recovery path arguments arrive as JSON builtins (a {@code
 * Map} for a record, a {@code List} for an array) and must be reshaped to each parameter's declared
 * (generic) type via Jackson — the analogue of {@code msgspec.convert}. A parameter typed {@link
 * Object} carries nothing to reshape and passes through (the {@code Any} analogue), exactly as an
 * unannotated Python parameter does. The recovered return value is reshaped the same way through
 * {@link #coerceResult}, so the live and replay paths stay symmetric.
 *
 * <p><b>Divergences from Python (no Java analogue).</b> Java has no keyword arguments, keyword-only
 * or positional-only parameters, or default parameter values, so the kwargs slot is carried for
 * wire compatibility but never bound by name, and {@code apply_defaults} has no counterpart. Java
 * methods always carry concrete parameter types, so the "unresolved/forward-ref annotation"
 * tolerance and the {@code __globals__} fallback are likewise moot.
 */
public final class Durable {

    // Jackson is this repo's msgspec analogue (see Types / Codec); convertValue == msgspec.convert.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Object target; // null for a static method
    private final Method fn;
    private final String name;

    // The user parameters: everything after the stripped leading ctx slot.
    private final Type[] userTypes; // declared (generic) types, length == user param count
    private final boolean varargs; // whether the last user parameter is a Java varargs
    private final Class<?> varargComponent; // element type of that varargs, else null
    private final Type returnType;

    // Construction is from a raw Method: internal plumbing the ref-based entrypoints
    // ({@code Resonate}/{@code Context} run/rpc/register) resolve into. Package-private so the
    // public SDK surface is uniformly ref-based, never a raw Method.

    /** Build from a static (or already-target-bound) {@link Method}. */
    Durable(Method fn) {
        this(null, fn);
    }

    /**
     * Build from a {@code Method} and the instance it is invoked on ({@code null} for a static
     * method). The instance is the analogue of a Python bound method / callable instance — {@code
     * self} is already supplied, so the function's first user-visible parameter is still {@code
     * ctx}.
     */
    Durable(Object target, Method fn) {
        if (fn == null) {
            throw new ApplicationError("expected a callable, got null");
        }
        this.target = target;
        this.fn = fn;
        this.name = fn.getName();

        int paramCount = fn.getParameterCount();
        if (paramCount == 0) {
            throw new ApplicationError(name + ": durable function must accept a Context as its first argument");
        }
        // ``ctx`` is injected positionally and stripped by position, so the first parameter must be
        // able to hold it. In Java the only shape that cannot is a lone varargs parameter — the
        // tell-tale of an unwrapped (*args)-style wrapper — since varargs is always the last
        // parameter. Reject it with a clear message rather than failing confusingly later.
        if (fn.isVarArgs() && paramCount == 1) {
            throw new ApplicationError(name
                    + ": durable function must accept a Context as its first positional argument, but its"
                    + " only parameter is variadic");
        }

        Type[] generic = fn.getGenericParameterTypes();
        Class<?>[] raw = fn.getParameterTypes();
        int userCount = paramCount - 1;
        this.userTypes = new Type[userCount];
        System.arraycopy(generic, 1, this.userTypes, 0, userCount);
        this.varargs = fn.isVarArgs();
        this.varargComponent = varargs ? raw[paramCount - 1].getComponentType() : null;
        this.returnType = fn.getGenericReturnType();

        fn.setAccessible(true);
    }

    /** Source name (child context {@code func_name} + error messages). */
    public String name() {
        return name;
    }

    /**
     * Validate a call's arity and pack it into a serializable {@link Args}.
     *
     * <p>Called at dispatch ({@code ctx.run}) time. The injected {@code Context} is never part of
     * the payload. Java has no keyword arguments, so only positional {@code args} are packed; the
     * {@code kwargs} slot stays empty. Raises {@link ApplicationError} if the call does not match
     * the signature.
     */
    public Args packArgs(Object... args) {
        checkArity(args == null ? 0 : args.length);
        List<Object> list = new ArrayList<>();
        if (args != null) {
            for (Object a : args) {
                list.add(a);
            }
        }
        return new Args(List.copyOf(list), Map.of());
    }

    /**
     * Re-bind {@code packed} to the signature and execute the function.
     *
     * <p>When {@code coerceArgs} is {@code true} (the root / dispatched path) arguments are coerced
     * to their declared types: that path's args arrive as JSON builtins decoded from the persisted
     * param, so they must be reshaped. A local {@code ctx.run} child passes {@code false}: its
     * arguments are never serialized into the param, so the in-memory objects reach the function
     * verbatim. Arity is validated regardless.
     *
     * <p>{@code ctx} is injected as the first positional argument. The function's return value is
     * returned as-is.
     */
    public Object invoke(Object ctx, Args packed, boolean coerceArgs) {
        List<Object> provided = packed.args();
        checkArity(provided.size());

        Object[] call = bind(ctx, provided, coerceArgs);
        try {
            return fn.invoke(target, call);
        } catch (InvocationTargetException exc) {
            // Surface the user's exception unchanged (ApplicationError, etc.), mirroring Python's
            // bare propagation of a raising function.
            Throwable cause = exc.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new ApplicationError(name + ": " + (cause == null ? exc.getMessage() : cause.getMessage()));
        } catch (IllegalAccessException exc) {
            throw new ApplicationError(name + ": " + exc.getMessage());
        }
    }

    /** {@link #invoke(Object, Args, boolean)} with {@code coerceArgs = true} (the dispatched path). */
    public Object invoke(Object ctx, Args packed) {
        return invoke(ctx, packed, true);
    }

    /**
     * Reshape a recovered return value into the declared return type — the symmetric counterpart of
     * argument coercion. A pass-through return type ({@code Object} / {@code void} / {@code Void})
     * reshapes nothing.
     */
    public Object coerceResult(Object value) {
        return coerceValue(value, returnType, null);
    }

    /**
     * The declared return type as a value safe to hand to a coercion call. A pass-through annotation
     * collapses to {@link Object}, so a top-level decode against this type is the same no-op {@link
     * #coerceResult} performs for those cases.
     */
    public Type returnType() {
        return isPassthrough(returnType) ? Object.class : returnType;
    }

    // --- internals ---

    private void checkArity(int n) {
        int fixed = varargs ? userTypes.length - 1 : userTypes.length;
        boolean ok = varargs ? n >= fixed : n == fixed;
        if (!ok) {
            String shape = varargs ? "at least " + fixed : Integer.toString(fixed);
            throw new ApplicationError(name + ": expected " + shape + " positional argument(s), got " + n);
        }
    }

    /** Build the reflective argument array {@code [ctx, user-args...]}, coercing when asked. */
    private Object[] bind(Object ctx, List<Object> provided, boolean coerce) {
        int fixed = varargs ? userTypes.length - 1 : userTypes.length;
        // ctx + fixed user params (+ one collapsed varargs array if present)
        Object[] call = new Object[1 + fixed + (varargs ? 1 : 0)];
        call[0] = ctx;

        for (int i = 0; i < fixed; i++) {
            Object v = provided.get(i);
            call[1 + i] = coerce ? coerceValue(v, userTypes[i], name) : v;
        }

        if (varargs) {
            int tailLen = provided.size() - fixed;
            Object array = java.lang.reflect.Array.newInstance(varargComponent, tailLen);
            for (int i = 0; i < tailLen; i++) {
                Object v = provided.get(fixed + i);
                java.lang.reflect.Array.set(array, i, coerce ? coerceValue(v, varargComponent, name) : v);
            }
            call[call.length - 1] = array;
        }
        return call;
    }

    /**
     * Reshape one value to {@code type} via Jackson, or pass it through. A {@code type} that carries
     * nothing to reshape (see {@link #isPassthrough}) leaves the value untouched. {@code context},
     * when non-null, is the function name added to a failure's message (the analogue of Python's
     * {@code add_note}).
     */
    private static Object coerceValue(Object value, Type type, String context) {
        if (isPassthrough(type)) {
            return value;
        }
        try {
            return MAPPER.convertValue(value, MAPPER.getTypeFactory().constructType(type));
        } catch (IllegalArgumentException exc) {
            if (context == null) {
                throw new SerializationError(exc);
            }
            throw new SerializationError(new IllegalArgumentException(
                    exc.getMessage() + " (while binding arguments for " + context + ")", exc));
        }
    }

    /**
     * Whether {@code type} carries no information to reshape a value. The Java analogues of Python's
     * four pass-through cases: {@code Object} (the {@code Any} analogue), {@code void} / {@code
     * Void} (the {@code None} / {@code -> None} analogue), and {@code null} (a missing type).
     */
    private static boolean isPassthrough(Type type) {
        return type == null || type == Object.class || type == void.class || type == Void.class;
    }
}
