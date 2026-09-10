package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.ApplicationError;
import java.io.Serializable;
import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

/**
 * Method-reference plumbing for durable functions, shared by {@link Resonate} (register/run/rpc) and
 * {@link Context} (run).
 *
 * <p>Functions are supplied as method references ({@code Owner::fn}). The {@code F1..F5} interfaces
 * are the concrete functional-interface targets a reference binds to (selected by arity), carrying
 * the function's return type {@code R} for handle inference -- a {@code Serializable} param could not
 * accept a method reference, since a method reference needs a functional-interface target. The
 * {@link #methodOf} helper reflects the reference back to its {@link Method} for (de)serialization,
 * so call sites stay free of {@code Class}/{@code String} boilerplate. These are the Java analogue of
 * Python passing the function object itself to register/run/rpc.
 */
public final class Fn {

    private Fn() {}

    public interface F0<R> extends Serializable {
        R apply(Context ctx);
    }

    public interface F1<A, R> extends Serializable {
        R apply(Context ctx, A a);
    }

    public interface F2<A, B, R> extends Serializable {
        R apply(Context ctx, A a, B b);
    }

    public interface F3<A, B, C, R> extends Serializable {
        R apply(Context ctx, A a, B b, C c);
    }

    public interface F4<A, B, C, D, R> extends Serializable {
        R apply(Context ctx, A a, B b, C c, D d);
    }

    public interface F5<A, B, C, D, E, R> extends Serializable {
        R apply(Context ctx, A a, B b, C c, D d, E e);
    }

    /**
     * Recover the {@link Method} behind an {@code Owner::fn} reference via its serialized lambda. The
     * implementing class, method name, and exact descriptor come straight out of {@link
     * SerializedLambda}; {@link MethodType#fromMethodDescriptorString} parses the descriptor so an
     * overloaded name still resolves to the right method.
     */
    static Method methodOf(Serializable ref) {
        try {
            Method writeReplace = ref.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda sl = (SerializedLambda) writeReplace.invoke(ref);
            ClassLoader cl = Fn.class.getClassLoader();
            Class<?> impl = Class.forName(sl.getImplClass().replace('/', '.'), false, cl);
            MethodType mt = MethodType.fromMethodDescriptorString(sl.getImplMethodSignature(), cl);
            return impl.getDeclaredMethod(sl.getImplMethodName(), mt.parameterArray());
        } catch (ReflectiveOperationException exc) {
            throw new ApplicationError("could not resolve method reference: " + exc);
        }
    }
}
