package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Types.Args;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_durable.py}.
 *
 * <p>Each Python test that has a Java analogue is ported. Python's {@code inspect}-driven machinery
 * maps to {@link java.lang.reflect.Method} reflection. Tests with no Java counterpart are skipped
 * with a one-line note: keyword args / keyword-only / positional-only parameters, default parameter
 * values, unresolved/forward-ref annotations, the {@code __globals__} fallback, msgspec-specific
 * strictness (bool-for-int, int→float widening), and pydantic/attrs (Java uses records).
 */
class DurableTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- a minimal context: Durable never inspects the ctx slot, it only injects it ---

    record Info(String id) {}

    record Ctx(Info info) {}

    private static Ctx context() {
        return new Ctx(new Info("root"));
    }

    record Point(int x, int y) {}

    // --- durable function bodies (reflected as Methods) ---

    static int leaf(Ctx ctx, int x) {
        return x * 2;
    }

    static String workflow(Ctx ctx, int x) {
        return ctx.info().id() + ":" + x;
    }

    static int ctxOnly(Ctx ctx) {
        return 42;
    }

    static int noArgs() {
        return 42;
    }

    static int firstParam(Object ctx, int unused, int x) {
        return x;
    }

    static int boom(Ctx ctx, int x) {
        throw new ApplicationError("boom");
    }

    static int variadic(Ctx ctx, int... args) {
        int s = 0;
        for (int a : args) {
            s += a;
        }
        return s;
    }

    static int sumPoint(Ctx ctx, Point p) {
        return p.x() + p.y();
    }

    static Point makePoint(Ctx ctx, int x, int y) {
        return new Point(x, y);
    }

    static int total(Ctx ctx, List<Integer> xs) {
        return xs.stream().mapToInt(Integer::intValue).sum();
    }

    static int sumPoints(Ctx ctx, Point... points) {
        int s = 0;
        for (Point p : points) {
            s += p.x() + p.y();
        }
        return s;
    }

    static Object anyParam(Ctx ctx, Object x) {
        return x;
    }

    static boolean maybe(Ctx ctx, Integer x) {
        return x == null;
    }

    static void sink(Ctx ctx, int x) {}

    static Object lonelyVarargs(Object... args) {
        return args;
    }

    static class Service {
        String step(Ctx ctx, int x) {
            return ctx.info().id() + ":" + x;
        }
    }

    // --- helpers ---

    private static Method m(String name, Class<?>... params) {
        try {
            return DurableTest.class.getDeclaredMethod(name, params);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /** Simulate the durability boundary: JSON-encode the packed Args, decode back to builtins. */
    private static Args roundtrip(Args packed) {
        try {
            return MAPPER.readValue(MAPPER.writeValueAsBytes(packed), Args.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // =========================================================================
    // Registration
    // =========================================================================

    @Test
    void leafRegistered() {
        assertEquals("leaf", new Durable(m("leaf", Ctx.class, int.class)).name());
    }

    @Test
    void ctxOnlyRegistered() {
        assertEquals("ctxOnly", new Durable(m("ctxOnly", Ctx.class)).name());
    }

    @Test
    void nonCallableRejected() {
        ApplicationError e = assertThrows(ApplicationError.class, () -> new Durable((Method) null));
        assertTrue(e.getMessage().contains("expected a callable"));
    }

    @Test
    void zeroArgFunctionRejected() {
        ApplicationError e = assertThrows(ApplicationError.class, () -> new Durable(m("noArgs")));
        assertTrue(e.getMessage().contains("must accept a Context"));
    }

    @Test
    void firstParamTreatedAsCtxRegardlessOfType() {
        // First param is the stripped ctx slot (here typed Object). The two remaining params are
        // the user arguments.
        Durable df = new Durable(m("firstParam", Object.class, int.class, int.class));
        assertEquals(new Args(List.of(7, 8), Map.of()), df.packArgs(7, 8));
        assertThrows(ApplicationError.class, () -> df.packArgs(1, 2, 3));
    }

    @Test
    void lonelyVarargsRejected() {
        // The analogue of an unwrapped (*args) decorator: a lone varargs parameter cannot hold ctx.
        ApplicationError e =
                assertThrows(ApplicationError.class, () -> new Durable(m("lonelyVarargs", Object[].class)));
        assertTrue(e.getMessage().contains("first positional argument"));
    }

    // =========================================================================
    // invoke: ctx injection, sync bodies, errors
    // =========================================================================

    @Test
    void invokeInjectsContext() {
        Durable df = new Durable(m("workflow", Ctx.class, int.class));
        assertEquals("root:7", df.invoke(context(), df.packArgs(7)));
    }

    @Test
    void invokeCtxOnlyNoUserArgs() {
        Durable df = new Durable(m("ctxOnly", Ctx.class));
        assertEquals(new Args(), df.packArgs());
        assertEquals(42, df.invoke(context(), new Args()));
    }

    @Test
    void leafCanIgnoreCtx() {
        Durable df = new Durable(m("leaf", Ctx.class, int.class));
        assertEquals(42, df.invoke(context(), df.packArgs(21)));
    }

    @Test
    void raisingFunctionPropagates() {
        Durable df = new Durable(m("boom", Ctx.class, int.class));
        ApplicationError e = assertThrows(ApplicationError.class, () -> df.invoke(context(), df.packArgs(1)));
        assertEquals("boom", e.getMessage());
    }

    @Test
    void functionReturningVoidIsNull() {
        Durable df = new Durable(m("sink", Ctx.class, int.class));
        assertNull(df.invoke(context(), df.packArgs(5)));
    }

    // =========================================================================
    // pack_args / invoke: arity and varargs round trip
    // =========================================================================

    @Test
    void packArgsValidatesArity() {
        Durable df = new Durable(m("leaf", Ctx.class, int.class));
        assertThrows(ApplicationError.class, () -> df.packArgs(1, 2));
    }

    @Test
    void packArgsMissingRequiredRaises() {
        Durable df = new Durable(m("leaf", Ctx.class, int.class));
        ApplicationError e = assertThrows(ApplicationError.class, df::packArgs);
        assertTrue(e.getMessage().contains("leaf"));
    }

    @Test
    void packArgsExcludesCtxParam() {
        // workflow is (ctx, x); passing two positionals (as if including ctx) overflows.
        Durable df = new Durable(m("workflow", Ctx.class, int.class));
        assertThrows(ApplicationError.class, () -> df.packArgs(context(), 7));
    }

    @Test
    void packArgsPacksIntoTypedArgs() {
        Durable df = new Durable(m("variadic", Ctx.class, int[].class));
        assertEquals(new Args(List.of(1, 2, 3), Map.of()), df.packArgs(1, 2, 3));
    }

    @Test
    void emptyVariadicPacksToEmptyArgs() {
        Durable df = new Durable(m("variadic", Ctx.class, int[].class));
        assertEquals(new Args(), df.packArgs());
    }

    @Test
    void variadicRoundTrip() {
        Durable df = new Durable(m("variadic", Ctx.class, int[].class));
        assertEquals(6, df.invoke(context(), roundtrip(df.packArgs(1, 2, 3))));
        assertEquals(0, df.invoke(context(), new Args()));
    }

    // =========================================================================
    // invoke: argument coercion on recovery (JSON builtins -> declared types)
    // =========================================================================

    @Test
    void structArgCoercedFromBuiltins() {
        Durable df = new Durable(m("sumPoint", Ctx.class, Point.class));
        // On recovery the arg arrives as a plain map, not a Point.
        Args payload = new Args(List.of(Map.of("x", 3, "y", 4)), Map.of());
        assertEquals(7, df.invoke(context(), payload));
    }

    @Test
    void coercionFailureRaisesSerializationError() {
        Durable df = new Durable(m("sumPoint", Ctx.class, Point.class));
        Args payload = new Args(List.of(Map.of("x", "not-an-int", "y", 4)), Map.of());
        assertThrows(SerializationError.class, () -> df.invoke(context(), payload));
    }

    @Test
    void coercionErrorNotesFunctionName() {
        Durable df = new Durable(m("sumPoint", Ctx.class, Point.class));
        Args payload = new Args(List.of(Map.of("x", "bad", "y", 1)), Map.of());
        SerializationError e = assertThrows(SerializationError.class, () -> df.invoke(context(), payload));
        assertTrue(e.getMessage().contains("sumPoint"));
    }

    @Test
    void listAnnotationCoercesElements() {
        Durable df = new Durable(m("total", Ctx.class, List.class));
        assertEquals(6, df.invoke(context(), new Args(List.of(List.of(1, 2, 3)), Map.of())));
    }

    @Test
    void varPositionalStructCoercion() {
        Durable df = new Durable(m("sumPoints", Ctx.class, Point[].class));
        Args payload = new Args(List.of(Map.of("x", 1, "y", 2), Map.of("x", 3, "y", 4)), Map.of());
        assertEquals(10, df.invoke(context(), payload));
    }

    @Test
    void unannotatedParamPassesThrough() {
        // Object is the Any analogue: the value reaches the function un-coerced.
        Durable df = new Durable(m("anyParam", Ctx.class, Object.class));
        assertEquals(Map.of("keep", "me"), df.invoke(context(), new Args(List.of(Map.of("keep", "me")), Map.of())));
    }

    @Test
    void optionalAnnotationAcceptsNull() {
        Durable df = new Durable(m("maybe", Ctx.class, Integer.class));
        List<Object> withNull = new java.util.ArrayList<>();
        withNull.add(null);
        assertEquals(true, df.invoke(context(), new Args(withNull, Map.of())));
        assertEquals(false, df.invoke(context(), df.packArgs(5)));
    }

    // =========================================================================
    // invoke: resilience to corrupt payloads (signature drift)
    // =========================================================================

    @Test
    void invokeTooManyArgsRaises() {
        Durable df = new Durable(m("leaf", Ctx.class, int.class));
        ApplicationError e =
                assertThrows(ApplicationError.class, () -> df.invoke(context(), new Args(List.of(1, 2), Map.of())));
        assertTrue(e.getMessage().contains("leaf"));
    }

    // =========================================================================
    // coerce_result: return-value coercion (symmetric counterpart of arg coercion)
    // =========================================================================

    @Test
    void coerceResultStructFromBuiltins() {
        Durable df = new Durable(m("makePoint", Ctx.class, int.class, int.class));
        assertEquals(new Point(1, 2), df.coerceResult(Map.of("x", 1, "y", 2)));
    }

    @Test
    void coerceResultFailureRaisesSerializationError() {
        Durable df = new Durable(m("makePoint", Ctx.class, int.class, int.class));
        assertThrows(SerializationError.class, () -> df.coerceResult(Map.of("x", "not-an-int", "y", 2)));
    }

    @Test
    void coerceResultPassthroughWhenObject() {
        Durable df = new Durable(m("anyParam", Ctx.class, Object.class));
        Map<String, String> sentinel = Map.of("raw", "dict");
        assertSame(sentinel, df.coerceResult(sentinel));
    }

    @Test
    void coerceResultPassthroughForVoid() {
        Durable df = new Durable(m("sink", Ctx.class, int.class));
        assertNull(df.coerceResult(null));
    }

    @Test
    void returnTypePassthroughCollapsesToObject() {
        assertEquals(Object.class, new Durable(m("anyParam", Ctx.class, Object.class)).returnType());
        assertEquals(Object.class, new Durable(m("sink", Ctx.class, int.class)).returnType());
        assertEquals(String.class, new Durable(m("workflow", Ctx.class, int.class)).returnType());
    }

    // =========================================================================
    // Replay parity: fresh in-process objects vs JSON-recovered builtins
    // =========================================================================

    @Test
    void replayParityStruct() {
        Durable df = new Durable(m("sumPoint", Ctx.class, Point.class));
        Args fresh = df.packArgs(new Point(3, 4));
        assertEquals(7, df.invoke(context(), fresh));
        assertEquals(7, df.invoke(context(), roundtrip(fresh)));
    }

    @Test
    void replayIsIdempotentAcrossRepeats() {
        Durable df = new Durable(m("variadic", Ctx.class, int[].class));
        Args payload = roundtrip(df.packArgs(1, 2, 3, 4));
        for (int i = 0; i < 5; i++) {
            assertEquals(10, df.invoke(context(), payload));
        }
        assertEquals("variadic", df.name());
    }

    @Test
    void distinctPayloadsDoNotInterfere() {
        Durable df = new Durable(m("leaf", Ctx.class, int.class));
        Args a = df.packArgs(1);
        Args b = df.packArgs(100);
        assertEquals(2, df.invoke(context(), a));
        assertEquals(200, df.invoke(context(), b));
        assertEquals(2, df.invoke(context(), a));
    }

    // =========================================================================
    // Registration: instance methods (the bound-method / callable-instance analogue)
    // =========================================================================

    @Test
    void boundMethodDropsTarget() throws Exception {
        Service svc = new Service();
        Method step = Service.class.getDeclaredMethod("step", Ctx.class, int.class);
        Durable df = new Durable(svc, step);
        assertEquals("step", df.name());
        assertEquals("root:9", df.invoke(context(), df.packArgs(9)));
    }

    // local-child path: coerceArgs=false leaves in-memory objects verbatim
    @Test
    void localChildPathSkipsArgCoercion() {
        Durable df = new Durable(m("sumPoint", Ctx.class, Point.class));
        Args fresh = df.packArgs(new Point(3, 4));
        assertEquals(7, df.invoke(context(), fresh, false));
    }
}
