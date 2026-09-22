package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Verifies {@link Fn#methodOf} recovers the exact Method behind a method reference. */
final class FnTest {

    static String one(Context ctx, String a) {
        return a;
    }

    // Overloaded name + a primitive arg: the descriptor must disambiguate, not the name alone.
    static int two(Context ctx, String a, int b) {
        return b;
    }

    static int two(Context ctx, int a) {
        return a;
    }

    @Test
    void resolvesMethodReferenceToExactOverload() throws Exception {
        Method m1 = Fn.methodOf((Fn.F1<String, String>) FnTest::one);
        assertEquals(FnTest.class.getDeclaredMethod("one", Context.class, String.class), m1);

        Method m2 = Fn.methodOf((Fn.F2<String, Integer, Integer>) FnTest::two);
        assertEquals(FnTest.class.getDeclaredMethod("two", Context.class, String.class, int.class), m2);

        Method m3 = Fn.methodOf((Fn.F1<Integer, Integer>) FnTest::two);
        assertEquals(FnTest.class.getDeclaredMethod("two", Context.class, int.class), m3);
    }
}
