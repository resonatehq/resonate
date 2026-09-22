package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.resonatehq.resonate.Errors.AlreadyRegisteredError;
import io.resonatehq.resonate.Registry.NameVersion;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_registry.py}.
 *
 * <p>Python's caller-supplied lookup name (stable across renames) maps directly; the registered
 * callable follows the ctx-first convention. The Python {@code fn.__name__} the wrapped
 * DurableFunction remembers maps to {@link Durable#name()} (the reflected {@code Method} name).
 */
class RegistryTest {

    record Ctx() {}

    static int leaf(Ctx ctx, int x) {
        return x;
    }

    static int flow(Ctx ctx, int x) {
        return x;
    }

    private static Method m(String name) {
        try {
            return RegistryTest.class.getDeclaredMethod(name, Ctx.class, int.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void registerAndGet() {
        Registry r = new Registry();
        r.register("leaf", m("leaf"));
        Durable df = r.get("leaf");
        assertNotNull(df);
        assertEquals("leaf", df.name());
    }

    @Test
    void customNameIsIndependentOfFnName() {
        Registry r = new Registry();
        r.register("custom", m("leaf"));
        Durable df = r.get("custom");
        assertNotNull(df);
        assertEquals("leaf", df.name()); // entry still remembers its source name
    }

    @Test
    void getUnknownReturnsNull() {
        assertNull(new Registry().get("missing"));
    }

    @Test
    void emptyNameRejected() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> new Registry().register("", m("leaf")));
        assertEquals("name is required", e.getMessage());
    }

    @Test
    void duplicateNameRejected() {
        Registry r = new Registry();
        r.register("dup", m("leaf"));
        AlreadyRegisteredError e = assertThrows(AlreadyRegisteredError.class, () -> r.register("dup", m("flow")));
        assertEquals("dup", e.name());
    }

    // --- Versioning ---

    @Test
    void defaultVersionIsOne() {
        Registry r = new Registry();
        r.register("leaf", m("leaf"));
        assertNotNull(r.get("leaf"));
        assertNotNull(r.get("leaf", 1));
    }

    @Test
    void sameNameDifferentVersionsCoexist() {
        Registry r = new Registry();
        r.register("flow", m("leaf"), 1);
        r.register("flow", m("flow"), 2);
        Durable v1 = r.get("flow", 1);
        Durable v2 = r.get("flow", 2);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotSame(v1, v2);
    }

    @Test
    void duplicateNameVersionRejected() {
        Registry r = new Registry();
        r.register("dup", m("leaf"), 2);
        r.register("dup", m("flow"), 3); // same name, different version is fine
        AlreadyRegisteredError e = assertThrows(AlreadyRegisteredError.class, () -> r.register("dup", m("flow"), 2));
        assertEquals(2, e.version());
    }

    @Test
    void unknownVersionReturnsNull() {
        Registry r = new Registry();
        r.register("leaf", m("leaf"), 1);
        assertNull(r.get("leaf", 2));
    }

    @Test
    void versionBelowOneRejected() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> new Registry().register("zero", m("leaf"), 0));
        assertEquals("version must be >= 1", e.getMessage());
    }

    // --- Reverse lookup ---

    @Test
    void reverseReturnsRegisteredKey() {
        Registry r = new Registry();
        r.register("custom", m("leaf"), 2);
        assertEquals(new NameVersion("custom", 2), r.reverse(m("leaf")));
    }

    @Test
    void reverseUnknownReturnsNull() {
        Registry r = new Registry();
        r.register("leaf", m("leaf"));
        assertNull(r.reverse(m("flow")));
    }

    @Test
    void reverseSameObjectKeepsLastKey() {
        Registry r = new Registry();
        r.register("a", m("leaf"), 1);
        r.register("b", m("leaf"), 2);
        assertEquals(new NameVersion("b", 2), r.reverse(m("leaf")));
    }
}
