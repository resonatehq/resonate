package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code tests/test_dependencies.py} from the Python SDK.
 *
 * <p>Each Python test maps 1:1 to a method here. Python's {@code msgspec.Struct(frozen=True)}
 * records are mirrored as Java records — both are immutable, value-typed, and keyed by their
 * concrete class in the dependency map.
 */
final class DependenciesTest {

    private record Config(String value) {}

    private record Counter(int count) {}

    @Test
    void insertAndGet() {
        Dependencies deps = new Dependencies();
        deps.insert(new Config("hello"));
        assertEquals("hello", deps.get(Config.class).value());
    }

    @Test
    void insertOverwritesSameType() {
        Dependencies deps = new Dependencies();
        deps.insert(new Config("first"));
        deps.insert(new Config("second"));
        assertEquals("second", deps.get(Config.class).value());
    }

    @Test
    void multipleDependenciesKeyedByType() {
        Dependencies deps = new Dependencies();
        deps.insert(new Config("multi"));
        deps.insert(new Counter(42));
        assertEquals("multi", deps.get(Config.class).value());
        assertEquals(42, deps.get(Counter.class).count());
    }

    @Test
    void getMissingRaises() {
        Dependencies deps = new Dependencies();
        // Python asserts pytest.raises(KeyError, match="with_dependency"). The Java analogue is
        // NoSuchElementException; we match on the camelCase Java method name `withDependency`.
        var ex = assertThrows(java.util.NoSuchElementException.class, () -> deps.get(Config.class));
        assertTrue(
                ex.getMessage().contains("withDependency"),
                "expected hint about withDependency, got: " + ex.getMessage());
    }
}
