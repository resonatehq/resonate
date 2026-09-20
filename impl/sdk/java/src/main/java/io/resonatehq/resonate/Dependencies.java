package io.resonatehq.resonate;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Type-keyed container for application dependencies (DB pools, clients, config).
 *
 * <p>Mirrors {@code resonate.dependencies.DependencyMap} from the Python SDK.
 *
 * <p><b>Type-keyed storage.</b> Python keys entries by {@code type(value)}, the concrete runtime
 * class of the stored value. The Java analogue is a {@link Class}-keyed heterogeneous container:
 * {@link #insert(Object)} keys by {@code value.getClass()} and {@link #get(Class)} performs a
 * checked cast via {@link Class#cast(Object)}. This preserves the Python behavior — register one
 * instance per concrete class, retrieve by that class — and makes the get type-safe without
 * unchecked casts at call sites.
 *
 * <p><b>Missing keys.</b> Python raises {@link KeyError} with a hint pointing at
 * {@code .with_dependency()}. We raise {@link NoSuchElementException} — the closest unchecked
 * "lookup miss" in the JDK — with the same hint, using {@link Class#getName()} for the type label
 * (analogue of Python's {@code __qualname__}).
 */
public final class Dependencies {

    private final Map<Class<?>, Object> map = new HashMap<>();

    /** Store a dependency, keyed by its concrete runtime class. */
    public <T> void insert(T value) {
        map.put(value.getClass(), value);
    }

    /** Retrieve a dependency by type. Throws {@link NoSuchElementException} if not found. */
    public <T> T get(Class<T> type) {
        Object value = map.get(type);
        if (value == null) {
            throw new NoSuchElementException("No dependency registered for `%s`. Register it using `.withDependency()`."
                    .formatted(type.getName()));
        }
        return type.cast(value);
    }
}
