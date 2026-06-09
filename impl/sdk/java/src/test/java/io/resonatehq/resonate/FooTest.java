package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FooTest {
    @Test
    void barReturnsBar() {
        assertEquals("bar", Foo.bar());
    }

    @Test
    void bazAppendsSuffix() {
        assertEquals("foo-baz", Foo.baz("foo"));
    }
}
