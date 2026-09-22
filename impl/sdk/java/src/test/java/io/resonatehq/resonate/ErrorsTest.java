package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.resonatehq.resonate.Errors.AlreadyRegisteredError;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.Base64DecodeError;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.HttpError;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.ResonateTimeoutError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Errors.StoppedError;
import io.resonatehq.resonate.Errors.Suspended;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the message strings, accessors, and type hierarchy for every class in {@link Errors}.
 *
 * <p>The Python SDK has no dedicated {@code test_error.py}, so this file is the Java side's own
 * regression check that the port stays 1:1 with {@code resonate.error}. Each test mirrors what
 * {@code repr(str(e))} would assert in Python — the exact formatted message — plus the stored field
 * accessors. Wrapper errors additionally pin the JDK cause chain that {@code Throwable.getCause()}
 * reads.
 */
class ErrorsTest {

    // --- type hierarchy ---

    @Test
    void catchableErrorsExtendResonateError() {
        // ResonateError subclasses (Python's Exception-rooted ones) — caught by user code.
        // A sampling that covers every constructor shape (name+version, code+message, msg-only,
        // cause-only, no-arg). If the base hierarchy regresses, this fails fast.
        assertInstanceOf(ResonateError.class, new FunctionNotFoundError("f"));
        assertInstanceOf(ResonateError.class, new AlreadyRegisteredError("f"));
        assertInstanceOf(ResonateError.class, new ServerError(500, "boom"));
        assertInstanceOf(ResonateError.class, new StoppedError());
        assertInstanceOf(ResonateError.class, new DecodingError("x"));
        assertInstanceOf(ResonateError.class, new SerializationError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new HttpError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new Base64DecodeError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new ApplicationError("x"));
        assertInstanceOf(ResonateError.class, new ResonateTimeoutError());
        // ResonateError itself is a RuntimeException -- mirrors Python's unchecked-Exception base.
        assertInstanceOf(RuntimeException.class, new StoppedError());
    }

    @Test
    void controlFlowSignalsAreNotCatchableAsException() {
        // Suspended and PlatformError mirror Python's BaseException subclasses: a user's
        // catch (Exception) must NOT swallow them. In Java that means they extend java.lang.Error.
        // The negative — that they are NOT an Exception / ResonateError — is enforced at COMPILE
        // time: because both classes are final and extend Error, `someSuspended instanceof Exception`
        // is a compile error ("incompatible types"), a stronger guarantee than any runtime assert.
        assertInstanceOf(java.lang.Error.class, new Suspended());
        assertInstanceOf(java.lang.Error.class, new PlatformError(List.of(new StoppedError())));
    }

    @Test
    void exceptionCatchDoesNotSwallowSuspended() {
        // Mirrors test_platform_errors.test_except_exception_does_not_swallow_platform_error:
        // a body that does `catch (Exception)` must not bury the signal — it propagates past it.
        boolean[] swallowed = {false};
        Suspended propagated = null;
        try {
            try {
                throw new Suspended();
            } catch (Exception userCatch) { // user code's catch-all
                swallowed[0] = true;
            }
        } catch (Suspended s) { // SDK boundary: the signal reaches here intact
            propagated = s;
        }
        assertFalse(swallowed[0], "catch (Exception) must not swallow Suspended");
        assertNotNull(propagated, "Suspended must propagate past catch (Exception)");
        assertEquals("execution suspended", propagated.getMessage());
    }

    @Test
    void exceptionCatchDoesNotSwallowPlatformError() {
        boolean[] swallowed = {false};
        PlatformError propagated = null;
        try {
            try {
                throw new PlatformError(List.of(new ServerError(503, "server unavailable")));
            } catch (RuntimeException userCatch) { // even catch (RuntimeException) cannot bury it
                swallowed[0] = true;
            }
        } catch (PlatformError p) {
            propagated = p;
        }
        assertFalse(swallowed[0], "catch (RuntimeException) must not swallow PlatformError");
        assertNotNull(propagated, "PlatformError must propagate past catch (RuntimeException)");
    }

    @Test
    void suspendedCanBeCaughtDeliberately() {
        // The signal is still catchable when code opts in — directly, or as Error/Throwable
        // (the SDK boundary's release path relies on this).
        assertInstanceOf(Suspended.class, catchThrown(() -> {
            throw new Suspended();
        }));
        // Catchable as its supertype java.lang.Error too.
        Throwable viaError;
        try {
            throw new Suspended();
        } catch (java.lang.Error e) {
            viaError = e;
        }
        assertInstanceOf(Suspended.class, viaError);
    }

    /** Runs {@code body}, returning whatever Throwable it throws (caught as Throwable). */
    private static Throwable catchThrown(Runnable body) {
        try {
            body.run();
        } catch (Throwable t) {
            return t;
        }
        throw new AssertionError("expected the body to throw");
    }

    // --- registry / lookup ---

    @Test
    void functionNotFoundErrorFormatsAndExposesFields() {
        FunctionNotFoundError e = new FunctionNotFoundError("greet", 2);
        assertEquals("function not found: greet (version 2)", e.getMessage());
        assertEquals("greet", e.name());
        assertEquals(2, e.version());
    }

    @Test
    void functionNotFoundErrorDefaultsVersionToOne() {
        FunctionNotFoundError e = new FunctionNotFoundError("greet");
        assertEquals("function not found: greet (version 1)", e.getMessage());
        assertEquals(1, e.version());
    }

    @Test
    void alreadyRegisteredErrorFormatsAndExposesFields() {
        AlreadyRegisteredError e = new AlreadyRegisteredError("greet", 3);
        assertEquals("function 'greet' (version 3) is already registered", e.getMessage());
        assertEquals("greet", e.name());
        assertEquals(3, e.version());
    }

    @Test
    void alreadyRegisteredErrorDefaultsVersionToOne() {
        AlreadyRegisteredError e = new AlreadyRegisteredError("greet");
        assertEquals("function 'greet' (version 1) is already registered", e.getMessage());
        assertEquals(1, e.version());
    }

    // --- server / transport ---

    @Test
    void serverErrorFormatsAndPreservesRawMessage() {
        ServerError e = new ServerError(503, "service unavailable");
        assertEquals("server error (code=503): service unavailable", e.getMessage());
        assertEquals(503, e.code());
        // ``message()`` returns the raw input, not the formatted ``getMessage()``.
        assertEquals("service unavailable", e.message());
    }

    // --- execution lifecycle ---

    @Test
    void stoppedErrorHasFixedMessage() {
        assertEquals("execution stopped", new StoppedError().getMessage());
    }

    // --- codec: message-only wrappers ---

    @Test
    void decodingErrorFormatsAndPreservesRawMessage() {
        DecodingError e = new DecodingError("bad bytes");
        assertEquals("decoding error: bad bytes", e.getMessage());
        assertEquals("bad bytes", e.message());
    }

    // --- codec / transport: cause-carrying wrappers ---
    // Python: f"... error: {error}" calls str(error) which yields the message for an exception.
    // Java: ``Throwable.getMessage()`` is the closest analogue; ``toString()`` would prefix the
    // class name. We pin the ``getMessage()`` choice and the ``getCause()`` chain at the same time.

    @Test
    void serializationErrorWrapsCauseAndExposesAccessor() {
        RuntimeException cause = new RuntimeException("not picklable");
        SerializationError e = new SerializationError(cause);
        assertEquals("serialization error: not picklable", e.getMessage());
        assertSame(cause, e.error());
        assertSame(cause, e.getCause());
    }

    @Test
    void httpErrorWrapsCauseAndExposesAccessor() {
        RuntimeException cause = new RuntimeException("connect refused");
        HttpError e = new HttpError(cause);
        assertEquals("http error: connect refused", e.getMessage());
        assertSame(cause, e.error());
        assertSame(cause, e.getCause());
    }

    @Test
    void base64DecodeErrorWrapsCauseAndExposesAccessor() {
        RuntimeException cause = new RuntimeException("invalid pad");
        Base64DecodeError e = new Base64DecodeError(cause);
        assertEquals("base64 decode error: invalid pad", e.getMessage());
        assertSame(cause, e.error());
        assertSame(cause, e.getCause());
    }

    // --- application escape hatch ---

    @Test
    void applicationErrorPassesMessageThroughUnchanged() {
        // ApplicationError is the cross-SDK-safe escape hatch: ``getMessage()`` == ``message()`` ==
        // the raw input, with no prefix. Pinning this guards against accidentally adding one.
        ApplicationError e = new ApplicationError("E_RATE_LIMIT: try later");
        assertEquals("E_RATE_LIMIT: try later", e.getMessage());
        assertEquals("E_RATE_LIMIT: try later", e.message());
    }

    @Test
    void resonateTimeoutErrorHasFixedMessage() {
        assertEquals("timeout", new ResonateTimeoutError().getMessage());
    }

    // --- control-flow signals ---

    @Test
    void suspendedHasFixedMessage() {
        assertEquals("execution suspended", new Suspended().getMessage());
    }

    @Test
    void platformErrorWrapsSingleCause() {
        StoppedError inner = new StoppedError();
        PlatformError e = new PlatformError(List.of(inner));
        assertEquals("platform error: execution stopped", e.getMessage());
        // cause() returns the primary ResonateError; getCause() chains it for stack traces.
        assertSame(inner, e.cause());
        assertSame(inner, e.getCause());
        assertEquals(List.of(inner), e.causes());
    }

    @Test
    void platformErrorAggregatesMultipleCauses() {
        StoppedError first = new StoppedError();
        ServerError second = new ServerError(500, "boom");
        PlatformError e = new PlatformError(List.of(first, second));
        assertEquals("platform error: execution stopped; server error (code=500): boom", e.getMessage());
        // The first cause is the primary one the outer boundary unwraps to.
        assertSame(first, e.cause());
        assertSame(first, e.getCause());
        assertEquals(List.of(first, second), e.causes());
    }

    @Test
    void platformErrorRejectsEmptyCauses() {
        // Mirrors Python raising ValueError ("needs at least one cause") rather than asserting,
        // so the bug surfaces at construction instead of as a later out-of-bounds on cause().
        assertThrows(IllegalArgumentException.class, () -> new PlatformError(List.of()));
    }
}
