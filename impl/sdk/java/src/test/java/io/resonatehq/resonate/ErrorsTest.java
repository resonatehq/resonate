package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.resonatehq.resonate.Errors.AlreadyRegisteredError;
import io.resonatehq.resonate.Errors.AlreadySettledError;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.Base64DecodeError;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.EncodingError;
import io.resonatehq.resonate.Errors.FunctionNotFoundError;
import io.resonatehq.resonate.Errors.HttpError;
import io.resonatehq.resonate.Errors.IoError;
import io.resonatehq.resonate.Errors.JoinError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Errors.SuspendedError;
import io.resonatehq.resonate.Errors.TimeoutError;
import io.resonatehq.resonate.Errors.Utf8Error;
import org.junit.jupiter.api.Test;

/**
 * Pins the message strings and accessors for every class in {@link Errors}.
 *
 * <p>The Python SDK has no dedicated {@code test_error.py}, so this file is the Java side's own
 * regression check. Each test mirrors what {@code repr(str(e))} would assert in Python — the exact
 * formatted message — plus the stored field accessors. Wrapper errors additionally pin the JDK
 * cause chain that {@code Throwable.getCause()} reads.
 */
class ErrorsTest {

    // --- type hierarchy: every concrete error must descend from ResonateError ---

    @Test
    void allErrorsExtendResonateError() {
        // A sampling that covers every constructor shape (name+version, code+message, msg-only,
        // cause-only, no-arg). If the base hierarchy regresses, this fails fast.
        assertInstanceOf(ResonateError.class, new FunctionNotFoundError("f"));
        assertInstanceOf(ResonateError.class, new AlreadyRegisteredError("f"));
        assertInstanceOf(ResonateError.class, new ServerError(500, "boom"));
        assertInstanceOf(ResonateError.class, new EncodingError("x"));
        assertInstanceOf(ResonateError.class, new DecodingError("x"));
        assertInstanceOf(ResonateError.class, new SerializationError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new HttpError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new Base64DecodeError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new Utf8Error(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new IoError(new RuntimeException("x")));
        assertInstanceOf(ResonateError.class, new SuspendedError());
        assertInstanceOf(ResonateError.class, new AlreadySettledError());
        assertInstanceOf(ResonateError.class, new JoinError("x"));
        assertInstanceOf(ResonateError.class, new ApplicationError("x"));
        assertInstanceOf(ResonateError.class, new TimeoutError());
        // ResonateError itself is a RuntimeException -- mirrors Python's unchecked-Exception base.
        assertInstanceOf(RuntimeException.class, new SuspendedError());
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

    // --- codec: message-only wrappers ---

    @Test
    void encodingErrorFormatsAndPreservesRawMessage() {
        EncodingError e = new EncodingError("bad bytes");
        assertEquals("encoding error: bad bytes", e.getMessage());
        assertEquals("bad bytes", e.message());
    }

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

    @Test
    void utf8ErrorWrapsCauseAndExposesAccessor() {
        RuntimeException cause = new RuntimeException("invalid sequence");
        Utf8Error e = new Utf8Error(cause);
        assertEquals("utf8 error: invalid sequence", e.getMessage());
        assertSame(cause, e.error());
        assertSame(cause, e.getCause());
    }

    @Test
    void ioErrorWrapsCauseAndExposesAccessor() {
        RuntimeException cause = new RuntimeException("disk full");
        IoError e = new IoError(cause);
        assertEquals("io error: disk full", e.getMessage());
        assertSame(cause, e.error());
        assertSame(cause, e.getCause());
    }

    // --- execution lifecycle ---

    @Test
    void suspendedErrorHasFixedMessage() {
        assertEquals("execution suspended", new SuspendedError().getMessage());
    }

    @Test
    void alreadySettledErrorHasFixedMessage() {
        assertEquals("promise already settled", new AlreadySettledError().getMessage());
    }

    @Test
    void joinErrorFormatsAndPreservesRawMessage() {
        JoinError e = new JoinError("worker died");
        assertEquals("task join error: worker died", e.getMessage());
        assertEquals("worker died", e.message());
    }

    @Test
    void applicationErrorPassesMessageThroughUnchanged() {
        // ApplicationError is the cross-SDK-safe escape hatch: ``getMessage()`` == ``message()`` ==
        // the raw input, with no prefix. Pinning this guards against accidentally adding one.
        ApplicationError e = new ApplicationError("E_RATE_LIMIT: try later");
        assertEquals("E_RATE_LIMIT: try later", e.getMessage());
        assertEquals("E_RATE_LIMIT: try later", e.message());
    }

    @Test
    void timeoutErrorHasFixedMessage() {
        assertEquals("timeout", new TimeoutError().getMessage());
    }
}
