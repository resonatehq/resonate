package io.resonatehq.resonate;

/**
 * Error types mirroring {@code resonate.error} from the Python SDK.
 *
 * <p>The Python module declares a base {@code ResonateError(Exception)} and 15 concrete subclasses.
 * We mirror those as nested classes inside {@link Errors}, all rooted at {@link ResonateError},
 * which extends {@link RuntimeException} — Python {@code Exception} is unchecked-like for callers
 * that don't catch, and {@link RuntimeException} preserves that ergonomics across the SDK.
 *
 * <p><b>Naming.</b> The Python classes use the {@code *Error} suffix and we keep those names
 * verbatim for cross-SDK parity, even though Java convention reserves {@code Error} for
 * unrecoverable JVM conditions ({@link java.lang.Error}) and uses {@code Exception} elsewhere.
 * Nesting under {@link Errors} disambiguates and prevents any clash with {@link java.lang.Error}
 * or {@link java.util.concurrent.TimeoutException}.
 *
 * <p><b>Message formatting.</b> Python {@code f"...: {error}"} calls {@code str(error)}, which for
 * an exception returns the message args. The closest Java analogue is {@link Throwable#getMessage()
 * Throwable.getMessage()} — not {@link Throwable#toString() Throwable.toString()}, which prefixes
 * the class name. We use {@code getMessage()} for cross-language string parity.
 *
 * <p><b>Causal chain.</b> Wrapper errors that carry an underlying {@link Throwable} pass it both as
 * the JDK {@link Throwable#getCause() cause} (so stack traces chain) and expose it via an {@code
 * error()} accessor that mirrors the Python {@code self.error} attribute.
 */
public final class Errors {
    private Errors() {}

    /**
     * Top-level error type for the Resonate SDK.
     *
     * <p>Constructors are {@code protected}: this class is meant to be caught (or matched on with
     * {@code instanceof}), not constructed directly. Use the relevant subclass instead.
     */
    public static class ResonateError extends RuntimeException {
        protected ResonateError(String message) {
            super(message);
        }

        protected ResonateError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // --- registry / lookup ---

    /** A function name (and version) is not registered. */
    public static final class FunctionNotFoundError extends ResonateError {
        private final String name;
        private final int version;

        public FunctionNotFoundError(String name, int version) {
            super("function not found: %s (version %d)".formatted(name, version));
            this.name = name;
            this.version = version;
        }

        /** Mirrors Python's default {@code version=1}. */
        public FunctionNotFoundError(String name) {
            this(name, 1);
        }

        public String name() {
            return name;
        }

        public int version() {
            return version;
        }
    }

    /** A function name (and version) is already registered. */
    public static final class AlreadyRegisteredError extends ResonateError {
        private final String name;
        private final int version;

        public AlreadyRegisteredError(String name, int version) {
            super("function '%s' (version %d) is already registered".formatted(name, version));
            this.name = name;
            this.version = version;
        }

        /** Mirrors Python's default {@code version=1}. */
        public AlreadyRegisteredError(String name) {
            this(name, 1);
        }

        public String name() {
            return name;
        }

        public int version() {
            return version;
        }
    }

    // --- server / transport ---

    /** The server returned a structured error code + message. */
    public static final class ServerError extends ResonateError {
        private final int code;
        private final String message;

        public ServerError(int code, String message) {
            super("server error (code=%d): %s".formatted(code, message));
            this.code = code;
            this.message = message;
        }

        public int code() {
            return code;
        }

        /** The raw server message — distinct from {@link #getMessage()}, which is the formatted form. */
        public String message() {
            return message;
        }
    }

    // --- codec: message-only wrappers ---

    /** Encoding the request side of the wire failed. */
    public static final class EncodingError extends ResonateError {
        private final String message;

        public EncodingError(String message) {
            super("encoding error: " + message);
            this.message = message;
        }

        /** The raw inner message — distinct from {@link #getMessage()}, which has the prefix. */
        public String message() {
            return message;
        }
    }

    /** Decoding the response side of the wire failed. */
    public static final class DecodingError extends ResonateError {
        private final String message;

        public DecodingError(String message) {
            super("decoding error: " + message);
            this.message = message;
        }

        /** The raw inner message — distinct from {@link #getMessage()}, which has the prefix. */
        public String message() {
            return message;
        }
    }

    // --- codec / transport: cause-carrying wrappers ---

    /** Wraps an arbitrary serialization failure. */
    public static final class SerializationError extends ResonateError {
        private final Throwable error;

        public SerializationError(Throwable error) {
            super("serialization error: " + (error == null ? "null" : error.getMessage()), error);
            this.error = error;
        }

        /** The wrapped cause; mirrors Python's {@code self.error}. Same value as {@link #getCause()}. */
        public Throwable error() {
            return error;
        }
    }

    /** Wraps an arbitrary HTTP-layer failure. */
    public static final class HttpError extends ResonateError {
        private final Throwable error;

        public HttpError(Throwable error) {
            super("http error: " + (error == null ? "null" : error.getMessage()), error);
            this.error = error;
        }

        /** The wrapped cause; mirrors Python's {@code self.error}. Same value as {@link #getCause()}. */
        public Throwable error() {
            return error;
        }
    }

    /** Wraps a base64 decoding failure. */
    public static final class Base64DecodeError extends ResonateError {
        private final Throwable error;

        public Base64DecodeError(Throwable error) {
            super("base64 decode error: " + (error == null ? "null" : error.getMessage()), error);
            this.error = error;
        }

        /** The wrapped cause; mirrors Python's {@code self.error}. Same value as {@link #getCause()}. */
        public Throwable error() {
            return error;
        }
    }

    /** Wraps a UTF-8 decoding failure. */
    public static final class Utf8Error extends ResonateError {
        private final Throwable error;

        public Utf8Error(Throwable error) {
            super("utf8 error: " + (error == null ? "null" : error.getMessage()), error);
            this.error = error;
        }

        /** The wrapped cause; mirrors Python's {@code self.error}. Same value as {@link #getCause()}. */
        public Throwable error() {
            return error;
        }
    }

    /** Wraps an underlying I/O failure. */
    public static final class IoError extends ResonateError {
        private final Throwable error;

        public IoError(Throwable error) {
            super("io error: " + (error == null ? "null" : error.getMessage()), error);
            this.error = error;
        }

        /** The wrapped cause; mirrors Python's {@code self.error}. Same value as {@link #getCause()}. */
        public Throwable error() {
            return error;
        }
    }

    // --- execution lifecycle ---

    /** Execution suspended (e.g. awaiting an external resolution). */
    public static final class SuspendedError extends ResonateError {
        public SuspendedError() {
            super("execution suspended");
        }
    }

    /** Attempted to settle a promise that is already settled. */
    public static final class AlreadySettledError extends ResonateError {
        public AlreadySettledError() {
            super("promise already settled");
        }
    }

    /** A task-join boundary surfaced an error message. */
    public static final class JoinError extends ResonateError {
        private final String message;

        public JoinError(String message) {
            super("task join error: " + message);
            this.message = message;
        }

        /** The raw inner message — distinct from {@link #getMessage()}, which has the prefix. */
        public String message() {
            return message;
        }
    }

    /**
     * User-supplied application error: the cross-SDK-safe escape hatch.
     *
     * <p>Python uses {@code raise ApplicationError("E_CODE: ...")} so an awaiter on any SDK can
     * match by message. The message is passed through unchanged with no prefix.
     */
    public static final class ApplicationError extends ResonateError {
        private final String message;

        public ApplicationError(String message) {
            super(message);
            this.message = message;
        }

        /** Same value as {@link #getMessage()}; mirrors Python's {@code self.message}. */
        public String message() {
            return message;
        }
    }

    /** Operation timed out. */
    public static final class TimeoutError extends ResonateError {
        public TimeoutError() {
            super("timeout");
        }
    }
}
