package io.resonatehq.resonate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Error types mirroring {@code resonate.error} from the Python SDK.
 *
 * <p>Python declares a base {@code ResonateError(Exception)} with ten concrete subclasses, plus two
 * control-flow signals — {@code Suspended} and {@code PlatformError} — that deliberately extend
 * {@code BaseException} rather than {@code Exception}. We mirror all of these as nested classes
 * inside {@link Errors}.
 *
 * <p><b>Two hierarchies, mirroring Python's two roots.</b>
 *
 * <ul>
 *   <li>{@link ResonateError} extends {@link RuntimeException} — the analogue of Python's
 *       {@code ResonateError(Exception)}. Unchecked, and meant to be caught/matched by callers.
 *   <li>{@link Suspended} and {@link PlatformError} extend {@link java.lang.Error} — the analogue of
 *       Python's {@code BaseException}. In Python these sit beside {@code Exception} so a user's
 *       {@code except Exception} cannot swallow them; the closest Java analogue is
 *       {@link java.lang.Error}, which is unchecked and which application code is documented not to
 *       catch (a plain {@code catch (Exception e)} / {@code catch (RuntimeException e)} will not
 *       catch it). These are SDK control-flow signals: the task must be released, not fulfilled.
 * </ul>
 *
 * <p><b>Naming.</b> Class names track Python verbatim ({@code Suspended}, {@code PlatformError},
 * {@code ResonateTimeoutError}, the {@code *Error} subclasses) for cross-SDK parity, even though
 * Java convention reserves {@code Error} for unrecoverable JVM conditions. Nesting under
 * {@link Errors} disambiguates and prevents any clash with {@link java.lang.Error} or
 * {@link java.util.concurrent.TimeoutException}.
 *
 * <p><b>Message formatting.</b> Python's {@code f"...: {error}"} calls {@code str(error)}, which for
 * an exception returns its message args. The closest Java analogue is {@link Throwable#getMessage()
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

    // --- execution lifecycle ---

    /**
     * Skipped op after a prior failure stopped the execution.
     *
     * <p>Not a server failure — the network was never touched.
     */
    public static final class StoppedError extends ResonateError {

        public StoppedError() {
            super("execution stopped");
        }
    }

    // --- codec: message-only wrappers ---

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

    // --- application escape hatch ---

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
    public static final class ResonateTimeoutError extends ResonateError {

        public ResonateTimeoutError() {
            super("timeout");
        }
    }

    // --- control-flow signals: NOT meant to be caught by user code ---
    // Mirrors Python's BaseException subclasses. They extend java.lang.Error so a user's
    // catch (Exception) / catch (RuntimeException) cannot swallow them — the task must be released,
    // not fulfilled.

    /**
     * Signals that an execution has suspended.
     *
     * <p>Mirrors Python's {@code Suspended(BaseException)}. Extends {@link java.lang.Error} so a
     * {@code try/catch (Exception)} in user code does not swallow it.
     */
    public static final class Suspended extends java.lang.Error {

        public Suspended() {
            super("execution suspended");
        }
    }

    /**
     * A Resonate platform failure inside a durable execution.
     *
     * <p>Mirrors Python's {@code PlatformError(BaseException)}. Extends {@link java.lang.Error} (like
     * {@link Suspended}) so user code's {@code catch (Exception)} cannot swallow it; the task must be
     * released, not fulfilled. Always constructed from the original {@link ResonateError}, which is
     * also kept on {@link #causes()} and chained as the JDK {@link #getCause() cause}.
     *
     * <p>Always carries a <em>list</em> of causes: a single durable op failing wraps one error,
     * while a flush of concurrent local work aggregates every failure into one error with all
     * causes. {@link #cause()} returns the first (primary) one so the outer-boundary unwrap keeps
     * surfacing a single {@link ResonateError}.
     */
    public static final class PlatformError extends java.lang.Error {
        private final List<ResonateError> causes;

        public PlatformError(List<ResonateError> causes) {
            // Mirrors Python: a ValueError (here, IllegalArgumentException) rather than an assert,
            // so an empty-causes bug surfaces here instead of as a later out-of-bounds on cause().
            super(
                    "platform error: "
                            + requireNonEmpty(causes).stream()
                                    .map(Throwable::getMessage)
                                    .collect(Collectors.joining("; ")),
                    causes.get(0));
            this.causes = List.copyOf(causes);
        }

        private static List<ResonateError> requireNonEmpty(List<ResonateError> causes) {
            if (causes == null || causes.isEmpty()) {
                throw new IllegalArgumentException("PlatformError needs at least one cause");
            }
            return causes;
        }

        /** All causes, in order. The single-op case has exactly one. */
        public List<ResonateError> causes() {
            return causes;
        }

        /** The first (primary) cause — what the outer boundary unwraps to. */
        public ResonateError cause() {
            return causes.get(0);
        }
    }
}
