package io.resonatehq.resonate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.Base64DecodeError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.Value;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encoding/decoding of values for the durability boundary, mirroring {@code resonate.codec} from the
 * Python SDK.
 *
 * <p>Encode: {@code value -> JSON -> encrypt -> base64 -> Value{headers, data}}. Decode: {@code
 * Value{headers, data} -> base64 -> decrypt -> JSON -> value}. Python's {@code msgspec.json} is the
 * JSON layer there; here it is Jackson (same choice as {@link Types}), and {@code msgspec.convert}
 * maps to {@link ObjectMapper#convertValue}.
 *
 * <p><b>Error recovery payload — the one deliberate, documented divergence.</b> Python's {@code
 * _encode_error} attaches a best-effort {@code __py_pickle} field (a base64 pickle of the original
 * exception) so a same-runtime awaiter can recover the exact exception type and attributes. Java
 * cannot produce a Python pickle, so the analogue — already adopted in {@code
 * ErrorSerializationTest} — is native {@link java.io.Serializable}: we attach a best-effort {@code
 * __java_serialized} field instead. The cross-SDK contract is unchanged: the {@code {"__type":
 * "error", "message": ...}} envelope is byte-for-byte the same, and a peer that cannot use the
 * native payload (a foreign SDK, an unimportable class, a corrupt blob) falls back to {@code
 * message} exactly as the Python fallback path does.
 */
public final class Codec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Pluggable encrypt/decrypt over the JSON bytes. Mirrors Python's {@code Encryptor} Protocol. */
    public interface Encryptor {
        byte[] encrypt(byte[] data);

        byte[] decrypt(byte[] data);
    }

    /** No-op encryptor (passthrough). */
    public static final class NoopEncryptor implements Encryptor {
        @Override
        public byte[] encrypt(byte[] data) {
            return data;
        }

        @Override
        public byte[] decrypt(byte[] data) {
            return data;
        }
    }

    private final Encryptor encryptor;

    public Codec(Encryptor encryptor) {
        this.encryptor = encryptor;
    }

    /**
     * Encode a serializable value into the wire format.
     *
     * <p>An {@link Exception} is always treated as a rejection: it is flattened to the error shape by
     * {@link #encodeError} (a plain {@code message} string, plus a best-effort {@code
     * __java_serialized} payload). Python keys off {@code isinstance(value, Exception)}, which
     * excludes its {@code BaseException}-based control-flow signals; {@code instanceof Exception} is
     * the exact analogue — it excludes {@link Errors.Suspended} / {@link Errors.PlatformError}, which
     * extend {@link Error}.
     */
    public Value encode(Object value) {
        if (value == null) {
            return new Value(null, null);
        }

        byte[] jsonBytes;
        try {
            Object payload = (value instanceof Exception ex) ? encodeError(ex) : value;
            jsonBytes = MAPPER.writeValueAsBytes(payload);
        } catch (JsonProcessingException | IllegalArgumentException exc) {
            throw new SerializationError(exc);
        }

        byte[] encrypted = encryptor.encrypt(jsonBytes);
        return new Value(null, Base64.getEncoder().encodeToString(encrypted));
    }

    /**
     * Decode a wire-format value into plain Java builtins ({@link Map}, {@link java.util.List},
     * {@link String}, numbers, {@link Boolean}, or {@code null}).
     *
     * <p>Crosses the wire ({@code base64 -> decrypt -> JSON}) and stops at builtins; returns {@code
     * null} for empty or absent {@code data}. Reshaping into a concrete type is {@link #convert}'s
     * job.
     */
    public Object decode(Value value) {
        // Python's single guard `if not value.data` collapses absent, JSON null, and the empty
        // string to None -- each short-circuits before base64 is attempted.
        Object raw = value.data();
        if (raw == null) {
            return null;
        }
        String encoded = (String) raw;
        if (encoded.isEmpty()) {
            return null;
        }

        byte[] data;
        try {
            data = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException exc) {
            throw new Base64DecodeError(exc);
        }

        try {
            return MAPPER.readValue(encryptor.decrypt(data), Object.class);
        } catch (IOException exc) {
            throw new SerializationError(exc);
        }
    }

    /**
     * Coerce an already-decoded value into {@code type}.
     *
     * <p>Wraps {@link ObjectMapper#convertValue} (the analogue of {@code msgspec.convert}) so
     * type-shaping of decoded payloads stays inside the codec. Distinct from {@link #decode}, which
     * crosses the wire boundary: this only reshapes a value already on the Java side.
     */
    public <T> T convert(Object value, Class<T> type) {
        try {
            return MAPPER.convertValue(value, type);
        } catch (IllegalArgumentException exc) {
            throw new SerializationError(exc);
        }
    }

    /**
     * Coerce an already-decoded value into a reflective {@link java.lang.reflect.Type}.
     *
     * <p>The {@link java.lang.reflect.Type} overload of {@link #convert(Object, Class)}, used when the
     * target type is recovered at runtime — and the carrier for parameterized shapes whose element
     * types are erased on a raw {@link Class}, e.g. {@code List<Point>} (mirroring Python's
     * list-of-structs round-trip, which {@code msgspec.convert} expresses with a parameterized type) — e.g. {@link Durable#returnType()} for a top-level
     * {@code run} / {@code rpc} result, which may be a plain class, a primitive, or a parameterized
     * container. A pass-through {@code Object} target reshapes nothing, matching Python's {@code Any}.
     */
    public Object convert(Object value, java.lang.reflect.Type type) {
        try {
            return MAPPER.convertValue(value, MAPPER.getTypeFactory().constructType(type));
        } catch (IllegalArgumentException exc) {
            throw new SerializationError(exc);
        }
    }

    /**
     * Decode a rejected promise's wire {@code value} into its originating error.
     *
     * <p>Returns the original exception when its {@code __java_serialized} payload round-trips (same
     * runtime, classpath-resolvable type), otherwise an {@link ApplicationError}.
     */
    public Throwable decodeError(Value value) {
        return deserializeError(decode(value));
    }

    /**
     * Decode a promise's {@code param} and {@code value} payloads in place.
     *
     * <p>Only the two {@link Value#data()} payloads change; every other field — including each
     * {@link Value#headers()} — is carried through untouched (the analogue of {@code
     * msgspec.structs.replace}), so the record stays robust to new fields.
     */
    public PromiseRecord decodePromise(PromiseRecord promise) {
        Value param = new Value(promise.param().headers(), decode(promise.param()));
        Value value = new Value(promise.value().headers(), decode(promise.value()));
        return new PromiseRecord(
                promise.id(),
                promise.state(),
                param,
                value,
                promise.tags(),
                promise.timeoutAt(),
                promise.createdAt(),
                promise.settledAt());
    }

    // -- error envelope helpers (Python module-level `_encode_error` / `_deserialize_error`) -------

    static final String TYPE_KEY = "__type";
    static final String MESSAGE_KEY = "message";

    /**
     * Java analogue of Python's {@code __py_pickle}: a base64 of the natively serialized exception,
     * attached best-effort so a same-runtime awaiter recovers the exact type and attributes.
     */
    static final String SERIALIZED_KEY = "__java_serialized";

    /**
     * Encode an error for durable storage.
     *
     * <p>{@code message} is a plain string and is always present. {@code __java_serialized} is a
     * best-effort extra field (see {@link #SERIALIZED_KEY}); native serialization can fail
     * (non-serializable fields, ...), and on failure the field is simply omitted and the awaiter
     * falls back to {@code message}.
     */
    static Map<String, String> encodeError(Throwable err) {
        Map<String, String> encoded = new LinkedHashMap<>();
        encoded.put(TYPE_KEY, "error");
        // Python uses str(err); Throwable.getMessage() is the cross-language analogue (see Errors).
        // getMessage() may be null (an exception with no message); Python's str(err) is "" in that
        // case, so coalesce to keep `message` a non-null string and the envelope identical.
        String message = err.getMessage();
        encoded.put(MESSAGE_KEY, message == null ? "" : message);
        // Best-effort: any serialization failure leaves only `message`, and the awaiter falls back
        // to ApplicationError.
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(err);
            }
            encoded.put(SERIALIZED_KEY, Base64.getEncoder().encodeToString(bytes.toByteArray()));
        } catch (Exception ignored) {
            // leave only message
        }
        return encoded;
    }

    /**
     * Deserialize an error value from a rejected promise.
     *
     * <p>Prefers the original exception via {@code __java_serialized} when it round-trips; otherwise
     * — a foreign producer, an unresolvable class, a corrupt or absent payload — falls back to an
     * {@link ApplicationError} carrying {@code message}. The deserialized object is type-checked: a
     * payload that does not deserialize to a {@link Throwable} is treated as a failure, not returned
     * verbatim.
     *
     * <p>Note: native deserialization, like {@code pickle.loads}, instantiates arbitrary classes
     * from the promise value; this trusts the Resonate server and peer workers as the payload
     * source.
     */
    static Throwable deserializeError(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object serialized = map.get(SERIALIZED_KEY);
            if (serialized instanceof String s) {
                Throwable recovered = null;
                try {
                    byte[] bytes = Base64.getDecoder().decode(s);
                    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                        Object obj = in.readObject();
                        if (obj instanceof Throwable t) {
                            recovered = t;
                        }
                    }
                } catch (Exception ignored) {
                    // best-effort; fall back to message
                }
                if (recovered != null) {
                    return recovered;
                }
            }

            Object msg = map.get(MESSAGE_KEY);
            if (msg instanceof String ms) {
                return new ApplicationError(ms);
            }
        }

        String rendered;
        try {
            rendered = MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException exc) {
            rendered = String.valueOf(value);
        }
        return new ApplicationError("unknown error: " + rendered);
    }

    /**
     * Map an already-decoded, settled record to its value, throwing on rejection.
     *
     * <p>The record's {@code value} has already been decoded by {@link #decodePromise}, so a resolved
     * payload is returned as-is and any rejected payload is turned back into the originating error
     * via {@link #deserializeError}. Mirrors Python's {@code decode_settled}: the {@code assert state
     * != "pending"} maps to an {@link AssertionError}, and {@code raise} maps to a faithful rethrow
     * of the recovered {@link Throwable}.
     *
     * <p>Declares {@code throws Throwable} because a rejected payload may recover an arbitrary, even
     * checked, originating exception (see {@link #deserializeError}); the signature surfaces that
     * honestly rather than laundering it through an unchecked cast.
     */
    public static Object decodeSettled(PromiseRecord record) throws Throwable {
        if ("pending".equals(record.state())) {
            throw new AssertionError("decode_settled on a pending record: " + record.id());
        }
        return switch (record.state()) {
            case "resolved" -> record.value().data();
            case "rejected", "rejected_canceled", "rejected_timedout" -> throw deserializeError(
                    record.value().data());
            default -> null;
        };
    }
}
