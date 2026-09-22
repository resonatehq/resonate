package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.resonatehq.resonate.Codec.Encryptor;
import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.Base64DecodeError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_codec.py}.
 *
 * <p>Behavioral tests (round-trip, null handling, error envelope, convert, decode_settled,
 * encryptor pipeline) port directly. The Python "user-defined struct styles" block exercises
 * msgspec/dataclass/attrs/NamedTuple/TypedDict/enum/pydantic — Python-ecosystem libraries with no
 * Java analogue — so those are replaced with the equivalent Java-side guarantee: a value reshapes
 * through {@code decode -> convert} into a Jackson record (value and type preserved), and a type
 * mismatch fails loudly as a {@link SerializationError}.
 */
class CodecTest {

    private static Codec codec() {
        return new Codec(new NoopEncryptor());
    }

    // -- encode/decode roundtrip for primitives -----------------------------------

    @Test
    void roundtripInteger() {
        Codec c = codec();
        assertEquals(42, c.decode(c.encode(42)));
    }

    @Test
    void roundtripString() {
        Codec c = codec();
        assertEquals("hello", c.decode(c.encode("hello")));
    }

    @Test
    void decodeFromEncodedDataRoundtrip() {
        Codec c = codec();
        Value encoded = c.encode(Map.of("x", 1));
        assertInstanceOf(String.class, encoded.data());
        assertEquals(Map.of("x", 1), c.decode(new Value(null, encoded.data())));
    }

    @Test
    void roundtripBool() {
        Codec c = codec();
        assertEquals(true, c.decode(c.encode(true)));
    }

    // -- encode/decode roundtrip for objects / arrays -----------------------------

    @Test
    void roundtripObject() {
        Codec c = codec();
        Map<String, Object> obj = Map.of("func", "f", "args", List.of(1, "two"));
        assertEquals(obj, c.decode(c.encode(obj)));
    }

    @Test
    void roundtripArray() {
        Codec c = codec();
        List<Integer> arr = List.of(1, 2, 3);
        assertEquals(arr, c.decode(c.encode(arr)));
    }

    // -- encode null produces empty data ------------------------------------------

    @Test
    void encodeNullProducesEmptyData() {
        Codec c = codec();
        Value encoded = c.encode(null);
        assertNull(encoded.data());
        assertNull(c.decode(encoded));
    }

    // -- decode_promise decodes both param and value ------------------------------

    @Test
    void decodePromiseDecodesParamAndValue() {
        Codec c = codec();
        Value paramEncoded = c.encode(Map.of("func", "f"));
        Value valueEncoded = c.encode(Map.of("result", 42));

        PromiseRecord record = new PromiseRecord("test", "resolved", paramEncoded, valueEncoded, Map.of(), 0L, 0L, 1L);

        PromiseRecord decoded = c.decodePromise(record);
        assertEquals(Map.of("func", "f"), decoded.param().data());
        assertEquals(Map.of("result", 42), decoded.value().data());
    }

    // -- decode invalid base64 returns error --------------------------------------

    @Test
    void decodeInvalidBase64ReturnsError() {
        Codec c = codec();
        assertThrows(ResonateError.class, () -> c.decode(new Value(null, "not-base64!!!")));
    }

    // -- encode error produces correct shape --------------------------------------

    @Test
    void encodeErrorProducesCorrectShape() {
        Map<String, String> encoded = Codec.encodeError(new ApplicationError("boom"));
        assertEquals("error", encoded.get("__type"));
        assertEquals("boom", encoded.get("message"));
    }

    // =============================================================================
    // Decoded values reshape through convert into a concrete type
    //
    // The Java analogue of the Python "user-defined struct styles" block: msgspec
    // is Jackson here, so the SDK's struct convention is a Java record, and a value
    // survives the durability boundary -- value and type preserved -- iff Jackson
    // can reshape the decoded builtins into it via convert (msgspec.convert).
    // =============================================================================

    record Point(@JsonProperty(required = true) int x, @JsonProperty(required = true) int y) {}

    record MixedShape(@JsonProperty(required = true) Point corner, String label) {}

    private static <T> T roundtrip(Object value, Class<T> type) {
        Codec c = codec();
        return c.convert(c.decode(c.encode(value)), type);
    }

    @Test
    void roundtripRecord() {
        Point p = new Point(3, 4);
        Point out = roundtrip(p, Point.class);
        assertEquals(p, out);
        assertInstanceOf(Point.class, out);
    }

    @Test
    void roundtripNestedRecord() {
        MixedShape shape = new MixedShape(new Point(1, 2), "tri");
        MixedShape out = roundtrip(shape, MixedShape.class);
        assertEquals(shape, out);
        assertInstanceOf(Point.class, out.corner());
    }

    @Test
    void roundtripNestedOptionalDefaultNull() {
        MixedShape shape = new MixedShape(new Point(0, 0), null);
        MixedShape out = roundtrip(shape, MixedShape.class);
        assertEquals(shape, out);
        assertNull(out.label());
    }

    // -- a collection's element type survives the boundary (Python's list-of-dataclasses case) --

    @Test
    void roundtripListOfRecords() {
        Codec c = codec();
        List<Point> points = List.of(new Point(1, 2), new Point(3, 4));
        @SuppressWarnings("unchecked")
        List<Point> out = (List<Point>) c.convert(
                c.decode(c.encode(points)),
                new com.fasterxml.jackson.core.type.TypeReference<List<Point>>() {}.getType());
        assertEquals(points, out);
        assertTrue(out.stream().allMatch(p -> p instanceof Point));
    }

    // -- a value Jackson cannot encode must fail loudly (Python's pydantic case) --

    @Test
    void encodeUnsupportedValueRaises() {
        // An empty bean has no serializable properties; Jackson's default
        // FAIL_ON_EMPTY_BEANS makes the codec surface a SerializationError rather
        // than silently emitting `{}`. Mirrors Python pinning pydantic as unencodable.
        Codec c = codec();
        assertThrows(SerializationError.class, () -> c.encode(new Object()));
    }

    // =============================================================================
    // decode returns null for absent/empty/null data
    // =============================================================================

    @Test
    void decodeNullDataReturnsNull() {
        assertNull(codec().decode(new Value(null, null)));
    }

    @Test
    void decodeEmptyStringDataReturnsNull() {
        assertNull(codec().decode(new Value(null, "")));
    }

    @Test
    void decodeDefaultValueReturnsNull() {
        assertNull(codec().decode(new Value()));
    }

    @Test
    void roundtripExplicitNull() {
        Codec c = codec();
        Value encoded = c.encode(null);
        assertNull(encoded.data());
        assertNull(c.decode(encoded));
    }

    // =============================================================================
    // decode surfaces corruption as a loud, typed error
    // =============================================================================

    @Test
    void decodeValidBase64ButNotJsonRaises() {
        Codec c = codec();
        // Valid base64, but the bytes (0xff 0xff) are not valid UTF-8/JSON.
        String notJson = java.util.Base64.getEncoder().encodeToString(new byte[] {(byte) 0xff, (byte) 0xff});
        assertThrows(SerializationError.class, () -> c.decode(new Value(null, notJson)));
    }

    @Test
    void decodeBase64WithWhitespaceRaises() {
        // The basic base64 decoder rejects embedded whitespace rather than silently
        // stripping it (the analogue of msgspec's validate=True), so a mangled payload fails fast.
        Codec c = codec();
        String valid = (String) c.encode(Map.of("x", 1)).data();
        String mangled = valid.substring(0, 2) + " " + valid.substring(2);
        assertThrows(Base64DecodeError.class, () -> c.decode(new Value(null, mangled)));
    }

    // =============================================================================
    // convert reshapes builtins and fails loudly on a type mismatch
    // =============================================================================

    @Test
    void convertCoercesBuiltinsIntoType() {
        assertEquals(new Point(1, 2), codec().convert(Map.of("x", 1, "y", 2), Point.class));
    }

    @Test
    void convertTypeMismatchRaises() {
        assertThrows(SerializationError.class, () -> codec().convert("not-an-int", Integer.class));
    }

    @Test
    void convertMissingFieldRaises() {
        assertThrows(SerializationError.class, () -> codec().convert(Map.of("x", 1), Point.class));
    }

    // =============================================================================
    // ApplicationError encodes to the tagged error envelope and decodes back
    // =============================================================================

    @Test
    void encodeApplicationErrorProducesEnvelope() {
        Codec c = codec();
        @SuppressWarnings("unchecked")
        Map<String, Object> decoded = (Map<String, Object>) c.decode(c.encode(new ApplicationError("boom")));
        assertEquals("error", decoded.get("__type"));
        assertEquals("boom", decoded.get("message"));
        // The original exception is also natively serialized best-effort so a same-runtime awaiter
        // can recover its exact type (see Codec#encodeError; analogue of Python's __py_pickle).
        assertTrue(decoded.containsKey("__java_serialized"));
    }

    @Test
    void decodeErrorRoundtripsApplicationError() {
        Codec c = codec();
        Throwable err = c.decodeError(c.encode(new ApplicationError("boom")));
        assertInstanceOf(ApplicationError.class, err);
        assertEquals("boom", err.getMessage());
    }

    @Test
    void decodeErrorOnEmptyValue() {
        Codec c = codec();
        Throwable err = c.decodeError(new Value());
        assertInstanceOf(ApplicationError.class, err);
        assertTrue(err.getMessage().contains("unknown error"));
    }

    // =============================================================================
    // deserializeError: only a string `message` is trusted
    // =============================================================================

    @Test
    void deserializeErrorDictWithStringMessage() {
        Throwable err = Codec.deserializeError(Map.of("__type", "error", "message", "boom"));
        assertInstanceOf(ApplicationError.class, err);
        assertEquals("boom", err.getMessage());
    }

    @Test
    void deserializeErrorDictWithoutMessage() {
        Throwable err = Codec.deserializeError(Map.of("code", 500));
        assertInstanceOf(ApplicationError.class, err);
        assertTrue(err.getMessage().contains("unknown error"));
        assertTrue(err.getMessage().contains("500"));
    }

    @Test
    void deserializeErrorDictWithNonStringMessage() {
        Throwable err = Codec.deserializeError(Map.of("message", 42));
        assertInstanceOf(ApplicationError.class, err);
        assertTrue(err.getMessage().contains("unknown error"));
    }

    @Test
    void deserializeErrorNonDictValues() {
        for (Object value : java.util.Arrays.asList("plain string", 42, List.of(1, 2, 3), null)) {
            Throwable err = Codec.deserializeError(value);
            assertInstanceOf(ApplicationError.class, err);
            assertTrue(err.getMessage().contains("unknown error"));
        }
    }

    // =============================================================================
    // decode_settled maps a decoded record to its result or the matching error
    // =============================================================================

    private static PromiseRecord settled(String state, Object data) {
        return new PromiseRecord("p", state, new Value(), new Value(null, data), Map.of(), 0L, 0L, null);
    }

    @Test
    void decodeSettledResolvedReturnsValue() throws Throwable {
        assertEquals(Map.of("result", 42), Codec.decodeSettled(settled("resolved", Map.of("result", 42))));
    }

    @Test
    void decodeSettledResolvedNoneValue() throws Throwable {
        assertNull(Codec.decodeSettled(settled("resolved", null)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"rejected", "rejected_canceled", "rejected_timedout"})
    void decodeSettledRejectedStatesRaiseError(String state) {
        PromiseRecord record = settled(state, Map.of("__type", "error", "message", "boom"));
        ApplicationError err = assertThrows(ApplicationError.class, () -> Codec.decodeSettled(record));
        assertEquals("boom", err.getMessage());
    }

    @Test
    void decodeSettledPendingRaisesUnexpectedState() {
        assertThrows(AssertionError.class, () -> Codec.decodeSettled(settled("pending", null)));
    }

    // =============================================================================
    // Full rejected-value path: encode error -> decode_promise -> decode_settled
    // =============================================================================

    @Test
    void rejectedPromiseRoundtripRaisesOriginatingError() {
        Codec c = codec();
        PromiseRecord record = new PromiseRecord(
                "p", "rejected", new Value(), c.encode(new ApplicationError("kaboom")), Map.of(), 0L, 0L, null);
        PromiseRecord decoded = c.decodePromise(record);
        ApplicationError err = assertThrows(ApplicationError.class, () -> Codec.decodeSettled(decoded));
        assertEquals("kaboom", err.getMessage());
    }

    // =============================================================================
    // decode_promise carries every field but the two decoded payloads
    // =============================================================================

    @Test
    void decodePromisePreservesOtherFields() {
        Codec c = codec();
        PromiseRecord record = new PromiseRecord(
                "abc",
                "resolved",
                new Value(Map.of("h", "1"), c.encode(Map.of("a", 1)).data()),
                new Value(Map.of("h", "2"), c.encode(Map.of("b", 2)).data()),
                Map.of("k", "v"),
                99L,
                7L,
                8L);
        PromiseRecord decoded = c.decodePromise(record);

        assertEquals("abc", decoded.id());
        assertEquals("resolved", decoded.state());
        assertEquals(99L, decoded.timeoutAt());
        assertEquals(Map.of("k", "v"), decoded.tags());
        assertEquals(7L, decoded.createdAt());
        assertEquals(8L, decoded.settledAt());
        // Headers ride through untouched while only data is decoded.
        assertEquals(Map.of("h", "1"), decoded.param().headers());
        assertEquals(Map.of("h", "2"), decoded.value().headers());
        assertEquals(Map.of("a", 1), decoded.param().data());
        assertEquals(Map.of("b", 2), decoded.value().data());
    }

    @Test
    void decodePromiseWithDefaultValues() {
        Codec c = codec();
        PromiseRecord record = new PromiseRecord("p", "pending", new Value(), new Value(), Map.of(), 0L, 0L, null);
        PromiseRecord decoded = c.decodePromise(record);
        assertNull(decoded.param().data());
        assertNull(decoded.value().data());
    }

    // =============================================================================
    // Encryptor integration pins the pipeline order
    // =============================================================================

    /** Reverses bytes on encrypt and back on decrypt (its own inverse). */
    static final class ReversingEncryptor implements Encryptor {
        @Override
        public byte[] encrypt(byte[] data) {
            return reverse(data);
        }

        @Override
        public byte[] decrypt(byte[] data) {
            return reverse(data);
        }

        private static byte[] reverse(byte[] data) {
            byte[] out = new byte[data.length];
            for (int i = 0; i < data.length; i++) {
                out[i] = data[data.length - 1 - i];
            }
            return out;
        }
    }

    @Test
    void noopEncryptorIsIdentity() {
        Encryptor enc = new NoopEncryptor();
        assertEquals("hello", new String(enc.encrypt("hello".getBytes())));
        assertEquals("hello", new String(enc.decrypt("hello".getBytes())));
    }

    @Test
    void roundtripWithNontrivialEncryptor() {
        Codec c = new Codec(new ReversingEncryptor());
        Map<String, Object> value = Map.of("func", "f", "args", List.of(1, "two"));
        assertEquals(value, c.decode(c.encode(value)));
    }

    @Test
    void encryptorActuallyTransformsWireData() {
        Map<String, Object> value = Map.of("x", 1);
        Value plain = new Codec(new NoopEncryptor()).encode(value);
        Value encrypted = new Codec(new ReversingEncryptor()).encode(value);
        assertNotEquals(plain.data(), encrypted.data());
    }

    @Test
    void decodeFailsWhenDecryptorDoesNotMatchEncryptor() {
        Value encrypted = new Codec(new ReversingEncryptor()).encode(Map.of("x", 1));
        assertThrows(SerializationError.class, () -> new Codec(new NoopEncryptor()).decode(encrypted));
    }
}
