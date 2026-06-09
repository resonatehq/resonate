package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Types.Args;
import io.resonatehq.resonate.Types.Info;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseRegisterCallbackData;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.TaskData;
import io.resonatehq.resonate.Types.TaskRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_types.py}.
 *
 * <p>Each Python test maps to a Java test that exercises the same wire-format guarantee using
 * Jackson in place of {@code msgspec.json}. Encoding tests assert exact byte output to lock in
 * field order; decoding tests assert that defaults from msgspec's {@code msgspec.field(default=...)}
 * are applied when the JSON omits the field.
 */
class TypesTest {

    // Default Jackson config: optional record components default to null / 0; properties marked
    // @JsonProperty(required=true) raise on absent input -- the analogue of msgspec's
    // ValidationError for missing required fields.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static byte[] encode(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T decode(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (JsonMappingException e) {
            // Surface as-is so assertThrows(JsonMappingException.class) can catch the mirror of
            // msgspec.ValidationError without an extra wrapper layer in between.
            throw new UncheckedJsonMappingException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Unchecked carrier that still preserves the original {@link JsonMappingException} type. */
    private static final class UncheckedJsonMappingException extends RuntimeException {
        UncheckedJsonMappingException(JsonMappingException cause) {
            super(cause);
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    // --- serialization: omit_defaults mirrors serde `skip_serializing_if = is_none` ---

    @Test
    void encodeEmptyValueIsEmptyObject() {
        assertArrayEquals(b("{}"), encode(new Value()));
    }

    @Test
    void encodeHeadersOnly() {
        assertArrayEquals(b("{\"headers\":{\"a\":\"b\"}}"), encode(new Value(Map.of("a", "b"), null)));
    }

    @Test
    void encodeDataOnly() {
        assertArrayEquals(b("{\"data\":42}"), encode(new Value(null, 42)));
    }

    @Test
    void encodeBothFieldsInFieldOrder() {
        assertArrayEquals(b("{\"headers\":{\"a\":\"b\"},\"data\":42}"), encode(new Value(Map.of("a", "b"), 42)));
    }

    // --- data_or_null ---

    @Test
    void dataOrNullDefaultsToNull() {
        assertNull(new Value().data());
    }

    @Test
    void dataOrNullReturnsData() {
        assertEquals(42, new Value(null, 42).data());
    }

    // --- wire decode: Jackson decodes the {headers, data} struct natively ---

    @Test
    void valueDecodesFromWireObject() {
        Value v = decode(b("{\"headers\":{\"a\":\"b\"},\"data\":1}"), Value.class);
        assertEquals(Map.of("a", "b"), v.headers());
        assertEquals(1, v.data());
    }

    @Test
    void valueDecodesEmptyObject() {
        Value v = decode(b("{}"), Value.class);
        assertNull(v.headers());
        assertNull(v.data());
    }

    @Test
    void promiseRecordDecodeMinimalAppliesDefaults() {
        // Only the required fields; the rest come from msgspec's `field(default=...)`.
        PromiseRecord r = decode(b("{\"id\":\"p1\",\"state\":\"pending\",\"timeoutAt\":10}"), PromiseRecord.class);
        assertNull(r.param().data());
        assertNull(r.param().headers());
        assertNull(r.value().data());
        assertEquals(Map.of(), r.tags());
        assertEquals(0L, r.createdAt());
        assertNull(r.settledAt());
    }

    @Test
    void promiseRecordDecodeMissingRequiredFieldRaises() {
        UncheckedJsonMappingException ex = assertThrows(
                UncheckedJsonMappingException.class,
                () -> decode(b("{\"id\":\"p1\",\"state\":\"pending\"}"), PromiseRecord.class));
        // The wrapped cause must be the real Jackson validation error -- the analogue of
        // msgspec.ValidationError.
        org.junit.jupiter.api.Assertions.assertInstanceOf(JsonMappingException.class, ex.getCause());
    }

    @Test
    void promiseRecordEncodeCamelAndFieldOrder() {
        PromiseRecord r =
                new PromiseRecord("p1", "pending", new Value(null, 1), new Value(), Map.of("k", "v"), 10L, 5L, null);
        assertArrayEquals(
                b("{\"id\":\"p1\",\"state\":\"pending\",\"param\":{\"data\":1},\"value\":{},"
                        + "\"tags\":{\"k\":\"v\"},\"timeoutAt\":10,\"createdAt\":5,\"settledAt\":null}"),
                encode(r));
    }

    // --- TaskRecord ---

    @Test
    void taskRecordDecodeMinimalAppliesDefaults() {
        TaskRecord r = decode(b("{\"id\":\"t1\",\"state\":\"pending\",\"version\":1}"), TaskRecord.class);
        assertNull(r.resumes());
        assertNull(r.ttl());
        assertNull(r.pid());
    }

    @Test
    void taskRecordResumesVariants() {
        // resumes: list[str] | int | bool | None
        record Case(String raw, Object expected) {}
        List<Case> cases = List.of(
                new Case("[\"a\",\"b\"]", List.of("a", "b")),
                new Case("5", 5),
                new Case("true", true),
                new Case("null", null));
        for (Case c : cases) {
            TaskRecord r = decode(
                    b("{\"id\":\"t\",\"state\":\"pending\",\"version\":1,\"resumes\":" + c.raw + "}"),
                    TaskRecord.class);
            assertEquals(c.expected, r.resumes());
            // Python's test guards against bool-is-int confusion (in Python, ``bool`` subclasses
            // ``int``). The equivalent worry in Java is autoboxing surprises between Boolean and
            // Integer. Verify the boxed scalar class exactly; for lists, just verify it's a List
            // (Jackson uses ArrayList rather than the unmodifiable factory class).
            if (c.expected == null) {
                assertNull(r.resumes());
            } else if (c.expected instanceof List) {
                org.junit.jupiter.api.Assertions.assertInstanceOf(List.class, r.resumes());
            } else {
                assertSame(c.expected.getClass(), r.resumes().getClass());
            }
        }
    }

    @Test
    void taskRecordEncode() {
        TaskRecord r = new TaskRecord("t1", "acquired", 2, List.of("a"), 30L, "x");
        assertArrayEquals(
                b("{\"id\":\"t1\",\"state\":\"acquired\",\"version\":2,\"resumes\":[\"a\"],\"ttl\":30,\"pid\":\"x\"}"),
                encode(r));
    }

    @Test
    void taskRecordEncodeDefaultsEmitNull() {
        TaskRecord r = new TaskRecord("t1", "pending", 1, null, null, null);
        assertArrayEquals(
                b("{\"id\":\"t1\",\"state\":\"pending\",\"version\":1,\"resumes\":null,\"ttl\":null,\"pid\":null}"),
                encode(r));
    }

    // --- PromiseCreateReq: camelCase wire format + default_with_id ---

    @Test
    void promiseCreateReqEncodeCamelAndFieldOrder() {
        PromiseCreateReq r = new PromiseCreateReq("p1", 10L, new Value(null, 1), Map.of("k", "v"));
        assertArrayEquals(
                b("{\"id\":\"p1\",\"timeoutAt\":10,\"param\":{\"data\":1},\"tags\":{\"k\":\"v\"}}"), encode(r));
    }

    @Test
    void promiseCreateReqDecodeCamel() {
        PromiseCreateReq r = decode(
                b("{\"id\":\"p1\",\"timeoutAt\":10,\"param\":{\"data\":1},\"tags\":{\"k\":\"v\"}}"),
                PromiseCreateReq.class);
        assertEquals("p1", r.id());
        assertEquals(10L, r.timeoutAt());
        assertEquals(1, r.param().data());
        assertEquals(Map.of("k", "v"), r.tags());
    }

    @Test
    void promiseCreateReqDefaultWithId() {
        PromiseCreateReq r = new PromiseCreateReq("p1");
        assertEquals("p1", r.id());
        assertEquals(0L, r.timeoutAt());
        assertNull(r.param().headers());
        assertNull(r.param().data());
        assertEquals(Map.of(), r.tags());
    }

    // --- PromiseSettleReq ---

    @Test
    void promiseSettleReqEncode() {
        PromiseSettleReq r = new PromiseSettleReq("p1", "resolved", new Value(null, 1));
        assertArrayEquals(b("{\"id\":\"p1\",\"state\":\"resolved\",\"value\":{\"data\":1}}"), encode(r));
    }

    @Test
    void promiseSettleReqDecode() {
        PromiseSettleReq r =
                decode(b("{\"id\":\"p1\",\"state\":\"rejected_canceled\",\"value\":{}}"), PromiseSettleReq.class);
        assertEquals("p1", r.id());
        assertEquals("rejected_canceled", r.state());
        assertNull(r.value().data());
    }

    // --- PromiseRegisterCallbackData ---

    @Test
    void promiseRegisterCallbackDataRoundtrip() {
        PromiseRegisterCallbackData d = new PromiseRegisterCallbackData("a", "b");
        byte[] encoded = encode(d);
        assertArrayEquals(b("{\"awaited\":\"a\",\"awaiter\":\"b\"}"), encoded);
        PromiseRegisterCallbackData back = decode(encoded, PromiseRegisterCallbackData.class);
        assertEquals("a", back.awaited());
        assertEquals("b", back.awaiter());
    }

    // --- Args: the packed *args / **kwargs slot (defaults empty) ---

    @Test
    void argsDecodeMinimalAppliesDefaults() {
        Args a = decode(b("{}"), Args.class);
        assertEquals(List.of(), a.args());
        assertEquals(Map.of(), a.kwargs());
    }

    @Test
    void argsDecodeFull() {
        Args a = decode(b("{\"args\":[1,2],\"kwargs\":{\"k\":3}}"), Args.class);
        assertEquals(List.of(1, 2), a.args());
        assertEquals(Map.of("k", 3), a.kwargs());
    }

    @Test
    void argsEncode() {
        Args a = new Args(List.of(1, 2), Map.of("k", 3));
        assertArrayEquals(b("{\"args\":[1,2],\"kwargs\":{\"k\":3}}"), encode(a));
    }

    // --- TaskData: Args fields flattened + `func` / `version` + into_value ---

    @Test
    void taskDataDecodeMinimalAppliesDefaultArgs() {
        TaskData d = decode(b("{\"func\":\"f\",\"version\":2}"), TaskData.class);
        assertEquals("f", d.func());
        assertEquals(List.of(), d.args());
        assertEquals(Map.of(), d.kwargs());
        assertEquals(2, d.version());
    }

    @Test
    void taskDataDecodeFull() {
        TaskData d = decode(b("{\"func\":\"f\",\"args\":[1,2],\"kwargs\":{\"k\":3},\"version\":1}"), TaskData.class);
        assertEquals("f", d.func());
        assertEquals(List.of(1, 2), d.args());
        assertEquals(Map.of("k", 3), d.kwargs());
        assertEquals(1, d.version());
    }

    @Test
    void taskDataVersionDefaultsToOne() {
        // ``version`` defaults to 1: an omitted version (e.g. a foreign-SDK payload) resolves
        // deterministically to the first registered version. Version 0 -- which once meant
        // "latest registered" -- is no longer used.
        TaskData d = decode(b("{\"func\":\"f\"}"), TaskData.class);
        assertEquals("f", d.func());
        assertEquals(1, d.version());
    }

    @Test
    void taskDataEncode() {
        // Inherited Args fields encode first, then ``func`` / ``version``.
        TaskData d = new TaskData(List.of(1, 2), Map.of("k", 3), "f", 2);
        assertArrayEquals(b("{\"args\":[1,2],\"kwargs\":{\"k\":3},\"func\":\"f\",\"version\":2}"), encode(d));
    }

    @Test
    void taskDataEncodeDefaultsEmitEmpty() {
        TaskData d = new TaskData("f");
        assertArrayEquals(b("{\"args\":[],\"kwargs\":{},\"func\":\"f\",\"version\":1}"), encode(d));
    }

    private static Info info() {
        return new Info("id", "parent", "origin", "branch", 123L, "func", Map.of("k", "v"));
    }

    @Test
    void infoFields() {
        Info i = info();
        assertEquals("id", i.id());
        assertEquals("parent", i.parentId());
        assertEquals("origin", i.originId());
        assertEquals("branch", i.branchId());
        assertEquals(123L, i.timeoutAt());
        assertEquals("func", i.funcName());
        assertEquals(Map.of("k", "v"), i.tags());
    }
}
