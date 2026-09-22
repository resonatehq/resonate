package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Errors.StoppedError;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_effects.py}.
 *
 * <p>{@link Effects} is the two durable ops (create / settle) over a {@link Sender}, with a decoded
 * {@link PromiseRecord} cache. These tests cover cache hits (preload and prior-call) skipping the
 * network, cache misses hitting it, the settle idempotency rule (cached non-pending is returned),
 * and that decoded {@code param} / {@code value} payloads round-trip correctly.
 *
 * <p>The Python {@code StubNetwork} (an in-memory promise store mimicking the server, tracking a
 * send count) is reproduced here: it speaks the protocol-envelope wire format (echoing kind /
 * corrId) so the real {@link Sender} / {@link Transport} validation passes unchanged, and handles
 * only {@code promise.create} / {@code promise.settle}.
 */
class EffectsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long I64_MAX = Long.MAX_VALUE;

    // =========================================================================
    // Test harness
    // =========================================================================

    private static Codec testCodec() {
        return new Codec(new NoopEncryptor());
    }

    /** Build a wire {@code {headers, data}} object from a parsed request node (or absent/null). */
    private static Map<String, Object> valueFromWire(JsonNode raw) {
        Map<String, Object> v = new HashMap<>();
        if (raw == null || raw.isNull() || !raw.isObject()) {
            v.put("headers", null);
            v.put("data", null);
            return v;
        }
        JsonNode headers = raw.get("headers");
        JsonNode data = raw.get("data");
        v.put("headers", (headers == null || headers.isNull()) ? null : MAPPER.convertValue(headers, Object.class));
        v.put("data", (data == null || data.isNull()) ? null : MAPPER.convertValue(data, Object.class));
        return v;
    }

    private static Map<String, Object> promiseJson(
            String id, String state, long timeoutAt, Object param, Object value, Map<String, ?> tags, Long settledAt) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("id", id);
        p.put("state", state);
        p.put("param", param);
        p.put("value", value);
        p.put("tags", tags);
        p.put("timeoutAt", timeoutAt);
        p.put("createdAt", 0);
        p.put("settledAt", settledAt);
        return p;
    }

    private static Map<String, Object> emptyValue() {
        Map<String, Object> v = new HashMap<>();
        v.put("headers", null);
        v.put("data", null);
        return v;
    }

    /**
     * An in-memory promise store mimicking the server, tracking send count. Implements {@link
     * Network}; only {@code promise.create} / {@code promise.settle} are handled.
     */
    static final class StubNetwork implements Network {
        final Map<String, Map<String, Object>> promises = new LinkedHashMap<>();
        int sendCount = 0;
        // When set, any request of this kind returns HTTP 500 (injects a server failure).
        String failKind = null;

        @Override
        public String pid() {
            return "test-pid";
        }

        @Override
        public String group() {
            return "test-group";
        }

        @Override
        public String unicast() {
            return "test-unicast";
        }

        @Override
        public String anycast() {
            return "test-anycast";
        }

        @Override
        public CompletableFuture<Void> start() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stop() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void recv(Consumer<String> callback) {}

        @Override
        public String targetResolver(String target) {
            return target;
        }

        @Override
        public CompletableFuture<String> send(String req) {
            sendCount++;
            try {
                JsonNode reqJson = MAPPER.readTree(req);
                String kind = reqJson.path("kind").asText("");
                String corrId = reqJson.path("head").path("corrId").asText(null);
                JsonNode data = reqJson.path("data");

                int status;
                Object respData;
                if (kind.equals(failKind)) {
                    status = 500;
                    respData = Map.of("error", "injected failure");
                } else
                    switch (kind) {
                        case "promise.create" -> {
                            status = 200;
                            respData = handleCreate(data);
                        }
                        case "promise.settle" -> {
                            status = 200;
                            respData = handleSettle(data);
                        }
                        default -> {
                            status = 400;
                            respData = "unknown request kind: " + kind;
                        }
                    }

                Map<String, Object> head = new LinkedHashMap<>();
                head.put("corrId", corrId);
                head.put("status", status);
                head.put("version", "2025-01-15");
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("kind", kind);
                resp.put("head", head);
                resp.put("data", respData);
                return CompletableFuture.completedFuture(MAPPER.writeValueAsString(resp));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Map<String, Object> handleCreate(JsonNode data) {
            String id = data.path("id").asText("");
            Map<String, Object> existing = promises.get(id);
            if (existing != null) {
                return Map.of("promise", existing);
            }
            long timeoutAt = data.has("timeoutAt") ? data.get("timeoutAt").asLong() : I64_MAX;
            Map<String, Object> tags = data.has("tags") && data.get("tags").isObject()
                    ? MAPPER.convertValue(data.get("tags"), new TypeReference<Map<String, Object>>() {})
                    : Map.of();
            Map<String, Object> record =
                    promiseJson(id, "pending", timeoutAt, valueFromWire(data.get("param")), emptyValue(), tags, null);
            promises.put(id, record);
            return Map.of("promise", record);
        }

        private Map<String, Object> handleSettle(JsonNode data) {
            String id = data.path("id").asText("");
            String stateStr = data.hasNonNull("state") ? data.get("state").asText() : "resolved";
            String promiseState = "resolved".equals(stateStr) ? "resolved" : "rejected";
            Map<String, Object> value = valueFromWire(data.get("value"));

            Map<String, Object> existing = promises.get(id);
            if (existing != null) {
                if (!"pending".equals(existing.get("state"))) {
                    return Map.of("promise", existing);
                }
                Map<String, Object> updated = new LinkedHashMap<>(existing);
                updated.put("state", promiseState);
                updated.put("value", value);
                updated.put("settledAt", 1);
                promises.put(id, updated);
                return Map.of("promise", updated);
            }
            Map<String, Object> record = promiseJson(id, promiseState, I64_MAX, emptyValue(), value, Map.of(), 1L);
            promises.put(id, record);
            return Map.of("promise", record);
        }
    }

    /** Wraps a {@link StubNetwork} and builds the client stack over it. */
    private static final class Harness {
        final StubNetwork network = new StubNetwork();

        int sendCount() {
            return network.sendCount;
        }

        void addPromise(PromiseRecord record) {
            network.promises.put(record.id(), toJson(record));
        }

        Effects buildEffects(List<PromiseRecord> preload) {
            Sender sender = new Sender(new Transport(network), null);
            return new Effects(sender, testCodec(), preload);
        }
    }

    private static Map<String, Object> toJson(PromiseRecord p) {
        Map<String, Object> param = new HashMap<>();
        param.put("headers", p.param().headers());
        param.put("data", p.param().data());
        Map<String, Object> value = new HashMap<>();
        value.put("headers", p.value().headers());
        value.put("data", p.value().data());
        return promiseJson(p.id(), p.state(), p.timeoutAt(), param, value, p.tags(), p.settledAt());
    }

    // -- promise builders (encoded params/values, like the Python fixtures) ----

    private static PromiseRecord pendingPromise(String id) {
        return pendingPromiseWithParam(id, mapOf("func", "test", "args", List.of()));
    }

    private static PromiseRecord pendingPromiseWithParam(String id, Object param) {
        Value encoded = testCodec().encode(param);
        return new PromiseRecord(id, "pending", encoded, new Value(), Map.of(), I64_MAX, 0L, null);
    }

    private static PromiseRecord resolvedPromise(String id, Object value) {
        Value encoded = testCodec().encode(value);
        return new PromiseRecord(id, "resolved", new Value(), encoded, Map.of(), I64_MAX, 0L, 1L);
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    // =========================================================================
    // create_promise
    // =========================================================================

    @Test
    void createReturnsCachedPromiseFromPreloadWithoutHittingNetwork() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of(pendingPromise("p1")));

        PromiseRecord record = effects.createPromise(new PromiseCreateReq("p1", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals("pending", record.state());
        assertEquals(0, harness.sendCount());
    }

    @Test
    void createHitsNetworkWhenPromiseNotInPreload() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        PromiseRecord record = effects.createPromise(new PromiseCreateReq("p2", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals("pending", record.state());
        assertEquals(1, harness.sendCount());
    }

    @Test
    void createAddsToCacheSecondCallUsesCache() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        PromiseCreateReq req = new PromiseCreateReq("p3", I64_MAX, new Value(), Map.of());
        effects.createPromise(req).join();
        assertEquals(1, harness.sendCount());

        PromiseRecord record = effects.createPromise(req).join();
        assertEquals("pending", record.state());
        assertEquals(1, harness.sendCount());
    }

    // =========================================================================
    // settle_promise
    // =========================================================================

    @Test
    void settleReturnsCachedWhenAlreadySettledInPreload() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of(resolvedPromise("s1", 42)));

        PromiseRecord record = effects.settlePromise("s1", 99).join();
        assertEquals("resolved", record.state());
        assertEquals(0, harness.sendCount());
    }

    @Test
    void settleHitsNetworkWhenPreloadedPromiseIsPending() {
        Harness harness = new Harness();
        harness.addPromise(pendingPromise("s2"));
        Effects effects = harness.buildEffects(List.of(pendingPromise("s2")));

        PromiseRecord record = effects.settlePromise("s2", "ok").join();
        assertEquals("resolved", record.state());
        assertEquals(1, harness.sendCount());
    }

    @Test
    void settleUpdatesCacheSecondSettleIsCached() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        effects.createPromise(new PromiseCreateReq("s3", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals(1, harness.sendCount());

        effects.settlePromise("s3", "done").join();
        assertEquals(2, harness.sendCount());

        PromiseRecord record = effects.settlePromise("s3", "done").join();
        assertEquals("resolved", record.state());
        assertEquals(2, harness.sendCount());
    }

    @Test
    void settleHitsNetworkWhenPromiseNotInCache() {
        Harness harness = new Harness();
        harness.addPromise(pendingPromise("s4"));
        Effects effects = harness.buildEffects(List.of());

        PromiseRecord record = effects.settlePromise("s4", "ok").join();
        assertEquals("resolved", record.state());
        assertEquals(1, harness.sendCount());
    }

    // =========================================================================
    // cached promise values
    // =========================================================================

    @Test
    void preloadedPendingPromiseHasDecodedParam() {
        Harness harness = new Harness();
        Map<String, Object> param = mapOf("func", "f", "args", List.of());
        Effects effects = harness.buildEffects(List.of(pendingPromiseWithParam("v1", param)));

        PromiseRecord record = effects.createPromise(new PromiseCreateReq("v1", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals(param, record.param().data());
    }

    @Test
    void preloadedResolvedPromiseHasDecodedValue() {
        Harness harness = new Harness();
        Map<String, Object> val = mapOf("answer", 42);
        Effects effects = harness.buildEffects(List.of(resolvedPromise("v2", val)));

        PromiseRecord record = effects.settlePromise("v2", 0).join();
        assertEquals(val, record.value().data());
    }

    @Test
    void promiseCreatedViaNetworkHasCorrectDecodedValues() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        Map<String, Object> paramData = mapOf("func", "myFunc", "args", new ArrayList<>(List.of(1, "two")));
        PromiseCreateReq req = new PromiseCreateReq("v3", I64_MAX, new Value(null, paramData), Map.of());
        effects.createPromise(req).join();

        PromiseRecord record = effects.createPromise(req).join();
        assertEquals(paramData, record.param().data());
    }

    @Test
    void promiseSettledViaNetworkHasCorrectDecodedValues() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        effects.createPromise(new PromiseCreateReq("v4", I64_MAX, new Value(), Map.of()))
                .join();

        Map<String, Object> val = mapOf("result", "success", "count", 7);
        effects.settlePromise("v4", val).join();

        PromiseRecord record = effects.settlePromise("v4", val).join();
        assertEquals(val, record.value().data());
    }

    @Test
    void multiplePreloadedPromisesEachHaveCorrectValues() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of(
                pendingPromiseWithParam("m1", mapOf("func", "f", "args", List.of())),
                resolvedPromise("m2", "hello"),
                resolvedPromise("m3", new ArrayList<>(List.of(1, 2, 3)))));

        PromiseRecord r1 = effects.createPromise(new PromiseCreateReq("m1", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals("pending", r1.state());
        assertEquals(mapOf("func", "f", "args", List.of()), r1.param().data());

        PromiseRecord r2 = effects.settlePromise("m2", 0).join();
        assertEquals("resolved", r2.state());
        assertEquals("hello", r2.value().data());

        PromiseRecord r3 = effects.settlePromise("m3", 0).join();
        assertEquals("resolved", r3.state());
        assertEquals(List.of(1, 2, 3), r3.value().data());

        assertEquals(0, harness.sendCount());
    }

    // =========================================================================
    // Behaviors exercised in Python via test_platform_errors.py (core/Context
    // not yet ported); verified here directly against Effects for full parity.
    // =========================================================================

    /**
     * The first durable-op failure arms the circuit breaker: it surfaces as a {@link PlatformError}
     * wrapping the originating {@link ResonateError}, and every later op short-circuits with a
     * {@link StoppedError} without touching the network. Mirrors Python's {@code self._stopped}
     * sticky flag and {@code raise PlatformError([StoppedError()])}.
     */
    @Test
    void firstFailureArmsCircuitBreakerThenShortCircuits() {
        Harness harness = new Harness();
        harness.network.failKind = "promise.create";
        Effects effects = harness.buildEffects(List.of());

        CompletionException first = assertThrows(CompletionException.class, () -> effects.createPromise(
                        new PromiseCreateReq("e1", I64_MAX, new Value(), Map.of()))
                .join());
        PlatformError pe1 = assertInstanceOf(PlatformError.class, first.getCause());
        assertInstanceOf(ServerError.class, pe1.cause());
        assertEquals(1, harness.sendCount());

        // Breaker is now armed: a second op (even a different id) never hits the network.
        CompletionException second = assertThrows(CompletionException.class, () -> effects.createPromise(
                        new PromiseCreateReq("e2", I64_MAX, new Value(), Map.of()))
                .join());
        PlatformError pe2 = assertInstanceOf(PlatformError.class, second.getCause());
        assertInstanceOf(StoppedError.class, pe2.cause());
        assertEquals(1, harness.sendCount());

        // settle is gated by the same breaker.
        CompletionException third = assertThrows(
                CompletionException.class, () -> effects.settlePromise("e1", 0).join());
        assertInstanceOf(
                StoppedError.class,
                assertInstanceOf(PlatformError.class, third.getCause()).cause());
        assertEquals(1, harness.sendCount());
    }

    /** A preloaded record that fails to decode is silently skipped, so its id is a cache miss. */
    @Test
    void preloadRecordThatFailsToDecodeIsSkipped() {
        Harness harness = new Harness();
        // Invalid base64 in param.data -> decodePromise raises Base64DecodeError -> skipped.
        PromiseRecord corrupt = new PromiseRecord(
                "c1", "pending", new Value(null, "!!!not-base64!!!"), new Value(), Map.of(), I64_MAX, 0L, null);
        Effects effects = harness.buildEffects(List.of(corrupt));

        // Not cached -> hits the network instead of returning the corrupt preload.
        PromiseRecord record = effects.createPromise(new PromiseCreateReq("c1", I64_MAX, new Value(), Map.of()))
                .join();
        assertEquals("pending", record.state());
        assertEquals(1, harness.sendCount());
    }

    /** An {@link Exception} result rejects the promise (Python's {@code isinstance(result, Exception)}). */
    @Test
    void exceptionResultRejectsPromise() {
        Harness harness = new Harness();
        Effects effects = harness.buildEffects(List.of());

        PromiseRecord record =
                effects.settlePromise("r1", new RuntimeException("boom")).join();
        assertEquals("rejected", record.state());
    }
}
