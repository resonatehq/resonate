package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Send.Conflict;
import io.resonatehq.resonate.Send.Created;
import io.resonatehq.resonate.Send.Envelope;
import io.resonatehq.resonate.Send.Head;
import io.resonatehq.resonate.Send.PromiseSearchResult;
import io.resonatehq.resonate.Send.Redirect;
import io.resonatehq.resonate.Send.ScheduleSearchResult;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.SubEnvelope;
import io.resonatehq.resonate.Send.SuspendResult;
import io.resonatehq.resonate.Send.Suspended;
import io.resonatehq.resonate.Send.TaskAcquireResult;
import io.resonatehq.resonate.Send.TaskCreateOutcome;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseRegisterCallbackData;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.ScheduleRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_send.py}.
 *
 * <p>{@link Send.Sender} is the typed request layer over {@link Transport}. These tests cover the
 * envelope wire format (kind / head / data, auth omitted when absent), the nested {@link
 * SubEnvelope} action shape, and the auth token threading into both the outer and the nested head.
 *
 * <p>The Python module's round-trip tests drive a real {@code LocalNetwork} (an in-process server).
 * That implementation is not yet ported to Java — only the {@link Network} interface exists — so
 * those round-trips are reproduced here against a {@link StubNetwork} that echoes the request's kind
 * / corrId and returns canned response data. This exercises the same {@code Sender} request-building
 * and response-parsing paths the round-trips do (success, redirect, conflict, server error, and
 * decode failure), which is what the {@code Sender} layer is responsible for.
 */
class SendTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final long I64_MAX = Long.MAX_VALUE;

    // -- stub network ---------------------------------------------------------

    /**
     * A {@link Network} stub that records every sent body and returns a response built from the
     * parsed request by {@code responder} (so it can echo the kind / corrId the {@link Transport}
     * validates against). Mirrors the Python {@code CapturingNetwork}.
     */
    static final class StubNetwork implements Network {
        final List<String> sent = new ArrayList<>();
        final Function<JsonNode, String> responder;

        StubNetwork(Function<JsonNode, String> responder) {
            this.responder = responder;
        }

        @Override
        public String pid() {
            return "test-pid";
        }

        @Override
        public String group() {
            return "default";
        }

        @Override
        public String unicast() {
            return "local://uni@default/test-pid";
        }

        @Override
        public String anycast() {
            return "local://any@default/test-pid";
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
        public CompletableFuture<String> send(String req) {
            sent.add(req);
            try {
                return CompletableFuture.completedFuture(responder.apply(MAPPER.readTree(req)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void recv(Consumer<String> callback) {}

        @Override
        public String targetResolver(String target) {
            return "local://any@" + target;
        }
    }

    /** Build a response envelope echoing the request's kind / corrId, with the given status and data. */
    private static String response(JsonNode req, int status, Object data) {
        try {
            String kind = req.get("kind").asText();
            String corrId = req.get("head").get("corrId").asText();
            return MAPPER.writeValueAsString(
                    Map.of("kind", kind, "head", Map.of("corrId", corrId, "status", status), "data", data));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** A canned pending promise record (with the given id) carrying every field the parser requires. */
    private static Map<String, Object> cannedPromise(String id) {
        return Map.of(
                "id",
                id,
                "state",
                "pending",
                "timeoutAt",
                I64_MAX,
                "param",
                Map.of(),
                "value",
                Map.of(),
                "tags",
                Map.of(),
                "createdAt",
                0);
    }

    private static Map<String, Object> cannedTask(String id) {
        return Map.of("id", id, "state", "acquired", "version", 0);
    }

    /** A canned schedule record (with the given id) carrying every field the parser requires. */
    private static Map<String, Object> cannedSchedule(String id) {
        return Map.of(
                "id",
                id,
                "cron",
                "* * * * *",
                "promiseId",
                id + "-p",
                "promiseTimeout",
                1000,
                "promiseParam",
                Map.of(),
                "promiseTags",
                Map.of(),
                "createdAt",
                0);
    }

    /**
     * The Python {@code CapturingNetwork} responder: {@code task.create} yields task+promise+preload, anything
     * else yields a bare promise. Like {@code LocalNetwork}, it echoes the id the request created/settled so the
     * round-trips can assert the same ids the Python tests do; the {@code id} lives at {@code data.id}
     * (promise.create), {@code data.action.data.id} (task.create / task.fulfill), or {@code data.action.data.id}.
     */
    private static String capturing(JsonNode req) {
        String kind = req.get("kind").asText();
        String id =
                switch (kind) {
                    case "promise.create" -> req.get("data").get("id").asText();
                    case "task.create", "task.fulfill" -> req.get("data")
                            .get("action")
                            .get("data")
                            .get("id")
                            .asText();
                    default -> "x";
                };
        Object data = kind.equals("task.create")
                ? Map.of("task", cannedTask(id), "promise", cannedPromise(id), "preload", List.of())
                : Map.of("promise", cannedPromise(id));
        return response(req, 200, data);
    }

    /** Run a future to completion, unwrapping {@link CompletionException} to its cause. */
    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    private static Sender sender(StubNetwork net, String auth) {
        return new Sender(new Transport(net), auth);
    }

    // -- Serialization: verify envelope wire format ---------------------------

    @Test
    void envelopeSerializesCorrectWireFormat() throws Exception {
        PromiseCreateReq data = new PromiseCreateReq("p1", 999L, new Value(), Map.of());
        Envelope envelope = new Envelope("promise.create", new Head("test-corr", "2025-01-15", null), data);
        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(envelope));

        assertEquals("promise.create", json.get("kind").asText());
        assertEquals("test-corr", json.get("head").get("corrId").asText());
        assertEquals("2025-01-15", json.get("head").get("version").asText());
        assertEquals("p1", json.get("data").get("id").asText());
        assertEquals(999, json.get("data").get("timeoutAt").asLong());
        // auth should be absent when null
        assertFalse(json.get("head").has("auth"));
    }

    @Test
    void subEnvelopeSerializesNestedAction() throws Exception {
        PromiseSettleReq action = new PromiseSettleReq("p1", "resolved", new Value());
        SubEnvelope sub = new SubEnvelope("promise.settle", new Head("sub-corr", "2025-01-15", "token123"), action);
        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(sub));

        assertEquals("promise.settle", json.get("kind").asText());
        assertEquals("sub-corr", json.get("head").get("corrId").asText());
        assertEquals("token123", json.get("head").get("auth").asText());
        assertEquals("p1", json.get("data").get("id").asText());
        assertEquals("resolved", json.get("data").get("state").asText());
    }

    // -- Round-trip through the stub network (parse paths) --------------------

    @Test
    void promiseCreateRoundtrip() {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, null);

        PromiseCreateReq req = new PromiseCreateReq("rt-p1", I64_MAX, new Value(), Map.of());
        PromiseRecord record = await(sender.promiseCreate(req));
        assertEquals("rt-p1", record.id());
        assertEquals("pending", record.state());
    }

    @Test
    void taskCreateRoundtrip() {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, null);

        TaskAcquireResult result =
                await(sender.taskCreate("pid1", 60000, new PromiseCreateReq("rt-p2", I64_MAX, new Value(), Map.of())));
        assertEquals("rt-p2", result.task().id());
        assertEquals("rt-p2", result.promise().id());
        assertEquals(List.of(), result.preload());
    }

    @Test
    void taskAcquireRoundtrip() {
        // task.acquire carries no promise id; the stub stands in for LocalNetwork's record of the acquired task.
        StubNetwork net = new StubNetwork(req -> response(
                req,
                200,
                Map.of("task", cannedTask("rt-p2"), "promise", cannedPromise("rt-p2"), "preload", List.of())));
        Sender sender = sender(net, null);

        TaskAcquireResult result = await(sender.taskAcquire("t1", 1, "pid1", 60000));
        assertEquals("rt-p2", result.promise().id());
        assertEquals("rt-p2", result.task().id());
    }

    @Test
    void taskFulfillRoundtrip() {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, null);

        PromiseRecord promise = await(
                sender.taskFulfill("t1", 0, new PromiseSettleReq("rt-p3", "resolved", new Value(null, "result"))));
        assertEquals("rt-p3", promise.id());
    }

    @Test
    void taskSuspendRoundtrip() {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of()));
        Sender sender = sender(net, null);

        SuspendResult result =
                await(sender.taskSuspend("t1", 0, List.of(new PromiseRegisterCallbackData("dep-a", "t1"))));
        assertInstanceOf(Suspended.class, result);
    }

    @Test
    void taskSuspendRedirect() {
        StubNetwork net =
                new StubNetwork(req -> response(req, 300, Map.of("preload", List.of(cannedPromise("dep-a")))));
        Sender sender = sender(net, null);

        SuspendResult result =
                await(sender.taskSuspend("t1", 0, List.of(new PromiseRegisterCallbackData("dep-a", "t1"))));
        Redirect redirect = assertInstanceOf(Redirect.class, result);
        assertEquals(1, redirect.preload().size());
        assertEquals("dep-a", redirect.preload().get(0).id());
    }

    @Test
    void taskReleaseRoundtrip() {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of()));
        Sender sender = sender(net, null);

        // Completes without error; mirrors the Python round-trip asserting nothing beyond success.
        await(sender.taskRelease("t1", 0));
        assertEquals(1, net.sent.size());
    }

    @Test
    void taskCreateOrConflictReturnsConflictOn409() {
        StubNetwork net = new StubNetwork(req -> response(req, 409, Map.of()));
        Sender sender = sender(net, null);

        TaskCreateOutcome outcome = await(sender.taskCreateOrConflict(
                "pid1", 60000, new PromiseCreateReq("rt-p6", I64_MAX, new Value(), Map.of())));
        assertInstanceOf(Conflict.class, outcome);
    }

    @Test
    void taskCreateOrConflictReturnsCreatedOnSuccess() {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, null);

        TaskCreateOutcome outcome = await(sender.taskCreateOrConflict(
                "pid1", 60000, new PromiseCreateReq("rt-p7", I64_MAX, new Value(), Map.of())));
        Created created = assertInstanceOf(Created.class, outcome);
        assertEquals("rt-p7", created.result().promise().id());
    }

    // -- error handling -------------------------------------------------------

    @Test
    void serverErrorRaisedOn500() {
        StubNetwork net = new StubNetwork(req -> response(req, 500, Map.of("error", "boom")));
        Sender sender = sender(net, null);

        ServerError exc = assertThrows(
                ServerError.class,
                () -> await(sender.promiseCreate(new PromiseCreateReq("e1", I64_MAX, new Value(), Map.of()))));
        assertEquals(500, exc.code());
        assertEquals("boom", exc.message());
    }

    @Test
    void decodingErrorWhenPromiseMissing() {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of()));
        Sender sender = sender(net, null);

        assertThrows(
                DecodingError.class,
                () -> await(sender.promiseCreate(new PromiseCreateReq("e2", I64_MAX, new Value(), Map.of()))));
    }

    // -- Auth token in envelope head ------------------------------------------

    @Test
    void envelopeHeadContainsAuthWhenTokenProvided() throws Exception {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, "my-secret-token");

        await(sender.promiseCreate(new PromiseCreateReq("auth-p1", I64_MAX, new Value(), Map.of())));

        assertEquals(1, net.sent.size());
        JsonNode head = MAPPER.readTree(net.sent.get(0)).get("head");
        assertEquals("my-secret-token", head.get("auth").asText());
        assertTrue(head.has("corrId"));
        assertTrue(head.has("version"));
    }

    @Test
    void envelopeHeadOmitsAuthWhenNoToken() throws Exception {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, null);

        await(sender.promiseCreate(new PromiseCreateReq("no-auth-p1", I64_MAX, new Value(), Map.of())));

        assertEquals(1, net.sent.size());
        JsonNode head = MAPPER.readTree(net.sent.get(0)).get("head");
        assertFalse(head.has("auth"));
    }

    @Test
    void subEnvelopeHeadContainsAuthWhenTokenProvided() throws Exception {
        StubNetwork net = new StubNetwork(SendTest::capturing);
        Sender sender = sender(net, "sub-token");

        await(sender.taskCreate("test-pid", 60000, new PromiseCreateReq("sub-p1", I64_MAX, new Value(), Map.of())));

        assertTrue(net.sent.size() >= 1);
        JsonNode subHead =
                MAPPER.readTree(net.sent.get(0)).get("data").get("action").get("head");
        assertEquals("sub-token", subHead.get("auth").asText());
    }

    // -- Decoding parity: required fields, null Value coalescing ---------------

    @Test
    void decodingErrorWhenPromiseMalformed() {
        // The 'promise' key is present but the record is missing required fields (state, timeoutAt). msgspec
        // raises -> DecodingError; Jackson's required=true enforcement must do the same via convertValue.
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of("promise", Map.of("id", "x"))));
        Sender sender = sender(net, null);

        assertThrows(DecodingError.class, () -> await(sender.promiseGet("x")));
    }

    @Test
    void promiseNullValueFieldsCoalesceToEmpty() {
        // Python's _normalize_record drops null param/value so the default_factory runs; the Java record
        // constructors coalesce a null param/value to an empty Value, reaching the same end state.
        StubNetwork net = new StubNetwork(req -> {
            ObjectNode p = MAPPER.createObjectNode();
            p.put("id", "x");
            p.put("state", "resolved");
            p.put("timeoutAt", I64_MAX);
            p.put("createdAt", 0);
            p.putNull("param");
            p.putNull("value");
            ObjectNode data = MAPPER.createObjectNode();
            data.set("promise", p);
            return response(req, 200, data);
        });
        Sender sender = sender(net, null);

        PromiseRecord rec = await(sender.promiseGet("x"));
        assertNull(rec.param().data());
        assertNull(rec.value().data());
    }

    // -- Search: drop malformed records, read cursor, omit null args -----------

    @Test
    void promiseSearchParsesDropsMalformedAndReadsCursor() {
        Map<String, Object> bad = Map.of("id", "broken"); // missing required state / timeoutAt
        StubNetwork net = new StubNetwork(
                req -> response(req, 200, Map.of("promises", List.of(cannedPromise("ok"), bad), "cursor", "next")));
        Sender sender = sender(net, null);

        PromiseSearchResult result = await(sender.promiseSearch(null, null, null, null));
        assertEquals(1, result.promises().size());
        assertEquals("ok", result.promises().get(0).id());
        assertEquals("next", result.cursor());
    }

    @Test
    void promiseSearchNullCursorWhenAbsent() {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of("promises", List.of(cannedPromise("ok")))));
        Sender sender = sender(net, null);

        assertNull(await(sender.promiseSearch(null, null, null, null)).cursor());
    }

    @Test
    void promiseSearchOmitsNullArguments() throws Exception {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of()));
        Sender sender = sender(net, null);

        await(sender.promiseSearch(null, null, null, null));
        JsonNode data = MAPPER.readTree(net.sent.get(0)).get("data");
        assertFalse(data.has("state"));
        assertFalse(data.has("tags"));
        assertFalse(data.has("limit"));
        assertFalse(data.has("cursor"));
    }

    @Test
    void promiseSearchIncludesProvidedArguments() throws Exception {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of()));
        Sender sender = sender(net, null);

        await(sender.promiseSearch("pending", Map.of("k", "v"), 10, "cur"));
        JsonNode data = MAPPER.readTree(net.sent.get(0)).get("data");
        assertEquals("pending", data.get("state").asText());
        assertEquals("v", data.get("tags").get("k").asText());
        assertEquals(10, data.get("limit").asInt());
        assertEquals("cur", data.get("cursor").asText());
    }

    @Test
    void scheduleSearchParsesDropsMalformedAndReadsCursor() {
        Map<String, Object> bad = Map.of("id", "broken"); // missing required cron / promiseId / promiseTimeout
        StubNetwork net = new StubNetwork(
                req -> response(req, 200, Map.of("schedules", List.of(cannedSchedule("s1"), bad), "cursor", "c2")));
        Sender sender = sender(net, null);

        ScheduleSearchResult result = await(sender.scheduleSearch(null, null, null));
        assertEquals(1, result.schedules().size());
        assertEquals("s1", result.schedules().get(0).id());
        assertEquals("c2", result.cursor());
    }

    @Test
    void scheduleGetParsesRecord() {
        StubNetwork net = new StubNetwork(req -> response(req, 200, Map.of("schedule", cannedSchedule("s1"))));
        Sender sender = sender(net, null);

        ScheduleRecord rec = await(sender.scheduleGet("s1"));
        assertEquals("s1", rec.id());
        assertEquals("s1-p", rec.promiseId());
    }
}
