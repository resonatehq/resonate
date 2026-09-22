package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Transport.ExecuteMsg;
import io.resonatehq.resonate.Transport.Message;
import io.resonatehq.resonate.Transport.UnblockMsg;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_transport.py}.
 *
 * <p>{@link Transport} adds JSON parsing, correlation validation, and tagged-union decoding over a
 * raw {@link Network}. These tests exercise the {@code send} envelope validation (kind / corrId
 * match, JSON-decode failure, missing fields treated as empty), the {@code recv} parse-or-discard
 * behavior across both message kinds and the malformed / unknown-kind discard paths, and the
 * {@link Transport#network()} accessor.
 *
 * <p>The Python {@code StubNetwork} stands in for {@code LocalNetwork}: it plays the server, with
 * {@code send} returning a canned response and {@code recv} capturing the registered callback so the
 * test can feed it raw frames.
 */
class TransportTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * A minimal in-process {@link Network}. {@code send} returns a canned response and records the
     * request; {@code recv} captures the callback so tests can feed it raw messages.
     */
    static final class StubNetwork implements Network {
        final String response;
        final List<String> sent = new ArrayList<>();
        final List<Consumer<String>> callbacks = new ArrayList<>();

        StubNetwork() {
            this("");
        }

        StubNetwork(String response) {
            this.response = response;
        }

        @Override
        public String pid() {
            return "test";
        }

        @Override
        public String group() {
            return "default";
        }

        @Override
        public String unicast() {
            return "local://uni@default/test";
        }

        @Override
        public String anycast() {
            return "local://any@default/test";
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
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public void recv(Consumer<String> callback) {
            callbacks.add(callback);
        }

        @Override
        public String targetResolver(String target) {
            return "local://any@" + target;
        }
    }

    /** Build a {@code {"kind": ..., "head": {"corrId": ...}, "data": ...}} envelope as JSON. */
    private static String envelope(String kind, String corrId, Object data) {
        try {
            return MAPPER.writeValueAsString(Map.of("kind", kind, "head", Map.of("corrId", corrId), "data", data));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run a transport future to completion, unwrapping {@link CompletionException} to its cause —
     * the analogue of Python's {@code asyncio.run}, which propagates the raw exception rather than a
     * wrapper.
     */
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

    // -- send: envelope validation --------------------------------------------

    @Test
    void sendAndValidateEnvelopeFormat() throws Exception {
        String response = envelope("promise.create", "env123", Map.of("promise", Map.of("id", "p2")));
        Transport transport = new Transport(new StubNetwork(response));

        String body = MAPPER.writeValueAsString(Map.of(
                "kind",
                "promise.create",
                "head",
                Map.of("corrId", "env123", "version", "2025-01-15"),
                "data",
                Map.of("id", "p2", "timeoutAt", Long.MAX_VALUE, "param", Map.of(), "tags", Map.of())));

        var resp = await(transport.send("promise.create", "env123", body));
        assertEquals("promise.create", resp.get("kind").asText());
        assertEquals("env123", resp.get("head").get("corrId").asText());
        assertEquals("p2", resp.get("data").get("promise").get("id").asText());
    }

    @Test
    void sendPassesBodyToNetwork() {
        StubNetwork net = new StubNetwork(envelope("k", "c", Map.of()));
        Transport transport = new Transport(net);
        await(transport.send("k", "c", "the-body"));
        assertEquals(List.of("the-body"), net.sent);
    }

    @Test
    void sendKindMismatch() {
        Transport transport = new Transport(new StubNetwork(envelope("other.kind", "c", Map.of())));
        ServerError exc = assertThrows(ServerError.class, () -> await(transport.send("expected.kind", "c", "{}")));
        assertEquals(500, exc.code());
        assertTrue(exc.getMessage().contains("expected 'expected.kind', got 'other.kind'"));
    }

    @Test
    void sendCorrIdMismatch() {
        Transport transport = new Transport(new StubNetwork(envelope("k", "wrong", Map.of())));
        ServerError exc = assertThrows(ServerError.class, () -> await(transport.send("k", "right", "{}")));
        assertEquals(500, exc.code());
        assertTrue(exc.getMessage().contains("expected 'right', got 'wrong'"));
    }

    @Test
    void sendInvalidJsonResponse() {
        Transport transport = new Transport(new StubNetwork("not json"));
        assertThrows(DecodingError.class, () -> await(transport.send("k", "c", "{}")));
    }

    @Test
    void sendMissingFieldsTreatedAsEmpty() {
        // A response with no kind/corrId fails validation against a non-empty kind.
        Transport transport = new Transport(new StubNetwork("{}"));
        assertThrows(ServerError.class, () -> await(transport.send("k", "c", "{}")));
    }

    @Test
    void sendEmptyResponseIsDecodingError() {
        // Jackson parses "" to a MissingNode rather than erroring; msgspec's decode("") raises, so
        // the transport must surface a DecodingError, not a (kind-mismatch) ServerError.
        Transport transport = new Transport(new StubNetwork(""));
        assertThrows(DecodingError.class, () -> await(transport.send("k", "c", "{}")));
    }

    @Test
    void sendTrailingTokensIsDecodingError() {
        // msgspec rejects trailing data after a complete value; mirror that rather than silently
        // parsing the first value.
        Transport transport = new Transport(new StubNetwork("{} {}"));
        assertThrows(DecodingError.class, () -> await(transport.send("k", "c", "{}")));
    }

    // -- recv -----------------------------------------------------------------

    private static List<Message> feed(Transport transport, StubNetwork net, String raw) {
        List<Message> received = new ArrayList<>();
        transport.recv(received::add);
        net.callbacks.get(0).accept(raw);
        return received;
    }

    @Test
    void recvParsesExecuteMessage() {
        StubNetwork net = new StubNetwork();
        String raw = "{\"kind\":\"execute\",\"data\":{\"task\":{\"id\":\"t1\",\"version\":3}}}";
        List<Message> received = feed(new Transport(net), net, raw);
        assertEquals(1, received.size());
        ExecuteMsg msg = assertInstanceOf(ExecuteMsg.class, received.get(0));
        assertEquals("t1", msg.taskId());
        assertEquals(3, msg.version());
    }

    @Test
    void recvExecuteMessageDefaultVersion() {
        StubNetwork net = new StubNetwork();
        String raw = "{\"kind\":\"execute\",\"data\":{\"task\":{\"id\":\"t1\"}}}";
        List<Message> received = feed(new Transport(net), net, raw);
        ExecuteMsg msg = assertInstanceOf(ExecuteMsg.class, received.get(0));
        assertEquals(0, msg.version());
    }

    @Test
    void recvParsesUnblockMessage() {
        StubNetwork net = new StubNetwork();
        String raw = "{\"kind\":\"unblock\",\"data\":{\"promise\":"
                + "{\"id\":\"p1\",\"state\":\"resolved\",\"value\":{\"data\":\"dmFs\"},\"timeoutAt\":123}}}";
        List<Message> received = feed(new Transport(net), net, raw);
        assertEquals(1, received.size());
        UnblockMsg msg = assertInstanceOf(UnblockMsg.class, received.get(0));
        assertEquals(
                new PromiseRecord("p1", "resolved", null, new Value(null, "dmFs"), null, 123L, 0L, null),
                msg.promise());
    }

    @Test
    void recvDiscardsInvalidJson() {
        StubNetwork net = new StubNetwork();
        assertEquals(List.of(), feed(new Transport(net), net, "not json"));
    }

    @Test
    void recvDiscardsUnknownKind() {
        StubNetwork net = new StubNetwork();
        assertEquals(List.of(), feed(new Transport(net), net, "{\"kind\":\"mystery\",\"data\":{}}"));
    }

    @Test
    void recvDiscardsMissingRequiredField() {
        // msgspec rejects a struct missing a required field; Jackson would otherwise build a partial
        // object (task=null). required=true makes the parse fail so the frame is discarded.
        StubNetwork net = new StubNetwork();
        assertEquals(List.of(), feed(new Transport(net), net, "{\"kind\":\"execute\",\"data\":{}}"));
        assertEquals(List.of(), feed(new Transport(net), net, "{\"kind\":\"execute\",\"data\":{\"task\":{}}}"));
    }

    @Test
    void recvDiscardsNullFrame() {
        // A literal null parses to a Java null without error; msgspec rejects it against the union.
        StubNetwork net = new StubNetwork();
        assertEquals(List.of(), feed(new Transport(net), net, "null"));
    }

    // -- network accessor -----------------------------------------------------

    @Test
    void networkAccessor() {
        StubNetwork net = new StubNetwork();
        assertSame(net, new Transport(net).network());
    }
}
