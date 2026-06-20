package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.HttpError;
import io.resonatehq.resonate.Network.HttpNetwork;
import io.resonatehq.resonate.Network.HttpSession;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Network.RealHttpSession;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_network.py}.
 *
 * <p>Exercises {@link LocalNetwork} (the in-process {@link Network.ServerState} simulation) end to
 * end through the envelope wire format, plus {@link HttpNetwork} identity and the {@code send}
 * retry/backoff resilience the Python tests cover by monkeypatching the session and sleep.
 */
class NetworkTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long I64_MAX = (1L << 63) - 1;

    // -- helpers --------------------------------------------------------------

    /** Encode {@code req}, send it through {@code net}, and decode the response. */
    private static JsonNode send(LocalNetwork net, Object req) throws Exception {
        String resp = net.send(MAPPER.writeValueAsString(req)).get();
        return MAPPER.readTree(resp);
    }

    private static int status(JsonNode resp) {
        JsonNode head = resp.get("head");
        JsonNode s = head != null ? head.get("status") : null;
        return s != null && s.isIntegralNumber() ? s.asInt() : 0;
    }

    private static JsonNode data(JsonNode resp) {
        return resp.has("data") ? resp.get("data") : resp;
    }

    // -- local tests ----------------------------------------------------------

    @Test
    void localNetworkCreatesAndGetsPromise() throws Exception {
        LocalNetwork net = new LocalNetwork("test-pid", "default");
        Map<String, Object> req = Map.of(
                "kind", "promise.create",
                "head", Map.of("corrId", "c1", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "id",
                                "p1",
                                "timeoutAt",
                                I64_MAX,
                                "param",
                                Map.of("data", "test"),
                                "tags",
                                Map.of("resonate:scope", "global")));
        JsonNode resp = send(net, req);
        assertTrue(status(resp) == 200 || status(resp) == 201);
        assertEquals("p1", data(resp).get("promise").get("id").asText());
        assertEquals("pending", data(resp).get("promise").get("state").asText());

        Map<String, Object> getReq = Map.of(
                "kind", "promise.get",
                "head", Map.of("corrId", "c2", "version", "2025-01-15"),
                "data", Map.of("id", "p1"));
        JsonNode getResp = send(net, getReq);
        assertEquals(200, status(getResp));
        assertEquals("p1", data(getResp).get("promise").get("id").asText());
    }

    @Test
    void localNetworkIdempotentPromiseCreate() throws Exception {
        LocalNetwork net = new LocalNetwork();
        Map<String, Object> req = Map.of(
                "kind", "promise.create",
                "head", Map.of("corrId", "c1", "version", "2025-01-15"),
                "data", Map.of("id", "p1", "timeoutAt", I64_MAX, "param", Map.of(), "tags", Map.of()));
        JsonNode r1 = send(net, req);
        assertTrue(status(r1) == 200 || status(r1) == 201);

        JsonNode r2 = send(net, req);
        assertEquals(200, status(r2));
        assertEquals("p1", data(r2).get("promise").get("id").asText());
    }

    @Test
    void localNetworkTaskCreateAndFulfill() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        Map<String, Object> req = Map.of(
                "kind", "task.create",
                "head", Map.of("corrId", "c1", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "pid",
                                "pid1",
                                "ttl",
                                60000,
                                "action",
                                Map.of(
                                        "kind", "promise.create",
                                        "head", Map.of("corrId", "c1a", "version", "2025-01-15"),
                                        "data",
                                                Map.of(
                                                        "id",
                                                        "p1",
                                                        "timeoutAt",
                                                        I64_MAX,
                                                        "param",
                                                        Map.of("data", "test"),
                                                        "tags",
                                                        Map.of()))));
        JsonNode resp = send(net, req);
        assertTrue(status(resp) == 200 || status(resp) == 201);
        assertEquals("acquired", data(resp).get("task").get("state").asText());
        assertEquals("p1", data(resp).get("promise").get("id").asText());

        String taskId = data(resp).get("task").get("id").asText();
        Map<String, Object> fulfill = Map.of(
                "kind", "task.fulfill",
                "head", Map.of("corrId", "c2", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "id",
                                taskId,
                                "version",
                                0,
                                "action",
                                Map.of(
                                        "kind", "promise.settle",
                                        "head", Map.of("corrId", "c2a", "version", "2025-01-15"),
                                        "data",
                                                Map.of(
                                                        "id",
                                                        "p1",
                                                        "state",
                                                        "resolved",
                                                        "value",
                                                        Map.of("data", "result")))));
        JsonNode fResp = send(net, fulfill);
        assertEquals(200, status(fResp));
    }

    @Test
    void localNetworkIdentity() {
        LocalNetwork net = new LocalNetwork("mypid", "mygroup");
        assertEquals("mypid", net.pid());
        assertEquals("mygroup", net.group());
        assertEquals("local://uni@mygroup/mypid", net.unicast());
        assertEquals("local://any@mygroup/mypid", net.anycast());
        assertEquals("local://any@target", net.targetResolver("target"));
    }

    @Test
    void promiseCreateWithTargetCreatesTaskAndDispatchesExecute() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        Map<String, Object> req = Map.of(
                "kind", "promise.create",
                "head", Map.of("corrId", "c1", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "id",
                                "rpc-1",
                                "timeoutAt",
                                I64_MAX,
                                "param",
                                Map.of("data", "test"),
                                "tags",
                                Map.of("resonate:target", "local://any@hello", "resonate:scope", "global")));
        JsonNode resp = send(net, req);
        assertEquals("pending", data(resp).get("promise").get("state").asText());

        Network.Task task = net.state.tasks.get("rpc-1");
        assertEquals("pending", task.state);
        assertEquals("rpc-1", task.id);
    }

    @Test
    void taskSuspendRegistersAwaitersAndSuspends() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        send(net, taskCreate("parent", "local://any@wf"));
        send(net, promiseCreate("child-1", "local://any@hello"));

        Map<String, Object> suspendReq = Map.of(
                "kind", "task.suspend",
                "head", Map.of("corrId", "c3", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "id",
                                "parent",
                                "version",
                                0,
                                "actions",
                                java.util.List.of(Map.of(
                                        "kind", "promise.register_callback",
                                        "head", Map.of("corrId", "c3a", "version", "2025-01-15"),
                                        "data", Map.of("awaited", "child-1", "awaiter", "parent")))));
        JsonNode resp = send(net, suspendReq);
        assertEquals(200, status(resp));

        assertEquals("suspended", net.state.tasks.get("parent").state);
        assertTrue(net.state.promises.get("child-1").awaiters.contains("parent"));
    }

    @Test
    void settlingChildResumesSuspendedParent() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        send(net, taskCreate("parent", "local://any@wf"));
        send(net, promiseCreate("child", "local://any@hello"));

        send(
                net,
                Map.of(
                        "kind", "task.suspend",
                        "head", Map.of("corrId", "c3", "version", "2025-01-15"),
                        "data",
                                Map.of(
                                        "id",
                                        "parent",
                                        "version",
                                        0,
                                        "actions",
                                        java.util.List.of(Map.of(
                                                "kind", "promise.register_callback",
                                                "head", Map.of("corrId", "c3a", "version", "2025-01-15"),
                                                "data", Map.of("awaited", "child", "awaiter", "parent"))))));

        send(
                net,
                Map.of(
                        "kind", "task.acquire",
                        "head", Map.of("corrId", "c4", "version", "2025-01-15"),
                        "data", Map.of("id", "child", "version", 0, "pid", "pid1", "ttl", 60000)));

        send(
                net,
                Map.of(
                        "kind", "task.fulfill",
                        "head", Map.of("corrId", "c5", "version", "2025-01-15"),
                        "data",
                                Map.of(
                                        "id",
                                        "child",
                                        "version",
                                        0,
                                        "action",
                                        Map.of(
                                                "kind", "promise.settle",
                                                "head", Map.of("corrId", "c5a", "version", "2025-01-15"),
                                                "data",
                                                        Map.of(
                                                                "id",
                                                                "child",
                                                                "state",
                                                                "resolved",
                                                                "value",
                                                                Map.of("data", "hello"))))));

        Network.Task parent = net.state.tasks.get("parent");
        assertEquals("pending", parent.state);
        assertEquals(1, parent.version);
    }

    @Test
    void taskSuspendRedirectWhenDependencyAlreadySettled() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        send(net, taskCreate("parent", "local://any@wf"));

        send(
                net,
                Map.of(
                        "kind", "promise.create",
                        "head", Map.of("corrId", "c2", "version", "2025-01-15"),
                        "data", Map.of("id", "child", "timeoutAt", I64_MAX, "param", Map.of(), "tags", Map.of())));

        send(
                net,
                Map.of(
                        "kind", "promise.settle",
                        "head", Map.of("corrId", "c3", "version", "2025-01-15"),
                        "data", Map.of("id", "child", "state", "resolved", "value", Map.of("data", "ok"))));

        Map<String, Object> suspendReq = Map.of(
                "kind", "task.suspend",
                "head", Map.of("corrId", "c4", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "id",
                                "parent",
                                "version",
                                0,
                                "actions",
                                java.util.List.of(Map.of(
                                        "kind", "promise.register_callback",
                                        "head", Map.of("corrId", "c4a", "version", "2025-01-15"),
                                        "data", Map.of("awaited", "child", "awaiter", "parent")))));
        JsonNode resp = send(net, suspendReq);
        assertEquals(300, status(resp));
    }

    private static Map<String, Object> taskCreate(String id, String target) {
        return Map.of(
                "kind", "task.create",
                "head", Map.of("corrId", "c1", "version", "2025-01-15"),
                "data",
                        Map.of(
                                "pid",
                                "pid1",
                                "ttl",
                                60000,
                                "action",
                                Map.of(
                                        "kind", "promise.create",
                                        "head", Map.of("corrId", "c1a", "version", "2025-01-15"),
                                        "data",
                                                Map.of(
                                                        "id",
                                                        id,
                                                        "timeoutAt",
                                                        I64_MAX,
                                                        "tags",
                                                        Map.of("resonate:target", target)))));
    }

    private static Map<String, Object> promiseCreate(String id, String target) {
        return Map.of(
                "kind", "promise.create",
                "head", Map.of("corrId", "c2", "version", "2025-01-15"),
                "data", Map.of("id", id, "timeoutAt", I64_MAX, "tags", Map.of("resonate:target", target)));
    }

    // -- http tests -----------------------------------------------------------

    @Test
    void httpNetworkIdentity() {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "mypid", "mygroup", null, null);
        assertEquals("mypid", net.pid());
        assertEquals("mygroup", net.group());
        assertEquals("poll://uni@mygroup/mypid", net.unicast());
        assertEquals("poll://any@mygroup/mypid", net.anycast());
    }

    @Test
    void httpNetworkMatchReturnsPollAnycast() {
        HttpNetwork net = new HttpNetwork("http://localhost:8001");
        assertEquals("poll://any@my-target", net.targetResolver("my-target"));
    }

    @Test
    void httpNetworkStripsTrailingSlash() {
        HttpNetwork net = new HttpNetwork("http://localhost:8001/", "pid", null, null, null);
        assertEquals("http://localhost:8001", net.url);
    }

    @Test
    void httpNetworkDefaultGroup() {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid1", null, null, null);
        assertEquals("default", net.group());
        assertEquals("poll://uni@default/pid1", net.unicast());
    }

    @Test
    void httpSessionConnectorLimitAboveDefault() throws Exception {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", null, null, null);
        net.running = true;
        try {
            RealHttpSession session = (RealHttpSession) net.ensureSession();
            assertEquals(HttpNetwork.DEFAULT_CONN_LIMIT, net.connLimit);
            assertTrue(HttpNetwork.DEFAULT_CONN_LIMIT > 100);
            assertEquals(HttpNetwork.DEFAULT_CONN_LIMIT, session.limit());
        } finally {
            net.stop().get();
        }
    }

    @Test
    void httpSessionConnectorLimitOverride() throws Exception {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", null, null, 7);
        net.running = true;
        try {
            assertEquals(7, net.ensureSession().limit());
        } finally {
            net.stop().get();
        }
    }

    /** Minimal {@link HttpSession} stand-in that fails {@code failTimes} then succeeds. */
    static final class FlakySession implements HttpSession {
        final int failTimes;
        final String body;
        int attempts;

        FlakySession(int failTimes, String body) {
            this.failTimes = failTimes;
            this.body = body;
        }

        @Override
        public String post(String url, String body, Map<String, String> headers) {
            attempts++;
            if (attempts <= failTimes) {
                throw new Network.ConnectionException(new RuntimeException("down"));
            }
            return this.body;
        }
    }

    @Test
    void httpSendRetriesThroughConnectionOutage() throws Exception {
        FlakySession flaky = new FlakySession(3, "{\"head\":{\"status\":200},\"data\":{}}");
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", "g", null, null) {
            @Override
            protected HttpSession ensureSession() {
                return flaky;
            }

            @Override
            protected void sleepOrStop(double secs) {
                // collapse the backoff
            }
        };
        net.running = true;

        String body = net.send("{}").get();
        assertEquals("{\"head\":{\"status\":200},\"data\":{}}", body);
        assertEquals(4, flaky.attempts); // three failures + one success
    }

    @Test
    void httpSendStopsRetryingAfterStop() throws Exception {
        FlakySession flaky = new FlakySession(10_000, null);
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", "g", null, null) {
            @Override
            protected HttpSession ensureSession() {
                return flaky;
            }
        };
        net.running = true;

        CompletableFuture<String> sendTask = net.send("{}");
        Thread.sleep(50); // let the retry enter its backoff sleep
        net.stop().get();

        ExecutionException ex = assertThrows(ExecutionException.class, () -> sendTask.get(2, TimeUnit.SECONDS));
        assertInstanceOf(HttpError.class, ex.getCause());
    }

    @Test
    void httpSendAfterStopRaisesHttpError() throws Exception {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", "g", null, null);
        net.running = false;
        ExecutionException ex =
                assertThrows(ExecutionException.class, () -> net.send("{}").get());
        assertInstanceOf(HttpError.class, ex.getCause());
    }

    @Test
    void httpSendDoesNotOpenSessionAfterStop() throws Exception {
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", "g", null, null);
        net.start().get();
        net.stop().get();
        assertNull(net.session);

        ExecutionException ex =
                assertThrows(ExecutionException.class, () -> net.send("{}").get());
        assertInstanceOf(HttpError.class, ex.getCause());
        assertNull(net.session);
    }

    // -- concurrency -----------------------------------------------------------

    @Test
    void localNetworkHandlesConcurrentSendsAndRecv() throws Exception {
        LocalNetwork net = new LocalNetwork("pid1", null);
        // A subscriber registered concurrently with dispatch must never throw / lose updates.
        java.util.concurrent.atomic.AtomicInteger delivered = new java.util.concurrent.atomic.AtomicInteger();
        net.recv(raw -> delivered.incrementAndGet());

        int threads = 16;
        int perThread = 50;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.List<CompletableFuture<Void>> futures = new java.util.ArrayList<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                new java.util.concurrent.atomic.AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(CompletableFuture.runAsync(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < perThread; i++) {
                                String id = "p-" + tid + "-" + i;
                                // create with a target so dispatch fires too
                                send(
                                        net,
                                        Map.of(
                                                "kind", "promise.create",
                                                "head", Map.of("corrId", id, "version", "2025-01-15"),
                                                "data",
                                                        Map.of(
                                                                "id",
                                                                id,
                                                                "timeoutAt",
                                                                I64_MAX,
                                                                "tags",
                                                                Map.of("resonate:target", "local://any@hello"))));
                                // register another subscriber mid-flight to stress recv vs dispatch
                                if (i % 10 == 0) {
                                    net.recv(raw -> delivered.incrementAndGet());
                                }
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    },
                    pool));
        }
        start.countDown();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertNull(failure.get(), () -> "concurrent send/recv threw: " + failure.get());
        // Every distinct id must have produced exactly one promise + task — no lost or duplicated state.
        assertEquals(threads * perThread, net.state.promises.size());
        assertEquals(threads * perThread, net.state.tasks.size());
    }

    @Test
    void httpSendDoesNotRetryServerErrors() throws Exception {
        FlakySession notFound = new FlakySession(0, "{\"head\":{\"status\":404},\"data\":{\"error\":\"nope\"}}");
        HttpNetwork net = new HttpNetwork("http://localhost:8001", "pid", "g", null, null) {
            @Override
            protected HttpSession ensureSession() {
                return notFound;
            }
        };
        net.running = true;

        String body = net.send("{}").get();
        assertTrue(body.contains("\"status\":404"));
        assertEquals(1, notFound.attempts); // one shot — no retry on a real HTTP response
    }

    @Test
    void closeUnblocksInflightGet() throws Exception {
        // A server that accepts the connection but never replies, modelling the SSE long-poll.
        try (java.net.ServerSocket server = new java.net.ServerSocket(0)) {
            Thread.ofVirtual().start(() -> {
                try {
                    server.accept(); // hold the connection open, send nothing
                } catch (Exception ignored) {
                }
            });

            RealHttpSession session = new RealHttpSession(8);
            String url = "http://localhost:" + server.getLocalPort() + "/poll";
            CompletableFuture<Void> get =
                    CompletableFuture.runAsync(() -> session.get(url, Map.of("Accept", "text/event-stream")));

            // The get is now blocked waiting for a response that never comes.
            Thread.sleep(200);
            assertTrue(!get.isDone(), "get should still be blocked before close");

            // close() must cancel the in-flight request so the daemon poll thread exits, rather than
            // surviving into JVM shutdown and dying with NoClassDefFoundError(SseResponse).
            session.close();
            assertThrows(ExecutionException.class, () -> get.get(2, TimeUnit.SECONDS));
        }
    }
}
