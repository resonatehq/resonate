package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Heartbeat.Async;
import io.resonatehq.resonate.Heartbeat.Noop;
import io.resonatehq.resonate.Send.Sender;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_heartbeat.py}.
 *
 * <p>Drives {@link Async} over a recording {@link Network} that echoes a 200 reply, then asserts the
 * sent {@code task.heartbeat} envelopes reflect the tracked task set over time.
 */
class HeartbeatTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A {@link Network} that records every sent body and echoes a matching 200 reply. */
    static final class RecordingNetwork implements Network {
        final List<String> sent = new CopyOnWriteArrayList<>();

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
        public void recv(Consumer<String> callback) {}

        @Override
        public String targetResolver(String target) {
            return "local://any@" + target;
        }

        @Override
        public CompletableFuture<String> send(String req) {
            sent.add(req);
            try {
                JsonNode parsed = MAPPER.readTree(req);
                String kind = parsed.get("kind").asText();
                String corrId = parsed.get("head").get("corrId").asText();
                String reply = MAPPER.writeValueAsString(
                        Map.of("kind", kind, "head", Map.of("corrId", corrId, "status", 200), "data", Map.of()));
                return CompletableFuture.completedFuture(reply);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private RecordingNetwork net;

    private Async heartbeat() {
        net = new RecordingNetwork();
        Sender sender = new Sender(new Transport(net), null);
        return new Async("test-pid", 50, sender);
    }

    /** Parsed {@code task.heartbeat} request data nodes, in send order. */
    private List<JsonNode> heartbeats() throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String raw : net.sent) {
            JsonNode req = MAPPER.readTree(raw);
            if ("task.heartbeat".equals(req.path("kind").asText())) {
                out.add(req.get("data"));
            }
        }
        return out;
    }

    @Test
    void heartbeatSendsRequestWithTrackedTasks() throws Exception {
        Async hb = heartbeat();
        hb.start("task-1", 1);
        hb.start("task-2", 5);

        Thread.sleep(120);
        hb.shutdown();

        List<JsonNode> hbs = heartbeats();
        assertFalse(hbs.isEmpty(), "should have sent at least one heartbeat");

        JsonNode last = hbs.get(hbs.size() - 1);
        assertEquals("test-pid", last.get("pid").asText());

        JsonNode tasks = last.get("tasks");
        assertEquals(2, tasks.size());

        List<String> ids = new ArrayList<>();
        tasks.forEach(t -> ids.add(t.get("id").asText()));
        assertTrue(ids.contains("task-1"));
        assertTrue(ids.contains("task-2"));
    }

    @Test
    void heartbeatReflectsTaskRemoval() throws Exception {
        Async hb = heartbeat();
        hb.start("task-1", 1);
        hb.start("task-2", 2);

        Thread.sleep(80);
        hb.stop("task-1");

        Thread.sleep(80);
        hb.shutdown();

        List<JsonNode> hbs = heartbeats();
        JsonNode last = hbs.get(hbs.size() - 1);
        JsonNode tasks = last.get("tasks");
        assertEquals(1, tasks.size());
        assertEquals("task-2", tasks.get(0).get("id").asText());
    }

    @Test
    void concurrentStartStopIsRaceFree() throws Exception {
        // Hammer start/stop from many threads while the scheduler thread ticks. A wrong primitive
        // (plain HashMap, or executor lifecycle not guarded) surfaces as ConcurrentModificationException,
        // NPE, or a leaked/duplicated loop. Correct impl: completes cleanly and stops sending after shutdown.
        Async hb = heartbeat();
        int threads = 8;
        int iters = 500;
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        var barrier = new java.util.concurrent.CountDownLatch(1);
        List<CompletableFuture<Void>> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers.add(CompletableFuture.runAsync(
                    () -> {
                        try {
                            barrier.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        for (int i = 0; i < iters; i++) {
                            String id = "task-" + tid + "-" + (i % 5);
                            hb.start(id, i);
                            if ((i & 1) == 0) {
                                hb.stop(id);
                            }
                        }
                    },
                    pool));
        }
        barrier.countDown();
        CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).join();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        hb.shutdown();
        // After shutdown the loop must be fully stopped: no heartbeat sent after a settling window.
        int before = net.sent.size();
        Thread.sleep(120);
        assertEquals(before, net.sent.size(), "no heartbeats should be sent after shutdown");
    }

    @Test
    void noopHeartbeatStartStopShutdownAreHarmless() {
        Noop hb = new Noop();
        hb.start("task-1", 1);
        hb.start("task-2", 2);
        hb.stop("task-1");
        hb.stop("nonexistent");
        hb.shutdown();
    }
}
