package io.resonatehq.resonate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.HttpError;
import io.resonatehq.resonate.Errors.ServerError;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * The transport abstraction for all server communication, mirroring the {@code Network} Protocol
 * from {@code resonate.network} in the Python SDK.
 *
 * <p>All communication between Resonate and the server (local or remote) flows through it as JSON
 * strings. Methods raise on error.
 *
 * <p>Python declares this as a {@code typing.Protocol} with two implementations ({@link
 * LocalNetwork} for an in-process server simulation and {@link HttpNetwork} for a real server).
 * Java has no structural typing, so it is a nominal {@code interface}; both implementations are
 * nested here (the Python package's {@code __init__.py}, {@code local.py}, and {@code http.py} all
 * map into this one file). {@link Transport} is the only consumer and depends on this interface
 * alone.
 *
 * <p><b>Async.</b> Python's {@code async def send} / {@code start} / {@code stop} become {@link
 * CompletableFuture}-returning methods — the idiomatic Java analogue of an awaitable. The
 * synchronous accessors ({@link #pid()}, {@link #group()}, {@link #unicast()}, {@link #anycast()},
 * {@link #targetResolver(String)}) and the callback registration {@link #recv(Consumer)} mirror
 * their plain (non-async) Python counterparts.
 */
public interface Network {

    /** The process id this network instance is bound to. */
    String pid();

    /** The process group this network instance participates in. */
    String group();

    /** The unicast address that targets this specific process. */
    String unicast();

    /** The anycast address that targets any process in the group. */
    String anycast();

    /** Start the network (open connections, begin the in-process server, ...). */
    CompletableFuture<Void> start();

    /** Stop the network and release its resources. */
    CompletableFuture<Void> stop();

    /** Send an already-serialized request, completing with the raw response string. */
    CompletableFuture<String> send(String req);

    /** Register a callback invoked with each raw incoming message string. */
    void recv(Consumer<String> callback);

    /** Resolve a logical target into a concrete address. */
    String targetResolver(String target);

    // =========================================================================
    // SHARED CONSTANTS
    // =========================================================================

    long I64_MAX = (1L << 63) - 1;
    long I64_MIN = -(1L << 63);
    BigInteger I64_MAX_BIG = BigInteger.valueOf(I64_MAX);
    BigInteger I64_MIN_BIG = BigInteger.valueOf(I64_MIN);
    BigInteger U64_MAX_BIG = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    /** Shared mapper. Rejects trailing tokens to mirror {@code msgspec.json.decode}. */
    ObjectMapper MAPPER = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    // =========================================================================
    // JSON FIELD ACCESSORS (mirror local.py module helpers)
    // =========================================================================

    /** {@code obj[key]} when {@code obj} is an object, else {@code null}. */
    private static JsonNode get(JsonNode obj, String key) {
        return obj != null && obj.isObject() ? obj.get(key) : null;
    }

    /** A string field, defaulting to {@code ""}. */
    private static String str(JsonNode obj, String key) {
        JsonNode v = get(obj, key);
        return v != null && v.isTextual() ? v.asText() : "";
    }

    /** An integer field, falling back to {@code default}. {@code bool} is excluded. */
    private static long i64(JsonNode obj, String key, long dflt) {
        JsonNode v = get(obj, key);
        return v != null && v.isIntegralNumber() ? v.asLong() : dflt;
    }

    /** A non-negative integer field, falling back to {@code default}. */
    private static long u64(Object value, long dflt) {
        if (value instanceof Number n && !(value instanceof Double) && !(value instanceof Float)) {
            long l = n.longValue();
            return l >= 0 ? l : dflt;
        }
        return dflt;
    }

    /** A TTL field clamped to the 64-bit signed range, else {@code default}. */
    private static long ttl(JsonNode obj, String key, long dflt) {
        JsonNode v = get(obj, key);
        if (v != null && v.isIntegralNumber()) {
            BigInteger b = v.bigIntegerValue();
            if (b.compareTo(I64_MIN_BIG) >= 0 && b.compareTo(I64_MAX_BIG) <= 0) {
                return b.longValue();
            }
            if (b.signum() >= 0 && b.compareTo(U64_MAX_BIG) <= 0) {
                return I64_MAX; // min(b, I64_MAX) — b > I64_MAX here
            }
        }
        return dflt;
    }

    /** {@code value} if it is an integer, else {@code null}. */
    private static Long optI64(JsonNode v) {
        return v != null && v.isIntegralNumber() ? v.asLong() : null;
    }

    /** A required non-empty string field; raises {@link ServerError} (400) otherwise. */
    private static String requireStr(JsonNode obj, String field) {
        JsonNode v = get(obj, field);
        if (v != null && v.isTextual() && !v.asText().isEmpty()) {
            return v.asText();
        }
        throw new ServerError(400, "missing or empty required field: " + field);
    }

    /** Add {@code a + b}, clamping to the 64-bit signed range. */
    private static long saturatingAdd(long a, long b) {
        long r = a + b;
        // Overflow occurred iff a and b share a sign that differs from the result's.
        if (((a ^ r) & (b ^ r)) < 0) {
            return b > 0 ? I64_MAX : I64_MIN;
        }
        return r;
    }

    /** Parse a settle state, defaulting to {@code "resolved"} for unknown values. */
    private static String parsePromiseState(JsonNode v) {
        String s = v != null && v.isTextual() ? v.asText() : "";
        return switch (s) {
            case "pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout" -> s;
            default -> "resolved";
        };
    }

    /** Parse a decimal string into a long, returning {@code null} if invalid. */
    private static Long parseLong(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Extract {@code tags} as a {@code str -> str} map (non-string values dropped). */
    private static Map<String, String> extractTags(JsonNode v) {
        Map<String, String> out = new LinkedHashMap<>();
        JsonNode tags = get(v, "tags");
        if (tags != null && tags.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = tags.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                if (e.getValue().isTextual()) {
                    out.put(e.getKey(), e.getValue().asText());
                }
            }
        }
        return out;
    }

    /** The {@code data} portion of a sub-envelope, or {@code val} if it is flat. */
    private static JsonNode extractActionData(JsonNode val) {
        if (val != null && val.isObject() && val.has("kind") && val.has("data")) {
            return val.get("data");
        }
        return val;
    }

    /** Unwrap a protocol envelope request into the flat format {@link ServerState} expects. */
    private static JsonNode unwrapRequestEnvelope(JsonNode req) {
        if (req != null && req.isObject() && req.has("head") && req.has("data")) {
            ObjectNode flat = MAPPER.createObjectNode();
            JsonNode data = req.get("data");
            if (data != null && data.isObject()) {
                flat.setAll((ObjectNode) data);
            }
            if (req.has("kind")) {
                flat.set("kind", req.get("kind"));
            }
            JsonNode head = req.get("head");
            if (head != null && head.isObject() && head.has("corrId")) {
                flat.set("corrId", head.get("corrId"));
            }
            return flat;
        }
        return req;
    }

    /** Wrap a flat {@link ServerState} response into the protocol envelope format. */
    private static Map<String, Object> wrapResponseEnvelope(Map<String, Object> flat) {
        Object kind = flat.get("kind");
        Object corrId = flat.get("corrId");
        long status = u64(flat.get("status"), 200);

        Map<String, Object> data = new LinkedHashMap<>(flat);
        data.remove("kind");
        data.remove("corrId");
        data.remove("status");

        Map<String, Object> head = new LinkedHashMap<>();
        head.put("corrId", corrId);
        head.put("status", status);
        head.put("version", Send.PROTOCOL_VERSION);

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("kind", kind);
        envelope.put("head", head);
        envelope.put("data", data);
        return envelope;
    }

    /** The task id of an {@code execute} outgoing message, else {@code null}. */
    private static String executeTaskId(Object message) {
        if (message instanceof Map<?, ?> m
                && "execute".equals(m.get("kind"))
                && m.get("data") instanceof Map<?, ?> d
                && d.get("task") instanceof Map<?, ?> t
                && t.get("id") instanceof String id) {
            return id;
        }
        return null;
    }

    /** Build an ordered map from {@code key, value, ...} pairs (values may be null). */
    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    // =========================================================================
    // SERVER STATE TYPES
    // =========================================================================

    /** A durable promise record as held by {@link ServerState}. Mutable in place, like the Python {@code Struct}. */
    final class DurablePromise {
        String id;
        String state;
        JsonNode param;
        JsonNode value;
        Map<String, String> tags;
        long timeoutAt;
        long createdAt;
        Long settledAt;
        final Set<String> awaiters = new LinkedHashSet<>();
        final Set<String> subscribers = new LinkedHashSet<>();

        Map<String, Object> toRecord() {
            Map<String, Object> obj = new LinkedHashMap<>();
            obj.put("id", id);
            obj.put("state", state);
            obj.put("param", param);
            obj.put("value", value);
            obj.put("tags", tags);
            obj.put("timeoutAt", timeoutAt);
            obj.put("createdAt", createdAt);
            if (settledAt != null) {
                obj.put("settledAt", settledAt);
            }
            return obj;
        }
    }

    /** A task record as held by {@link ServerState}. */
    final class Task {
        String id;
        String state;
        int version;
        String pid;
        Long ttl;
        final Set<String> resumes = new LinkedHashSet<>();

        Map<String, Object> toRecord() {
            Map<String, Object> obj = new LinkedHashMap<>();
            obj.put("id", id);
            obj.put("state", state);
            obj.put("version", version);
            obj.put("promiseId", id);
            if (pid != null) {
                obj.put("pid", pid);
            }
            if (ttl != null) {
                obj.put("ttl", ttl);
            }
            return obj;
        }
    }

    /** A schedule stub for local mode. */
    final class ScheduleStub {
        String id;
        String cron;
        String promiseId;
        long promiseTimeout;
        JsonNode promiseParam;
        Object promiseTags;
        long createdAt;

        Map<String, Object> toRecord() {
            Map<String, Object> obj = new LinkedHashMap<>();
            obj.put("id", id);
            obj.put("cron", cron);
            obj.put("promiseId", promiseId);
            obj.put("promiseTimeout", promiseTimeout);
            obj.put("promiseParam", promiseParam);
            obj.put("promiseTags", promiseTags);
            obj.put("createdAt", createdAt);
            obj.put("nextRunAt", 0);
            obj.put("lastRunAt", null);
            return obj;
        }
    }

    final class PTimeout {
        String id;
        long timeout;

        PTimeout(String id, long timeout) {
            this.id = id;
            this.timeout = timeout;
        }
    }

    final class TTimeout {
        String id;
        int typ;
        long timeout;

        TTimeout(String id, int typ, long timeout) {
            this.id = id;
            this.typ = typ;
            this.timeout = timeout;
        }
    }

    final class OutgoingMessage {
        String address;
        Object message;

        OutgoingMessage(String address, Object message) {
            this.address = address;
            this.message = message;
        }
    }

    // =========================================================================
    // SERVER STATE MACHINE
    // =========================================================================

    /**
     * The in-process server state machine driving {@link LocalNetwork}.
     *
     * <p>Owns promises, tasks, schedules, pending timeouts, and the queue of outgoing messages
     * produced while applying a request or ticking. A direct port of {@code local.ServerState}.
     */
    final class ServerState {
        static final int PENDING_RETRY_TTL = 30_000;

        public final Map<String, DurablePromise> promises = new LinkedHashMap<>();
        public final Map<String, Task> tasks = new LinkedHashMap<>();
        public final Map<String, ScheduleStub> schedules = new LinkedHashMap<>();
        final List<PTimeout> pTimeouts = new ArrayList<>();
        final List<TTimeout> tTimeouts = new ArrayList<>();
        List<OutgoingMessage> outgoing = new ArrayList<>();

        Map<String, Object> apply(long now, JsonNode req) {
            outgoing.clear();

            String kind = str(req, "kind");
            JsonNode corrId = get(req, "corrId");

            switch (kind) {
                case "promise.get", "promise.create", "promise.settle" -> tryAutoTimeout(now, str(req, "id"));
                case "promise.register_listener" -> tryAutoTimeout(now, str(req, "awaited"));
                case "task.create" -> tryAutoTimeout(now, str(extractActionData(get(req, "action")), "id"));
                case "task.acquire", "task.release", "task.fulfill" -> tryAutoTimeout(now, str(req, "id"));
                case "task.suspend" -> {
                    tryAutoTimeout(now, str(req, "id"));
                    JsonNode actions = get(req, "actions");
                    if (actions != null && actions.isArray()) {
                        for (JsonNode action : actions) {
                            String awaited = str(extractActionData(action), "awaited");
                            if (!awaited.isEmpty()) {
                                tryAutoTimeout(now, awaited);
                            }
                        }
                    }
                }
                default -> {}
            }

            return switch (kind) {
                case "promise.get" -> promiseGet(corrId, req);
                case "promise.create" -> promiseCreate(now, corrId, req);
                case "promise.settle" -> promiseSettle(now, corrId, req);
                case "promise.register_listener" -> promiseRegisterListener(corrId, req);
                case "task.create" -> taskCreate(now, corrId, req);
                case "task.acquire" -> taskAcquire(now, corrId, req);
                case "task.release" -> taskRelease(now, corrId, req);
                case "task.fulfill" -> taskFulfill(now, corrId, req);
                case "task.suspend" -> taskSuspend(now, corrId, req);
                case "task.heartbeat" -> taskHeartbeat(now, corrId, req);
                case "schedule.create" -> scheduleCreate(now, corrId, req);
                case "schedule.get" -> scheduleGet(corrId, req);
                case "schedule.delete" -> scheduleDelete(corrId, req);
                case "schedule.search" -> scheduleSearch(corrId, req);
                default -> throw new ServerError(400, "unknown request kind: " + kind);
            };
        }

        void tick(long now) {
            List<String> promiseSettles = new ArrayList<>();
            List<String> releaseIds = new ArrayList<>();
            List<Integer> releaseVersions = new ArrayList<>();
            List<String> retryIds = new ArrayList<>();

            for (PTimeout pt : pTimeouts) {
                if (now >= pt.timeout) {
                    DurablePromise p = promises.get(pt.id);
                    if (p != null && "pending".equals(p.state)) {
                        promiseSettles.add(pt.id);
                    }
                }
            }
            for (TTimeout tt : tTimeouts) {
                if (now < tt.timeout) {
                    continue;
                }
                if (tt.typ == 1) {
                    Task t = tasks.get(tt.id);
                    if (t != null && "acquired".equals(t.state)) {
                        releaseIds.add(tt.id);
                        releaseVersions.add(t.version);
                    }
                } else if (tt.typ == 0) {
                    Task t = tasks.get(tt.id);
                    if (t != null && "pending".equals(t.state)) {
                        retryIds.add(tt.id);
                    }
                }
            }

            // Phase 1: Settle promises.
            for (String pid : promiseSettles) {
                DurablePromise p = promises.get(pid);
                if (p == null || !"pending".equals(p.state)) {
                    continue;
                }
                p.state = timeoutState(p.tags);
                p.value = null;
                p.settledAt = p.timeoutAt;
                delPTimeout(pid);
            }
            // Phase 2: Fulfill tasks whose own promise settled.
            for (String pid : promiseSettles) {
                enqueueSettle(pid);
            }
            // Phase 3: Resume awaiters and notify subscribers.
            for (String pid : promiseSettles) {
                resumeAwaiters(pid, now);
                notifySubscribers(pid);
            }

            // Phase 4: Release expired leases.
            for (int i = 0; i < releaseIds.size(); i++) {
                String tid = releaseIds.get(i);
                int version = releaseVersions.get(i);
                Task t = tasks.get(tid);
                if (t != null && "acquired".equals(t.state) && t.version == version) {
                    int newVersion = t.version + 1;
                    t.state = "pending";
                    t.version = newVersion;
                    t.pid = null;
                    t.ttl = null;
                    setTTimeout(tid, 0, now + PENDING_RETRY_TTL);
                    DurablePromise p = promises.get(tid);
                    if (p != null) {
                        String addr = p.tags.get("resonate:target");
                        if (addr != null) {
                            sendExecuteMessage(addr, tid, newVersion);
                        }
                    }
                }
            }

            // Phase 5: Retry pending tasks.
            for (String tid : retryIds) {
                Task t = tasks.get(tid);
                if (t != null && "pending".equals(t.state)) {
                    int v = t.version;
                    setTTimeout(tid, 0, now + PENDING_RETRY_TTL);
                    DurablePromise p = promises.get(tid);
                    if (p != null) {
                        String addr = p.tags.get("resonate:target");
                        if (addr != null) {
                            sendExecuteMessage(addr, tid, v);
                        }
                    }
                }
            }
        }

        // ---- PROMISE OPERATIONS --------------------------------------------

        Map<String, Object> promiseGet(Object corrId, JsonNode req) {
            String promiseId = requireStr(req, "id");
            DurablePromise p = promises.get(promiseId);
            if (p != null) {
                return map("kind", "promise.get", "corrId", corrId, "status", 200, "promise", p.toRecord());
            }
            return map(
                    "kind",
                    "promise.get",
                    "corrId",
                    corrId,
                    "status",
                    404,
                    "error",
                    "promise " + promiseId + " not found");
        }

        Map<String, Object> promiseCreate(long now, Object corrId, JsonNode req) {
            String promiseId = requireStr(req, "id");

            DurablePromise existing = promises.get(promiseId);
            if (existing != null) {
                return map("kind", "promise.create", "corrId", corrId, "status", 200, "promise", existing.toRecord());
            }

            long timeoutAt = i64(req, "timeoutAt", I64_MAX);
            JsonNode param = get(req, "param");
            Map<String, String> tags = extractTags(req);

            if (now >= timeoutAt) {
                DurablePromise promise = new DurablePromise();
                promise.id = promiseId;
                promise.state = timeoutState(tags);
                promise.param = param;
                promise.value = null;
                promise.tags = tags;
                promise.timeoutAt = timeoutAt;
                promise.createdAt = timeoutAt;
                promise.settledAt = timeoutAt;
                Map<String, Object> record = promise.toRecord();
                promises.put(promiseId, promise);
                enqueueSettle(promiseId);
                resumeAwaiters(promiseId, now);
                notifySubscribers(promiseId);
                return map("kind", "promise.create", "corrId", corrId, "status", 200, "promise", record);
            }

            DurablePromise promise = new DurablePromise();
            promise.id = promiseId;
            promise.state = "pending";
            promise.param = param;
            promise.value = null;
            promise.tags = tags;
            promise.timeoutAt = timeoutAt;
            promise.createdAt = now;
            promise.settledAt = null;
            Map<String, Object> record = promise.toRecord();
            promises.put(promiseId, promise);
            setPTimeout(promiseId, timeoutAt);

            String address = tags.get("resonate:target");
            if (address != null) {
                Long delay = parseLong(tags.get("resonate:delay"));
                boolean deferred = delay != null && now < delay;
                Task task = new Task();
                task.id = promiseId;
                task.state = "pending";
                task.version = 0;
                tasks.put(promiseId, task);
                setTTimeout(promiseId, 0, deferred ? delay : now + PENDING_RETRY_TTL);
                if (!deferred) {
                    sendExecuteMessage(address, promiseId, 0);
                }
            }

            return map("kind", "promise.create", "corrId", corrId, "status", 201, "promise", record);
        }

        Map<String, Object> promiseSettle(long now, Object corrId, JsonNode req) {
            String promiseId = requireStr(req, "id");
            String settleState = parsePromiseState(get(req, "state"));
            JsonNode value = get(req, "value");

            DurablePromise p = promises.get(promiseId);
            if (p == null) {
                return map(
                        "kind",
                        "promise.settle",
                        "corrId",
                        corrId,
                        "status",
                        404,
                        "error",
                        "promise " + promiseId + " not found");
            }
            if (!"pending".equals(p.state)) {
                return map("kind", "promise.settle", "corrId", corrId, "status", 200, "promise", p.toRecord());
            }

            p.state = settleState;
            p.value = value;
            p.settledAt = now;
            Map<String, Object> record = p.toRecord();
            delPTimeout(promiseId);
            enqueueSettle(promiseId);
            resumeAwaiters(promiseId, now);
            notifySubscribers(promiseId);
            return map("kind", "promise.settle", "corrId", corrId, "status", 200, "promise", record);
        }

        Map<String, Object> promiseRegisterListener(Object corrId, JsonNode req) {
            String awaited = requireStr(req, "awaited");
            String address = requireStr(req, "address");

            DurablePromise p = promises.get(awaited);
            if (p != null) {
                if ("pending".equals(p.state)) {
                    p.subscribers.add(address);
                }
                return map(
                        "kind", "promise.register_listener", "corrId", corrId, "status", 200, "promise", p.toRecord());
            }
            return map(
                    "kind",
                    "promise.register_listener",
                    "corrId",
                    corrId,
                    "status",
                    404,
                    "error",
                    "promise " + awaited + " not found");
        }

        // ---- TASK OPERATIONS -----------------------------------------------

        Map<String, Object> taskCreate(long now, Object corrId, JsonNode req) {
            String pid = requireStr(req, "pid");
            long ttl = ttl(req, "ttl", 60_000);
            JsonNode promiseReq = extractActionData(get(req, "action"));
            String promiseId = requireStr(promiseReq, "id");

            Task existingTask = tasks.get(promiseId);
            if (existingTask != null) {
                String taskState = existingTask.state;
                DurablePromise p = promises.get(promiseId);
                Map<String, Object> promiseRecord = p != null ? p.toRecord() : null;

                if ("pending".equals(taskState)) {
                    List<Map<String, Object>> preload = preload(promiseId);
                    existingTask.state = "acquired";
                    existingTask.pid = pid;
                    existingTask.ttl = ttl;
                    existingTask.resumes.clear();
                    setTTimeout(promiseId, 1, saturatingAdd(now, ttl));
                    return map(
                            "kind",
                            "task.create",
                            "corrId",
                            corrId,
                            "status",
                            200,
                            "task",
                            existingTask.toRecord(),
                            "promise",
                            promiseRecord,
                            "preload",
                            preload);
                }
                if ("fulfilled".equals(taskState)) {
                    return map(
                            "kind",
                            "task.create",
                            "corrId",
                            corrId,
                            "status",
                            200,
                            "task",
                            existingTask.toRecord(),
                            "promise",
                            promiseRecord,
                            "preload",
                            preload(promiseId));
                }
                return map("kind", "task.create", "corrId", corrId, "status", 409, "error", "Already exists");
            }

            if (promises.containsKey(promiseId)) {
                return map("kind", "task.create", "corrId", corrId, "status", 409, "error", "Already exists");
            }

            long timeoutAt = i64(promiseReq, "timeoutAt", I64_MAX);
            JsonNode param = get(promiseReq, "param");
            Map<String, String> tags = extractTags(promiseReq);

            if (now >= timeoutAt) {
                DurablePromise promise = new DurablePromise();
                promise.id = promiseId;
                promise.state = timeoutState(tags);
                promise.param = param;
                promise.value = null;
                promise.tags = tags;
                promise.timeoutAt = timeoutAt;
                promise.createdAt = timeoutAt;
                promise.settledAt = timeoutAt;
                Map<String, Object> pr = promise.toRecord();
                promises.put(promiseId, promise);
                Task task = new Task();
                task.id = promiseId;
                task.state = "fulfilled";
                task.version = 0;
                Map<String, Object> tr = task.toRecord();
                tasks.put(promiseId, task);
                return map(
                        "kind",
                        "task.create",
                        "corrId",
                        corrId,
                        "status",
                        200,
                        "task",
                        tr,
                        "promise",
                        pr,
                        "preload",
                        List.of());
            }

            DurablePromise promise = new DurablePromise();
            promise.id = promiseId;
            promise.state = "pending";
            promise.param = param;
            promise.value = null;
            promise.tags = tags;
            promise.timeoutAt = timeoutAt;
            promise.createdAt = now;
            promise.settledAt = null;
            Map<String, Object> pr = promise.toRecord();
            promises.put(promiseId, promise);
            setPTimeout(promiseId, timeoutAt);

            Task task = new Task();
            task.id = promiseId;
            task.state = "acquired";
            task.version = 0;
            task.pid = pid;
            task.ttl = ttl;
            Map<String, Object> tr = task.toRecord();
            tasks.put(promiseId, task);
            setTTimeout(promiseId, 1, saturatingAdd(now, ttl));

            return map(
                    "kind",
                    "task.create",
                    "corrId",
                    corrId,
                    "status",
                    201,
                    "task",
                    tr,
                    "promise",
                    pr,
                    "preload",
                    preload(promiseId));
        }

        Map<String, Object> taskAcquire(long now, Object corrId, JsonNode req) {
            String taskId = requireStr(req, "id");
            String pid = str(req, "pid");
            long ttl = Math.max(ttl(req, "ttl", 60_000), 1);

            Task t = tasks.get(taskId);
            if (t == null) {
                return map(
                        "kind",
                        "task.acquire",
                        "corrId",
                        corrId,
                        "status",
                        404,
                        "error",
                        "task " + taskId + " not found");
            }
            if (!"pending".equals(t.state)) {
                return map(
                        "kind",
                        "task.acquire",
                        "corrId",
                        corrId,
                        "status",
                        409,
                        "error",
                        "task not in pending state (state: " + capitalize(t.state) + ")");
            }

            List<Map<String, Object>> preload = preload(taskId);
            t.state = "acquired";
            t.pid = pid;
            t.ttl = ttl;
            t.resumes.clear();
            setTTimeout(taskId, 1, saturatingAdd(now, ttl));
            DurablePromise p = promises.get(taskId);
            Map<String, Object> promiseRecord = p != null ? p.toRecord() : null;
            return map(
                    "kind",
                    "task.acquire",
                    "corrId",
                    corrId,
                    "status",
                    200,
                    "task",
                    t.toRecord(),
                    "promise",
                    promiseRecord,
                    "preload",
                    preload);
        }

        Map<String, Object> taskRelease(long now, Object corrId, JsonNode req) {
            String taskId = requireStr(req, "id");

            Task t = tasks.get(taskId);
            if (t == null) {
                return map("kind", "task.release", "corrId", corrId, "status", 404);
            }
            if (!"acquired".equals(t.state)) {
                return map("kind", "task.release", "corrId", corrId, "status", 409);
            }

            int newVersion = t.version + 1;
            t.state = "pending";
            t.version = newVersion;
            t.pid = null;
            t.ttl = null;
            setTTimeout(taskId, 0, now + PENDING_RETRY_TTL);
            DurablePromise p = promises.get(taskId);
            if (p != null) {
                String addr = p.tags.get("resonate:target");
                if (addr != null) {
                    sendExecuteMessage(addr, taskId, newVersion);
                }
            }
            return map("kind", "task.release", "corrId", corrId, "status", 200);
        }

        Map<String, Object> taskFulfill(long now, Object corrId, JsonNode req) {
            String taskId = requireStr(req, "id");

            Task t = tasks.get(taskId);
            if (t == null) {
                return map("kind", "task.fulfill", "corrId", corrId, "status", 404);
            }
            if (!"acquired".equals(t.state)) {
                return map("kind", "task.fulfill", "corrId", corrId, "status", 409);
            }

            JsonNode settle = extractActionData(get(req, "action"));
            JsonNode settleId = get(settle, "id");
            String promiseId = settleId != null && settleId.isTextual() ? settleId.asText() : taskId;
            String settleState = parsePromiseState(get(settle, "state"));
            JsonNode value = get(settle, "value");

            DurablePromise existing = promises.get(promiseId);
            if (existing != null && "pending".equals(existing.state)) {
                existing.state = settleState;
                existing.value = value;
                existing.settledAt = now;
                delPTimeout(promiseId);
            }

            DurablePromise p = promises.get(promiseId);
            Map<String, Object> promiseRecord = p != null ? p.toRecord() : null;

            enqueueSettle(taskId);
            resumeAwaiters(promiseId, now);
            notifySubscribers(promiseId);

            return map("kind", "task.fulfill", "corrId", corrId, "status", 200, "promise", promiseRecord);
        }

        Map<String, Object> taskSuspend(long now, Object corrId, JsonNode req) {
            String taskId = requireStr(req, "id");

            Task t = tasks.get(taskId);
            if (t == null) {
                return map("kind", "task.suspend", "corrId", corrId, "status", 404);
            }
            if (!"acquired".equals(t.state)) {
                return map("kind", "task.suspend", "corrId", corrId, "status", 409);
            }

            if (!t.resumes.isEmpty()) {
                t.resumes.clear();
                return map(
                        "kind",
                        "task.suspend",
                        "corrId",
                        corrId,
                        "status",
                        300,
                        "redirect",
                        true,
                        "preload",
                        preload(taskId));
            }

            List<String> callbacks = new ArrayList<>();
            JsonNode actions = get(req, "actions");
            if (actions != null && actions.isArray()) {
                for (JsonNode action : actions) {
                    JsonNode awaited = get(extractActionData(action), "awaited");
                    if (awaited != null && awaited.isTextual()) {
                        callbacks.add(awaited.asText());
                    }
                }
            }

            boolean anySettled = false;
            for (String awaitedId : callbacks) {
                DurablePromise p = promises.get(awaitedId);
                if (p == null) {
                    continue;
                }
                if ("pending".equals(p.state)) {
                    p.awaiters.add(taskId);
                } else {
                    anySettled = true;
                }
            }

            if (anySettled) {
                return map(
                        "kind",
                        "task.suspend",
                        "corrId",
                        corrId,
                        "status",
                        300,
                        "redirect",
                        true,
                        "preload",
                        preload(taskId));
            }

            t.state = "suspended";
            t.pid = null;
            t.ttl = null;
            t.resumes.clear();
            delTTimeout(taskId);

            return map("kind", "task.suspend", "corrId", corrId, "status", 200);
        }

        Map<String, Object> taskHeartbeat(long now, Object corrId, JsonNode req) {
            String pid = requireStr(req, "pid");

            JsonNode tasksNode = get(req, "tasks");
            if (tasksNode != null && tasksNode.isArray()) {
                for (JsonNode taskRef : tasksNode) {
                    String taskRefId = requireStr(taskRef, "id");
                    Long version = optI64(get(taskRef, "version"));
                    Task t = tasks.get(taskRefId);
                    if (t != null
                            && "acquired".equals(t.state)
                            && pid.equals(t.pid)
                            && (version == null || version == t.version)) {
                        long ttl = t.ttl != null ? t.ttl : 30_000;
                        setTTimeout(taskRefId, 1, saturatingAdd(now, ttl));
                    }
                }
            }

            return map("kind", "task.heartbeat", "corrId", corrId, "status", 200);
        }

        // ---- SCHEDULE OPERATIONS (stubs) -----------------------------------

        Map<String, Object> scheduleCreate(long now, Object corrId, JsonNode req) {
            String scheduleId = requireStr(req, "id");
            ScheduleStub existing = schedules.get(scheduleId);
            if (existing != null) {
                return map("kind", "schedule.create", "corrId", corrId, "status", 200, "schedule", existing.toRecord());
            }
            JsonNode promiseTags = get(req, "promiseTags");
            ScheduleStub stub = new ScheduleStub();
            stub.id = scheduleId;
            stub.cron = requireStr(req, "cron");
            stub.promiseId = requireStr(req, "promiseId");
            stub.promiseTimeout = i64(req, "promiseTimeout", 0);
            stub.promiseParam = get(req, "promiseParam");
            stub.promiseTags = promiseTags != null ? promiseTags : new LinkedHashMap<>();
            stub.createdAt = now;
            Map<String, Object> record = stub.toRecord();
            schedules.put(scheduleId, stub);
            return map("kind", "schedule.create", "corrId", corrId, "status", 201, "schedule", record);
        }

        Map<String, Object> scheduleGet(Object corrId, JsonNode req) {
            String scheduleId = requireStr(req, "id");
            ScheduleStub stub = schedules.get(scheduleId);
            if (stub != null) {
                return map("kind", "schedule.get", "corrId", corrId, "status", 200, "schedule", stub.toRecord());
            }
            return map(
                    "kind",
                    "schedule.get",
                    "corrId",
                    corrId,
                    "status",
                    404,
                    "error",
                    "schedule " + scheduleId + " not found");
        }

        Map<String, Object> scheduleDelete(Object corrId, JsonNode req) {
            String scheduleId = requireStr(req, "id");
            if (schedules.remove(scheduleId) != null) {
                return map("kind", "schedule.delete", "corrId", corrId, "status", 200);
            }
            return map(
                    "kind",
                    "schedule.delete",
                    "corrId",
                    corrId,
                    "status",
                    404,
                    "error",
                    "schedule " + scheduleId + " not found");
        }

        Map<String, Object> scheduleSearch(Object corrId, JsonNode req) {
            List<Map<String, Object>> out = new ArrayList<>();
            for (ScheduleStub s : schedules.values()) {
                out.add(s.toRecord());
            }
            return map("kind", "schedule.search", "corrId", corrId, "status", 200, "schedules", out, "cursor", null);
        }

        // ---- HELPERS -------------------------------------------------------

        void tryAutoTimeout(long now, String promiseId) {
            DurablePromise p = promises.get(promiseId);
            if (p == null || !"pending".equals(p.state) || now < p.timeoutAt) {
                return;
            }
            p.state = timeoutState(p.tags);
            p.settledAt = p.timeoutAt;
            delPTimeout(promiseId);
            enqueueSettle(promiseId);
            resumeAwaiters(promiseId, now);
            notifySubscribers(promiseId);
        }

        void enqueueSettle(String promiseId) {
            Task t = tasks.get(promiseId);
            if (t == null) {
                DurablePromise p = promises.get(promiseId);
                if (p != null && p.tags.containsKey("resonate:target")) {
                    Task created = new Task();
                    created.id = promiseId;
                    created.state = "fulfilled";
                    created.version = 0;
                    tasks.put(promiseId, created);
                }
                return;
            }
            if ("fulfilled".equals(t.state)) {
                return;
            }
            t.state = "fulfilled";
            t.pid = null;
            t.ttl = null;
            t.resumes.clear();
            delTTimeout(promiseId);
            for (DurablePromise p : promises.values()) {
                p.awaiters.remove(promiseId);
            }
        }

        void resumeAwaiters(String promiseId, long now) {
            DurablePromise p = promises.get(promiseId);
            List<String> awaiterIds = p != null ? new ArrayList<>(p.awaiters) : List.of();

            for (String awaiterId : awaiterIds) {
                Task task = tasks.get(awaiterId);
                if (task == null) {
                    continue;
                }
                if ("suspended".equals(task.state)) {
                    int newVersion = task.version + 1;
                    task.state = "pending";
                    task.version = newVersion;
                    task.resumes.clear();
                    task.resumes.add(promiseId);
                    setTTimeout(awaiterId, 0, now + PENDING_RETRY_TTL);
                    DurablePromise ap = promises.get(awaiterId);
                    if (ap != null) {
                        String addr = ap.tags.get("resonate:target");
                        if (addr != null) {
                            sendExecuteMessage(addr, awaiterId, newVersion);
                        }
                    }
                } else if ("pending".equals(task.state)
                        || "acquired".equals(task.state)
                        || "halted".equals(task.state)) {
                    task.resumes.add(promiseId);
                }
            }

            DurablePromise p2 = promises.get(promiseId);
            if (p2 != null) {
                p2.awaiters.clear();
            }
        }

        void notifySubscribers(String promiseId) {
            DurablePromise p = promises.get(promiseId);
            if (p == null || p.subscribers.isEmpty()) {
                return;
            }
            List<String> subscribers = new ArrayList<>(p.subscribers);
            Map<String, Object> record = p.toRecord();
            for (String address : subscribers) {
                outgoing.add(new OutgoingMessage(address, map("kind", "unblock", "data", map("promise", record))));
            }
            p.subscribers.clear();
        }

        List<Map<String, Object>> preload(String promiseId) {
            DurablePromise p = promises.get(promiseId);
            if (p == null) {
                return new ArrayList<>();
            }
            String branch = p.tags.get("resonate:branch");
            if (branch == null) {
                return new ArrayList<>();
            }
            List<Map<String, Object>> out = new ArrayList<>();
            for (DurablePromise other : promises.values()) {
                if (!other.id.equals(promiseId) && branch.equals(other.tags.get("resonate:branch"))) {
                    out.add(other.toRecord());
                }
            }
            return out;
        }

        String timeoutState(Map<String, String> tags) {
            return "true".equals(tags.get("resonate:timer")) ? "resolved" : "rejected_timedout";
        }

        void setPTimeout(String promiseId, long timeout) {
            for (PTimeout pt : pTimeouts) {
                if (pt.id.equals(promiseId)) {
                    pt.timeout = timeout;
                    return;
                }
            }
            pTimeouts.add(new PTimeout(promiseId, timeout));
        }

        void delPTimeout(String promiseId) {
            pTimeouts.removeIf(pt -> pt.id.equals(promiseId));
        }

        void setTTimeout(String taskId, int typ, long timeout) {
            for (TTimeout tt : tTimeouts) {
                if (tt.id.equals(taskId)) {
                    tt.typ = typ;
                    tt.timeout = timeout;
                    return;
                }
            }
            tTimeouts.add(new TTimeout(taskId, typ, timeout));
        }

        void delTTimeout(String taskId) {
            tTimeouts.removeIf(tt -> tt.id.equals(taskId));
        }

        void sendExecuteMessage(String address, String taskId, int version) {
            Map<String, Object> msg =
                    map("kind", "execute", "data", map("task", map("id", taskId, "version", version)));
            for (OutgoingMessage existing : outgoing) {
                if (taskId.equals(executeTaskId(existing.message))) {
                    existing.address = address;
                    existing.message = msg;
                    return;
                }
            }
            outgoing.add(new OutgoingMessage(address, msg));
        }

        private String capitalize(String s) {
            return s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
        }
    }

    // =========================================================================
    // LOCAL NETWORK
    // =========================================================================

    /**
     * In-process {@link Network} backed by a {@link ServerState} simulation. A direct port of
     * {@code local.LocalNetwork}.
     *
     * <p>A background tick loop advances time once per second; outgoing messages are dispatched to
     * registered callbacks off the critical path.
     */
    final class LocalNetwork implements Network {
        public final ServerState state = new ServerState();

        private final String pid;
        private final String group;
        private final String unicast;
        private final String anycast;
        private final ReentrantLock lock = new ReentrantLock();
        private final List<Consumer<String>> subscribers = new ArrayList<>();
        private volatile Thread tickThread;
        // Single virtual worker so dispatch stays FIFO (ordering matters) while still being
        // JVM-managed and always daemon.
        private final ExecutorService dispatchExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("resonate-local-dispatch").factory());

        public LocalNetwork() {
            this(null, null);
        }

        public LocalNetwork(String pid, String group) {
            this.pid = pid != null ? pid : "default";
            this.group = group != null ? group : "default";
            this.unicast = "local://uni@" + this.group + "/" + this.pid;
            this.anycast = "local://any@" + this.group + "/" + this.pid;
        }

        @Override
        public String pid() {
            return pid;
        }

        @Override
        public String group() {
            return group;
        }

        @Override
        public String unicast() {
            return unicast;
        }

        @Override
        public String anycast() {
            return anycast;
        }

        @Override
        public CompletableFuture<Void> start() {
            // Runs once immediately, then roughly once per second thereafter (fixed delay). Dedicated
            // daemon platform thread: a single long-lived timer, so a virtual thread would only add
            // carrier-pool contention with no benefit.
            tickThread = Thread.ofPlatform()
                    .name("resonate-local-tick")
                    .daemon()
                    .start(() -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            tickOnce();
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    });
            return CompletableFuture.completedFuture(null);
        }

        private void tickOnce() {
            long now = Send.nowMs();
            lock.lock();
            try {
                state.outgoing.clear();
                state.tick(now);
                List<OutgoingMessage> outgoing = state.outgoing;
                state.outgoing = new ArrayList<>();
                // Submit under the lock so dispatch order follows mutation order: send() and
                // tickOnce() run on different threads, and a post-unlock submit could otherwise
                // enqueue a later batch ahead of an earlier one.
                dispatchMessages(outgoing);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public CompletableFuture<Void> stop() {
            Thread tick = tickThread;
            tickThread = null;
            if (tick != null) {
                tick.interrupt();
                try {
                    tick.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            lock.lock();
            try {
                subscribers.clear();
            } finally {
                lock.unlock();
            }
            // Drain the dispatch worker so its virtual thread is gone once stop() returns (the tick
            // thread is already joined above, so no new work arrives). Not restarted after stop.
            dispatchExecutor.shutdown();
            try {
                if (!dispatchExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    dispatchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                dispatchExecutor.shutdownNow();
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<String> send(String req) {
            JsonNode reqJson;
            try {
                reqJson = MAPPER.readTree(req);
            } catch (JsonProcessingException exc) {
                return CompletableFuture.failedFuture(new DecodingError("invalid JSON request: " + exc.getMessage()));
            }
            if (reqJson == null || reqJson.isMissingNode()) {
                return CompletableFuture.failedFuture(new DecodingError("invalid JSON request: no content"));
            }

            JsonNode flatReq = unwrapRequestEnvelope(reqJson);

            long now = Send.nowMs();
            Map<String, Object> flatResponse;
            lock.lock();
            try {
                try {
                    flatResponse = state.apply(now, flatReq);
                } catch (RuntimeException exc) {
                    return CompletableFuture.failedFuture(exc);
                }
                List<OutgoingMessage> outgoing = state.outgoing;
                state.outgoing = new ArrayList<>();
                // Submit under the lock so dispatch order follows mutation order across the
                // concurrent send()/tickOnce() threads (see tickOnce).
                dispatchMessages(outgoing);
            } finally {
                lock.unlock();
            }

            Map<String, Object> envelope = wrapResponseEnvelope(flatResponse);
            String respStr;
            try {
                respStr = MAPPER.writeValueAsString(envelope);
            } catch (JsonProcessingException exc) {
                return CompletableFuture.failedFuture(new DecodingError("invalid JSON response: " + exc.getMessage()));
            }

            return CompletableFuture.completedFuture(respStr);
        }

        @Override
        public void recv(Consumer<String> callback) {
            lock.lock();
            try {
                subscribers.add(callback);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public String targetResolver(String target) {
            return "local://any@" + target;
        }

        /**
         * Hand a detached message batch to the dispatch thread. Must be called with {@code lock}
         * held: the subscriber snapshot and the submit happen under the same lock that mutated the
         * state, so dispatch order matches mutation order across the send/tick threads. The detached
         * {@code messages} are owned by the dispatch thread from here on -- the state never touches
         * them again -- so the actual serialization runs off the lock on {@code dispatchExecutor}.
         */
        private void dispatchMessages(List<OutgoingMessage> messages) {
            if (messages.isEmpty() || subscribers.isEmpty()) {
                return;
            }
            List<Consumer<String>> subs = new ArrayList<>(subscribers);
            dispatchExecutor.execute(() -> {
                for (OutgoingMessage msg : messages) {
                    String msgStr;
                    try {
                        msgStr = MAPPER.writeValueAsString(msg.message);
                    } catch (JsonProcessingException exc) {
                        continue;
                    }
                    for (Consumer<String> cb : subs) {
                        cb.accept(msgStr);
                    }
                }
            });
        }
    }

    // =========================================================================
    // HTTP NETWORK
    // =========================================================================

    /** A retriable connection failure — the analogue of {@code aiohttp.ClientError}. */
    final class ConnectionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ConnectionException(Throwable cause) {
            super(cause);
        }
    }

    /** The session was closed mid-flight — the analogue of {@code RuntimeError("Session is closed")}. */
    final class SessionClosedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SessionClosedException(String message) {
            super(message);
        }
    }

    /** SSE response handle: status plus the body stream. */
    final class SseResponse {
        final int status;
        final InputStream body;

        SseResponse(int status, InputStream body) {
            this.status = status;
            this.body = body;
        }
    }

    /**
     * The HTTP transport seam — the analogue of an {@code aiohttp.ClientSession}. {@code post}
     * throws {@link ConnectionException} on a connection failure (retriable) and {@link
     * SessionClosedException} once closed; any HTTP response (200/404/500/…) returns its body.
     */
    interface HttpSession {
        String post(String url, String body, Map<String, String> headers);

        default SseResponse get(String url, Map<String, String> headers) {
            throw new UnsupportedOperationException();
        }

        default void close() {}

        /** The configured connection cap — mirrors {@code session.connector.limit}. */
        default int limit() {
            return -1;
        }
    }

    /** Real {@link HttpSession} backed by {@link HttpClient}. */
    final class RealHttpSession implements HttpSession {
        private final HttpClient client = HttpClient.newHttpClient();
        private final int limit;
        private volatile boolean closed;

        RealHttpSession(int limit) {
            // ponytail: limit is stored for parity with aiohttp's connector cap (test asserts it);
            // HttpClient pools connections internally with no simple per-client cap to wire it to.
            this.limit = limit;
        }

        @Override
        public String post(String url, String body, Map<String, String> headers) {
            if (closed) {
                throw new SessionClosedException("Session is closed");
            }
            HttpRequest.Builder b =
                    HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString(body));
            headers.forEach(b::header);
            try {
                return client.send(b.build(), HttpResponse.BodyHandlers.ofString())
                        .body();
            } catch (IOException e) {
                if (closed) {
                    throw new SessionClosedException("Session is closed");
                }
                throw new ConnectionException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectionException(e);
            }
        }

        @Override
        public SseResponse get(String url, Map<String, String> headers) {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).GET();
            headers.forEach(b::header);
            try {
                HttpResponse<InputStream> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
                return new SseResponse(resp.statusCode(), resp.body());
            } catch (IOException e) {
                throw new ConnectionException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectionException(e);
            }
        }

        @Override
        public void close() {
            closed = true;
            // shutdownNow cancels the in-flight SSE long-poll so the daemon poll thread unblocks and
            // exits promptly; otherwise it survives into JVM shutdown and dies with a
            // NoClassDefFoundError trying to load a not-yet-loaded class (e.g. SseResponse).
            client.shutdownNow();
        }

        @Override
        public int limit() {
            return limit;
        }
    }

    /**
     * {@link Network} implementation that talks to a Resonate server over HTTP. A port of {@code
     * http.HttpNetwork}.
     *
     * <p>Requests go via {@code POST /}; incoming messages arrive over SSE on {@code
     * GET /poll/{group}/{pid}}. Non-final and the I/O seams ({@link #ensureSession()}, {@link
     * #sleepOrStop(double)}) are overridable so tests can inject a fake session and collapse the
     * backoff, mirroring the Python tests' monkeypatching.
     */
    class HttpNetwork implements Network {
        static final double INITIAL_BACKOFF_SECS = 1;
        static final double MAX_BACKOFF_SECS = 60;
        public static final int DEFAULT_CONN_LIMIT = 256;

        // Per-instance virtual-thread pool for the blocking send/backoff loop: a request parked in its
        // 60s backoff unmounts its carrier instead of starving a shared pool. Owned by this network so
        // stop() can drain it: the send future's .thenApply continuations (e.g. building Send.Created)
        // run on these threads, and if one ran during JVM shutdown it would NoClassDefFoundError on a
        // not-yet-loaded class.
        private final ExecutorService sendExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("resonate-http-send-", 0).factory());

        private final String pid;
        private final String group;
        private final String unicast;
        private final String anycast;
        final String url;
        private final String auth;
        final int connLimit;

        // recv (caller thread) adds while the SSE thread iterates: copy-on-write keeps both lock-free
        // and free of ConcurrentModificationException, which the single-threaded Python list got for free.
        private final List<Consumer<String>> subscribers = new CopyOnWriteArrayList<>();
        volatile HttpSession session;
        private volatile Thread sseThread;
        volatile boolean running;

        // ReentrantLock + Condition, not synchronized/wait(): a virtual send or SSE thread parked here
        // during backoff must unmount its carrier. Object.wait() inside a monitor pins the carrier on
        // JDK 21, which would stall the whole carrier pool for up to a 60s backoff.
        private final ReentrantLock stopLock = new ReentrantLock();
        private final Condition stopCond = stopLock.newCondition();
        private boolean stopSignaled;

        public HttpNetwork(String url) {
            this(url, null, null, null, null);
        }

        public HttpNetwork(String url, String pid, String group, String auth, Integer connLimit) {
            this.pid = pid != null ? pid : UUID.randomUUID().toString().replace("-", "");
            this.group = group != null ? group : "default";
            this.unicast = "poll://uni@" + this.group + "/" + this.pid;
            this.anycast = "poll://any@" + this.group + "/" + this.pid;
            this.url = stripTrailingSlash(url);
            this.auth = auth;
            this.connLimit = connLimit != null ? connLimit : DEFAULT_CONN_LIMIT;
        }

        private static String stripTrailingSlash(String url) {
            int end = url.length();
            while (end > 0 && url.charAt(end - 1) == '/') {
                end--;
            }
            return url.substring(0, end);
        }

        @Override
        public String pid() {
            return pid;
        }

        @Override
        public String group() {
            return group;
        }

        @Override
        public String unicast() {
            return unicast;
        }

        @Override
        public String anycast() {
            return anycast;
        }

        @Override
        public CompletableFuture<Void> start() {
            running = true;
            stopLock.lock();
            try {
                stopSignaled = false;
            } finally {
                stopLock.unlock();
            }
            sseThread = Thread.ofVirtual().name("resonate-http-sse").start(this::sseLoop);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stop() {
            running = false;
            stopLock.lock();
            try {
                stopSignaled = true;
                stopCond.signalAll();
            } finally {
                stopLock.unlock();
            }
            // Close the session first so the SSE long-poll and any in-flight POST are cancelled
            // deterministically (client.shutdownNow), instead of relying on Thread.interrupt
            // propagating through HttpClient.send. Guarded by the same monitor as ensureSession so a
            // send/SSE thread cannot resurrect the session we are tearing down.
            synchronized (this) {
                if (session != null) {
                    session.close();
                    session = null;
                }
            }
            Thread handle = sseThread;
            sseThread = null;
            if (handle != null) {
                handle.interrupt();
                try {
                    handle.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // Drain the send pool so no send continuation runs on a pool thread after we return: with
            // running=false and the session closed every queued/in-flight send fails fast, so this
            // settles quickly and nothing survives into JVM shutdown to NoClassDefFoundError.
            sendExecutor.shutdown();
            try {
                if (!sendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    sendExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendExecutor.shutdownNow();
            }
            subscribers.clear();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<String> send(String req) {
            if (!running) {
                return CompletableFuture.failedFuture(new HttpError(new RuntimeException("network has been stopped")));
            }
            try {
                return CompletableFuture.supplyAsync(() -> sendBlocking(req), sendExecutor);
            } catch (RejectedExecutionException e) {
                // stop() raced us and shut the pool down; surface the same stopped-network error.
                return CompletableFuture.failedFuture(new HttpError(e));
            }
        }

        private String sendBlocking(String req) {
            Map<String, Object> headers = authHeaders(map("Content-Type", "application/json"));
            double backoff = INITIAL_BACKOFF_SECS;
            while (true) {
                if (!running) {
                    throw new HttpError(new RuntimeException("network has been stopped"));
                }
                HttpSession session = ensureSession();
                try {
                    return session.post(url + "/", req, toStringMap(headers));
                } catch (RuntimeException exc) {
                    if (!running) {
                        throw new HttpError(exc);
                    }
                    // Only a connection failure is retriable; anything else (session-closed, a real
                    // bug) is re-raised so it is not hidden behind infinite backoff.
                    if (!(exc instanceof ConnectionException)) {
                        throw exc;
                    }
                    sleepOrStop(backoff);
                    if (!running) {
                        throw new HttpError(exc);
                    }
                    backoff = Math.min(backoff * 2, MAX_BACKOFF_SECS);
                }
            }
        }

        @Override
        public void recv(Consumer<String> callback) {
            subscribers.add(callback);
        }

        @Override
        public String targetResolver(String target) {
            return "poll://any@" + target;
        }

        // -- internals --------------------------------------------------------

        protected synchronized HttpSession ensureSession() {
            if (session == null) {
                if (!running) {
                    throw new RuntimeException("network has been stopped");
                }
                session = new RealHttpSession(connLimit);
            }
            return session;
        }

        private Map<String, Object> authHeaders(Map<String, Object> headers) {
            if (auth != null) {
                headers.put("Authorization", "Bearer " + auth);
            }
            return headers;
        }

        private static Map<String, String> toStringMap(Map<String, Object> m) {
            Map<String, String> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(k, String.valueOf(v)));
            return out;
        }

        protected void sleepOrStop(double secs) {
            long remNanos = (long) (secs * 1_000_000_000L);
            stopLock.lock();
            try {
                while (!stopSignaled && remNanos > 0) {
                    try {
                        remNanos = stopCond.awaitNanos(remNanos);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } finally {
                stopLock.unlock();
            }
        }

        private void sseLoop() {
            String sseUrl = url + "/poll/" + group + "/" + pid;
            Map<String, String> headers = toStringMap(authHeaders(map("Accept", "text/event-stream")));
            double backoff = INITIAL_BACKOFF_SECS;
            while (running) {
                try {
                    HttpSession session = ensureSession();
                    SseResponse resp = session.get(sseUrl, headers);
                    try (InputStream in = resp.body) {
                        if (!(resp.status >= 200 && resp.status < 300)) {
                            sleepOrStop(backoff);
                            backoff = Math.min(backoff * 2, MAX_BACKOFF_SECS);
                            continue;
                        }
                        backoff = INITIAL_BACKOFF_SECS;
                        readStream(in);
                    }
                } catch (ConnectionException exc) {
                    sleepOrStop(backoff);
                    backoff = Math.min(backoff * 2, MAX_BACKOFF_SECS);
                    continue;
                } catch (Exception exc) {
                    // Session closed or shutting down — fall through to the running check.
                }
                if (!running) {
                    break;
                }
                sleepOrStop(backoff);
                backoff = Math.min(backoff * 2, MAX_BACKOFF_SECS);
            }
        }

        private void readStream(InputStream in) throws IOException {
            StringBuilder buffer = new StringBuilder();
            byte[] chunk = new byte[4096];
            int n;
            while (running && (n = in.read(chunk)) != -1) {
                buffer.append(new String(chunk, 0, n, StandardCharsets.UTF_8));
                int sep;
                while ((sep = buffer.indexOf("\n\n")) != -1) {
                    String block = buffer.substring(0, sep);
                    buffer.delete(0, sep + 2);
                    for (String line : block.split("\n", -1)) {
                        String dataLine = stripDataPrefix(line);
                        if (dataLine == null) {
                            continue;
                        }
                        for (Consumer<String> cb : new ArrayList<>(subscribers)) {
                            cb.accept(dataLine);
                        }
                    }
                }
            }
        }

        private static String stripDataPrefix(String line) {
            if (line.startsWith("data:")) {
                String data = line.substring("data:".length()).strip();
                return data.isEmpty() ? null : data;
            }
            return null;
        }
    }
}
