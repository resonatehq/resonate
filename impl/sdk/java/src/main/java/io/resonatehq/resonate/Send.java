package io.resonatehq.resonate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.SerializationError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseRegisterCallbackData;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.ScheduleRecord;
import io.resonatehq.resonate.Types.TaskRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The typed request layer over {@link Transport}, mirroring {@code resonate.send} from the Python
 * SDK.
 *
 * <p>{@link Sender} turns each protocol operation (task / promise / schedule) into a typed method:
 * it builds the request {@link Envelope}, serializes it to JSON, hands it to the {@link Transport},
 * and parses the response's {@code data} portion back into a record. Python's {@code send.py} module
 * has one primary class ({@code Sender}) plus a cluster of result types, the wire-format envelope
 * structs, and module-level parsing helpers; we mirror the whole module as this {@code Send} holder
 * (the same pattern as {@link Types} / {@link Errors}), with {@code Sender} nested inside.
 *
 * <p><b>Async.</b> Python's {@code async def} methods {@code await} the transport; the Java analogue
 * returns a {@link CompletableFuture}. A server status {@code >= 400} (other than an allowed 409)
 * completes the future exceptionally with a {@link ServerError}; a response whose {@code data} fails
 * to parse completes with a {@link DecodingError} — both surfacing through {@code join()} wrapped in
 * a {@link java.util.concurrent.CompletionException}, exactly as the transport futures do.
 *
 * <p><b>Response parsing.</b> Python parses the response with {@code msgspec.convert} over a plain
 * {@code dict}; here the response {@code data} arrives as a Jackson {@link JsonNode} and {@link
 * ObjectMapper#convertValue} stands in for {@code msgspec.convert}. Python's {@code _normalize_record}
 * — which drops {@code null} {@code Value} fields because {@code msgspec} rejects {@code null} for a
 * struct field — has no Java counterpart: the {@link Value} / {@link PromiseRecord} record
 * constructors already coalesce a {@code null} {@code param} / {@code value} / {@code tags} to its
 * empty default, so a JSON {@code null} reshapes cleanly with no pre-pass.
 */
public final class Send {

    private Send() {}

    // Ignore unknown fields when reshaping responses, matching msgspec.convert's default (the
    // server may add fields like a schedule's nextRunAt/lastRunAt that the record doesn't model).
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Protocol version string sent in every request head. Mirrors {@code resonate.PROTOCOL_VERSION}
     * (declared in the Python package's {@code __init__.py}); kept here until the package root is
     * ported.
     */
    static final String PROTOCOL_VERSION = "2026-04-01";

    /**
     * Current time in milliseconds since the UNIX epoch. Mirrors {@code resonate.now_ms}; {@link
     * System#currentTimeMillis()} is the analogue of Python's {@code time.time_ns() // 1_000_000}.
     */
    static long nowMs() {
        return System.currentTimeMillis();
    }

    // =========================================================================
    // Public result types
    // =========================================================================

    /** Result of acquiring (or creating) a task: the task, its promise, and any preloaded promises. */
    public record TaskAcquireResult(TaskRecord task, PromiseRecord promise, List<PromiseRecord> preload) {}

    /**
     * Outcome of a {@code task.suspend}: either {@link Suspended} (the task was actually suspended,
     * HTTP 200) or a {@link Redirect} (HTTP 300). Mirrors Python's {@code Literal["suspended"] |
     * Redirect}.
     */
    public sealed interface SuspendResult permits Suspended, Redirect {}

    /**
     * The task was suspended. Mirrors Python's {@code "suspended"} literal — distinct from {@link
     * Errors.Suspended}, which is the control-flow signal raised inside a durable execution.
     */
    public record Suspended() implements SuspendResult {}

    /** The task was redirected rather than suspended; carries the preloaded promises. */
    public record Redirect(List<PromiseRecord> preload) implements SuspendResult {}

    /** A reference to a task by id and version, used by {@link Sender#taskHeartbeat}. */
    @JsonPropertyOrder({"id", "version"})
    public record TaskRef(String id, int version) {}

    /** A page of promises from {@link Sender#promiseSearch}, with an optional continuation cursor. */
    public record PromiseSearchResult(List<PromiseRecord> promises, String cursor) {}

    /**
     * Outcome of {@link Sender#taskCreateOrConflict}: either the created task ({@link Created}) or
     * {@link Conflict} when the server responds 409. Mirrors Python's {@code TaskCreateResult |
     * Literal["conflict"]}.
     *
     * <p>The 409 response carries no promise data — callers receiving {@link Conflict} must subscribe
     * to the existing promise themselves (via {@link Sender#promiseRegisterListener}).
     */
    public sealed interface TaskCreateOutcome permits Created, Conflict {}

    /** The task was created; wraps the {@link TaskAcquireResult}. */
    public record Created(TaskAcquireResult result) implements TaskCreateOutcome {}

    /** The server responded 409: a task/promise with this id already exists. */
    public record Conflict() implements TaskCreateOutcome {}

    /** A page of schedules from {@link Sender#scheduleSearch}, with an optional continuation cursor. */
    public record ScheduleSearchResult(List<ScheduleRecord> schedules, String cursor) {}

    /** Request body for creating a schedule. Wire format uses camelCase. */
    @JsonPropertyOrder({"id", "cron", "promiseId", "promiseTimeout", "promiseParam", "promiseTags"})
    public record ScheduleCreateReq(
            String id,
            String cron,
            @JsonProperty("promiseId") String promiseId,
            @JsonProperty("promiseTimeout") long promiseTimeout,
            @JsonProperty("promiseParam") Value promiseParam,
            @JsonProperty("promiseTags") Map<String, String> promiseTags) {}

    // =========================================================================
    // Typed envelope structs -- serialize directly to wire format
    // =========================================================================

    /**
     * The {@code head} of a protocol envelope. {@code auth} is left out of the wire format when
     * {@code null} (the analogue of msgspec's {@code omit_defaults=True}).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"corrId", "version", "auth"})
    public record Head(@JsonProperty("corrId") String corrId, String version, String auth) {
        /** Mirrors Python's {@code auth: str | None = None} default. */
        public Head(String corrId, String version) {
            this(corrId, version, null);
        }
    }

    /** A protocol request envelope: {@code { kind, head, data }}. */
    @JsonPropertyOrder({"kind", "head", "data"})
    public record Envelope(String kind, Head head, Object data) {}

    /** A nested action envelope, embedded in a parent envelope's {@code data}. */
    @JsonPropertyOrder({"kind", "head", "data"})
    public record SubEnvelope(String kind, Head head, Object data) {}

    // =========================================================================
    // Sender -- typed interface over Transport
    // =========================================================================

    /** A typed, request-building facade over a {@link Transport}. */
    public static final class Sender {

        private final Transport transport;
        private final String auth;

        public Sender(Transport transport, String auth) {
            this.transport = transport;
            this.auth = auth;
        }

        // -- task operations --------------------------------------------------

        /** Acquire a task, taking its lock and loading its promise. */
        public CompletableFuture<TaskAcquireResult> taskAcquire(String id, int version, String pid, int ttl) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("id", id);
            data.put("version", version);
            data.put("pid", pid);
            data.put("ttl", ttl);
            return sendEnvelope("task.acquire", data, false).thenApply(sd -> parseTaskAcquire(sd.data()));
        }

        /** Fulfill a task by settling its promise. */
        public CompletableFuture<PromiseRecord> taskFulfill(String id, int version, PromiseSettleReq action) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("id", id);
            data.put("version", version);
            data.put("action", new SubEnvelope("promise.settle", makeHead(), action));
            return sendEnvelope("task.fulfill", data, false).thenApply(sd -> parsePromise(sd.data()));
        }

        /**
         * Suspend a task, registering callbacks for awaited promises.
         *
         * <p>Returns whether the task was actually suspended or redirected.
         */
        public CompletableFuture<SuspendResult> taskSuspend(
                String id, int version, List<PromiseRegisterCallbackData> actions) {
            List<SubEnvelope> wrapped = new ArrayList<>();
            for (PromiseRegisterCallbackData action : actions) {
                wrapped.add(new SubEnvelope("promise.register_callback", makeHead(), action));
            }
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("id", id);
            data.put("version", version);
            data.put("actions", wrapped);
            return sendEnvelope("task.suspend", data, false)
                    .thenApply(sd -> parseSuspendResult(sd.status(), sd.data()));
        }

        /** Release a task (give up the lock without fulfilling). */
        public CompletableFuture<Void> taskRelease(String id, int version) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("id", id);
            data.put("version", version);
            return sendEnvelope("task.release", data, false).thenApply(sd -> null);
        }

        /** Create a task and its associated promise. */
        public CompletableFuture<TaskAcquireResult> taskCreate(String pid, int ttl, PromiseCreateReq action) {
            return sendTaskCreate(pid, ttl, action, false).thenApply(sd -> parseTaskAcquire(sd.data()));
        }

        /**
         * Create a task and its associated promise, returning {@link Conflict} on 409.
         *
         * <p>Unlike {@link #taskCreate}, this does not fail on 409. The server's 409 body carries no
         * promise data; callers receiving {@link Conflict} are expected to subscribe to the existing
         * promise via {@link #promiseRegisterListener}.
         */
        public CompletableFuture<TaskCreateOutcome> taskCreateOrConflict(String pid, int ttl, PromiseCreateReq action) {
            return sendTaskCreate(pid, ttl, action, true).thenApply(sd -> {
                if (sd.status() == 409) {
                    return new Conflict();
                }
                return new Created(parseTaskAcquire(sd.data()));
            });
        }

        /** Extend the lease for one or more tasks. */
        public CompletableFuture<Void> taskHeartbeat(String pid, List<TaskRef> tasks) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("pid", pid);
            data.put("tasks", tasks);
            return sendEnvelope("task.heartbeat", data, false).thenApply(sd -> null);
        }

        // -- promise operations -----------------------------------------------

        /** Get a promise by ID. */
        public CompletableFuture<PromiseRecord> promiseGet(String id) {
            return sendEnvelope("promise.get", Map.of("id", id), false).thenApply(sd -> parsePromise(sd.data()));
        }

        /** Create a durable promise. */
        public CompletableFuture<PromiseRecord> promiseCreate(PromiseCreateReq req) {
            return sendEnvelope("promise.create", req, false).thenApply(sd -> parsePromise(sd.data()));
        }

        /** Settle (resolve/reject) a durable promise. */
        public CompletableFuture<PromiseRecord> promiseSettle(PromiseSettleReq req) {
            return sendEnvelope("promise.settle", req, false).thenApply(sd -> parsePromise(sd.data()));
        }

        /** Register a listener for a promise. */
        public CompletableFuture<PromiseRecord> promiseRegisterListener(String awaited, String address) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("awaited", awaited);
            data.put("address", address);
            return sendEnvelope("promise.register_listener", data, false).thenApply(sd -> parsePromise(sd.data()));
        }

        /** Search for promises matching criteria. A {@code null} argument is omitted from the request. */
        public CompletableFuture<PromiseSearchResult> promiseSearch(
                String state, Map<String, String> tags, Integer limit, String cursor) {
            Map<String, Object> data = new LinkedHashMap<>();
            if (state != null) {
                data.put("state", state);
            }
            if (tags != null) {
                data.put("tags", tags);
            }
            if (limit != null) {
                data.put("limit", limit);
            }
            if (cursor != null) {
                data.put("cursor", cursor);
            }
            return sendEnvelope("promise.search", data, false).thenApply(sd -> {
                List<PromiseRecord> promises = decodeList(sd.data(), "promises", PromiseRecord.class);
                return new PromiseSearchResult(promises, cursor(sd.data()));
            });
        }

        // -- schedule operations ----------------------------------------------

        /** Get a schedule by ID. */
        public CompletableFuture<ScheduleRecord> scheduleGet(String id) {
            return sendEnvelope("schedule.get", Map.of("id", id), false).thenApply(sd -> parseSchedule(sd.data()));
        }

        /** Create a schedule. */
        public CompletableFuture<ScheduleRecord> scheduleCreate(ScheduleCreateReq req) {
            return sendEnvelope("schedule.create", req, false).thenApply(sd -> parseSchedule(sd.data()));
        }

        /** Delete a schedule. */
        public CompletableFuture<Void> scheduleDelete(String id) {
            return sendEnvelope("schedule.delete", Map.of("id", id), false).thenApply(sd -> null);
        }

        /** Search for schedules. A {@code null} argument is omitted from the request. */
        public CompletableFuture<ScheduleSearchResult> scheduleSearch(
                Map<String, String> tags, Integer limit, String cursor) {
            Map<String, Object> data = new LinkedHashMap<>();
            if (tags != null) {
                data.put("tags", tags);
            }
            if (limit != null) {
                data.put("limit", limit);
            }
            if (cursor != null) {
                data.put("cursor", cursor);
            }
            return sendEnvelope("schedule.search", data, false).thenApply(sd -> {
                List<ScheduleRecord> schedules = decodeList(sd.data(), "schedules", ScheduleRecord.class);
                return new ScheduleSearchResult(schedules, cursor(sd.data()));
            });
        }

        // -- internal helpers -------------------------------------------------

        private Head makeHead() {
            return new Head("sr-" + nowMs(), PROTOCOL_VERSION, auth);
        }

        /** Shared helper for {@link #taskCreate} and {@link #taskCreateOrConflict}. */
        private CompletableFuture<StatusData> sendTaskCreate(
                String pid, int ttl, PromiseCreateReq action, boolean allow409) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("pid", pid);
            data.put("ttl", ttl);
            data.put("action", new SubEnvelope("promise.create", makeHead(), action));
            return sendEnvelope("task.create", data, allow409);
        }

        /**
         * Serialize an envelope, send it, and complete with {@code (status, data)}.
         *
         * <p>{@code status} defaults to 200 and {@code data} to an empty object when absent. A status
         * {@code >= 400} (other than an allowed 409) completes the future exceptionally with a {@link
         * ServerError}.
         */
        private CompletableFuture<StatusData> sendEnvelope(String kind, Object data, boolean allow409) {
            Head head = makeHead();
            String corrId = head.corrId();
            Envelope envelope = new Envelope(kind, head, data);

            String body;
            try {
                body = MAPPER.writeValueAsString(envelope);
            } catch (JsonProcessingException exc) {
                // Python's msgspec.json.encode would raise on an unencodable payload; surface it as a
                // failed future rather than throwing synchronously, keeping all errors on the future.
                return CompletableFuture.failedFuture(new SerializationError(exc));
            }

            return transport.send(kind, corrId, body).thenApply(resp -> {
                int status = respStatus(resp);
                JsonNode respData = respData(resp);

                if (status >= 400 && !(allow409 && status == 409)) {
                    String message;
                    if (respData.isTextual()) {
                        message = respData.asText();
                    } else if (respData.isObject()
                            && respData.get("error") != null
                            && respData.get("error").isTextual()) {
                        message = respData.get("error").asText();
                    } else {
                        message = "server error (status " + status + ")";
                    }
                    throw new ServerError(status, message);
                }

                return new StatusData(status, respData);
            });
        }
    }

    /** The {@code (status, data)} pair returned by {@code _send_envelope} in Python. */
    private record StatusData(int status, JsonNode data) {}

    // =========================================================================
    // Response parsing helpers (internal)
    // =========================================================================

    /** Convert a JSON node into {@code type}, raising {@link DecodingError} on failure. */
    private static <T> T decodeOrRaise(JsonNode raw, Class<T> type, String what) {
        try {
            return MAPPER.convertValue(raw, type);
        } catch (IllegalArgumentException exc) {
            throw new DecodingError("invalid " + what + ": " + exc.getMessage());
        }
    }

    /** Decode the array at {@code data[key]}, silently dropping records that fail to parse. */
    private static <T> List<T> decodeList(JsonNode data, String key, Class<T> type) {
        List<T> out = new ArrayList<>();
        if (data == null || !data.isObject()) {
            return out;
        }
        JsonNode arr = data.get(key);
        if (arr == null || !arr.isArray()) {
            return out;
        }
        for (JsonNode v : arr) {
            try {
                out.add(MAPPER.convertValue(v, type));
            } catch (IllegalArgumentException ignored) {
                // drop records that fail to parse, matching Python's silent skip
            }
        }
        return out;
    }

    /** Extract a string {@code cursor} field, defaulting to {@code null}. */
    private static String cursor(JsonNode data) {
        if (data != null && data.isObject()) {
            JsonNode cursor = data.get("cursor");
            if (cursor != null && cursor.isTextual()) {
                return cursor.asText();
            }
        }
        return null;
    }

    /** Extract {@code head.status}, defaulting to 200. A boolean or negative value is ignored. */
    private static int respStatus(JsonNode resp) {
        if (resp != null && resp.isObject()) {
            JsonNode head = resp.get("head");
            if (head != null && head.isObject()) {
                JsonNode status = head.get("status");
                // isIntegralNumber excludes JSON booleans (BooleanNode) and floats, mirroring Python's
                // `isinstance(status, int) and not isinstance(status, bool)`.
                if (status != null && status.isIntegralNumber() && status.asInt() >= 0) {
                    return status.asInt();
                }
            }
        }
        return 200;
    }

    /** Extract the {@code data} portion, defaulting to an empty object. */
    private static JsonNode respData(JsonNode resp) {
        if (resp != null && resp.isObject() && resp.has("data")) {
            return resp.get("data");
        }
        return MAPPER.createObjectNode();
    }

    /** Parse a promise record from a server response's data portion. */
    static PromiseRecord parsePromise(JsonNode data) {
        JsonNode promise = field(data, "promise");
        if (promise == null) {
            throw new DecodingError("missing 'promise' in response");
        }
        return decodeOrRaise(promise, PromiseRecord.class, "promise record");
    }

    /** Parse a {@code task.acquire} response. */
    static TaskAcquireResult parseTaskAcquire(JsonNode data) {
        JsonNode taskVal = field(data, "task");
        if (taskVal == null) {
            throw new DecodingError("missing 'task' in task.acquire response");
        }
        TaskRecord task = decodeOrRaise(taskVal, TaskRecord.class, "task in task.acquire");

        JsonNode promiseVal = field(data, "promise");
        if (promiseVal == null) {
            throw new DecodingError("missing 'promise' in task.acquire response");
        }
        PromiseRecord promise = decodeOrRaise(promiseVal, PromiseRecord.class, "promise in task.acquire");

        return new TaskAcquireResult(task, promise, parsePreloaded(data));
    }

    /** Parse a {@code task.suspend} response -- {@link Suspended} (200) or {@link Redirect} (300). */
    static SuspendResult parseSuspendResult(int status, JsonNode data) {
        if (status == 300) {
            return new Redirect(parsePreloaded(data));
        }
        return new Suspended();
    }

    /** Extract preloaded promises from a response, dropping any that fail to parse. */
    static List<PromiseRecord> parsePreloaded(JsonNode data) {
        return decodeList(data, "preload", PromiseRecord.class);
    }

    private static ScheduleRecord parseSchedule(JsonNode data) {
        JsonNode schedule = field(data, "schedule");
        if (schedule == null) {
            throw new DecodingError("missing 'schedule' in response");
        }
        return decodeOrRaise(schedule, ScheduleRecord.class, "schedule record");
    }

    /**
     * Look up {@code key} on an object node, returning {@code null} when the node is not an object,
     * the key is absent, or the value is JSON {@code null}. Mirrors Python's {@code data.get(key) if
     * isinstance(data, dict) else None}, where a decoded JSON {@code null} is {@code None} too.
     */
    private static JsonNode field(JsonNode data, String key) {
        if (data == null || !data.isObject()) {
            return null;
        }
        JsonNode val = data.get(key);
        return (val == null || val.isNull()) ? null : val;
    }
}
