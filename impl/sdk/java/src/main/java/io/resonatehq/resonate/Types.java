package io.resonatehq.resonate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.Map;

/**
 * Wire-format types mirroring {@code resonate.types} from the Python SDK.
 *
 * <p>The Python module uses {@code msgspec.Struct} with {@code rename="camel"}, {@code
 * omit_defaults=True}, {@code kw_only=True}, and {@code frozen=True}. We mirror those with Java
 * records (frozen, ordered by component declaration), Jackson camelCase property names, and {@link
 * JsonInclude} on {@link Value} only to mirror {@code omit_defaults=True}.
 *
 * <p>{@code PromiseState} / task-state / settle-state are encoded as {@link String} on the wire. We
 * keep them as strings here to match the Python {@code Literal[...]} usage (no runtime enum).
 */
public final class Types {
    private Types() {}

    /** Allowed values for {@link PromiseRecord#state()}: pending, resolved, rejected, rejected_canceled, rejected_timedout. */
    public static final List<String> PROMISE_STATES =
            List.of("pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout");

    /** Allowed values for {@link TaskRecord#state()}: pending, acquired, suspended, halted, fulfilled. */
    public static final List<String> TASK_STATES = List.of("pending", "acquired", "suspended", "halted", "fulfilled");

    /** Allowed values for {@link PromiseSettleReq#state()}: resolved, rejected, rejected_canceled. */
    public static final List<String> SETTLE_STATES = List.of("resolved", "rejected", "rejected_canceled");

    /** Allowed values for {@link Info} status: done, suspended, error. */
    public static final List<String> STATUSES = List.of("done", "suspended", "error");

    /**
     * Wire format for data crossing the durability boundary.
     *
     * <p>Both fields default to {@code null} and are omitted from JSON when null — the equivalent
     * of serde's {@code skip_serializing_if = "Option::is_none"} (msgspec's {@code
     * omit_defaults=True}).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"headers", "data"})
    public record Value(Map<String, String> headers, Object data) {
        public Value() {
            this(null, null);
        }
    }

    /** A durable promise record as stored by the server. Wire format uses camelCase. */
    @JsonPropertyOrder({"id", "state", "param", "value", "tags", "timeoutAt", "createdAt", "settledAt"})
    public record PromiseRecord(
            @JsonProperty(value = "id", required = true) String id,
            @JsonProperty(value = "state", required = true) String state,
            Value param,
            Value value,
            Map<String, String> tags,
            @JsonProperty(value = "timeoutAt", required = true) long timeoutAt,
            long createdAt,
            Long settledAt) {
        public PromiseRecord {
            if (param == null) param = new Value();
            if (value == null) value = new Value();
            if (tags == null) tags = Map.of();
        }
    }

    /**
     * A task record. {@code resumes} is heterogeneous: list of strings, an int, a bool, or null —
     * so it is typed as {@link Object}.
     */
    @JsonPropertyOrder({"id", "state", "version", "resumes", "ttl", "pid"})
    public record TaskRecord(
            @JsonProperty(value = "id", required = true) String id,
            @JsonProperty(value = "state", required = true) String state,
            @JsonProperty(value = "version", required = true) int version,
            Object resumes,
            Long ttl,
            String pid) {}

    /** A schedule record as stored by the server. Wire format uses camelCase. */
    @JsonPropertyOrder({"id", "cron", "promiseId", "promiseTimeout", "promiseParam", "promiseTags", "createdAt"})
    public record ScheduleRecord(
            @JsonProperty(value = "id", required = true) String id,
            @JsonProperty(value = "cron", required = true) String cron,
            @JsonProperty(value = "promiseId", required = true) String promiseId,
            @JsonProperty(value = "promiseTimeout", required = true) long promiseTimeout,
            Value promiseParam,
            Map<String, String> promiseTags,
            long createdAt) {
        public ScheduleRecord {
            if (promiseParam == null) promiseParam = new Value();
            if (promiseTags == null) promiseTags = Map.of();
        }
    }

    /** Request body for creating a promise. */
    @JsonPropertyOrder({"id", "timeoutAt", "param", "tags"})
    public record PromiseCreateReq(
            @JsonProperty(value = "id", required = true) String id,
            long timeoutAt,
            Value param,
            Map<String, String> tags) {
        public PromiseCreateReq {
            if (param == null) param = new Value();
            if (tags == null) tags = Map.of();
        }

        /** Convenience for the "id-only" form used by the Python {@code default_with_id} factory. */
        public PromiseCreateReq(String id) {
            this(id, 0L, new Value(), Map.of());
        }
    }

    /** Request body for settling a promise. */
    @JsonPropertyOrder({"id", "state", "value"})
    public record PromiseSettleReq(
            @JsonProperty(value = "id", required = true) String id,
            @JsonProperty(value = "state", required = true) String state,
            @JsonProperty(value = "value", required = true) Value value) {}

    /** Payload for the register-callback request body. */
    @JsonPropertyOrder({"awaited", "awaiter"})
    public record PromiseRegisterCallbackData(
            @JsonProperty(value = "awaited", required = true) String awaited,
            @JsonProperty(value = "awaiter", required = true) String awaiter) {}

    /** The packed {@code *args} / {@code **kwargs} slot. Both default to empty. */
    @JsonPropertyOrder({"args", "kwargs"})
    public record Args(List<Object> args, Map<String, Object> kwargs) {
        public Args {
            if (args == null) args = List.of();
            if (kwargs == null) kwargs = Map.of();
        }

        public Args() {
            this(List.of(), Map.of());
        }
    }

    /**
     * Task payload. Mirrors Python {@code TaskData(Args, ...)}: the inherited {@code args} /
     * {@code kwargs} fields encode first, then {@code func} / {@code version}.
     */
    @JsonPropertyOrder({"args", "kwargs", "func", "version"})
    public record TaskData(
            List<Object> args,
            Map<String, Object> kwargs,
            @JsonProperty(value = "func", required = true) String func,
            Integer version) {
        public TaskData {
            if (args == null) args = List.of();
            if (kwargs == null) kwargs = Map.of();
            // ``version`` defaults to 1: an omitted version (e.g. a foreign-SDK payload) resolves
            // deterministically to the first registered version. Version 0 -- which once meant
            // "latest registered" -- is no longer used.
            if (version == null) version = 1;
        }

        /** Convenience matching the Python default of {@code version=1} and empty args. */
        public TaskData(String func) {
            this(List.of(), Map.of(), func, 1);
        }
    }

    /** Per-invocation metadata passed into a registered function. */
    public record Info(
            String id,
            String parentId,
            String originId,
            String branchId,
            long timeoutAt,
            String funcName,
            Map<String, String> tags) {}
}
