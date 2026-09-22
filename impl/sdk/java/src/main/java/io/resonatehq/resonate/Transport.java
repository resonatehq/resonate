package io.resonatehq.resonate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Errors.DecodingError;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Types.PromiseRecord;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Adds JSON serialization, deserialization, and correlation validation on top of a {@link Network},
 * mirroring {@code resonate.transport.Transport} from the Python SDK.
 *
 * <p>Resonate and its sub-components use the transport — never the raw {@link Network}.
 *
 * <p><b>Parsed responses.</b> Python's {@code send} returns the response parsed to {@code Any} (a
 * plain {@code dict}). The Java analogue is a Jackson {@link JsonNode} tree — an untyped,
 * navigable parse that callers index the same way Python indexes the dict.
 *
 * <p><b>Incoming messages.</b> Python decodes incoming frames into a {@code msgspec} tagged union
 * ({@code ExecuteMsg | UnblockMsg}) discriminated by the {@code kind} field. The Java analogue is
 * the sealed {@link Message} interface with Jackson polymorphic deserialization keyed on the same
 * {@code kind} property; an unknown {@code kind} (or any malformed frame) fails to parse and is
 * discarded, exactly as the Python {@code recv} swallows {@code msgspec.MsgspecError}.
 *
 * <p><b>Async.</b> {@code send} is async in Python ({@code await network.send(...)}); here it
 * returns a {@link CompletableFuture} that completes with the validated {@link JsonNode} or
 * completes exceptionally with {@link DecodingError} / {@link ServerError}. Awaiting it with {@code
 * join()} is the analogue of Python's {@code asyncio.run}.
 */
public final class Transport {

    private static final Logger LOGGER = System.getLogger(Transport.class.getName());

    // Mirror msgspec's parse semantics: unknown fields are ignored by default (leniency, so
    // foreign-SDK frames with extra fields still parse), but trailing data after a complete value is
    // rejected (strictness — ``msgspec.json.decode`` errors on trailing tokens, so a frame like
    // ``"{} {}"`` must fail rather than silently parse the first value).
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    // =========================================================================
    // Incoming messages (recv path)
    // =========================================================================

    /**
     * A parsed incoming message from the network, discriminated by its {@code kind} field. Mirrors
     * the Python {@code Message = ExecuteMsg | UnblockMsg} tagged union.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ExecuteMsg.class, name = "execute"),
        @JsonSubTypes.Type(value = UnblockMsg.class, name = "unblock"),
    })
    public sealed interface Message permits ExecuteMsg, UnblockMsg {}

    /** Reference to a task carried by an {@link ExecuteMsg}. {@code version} defaults to {@code 0}. */
    public record TaskRef(@JsonProperty(value = "id", required = true) String id, int version) {}

    /** Payload of an {@link ExecuteMsg}. */
    public record ExecuteData(@JsonProperty(value = "task", required = true) TaskRef task) {}

    /** "execute" message: the server is asking this worker to run a task. */
    public record ExecuteMsg(@JsonProperty(value = "data", required = true) ExecuteData data) implements Message {

        /** Shorthand for {@code data.task().id()}. */
        public String taskId() {
            return data.task().id();
        }

        /** Shorthand for {@code data.task().version()}. */
        public int version() {
            return data.task().version();
        }
    }

    /** Payload of an {@link UnblockMsg}. */
    public record UnblockData(@JsonProperty(value = "promise", required = true) PromiseRecord promise) {}

    /** "unblock" message: a promise an execution was awaiting has settled. */
    public record UnblockMsg(@JsonProperty(value = "data", required = true) UnblockData data) implements Message {

        /** The settled promise — shorthand for {@code data.promise()}. */
        public PromiseRecord promise() {
            return data.promise();
        }
    }

    // =========================================================================
    // Transport
    // =========================================================================

    private final Network network;

    public Transport(Network network) {
        this.network = network;
    }

    /**
     * Send an already-serialized request, completing with the parsed response.
     *
     * <p>Validates that the response {@code kind} and {@code head.corrId} match the expected values;
     * a mismatch — or an absent/non-string field, which collapses to {@code ""} — completes the
     * future exceptionally with a {@link ServerError} (code {@code 500}). A response that is not
     * valid JSON completes with a {@link DecodingError}.
     */
    public CompletableFuture<JsonNode> send(String kind, String corrId, String body) {
        LOGGER.log(Level.DEBUG, "transport send_req: {0}", body);

        return network.send(body).thenApply(respStr -> {
            LOGGER.log(Level.DEBUG, "transport send_res: {0}", respStr);

            JsonNode response;
            try {
                response = MAPPER.readTree(respStr);
            } catch (JsonProcessingException exc) {
                throw new DecodingError("invalid response JSON: " + exc.getMessage() + ", resp: " + respStr);
            }
            // Jackson returns a MissingNode (not an error) for empty or whitespace-only input;
            // msgspec's ``decode("")`` raises, so treat no-content as a decode failure to match.
            if (response == null || response.isMissingNode()) {
                throw new DecodingError("invalid response JSON: no content, resp: " + respStr);
            }

            String respKind = nestedStr(response, "kind");
            if (!respKind.equals(kind)) {
                throw new ServerError(500, "response kind mismatch: expected '" + kind + "', got '" + respKind + "'");
            }

            String respCorr = nestedStr(response, "head", "corrId");
            if (!respCorr.equals(corrId)) {
                throw new ServerError(
                        500, "response corrId mismatch: expected '" + corrId + "', got '" + respCorr + "'");
            }

            return response;
        });
    }

    /** Register a callback for incoming messages. */
    public void recv(Consumer<Message> callback) {
        network.recv(raw -> {
            Message msg;
            try {
                msg = MAPPER.readValue(raw, Message.class);
            } catch (Exception exc) {
                // Mirrors Python catching msgspec.MsgspecError: a malformed frame or unknown ``kind``
                // is logged and dropped, never surfaced to the callback.
                LOGGER.log(Level.WARNING, "failed to parse incoming message: {0}; raw: {1}", exc, raw);
                return;
            }
            // A literal ``null`` frame parses to a Java null without error; msgspec rejects it as not
            // matching the union, so drop it rather than handing null to the callback.
            if (msg == null) {
                LOGGER.log(Level.WARNING, "discarding null incoming message; raw: {0}", raw);
                return;
            }
            LOGGER.log(Level.DEBUG, "transport recv: {0}", raw);
            callback.accept(msg);
        });
    }

    /** Access the underlying network. */
    public Network network() {
        return network;
    }

    // =========================================================================
    // Envelope helpers
    // =========================================================================

    /**
     * Walk nested object {@code keys} and return the final string, or {@code ""}.
     *
     * <p>Any missing key, non-object node, or non-string leaf collapses to {@code ""} — the analogue
     * of Python's {@code _nested_str}, where a missing dict key, non-mapping node, or non-string leaf
     * all yield the empty string.
     */
    private static String nestedStr(JsonNode value, String... keys) {
        for (String key : keys) {
            if (value == null || !value.isObject()) {
                return "";
            }
            value = value.get(key);
        }
        return value != null && value.isTextual() ? value.asText() : "";
    }
}
