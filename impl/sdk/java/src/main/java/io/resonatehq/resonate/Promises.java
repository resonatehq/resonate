package io.resonatehq.resonate;

import io.resonatehq.resonate.Send.PromiseSearchResult;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import io.resonatehq.resonate.Types.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sub-client for durable promise operations, mirroring {@code resonate.promises} from the Python
 * SDK.
 *
 * <p>Each operation is a thin pairing of {@link Sender} (the typed request layer) and {@link Codec}
 * (the durability boundary): create/settle encode the outgoing {@link Value} payload, and every
 * response promise is run back through {@link Codec#decodePromise} to decode its {@code param} /
 * {@code value} in place. Python's {@code async def} methods become {@link CompletableFuture}s here,
 * composed with {@link CompletableFuture#thenApply}.
 */
public final class Promises {

    private final Sender sender;
    private final Codec codec;

    public Promises(Sender sender, Codec codec) {
        this.sender = sender;
        this.codec = codec;
    }

    /** Get a promise by ID. */
    public CompletableFuture<PromiseRecord> get(String id) {
        return sender.promiseGet(id).thenApply(codec::decodePromise);
    }

    /** Create a promise. */
    public CompletableFuture<PromiseRecord> create(String id, long timeoutAt, Value param, Map<String, String> tags) {
        return sender.promiseCreate(new PromiseCreateReq(id, timeoutAt, codec.encode(param.data()), tags))
                .thenApply(codec::decodePromise);
    }

    /** Resolve a promise. */
    public CompletableFuture<PromiseRecord> resolve(String id, Value value) {
        return settle(id, "resolved", value);
    }

    /** Reject a promise. */
    public CompletableFuture<PromiseRecord> reject(String id, Value value) {
        return settle(id, "rejected", value);
    }

    /** Cancel a promise (settles as {@code rejected_canceled}). */
    public CompletableFuture<PromiseRecord> cancel(String id, Value value) {
        return settle(id, "rejected_canceled", value);
    }

    private CompletableFuture<PromiseRecord> settle(String id, String state, Value value) {
        return sender.promiseSettle(new PromiseSettleReq(id, state, codec.encode(value.data())))
                .thenApply(codec::decodePromise);
    }

    /** Search for promises matching optional state/tags filters. */
    public CompletableFuture<PromiseSearchResult> search(
            String state, Map<String, String> tags, Integer limit, String cursor) {
        return sender.promiseSearch(state, tags, limit, cursor).thenApply(result -> {
            List<PromiseRecord> promises = new ArrayList<>();
            for (PromiseRecord p : result.promises()) {
                promises.add(codec.decodePromise(p));
            }
            return new PromiseSearchResult(promises, result.cursor());
        });
    }
}
