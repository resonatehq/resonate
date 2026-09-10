package io.resonatehq.resonate;

import io.resonatehq.resonate.Send.ScheduleCreateReq;
import io.resonatehq.resonate.Send.ScheduleSearchResult;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.ScheduleRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sub-client for schedule operations, mirroring {@code resonate.schedules} from the Python SDK.
 *
 * <p>Schedule records carry no payload that crosses the durability boundary on read, so only {@link
 * #create} touches the {@link Codec} (to encode the promise param); the rest are pass-throughs over
 * {@link Sender}. Python's {@code async def} methods become {@link CompletableFuture}s here.
 */
public final class Schedules {

    private final Sender sender;
    private final Codec codec;

    public Schedules(Sender sender, Codec codec) {
        this.sender = sender;
        this.codec = codec;
    }

    /** Create a schedule with no promise tags. */
    public CompletableFuture<ScheduleRecord> create(
            String id, String cron, String promiseId, long promiseTimeout, Value promiseParam) {
        return create(id, cron, promiseId, promiseTimeout, promiseParam, Map.of());
    }

    /**
     * Create a schedule.
     *
     * <p>{@code promiseTags} are stamped onto every promise the schedule fires. The server requires
     * a {@code resonate:target} tag (the routing target for the fired promise) and rejects the
     * create without one.
     */
    public CompletableFuture<ScheduleRecord> create(
            String id,
            String cron,
            String promiseId,
            long promiseTimeout,
            Value promiseParam,
            Map<String, String> promiseTags) {
        return sender.scheduleCreate(new ScheduleCreateReq(
                id, cron, promiseId, promiseTimeout, codec.encode(promiseParam.data()), promiseTags));
    }

    /** Get a schedule by ID. */
    public CompletableFuture<ScheduleRecord> get(String id) {
        return sender.scheduleGet(id);
    }

    /** Delete a schedule. */
    public CompletableFuture<Void> delete(String id) {
        return sender.scheduleDelete(id);
    }

    /** Search for schedules matching optional tag filter. */
    public CompletableFuture<ScheduleSearchResult> search(Map<String, String> tags, Integer limit, String cursor) {
        return sender.scheduleSearch(tags, limit, cursor);
    }
}
