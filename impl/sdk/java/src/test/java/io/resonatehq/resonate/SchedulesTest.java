package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Send.ScheduleSearchResult;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.ScheduleRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_schedules.py}.
 *
 * <p>The {@link Schedules} client is built directly over a real {@link Sender} + {@link Transport} +
 * {@link LocalNetwork} with a {@code Codec(NoopEncryptor())}, just like {@code Resonate.local()}. A
 * delete of a missing schedule surfaces a {@link ServerError} (wrapped in a {@link
 * CompletionException} on join).
 */
class SchedulesTest {

    private static Schedules local() {
        LocalNetwork net = new LocalNetwork();
        Sender sender = new Sender(new Transport(net), null);
        Codec codec = new Codec(new NoopEncryptor());
        return new Schedules(sender, codec);
    }

    @Test
    void createGetDeleteRoundtrip() {
        Schedules schedules = local();

        ScheduleRecord created = schedules
                .create("unit-s1", "*/5 * * * *", "unit-s1.{{.timestamp}}", 60_000, new Value())
                .join();
        assertEquals("unit-s1", created.id());
        assertEquals("*/5 * * * *", created.cron());

        ScheduleRecord fetched = schedules.get("unit-s1").join();
        assertEquals("unit-s1", fetched.id());

        schedules.delete("unit-s1").join();
    }

    @Test
    void deleteMissingReturnsServerError() {
        Schedules schedules = local();
        CompletionException exc = assertThrows(
                CompletionException.class,
                () -> schedules.delete("no-such-schedule").join());
        assertInstanceOf(ServerError.class, exc.getCause());
    }

    @Test
    void searchReturnsRecord() {
        Schedules schedules = local();

        schedules
                .create("unit-s-search", "* * * * *", "unit-s-search.{{.timestamp}}", 60_000, new Value())
                .join();

        ScheduleSearchResult result = schedules.search(null, 100, null).join();
        assertTrue(result.schedules().stream().anyMatch(s -> s.id().equals("unit-s-search")));
    }
}
