package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Errors.ServerError;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.Value;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_promises.py}.
 *
 * <p>The {@link Promises} client is built directly over a real {@link Sender} + {@link Transport} +
 * {@link LocalNetwork} -- the same wiring {@code Resonate.local()} performs -- with a {@code
 * Codec(NoopEncryptor())}. A missing-promise lookup surfaces a {@link ServerError}, wrapped in a
 * {@link CompletionException} when joined (the Java analogue of Python's raised {@code ServerError}).
 */
class PromisesTest {

    private static final long I64_MAX = Long.MAX_VALUE;

    private static Promises local() {
        LocalNetwork net = new LocalNetwork();
        Sender sender = new Sender(new Transport(net), null);
        Codec codec = new Codec(new NoopEncryptor());
        return new Promises(sender, codec);
    }

    @Test
    void createGetResolveRoundtrip() {
        Promises promises = local();

        PromiseRecord created = promises.create("unit-p1", I64_MAX, new Value(null, Map.of("x", 1)), Map.of())
                .join();
        assertEquals("unit-p1", created.id());
        assertEquals("pending", created.state());

        PromiseRecord fetched = promises.get("unit-p1").join();
        assertEquals("unit-p1", fetched.id());

        PromiseRecord settled = promises.resolve("unit-p1", new Value(null, Map.of("result", "ok")))
                .join();
        assertEquals("resolved", settled.state());

        PromiseRecord after = promises.get("unit-p1").join();
        assertEquals("resolved", after.state());
    }

    @Test
    void createGetRejectRoundtrip() {
        Promises promises = local();

        PromiseRecord created = promises.create("unit-p-reject", I64_MAX, new Value(null, Map.of("x", 1)), Map.of())
                .join();
        assertEquals("pending", created.state());

        PromiseRecord settled = promises.reject("unit-p-reject", new Value(null, Map.of("error", "boom")))
                .join();
        assertEquals("rejected", settled.state());

        PromiseRecord after = promises.get("unit-p-reject").join();
        assertEquals("rejected", after.state());
    }

    @Test
    void getMissingReturnsServerError() {
        Promises promises = local();
        CompletionException exc = assertThrows(
                CompletionException.class, () -> promises.get("does-not-exist").join());
        assertInstanceOf(ServerError.class, exc.getCause());
    }
}
