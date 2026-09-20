package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Codec.NoopEncryptor;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.ResonateTimeoutError;
import io.resonatehq.resonate.Handle.PromiseResult;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Handle.Subscription;
import io.resonatehq.resonate.Types.Value;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/** Mirrors {@code resonate-sdk-py/tests/test_handle.py}. */
final class HandleTest {

    private static Codec codec() {
        return new Codec(new NoopEncryptor());
    }

    private static Subscription ready(PromiseResult result) {
        Subscription sub = new Subscription();
        sub.settle(result);
        return sub;
    }

    /** A resolved creation future: these handles model created promises. */
    private static CompletableFuture<Void> created() {
        return CompletableFuture.completedFuture(null);
    }

    /** The throwable {@code result()} raises directly (durable rejections surface unwrapped). */
    private static Throwable cause(ResonateHandle<?> handle) {
        return assertThrows(Throwable.class, handle::result);
    }

    @Test
    void resultResolvedDecodesValue() {
        Codec c = codec();
        PromiseResult result = new PromiseResult("resolved", c.encode(42));
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), c, Integer.class, created());
        assertEquals(42, handle.result());
    }

    @Test
    void resultRejectedRaisesApplicationError() {
        Codec c = codec();
        Value value = c.encode(Map.of("message", "boom"));
        PromiseResult result = new PromiseResult("rejected", value);
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), c, Integer.class, created());
        Throwable t = cause(handle);
        assertInstanceOf(ApplicationError.class, t);
        assertTrue(t.getMessage().contains("boom"));
    }

    @Test
    void resultRejectedCanceledRaises() {
        PromiseResult result = new PromiseResult("rejected_canceled", new Value());
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), codec(), Integer.class, created());
        Throwable t = cause(handle);
        assertInstanceOf(ApplicationError.class, t);
        assertTrue(t.getMessage().contains("Promise canceled"));
    }

    @Test
    void resultRejectedTimedoutRaisesTimeout() {
        PromiseResult result = new PromiseResult("rejected_timedout", new Value());
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), codec(), Integer.class, created());
        assertInstanceOf(ResonateTimeoutError.class, cause(handle));
    }

    @Test
    void resultPendingRaises() {
        PromiseResult result = new PromiseResult("pending", new Value());
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), codec(), Integer.class, created());
        Throwable t = cause(handle);
        assertInstanceOf(ApplicationError.class, t);
        assertTrue(t.getMessage().contains("promise still pending"));
    }

    @Test
    void resultEmptyDataDecodesToNull() {
        PromiseResult result = new PromiseResult("resolved", new Value(null, ""));
        ResonateHandle<Object> handle = new ResonateHandle<>("p1", ready(result), codec(), Object.class, created());
        assertEquals(null, handle.result());
    }

    @Test
    void resultAbsentDataDecodesToNull() {
        PromiseResult result = new PromiseResult("resolved", new Value());
        ResonateHandle<Object> handle = new ResonateHandle<>("p1", ready(result), codec(), Object.class, created());
        assertEquals(null, handle.result());
    }

    @Test
    void resultObjectRoundTrips() {
        Codec c = codec();
        PromiseResult result = new PromiseResult("resolved", c.encode(Map.of("x", 1)));
        ResonateHandle<Object> handle = new ResonateHandle<>("p1", ready(result), c, Object.class, created());
        assertEquals(Map.of("x", 1), handle.result());
    }

    @Test
    void resultScalarRoundTrips() {
        Codec c = codec();
        PromiseResult result = new PromiseResult("resolved", c.encode(7));
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", ready(result), c, Integer.class, created());
        assertEquals(7, handle.result());
    }

    @Test
    void doneReflectsChannelState() {
        Subscription sub = new Subscription();
        ResonateHandle<Integer> handle = new ResonateHandle<>("p1", sub, codec(), Integer.class, created());
        assertFalse(handle.done());
        sub.settle(new PromiseResult("resolved", new Value(null, "")));
        assertTrue(handle.done());
    }

    @Test
    void idBlocksUntilCreationConfirmed() {
        CompletableFuture<Void> created = new CompletableFuture<>();
        ResonateHandle<Integer> handle = new ResonateHandle<>(
                "p1", ready(new PromiseResult("pending", new Value())), codec(), Integer.class, created);
        CompletableFuture<String> idFut = handle.id();
        assertFalse(idFut.isDone());
        created.complete(null);
        assertEquals("p1", idFut.join());
    }

    @Test
    void idRaisesWhenCreationFails() {
        CompletableFuture<Void> created = new CompletableFuture<>();
        ResonateHandle<Integer> handle = new ResonateHandle<>(
                "p1", ready(new PromiseResult("pending", new Value())), codec(), Integer.class, created);
        created.completeExceptionally(new ApplicationError("create failed"));
        CompletionException ex =
                assertThrows(CompletionException.class, () -> handle.id().join());
        assertInstanceOf(ApplicationError.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("create failed"));
    }
}
