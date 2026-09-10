package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.PlatformError;
import io.resonatehq.resonate.Errors.ResonateError;
import io.resonatehq.resonate.Errors.StoppedError;
import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Types.PromiseCreateReq;
import io.resonatehq.resonate.Types.PromiseRecord;
import io.resonatehq.resonate.Types.PromiseSettleReq;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The two durable operations the SDK needs — create and settle a promise — built from a {@link
 * Sender} and {@link Codec}, mirroring {@code resonate.effects.ResonateEffects} from the Python SDK.
 *
 * <p>Maintains a decoded {@link PromiseRecord} cache so a promise created or settled once (or
 * supplied via {@code preload}) is returned without touching the network. The cache is a plain
 * {@link Map}: every writer runs on the worker that owns the attempt and no operation holds it
 * across an {@code await}, so no synchronization is needed (the analogue of Python's single-threaded
 * asyncio dict).
 *
 * <p><b>Circuit breaker.</b> The first durable-op failure flips {@link #stopped}; every later op
 * then short-circuits with a {@link PlatformError} carrying a {@link StoppedError}, so no further
 * work happens and the task is released for re-delivery.
 *
 * <p><b>No Protocol interface.</b> Python declares an {@code Effects} {@code Protocol} beside the
 * one concrete {@code ResonateEffects}; with a single implementation there is nothing to abstract
 * over in Java, so this is just the class. Add an interface if a second implementation appears.
 *
 * <p><b>Async.</b> Python's {@code async def} methods {@code await} the sender; here they return a
 * {@link CompletableFuture}. A failure surfaces through {@code join()} as a {@link
 * CompletionException} wrapping the {@link PlatformError} — the analogue of Python's {@code raise
 * PlatformError(...)} — exactly as the {@link Sender} / {@link Transport} futures surface their
 * errors.
 */
public final class Effects {

    // The validation harness keys off this logger name rather than the class name, so it must stay
    // "resonate.validation" (matching Python's logging.getLogger("resonate.validation")).
    private static final Logger LOGGER = System.getLogger("resonate.validation");

    private final Sender sender;
    private final Codec codec;
    // Python's plain dict is safe because asyncio is single-threaded. In Java the .handle()
    // continuations below run on whichever thread completes the sender future -- a SEND_EXECUTOR
    // thread for the HTTP network -- while the entry reads run on the caller thread, so the writes
    // and reads cross threads. ConcurrentHashMap makes put/get atomic and a double-put is
    // idempotent (same id -> same decoded record); the breaker flag is volatile and monotonic.
    private final Map<String, PromiseRecord> cache = new ConcurrentHashMap<>();
    private volatile boolean stopped = false;

    /**
     * Build effects from a {@link Sender}, {@link Codec}, and optional preloaded promises.
     *
     * <p>Each preloaded record is decoded into the cache; one that fails to decode (a {@link
     * ResonateError}) is silently skipped, mirroring Python.
     */
    /**
     * The decoded-record cache. Package-private: exposed for the behaviour tests, mirroring Python's
     * directly-reachable {@code effects.cache}. The runtime reaches it only through {@link
     * #createPromise} / {@link #settlePromise}.
     */
    Map<String, PromiseRecord> cache() {
        return cache;
    }

    public Effects(Sender sender, Codec codec, List<PromiseRecord> preload) {
        this.sender = sender;
        this.codec = codec;
        for (PromiseRecord p : preload) {
            PromiseRecord decoded;
            try {
                decoded = codec.decodePromise(p);
            } catch (ResonateError exc) {
                continue;
            }
            cache.put(decoded.id(), decoded);
        }
    }

    /**
     * Create a durable promise, completing with the decoded record.
     *
     * <p>Idempotent: a cached record (from preload or a prior call) is returned without touching the
     * network.
     */
    public CompletableFuture<PromiseRecord> createPromise(PromiseCreateReq req) {
        if (stopped) {
            return CompletableFuture.failedFuture(new PlatformError(List.of(new StoppedError())));
        }

        PromiseRecord cached = cache.get(req.id());
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        PromiseCreateReq encodedReq;
        String invocation;
        try {
            encodedReq = new PromiseCreateReq(
                    req.id(), req.timeoutAt(), codec.encode(req.param().data()), req.tags());
            invocation = invocationFor(encodedReq.tags().get("resonate:scope"));
            LOGGER.log(Level.INFO, "promise_create_request promise_id={0} invocation={1}", encodedReq.id(), invocation);
        } catch (ResonateError exc) {
            stopped = true;
            return CompletableFuture.failedFuture(new PlatformError(List.of(exc)));
        }

        String inv = invocation;
        return sender.promiseCreate(encodedReq).handle((record, exc) -> {
            if (exc != null) {
                throw platformFrom(exc);
            }
            PromiseRecord decoded = decodeOrStop(record);
            cache.put(decoded.id(), decoded);
            LOGGER.log(
                    Level.INFO,
                    "promise_create_response promise_id={0} invocation={1} state={2}",
                    decoded.id(),
                    inv,
                    decoded.state());
            return decoded;
        });
    }

    /**
     * Settle a durable promise with a result, completing with the decoded record.
     *
     * <p>Idempotent: a cached non-pending record is returned without touching the network. A plain
     * value resolves the promise; any {@link Exception} rejects it (the codec flattens it to the
     * error shape). The state choice keys off {@code instanceof Exception} — the exact analogue of
     * Python's {@code isinstance(result, Exception)}, which excludes the {@code BaseException}-based
     * control-flow signals.
     */
    public CompletableFuture<PromiseRecord> settlePromise(String id, Object result) {
        if (stopped) {
            return CompletableFuture.failedFuture(new PlatformError(List.of(new StoppedError())));
        }

        PromiseRecord cached = cache.get(id);
        if (cached != null && !"pending".equals(cached.state())) {
            return CompletableFuture.completedFuture(cached);
        }

        String state = (result instanceof Exception) ? "rejected" : "resolved";

        PromiseSettleReq req;
        try {
            req = new PromiseSettleReq(id, state, codec.encode(result));
            LOGGER.log(Level.INFO, "promise_settle_request promise_id={0} state={1}", req.id(), req.state());
        } catch (ResonateError exc) {
            stopped = true;
            return CompletableFuture.failedFuture(new PlatformError(List.of(exc)));
        }

        return sender.promiseSettle(req).handle((record, exc) -> {
            if (exc != null) {
                throw platformFrom(exc);
            }
            PromiseRecord decoded = decodeOrStop(record);
            LOGGER.log(Level.INFO, "promise_settle_response promise_id={0} state={1}", decoded.id(), decoded.state());
            cache.put(decoded.id(), decoded);
            return decoded;
        });
    }

    // -- internal helpers -----------------------------------------------------

    /** Decode a record, arming the circuit breaker and surfacing a {@link PlatformError} on failure. */
    private PromiseRecord decodeOrStop(PromiseRecord record) {
        try {
            return codec.decodePromise(record);
        } catch (ResonateError exc) {
            throw platformFrom(exc);
        }
    }

    /**
     * Map a failure into the {@link CompletionException} to complete the future with.
     *
     * <p>A {@link ResonateError} (the only thing Python's {@code except ResonateError} catches) arms
     * the circuit breaker and is wrapped in a {@link PlatformError}; anything else propagates raw and
     * leaves the breaker untouched, exactly as an uncaught Python exception would.
     */
    private CompletionException platformFrom(Throwable exc) {
        Throwable cause = (exc instanceof CompletionException && exc.getCause() != null) ? exc.getCause() : exc;
        if (cause instanceof ResonateError re) {
            stopped = true;
            return new CompletionException(new PlatformError(List.of(re)));
        }
        return new CompletionException(cause);
    }

    /** Map the {@code resonate:scope} tag to the validation-log invocation kind. */
    private static String invocationFor(String scope) {
        if ("local".equals(scope)) {
            return "run";
        }
        if ("global".equals(scope)) {
            return "rpc";
        }
        return "unknown";
    }
}
