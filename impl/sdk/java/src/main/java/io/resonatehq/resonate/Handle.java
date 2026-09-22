package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.ResonateTimeoutError;
import io.resonatehq.resonate.Types.Value;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A handle to a durable promise, mirroring {@code resonate.handle} from the Python SDK.
 *
 * <p>Python's coroutines map to {@link CompletableFuture} (the same async analogue used across
 * {@link Network} and friends). The decode type, which Python passes explicitly as {@code type_}
 * because it cannot resolve the type variable at runtime, is passed here as a {@link Class}.
 */
public final class Handle {
    private Handle() {}

    /**
     * The settled state of a durable promise: the {@code state} plus the raw, still-encoded wire
     * {@code value}. Decoding is deferred to {@link Codec} when the holder reads the result.
     */
    public record PromiseResult(String state, Value value) {}

    /**
     * A settle-once signal shared by every handle to one promise id.
     *
     * <p>Built on a {@link CompletableFuture} carrying the result: a single {@link #settle} wakes
     * every waiter at once, and is idempotent.
     */
    public static final class Subscription {
        private final CompletableFuture<PromiseResult> done = new CompletableFuture<>();

        /** Record the settled result and wake all waiters. Idempotent. */
        public void settle(PromiseResult result) {
            done.complete(result);
        }

        /** Whether the subscription has been settled (non-blocking). */
        public boolean settled() {
            return done.isDone();
        }

        /** Resolves to the settled result once settled. */
        public CompletableFuture<PromiseResult> await() {
            return done;
        }
    }

    /**
     * A handle to a durable promise, returned from {@code run}, {@code rpc}, and {@code get}.
     *
     * <p>The promise id is not exposed until creation is confirmed: {@link #id} awaits the {@code
     * created} future, which resolves to {@code null} once the durable promise exists or completes
     * exceptionally if the creation round-trip failed. The {@code get} path passes an
     * already-completed future.
     */
    public static final class ResonateHandle<T> {
        private final String id;
        private final Subscription sub;
        private final Codec codec;
        private final java.lang.reflect.Type type;
        private final CompletableFuture<Void> created;

        public ResonateHandle(
                String id,
                Subscription sub,
                Codec codec,
                java.lang.reflect.Type type,
                CompletableFuture<Void> created) {
            this.id = id;
            this.sub = sub;
            this.codec = codec;
            this.type = type;
            this.created = created;
        }

        /**
         * The decode type this handle coerces a resolved value to. Package-private: exposed for the
         * behaviour tests, mirroring Python's directly-reachable {@code handle._type}.
         */
        java.lang.reflect.Type type() {
            return type;
        }

        /**
         * Return the durable promise id, once its creation is confirmed.
         *
         * <p>Awaits the creation future so a caller never observes an id before the backing durable
         * promise is known to exist; if creation failed, that error surfaces here.
         */
        public CompletableFuture<String> id() {
            return created.thenApply(v -> id);
        }

        /**
         * Block until the promise completes, returning the result or raising, the blocking analogue
         * of Python's {@code await handle.result()}.
         *
         * <p>Java has no event loop to interleave creation with this wait, so the call chains through
         * {@code created} first: it only exits once the durable promise is known to exist, and a
         * creation failure surfaces here instead of hanging forever on a subscription that will never
         * settle. A durable rejection is unwrapped from its {@link CompletionException} wrapper so it
         * surfaces as the original throwable.
         */
        public T result() {
            try {
                return created.thenCompose(v -> sub.await())
                        .thenApply(this::decodeResult)
                        .join();
            } catch (CompletionException exc) {
                Throwable cause = exc.getCause() != null ? exc.getCause() : exc;
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                if (cause instanceof Error err) {
                    throw err;
                }
                throw exc;
            }
        }

        /** Check if the promise is done (non-blocking). */
        public boolean done() {
            return sub.settled();
        }

        @SuppressWarnings("unchecked")
        private T decodeResult(PromiseResult result) {
            return switch (result.state()) {
                    // ``type`` is recovered at runtime (Durable.returnType / Object for the untyped
                    // paths), so the coercion is dynamic and the cast to T is unchecked -- exactly the
                    // gap Python closes by passing ``type_`` explicitly.
                case "resolved" -> (T) codec.convert(codec.decode(result.value()), type);
                    // Python does a bare ``raise`` of the recovered error. CompletableFuture funnels
                    // every failure through CompletionException, so wrapping here lands the original
                    // throwable on getCause() exactly as an unwrapped throw would.
                case "rejected" -> throw new CompletionException(codec.decodeError(result.value()));
                case "rejected_canceled" -> throw new ApplicationError("Promise canceled");
                case "rejected_timedout" -> throw new ResonateTimeoutError();
                case "pending" -> throw new ApplicationError("promise still pending");
                default -> throw new ApplicationError("unknown promise state: " + result.state());
            };
        }
    }
}
