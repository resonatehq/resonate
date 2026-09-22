package io.resonatehq.resonate;

import io.resonatehq.resonate.Errors.PlatformError;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Serializes async work in acquisition order under concurrency.
 *
 * <p>Mirrors {@code resonate.chain.Chain} from the Python SDK. Backs the durable ops on the
 * execution context ({@code run}/{@code rpc}/{@code sleep}/{@code promise}/{@code detached}). Each op
 * acquires a {@link Link} <em>synchronously</em> at call time -- so chain position matches call order
 * regardless of when the background tasks happen to run -- and then runs its work through that link
 * from a background task. A link runs only after its predecessor has completed, and a failed link
 * poisons every successor: the chain fails as a unit.
 *
 * <p>One chain lives per execution state (root and each child each own one), so sibling ops at the
 * same level share an ordering while different levels are independent.
 *
 * <p><b>Async model.</b> Python's {@code asyncio.Future[None]} becomes a {@link CompletableFuture
 * CompletableFuture&lt;Void&gt;} here, and the {@code async def} {@code work} -- typed
 * {@code Callable[[], Awaitable[T]]} -- becomes a {@link Supplier} of a {@code CompletableFuture<T>}.
 * {@code await self._prev} becomes a {@link CompletableFuture#whenComplete whenComplete} gate on the
 * predecessor's {@code done}.
 *
 * <p>This class is not thread-safe: like the Python original it assumes {@link #link()} is called
 * synchronously from a single ordering thread (the one driving the context).
 */
public final class Chain {

    /**
     * Tail of the chain: the {@code done} future of the most recently acquired link, or {@code null}
     * before the first. Advanced synchronously in {@link #link()}.
     */
    private CompletableFuture<Void> tail;

    public Chain() {
        this.tail = null;
    }

    /** Acquire the next link, advancing the tail synchronously. */
    public Link link() {
        CompletableFuture<Void> prev = this.tail;
        CompletableFuture<Void> done = new CompletableFuture<>();
        this.tail = done;
        return new Link(prev, done);
    }

    /**
     * One position in a {@link Chain}.
     *
     * <p>{@link #done()} resolves when this link's work completes (or fails with its exception). It is
     * the gate the next link waits on, and is exposed so an external awaiter -- e.g. a future that
     * reports the created promise id -- observes the same outcome.
     */
    public static final class Link {

        /** The predecessor's {@code done}, or {@code null} for the first link. */
        private final CompletableFuture<Void> prev;

        private final CompletableFuture<Void> done;

        Link(CompletableFuture<Void> prev, CompletableFuture<Void> done) {
            this.prev = prev;
            this.done = done;
        }

        /**
         * Resolves with {@code null} when this link's work completes, or completes exceptionally with
         * its exception. The gate every successor waits on.
         */
        public CompletableFuture<Void> done() {
            return done;
        }

        /**
         * Wait for the predecessor, run {@code work}, then release the successor.
         *
         * <p>A predecessor that failed propagates its exception through the gate; we re-install it on
         * {@link #done()} and on the returned future, so the whole chain fails as a unit rather than
         * running work on top of an inconsistent prefix. The <em>same</em> upstream exception object
         * travels down every successor link, so the failure surfaces identically no matter how far down
         * it is observed.
         *
         * <p>{@link PlatformError} (Python's {@code BaseException}, here a {@link java.lang.Error}) is
         * settled explicitly alongside ordinary exceptions: a platform failure must still settle
         * {@link #done()} or successors and id-awaiters deadlock. {@link CancellationException} is
         * deliberately <em>not</em> settled -- that mirrors Python leaving {@code CancelledError}
         * uncaught so task cancellation is never swallowed.
         */
        public <T> CompletableFuture<T> run(Supplier<CompletableFuture<T>> work) {
            CompletableFuture<T> result = new CompletableFuture<>();
            CompletableFuture<Void> gate = (prev != null) ? prev : CompletableFuture.completedFuture(null);
            gate.whenComplete((ignored, prevErr) -> {
                if (prevErr != null) {
                    // Predecessor failed: poison this link without running work.
                    fail(unwrap(prevErr), result);
                    return;
                }
                CompletableFuture<T> w;
                try {
                    w = work.get();
                } catch (Throwable t) {
                    fail(t, result);
                    return;
                }
                w.whenComplete((value, workErr) -> {
                    if (workErr == null) {
                        done.complete(null);
                        result.complete(value);
                    } else {
                        fail(unwrap(workErr), result);
                    }
                });
            });
            return result;
        }

        private void fail(Throwable e, CompletableFuture<?> result) {
            if (shouldSettle(e)) {
                done.completeExceptionally(e);
            }
            result.completeExceptionally(e);
        }

        /**
         * Mirrors Python's {@code except (Exception, PlatformError)}: settle {@code done} for an
         * ordinary {@link Exception} or a {@link PlatformError}, but never for a
         * {@link CancellationException}. Python's {@code CancelledError} is a {@code BaseException} that
         * must propagate without settling so task cancellation works; {@code CancellationException} is
         * its Java analogue (and, unlike Python's, an {@link Exception}), so it is excluded explicitly.
         */
        private static boolean shouldSettle(Throwable e) {
            if (e instanceof CancellationException) {
                return false;
            }
            return e instanceof Exception || e instanceof PlatformError;
        }

        /** Strip the {@link CompletionException} wrapper so the original object travels down the chain. */
        private static Throwable unwrap(Throwable t) {
            if (t instanceof CompletionException && t.getCause() != null) {
                return t.getCause();
            }
            return t;
        }
    }
}
