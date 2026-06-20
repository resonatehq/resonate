package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.resonatehq.resonate.Chain.Link;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Errors.PlatformError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_chain.py}.
 *
 * <p>Python's {@code asyncio.Future} becomes a {@link CompletableFuture}; an {@code async def work}
 * becomes a {@link java.util.function.Supplier} returning a future. Python's {@code link.done.result()}
 * / {@code link.done.exception()} are recovered via {@link #exceptionOf(CompletableFuture)} since
 * {@code CompletableFuture} has no direct exception getter.
 */
class ChainTest {

    @Test
    void linksRunInAcquisitionOrderUnderConcurrency() {
        // Acquire links in order, but run them as concurrent tasks whose work yields to another
        // thread before recording. Order must follow acquisition, not scheduling.
        Chain chain = new Chain();
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());

        // Acquire synchronously in order.
        List<Link> links = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            links.add(chain.link());
        }

        // Run later links first to prove the chain -- not task start order -- wins.
        List<CompletableFuture<Integer>> results = new ArrayList<>(Collections.nCopies(5, null));
        for (int i = 4; i >= 0; i--) {
            int n = i;
            results.set(
                    n,
                    links.get(n)
                            .run(() -> CompletableFuture.supplyAsync(() -> {
                                order.add(n); // let a non-chained impl race ahead
                                return n;
                            })));
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0])).join();

        assertEquals(List.of(0, 1, 2, 3, 4), order);
        List<Integer> resolved =
                results.stream().map(CompletableFuture::join).sorted().toList();
        assertEquals(List.of(0, 1, 2, 3, 4), resolved);
    }

    @Test
    void doneResolvesOnSuccess() {
        Chain chain = new Chain();
        Link link = chain.link();
        link.run(() -> CompletableFuture.completedFuture("ok")).join();
        assertTrue(link.done().isDone());
        assertNull(link.done().join());
    }

    @Test
    void failurePoisonsEveryLaterLink() {
        // A failing link must propagate its exception the full length of the chain: every successor
        // inherits the same exception object and never runs work.
        Chain chain = new Chain();
        RuntimeException boom = new RuntimeException("boom");
        List<Integer> ran = Collections.synchronizedList(new ArrayList<>());

        Link l1 = chain.link();
        Link l2 = chain.link();
        Link l3 = chain.link();

        CompletableFuture<Void> t1 = l1.run(() -> CompletableFuture.failedFuture(boom));
        CompletableFuture<Void> t2 = l2.run(() -> CompletableFuture.runAsync(() -> ran.add(2)));
        CompletableFuture<Void> t3 = l3.run(() -> CompletableFuture.runAsync(() -> ran.add(3)));

        for (CompletableFuture<Void> t : List.of(t1, t2, t3)) {
            CompletionException ex = assertThrows(CompletionException.class, t::join);
            assertSame(boom, ex.getCause()); // same object travels down the chain
        }

        assertEquals(List.of(), ran); // no successor ran its work
        assertSame(boom, exceptionOf(l1.done()));
        assertSame(boom, exceptionOf(l2.done()));
        assertSame(boom, exceptionOf(l3.done()));
    }

    @Test
    void platformErrorSettlesDone() {
        // PlatformError is a BaseException (not Exception) in Python -- here a java.lang.Error -- so it
        // is settled explicitly: a platform failure must still settle ``done`` or successors and
        // id-awaiters would deadlock.
        Chain chain = new Chain();
        Link link = chain.link();
        PlatformError boom = new PlatformError(List.of(new ApplicationError("boom")));

        CompletableFuture<Void> result = link.run(() -> CompletableFuture.failedFuture(boom));

        CompletionException ex = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(PlatformError.class, ex.getCause());
        assertSame(boom, exceptionOf(link.done()));
    }

    @Test
    void cancellationIsNotSwallowed() {
        // CancellationException (Java analogue of Python's CancelledError, which is a BaseException
        // and neither an Exception nor a PlatformError) must propagate without settling ``done``.
        Chain chain = new Chain();
        Link link = chain.link();
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> workFuture = new CompletableFuture<>();

        CompletableFuture<Void> task = link.run(() -> {
            started.complete(null);
            return workFuture;
        });

        started.join();
        workFuture.cancel(true);

        // join() surfaces a CancellationException directly (not wrapped), mirroring Python raising
        // asyncio.CancelledError out of the awaited task.
        assertThrows(CancellationException.class, task::join);
        assertFalse(link.done().isDone()); // done left unsettled -- cancellation was not swallowed
    }

    /** Recovers the raw exception a future completed with -- Python's {@code Future.exception()}. */
    private static Throwable exceptionOf(CompletableFuture<?> f) {
        return f.handle((value, t) -> t).join();
    }
}
