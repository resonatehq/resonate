package io.resonatehq.resonate;

/**
 * Retry policies mirroring {@code resonate.retry} from the Python SDK.
 *
 * <p>The Python module exposes a {@code RetryPolicy} {@link Protocol} with four concrete
 * implementations ({@code Exponential}, {@code Linear}, {@code Constant}, {@code Never}). We mirror
 * those with a plain Java interface (Python's {@code Protocol} is structural / duck-typed, so the
 * interface is intentionally non-sealed — users can plug in their own policies) and four immutable
 * records, one per Python {@code msgspec.Struct(frozen=True)}.
 *
 * <p>{@link RetryPolicy#next(int)} returns a boxed {@link Long} so {@code null} can stand in for
 * Python's {@code None} ("stop retrying"). A delay of {@code 0} means "retry immediately, no
 * sleep".
 */
public final class Retry {
    private Retry() {}

    /**
     * A retry policy: given an upcoming {@code attempt} number, decide how long to sleep before it,
     * or return {@code null} to stop retrying.
     *
     * <p>{@code attempt} is the upcoming attempt number: the initial execution is attempt 0 and
     * never consults the policy, the first retry is attempt 1, the second retry is attempt 2, and
     * so on. A policy that wants to allow {@code N} retries (so {@code 1 + N} total executions)
     * returns a delay for {@code attempt} in {@code 1..=N} and {@code null} for {@code attempt > N}.
     */
    public interface RetryPolicy {
        /**
         * @return seconds to sleep before {@code attempt}, or {@code null} to stop retrying.
         */
        Long next(int attempt);
    }

    /**
     * Exponential backoff: {@code delay * factor^attempt}, capped at {@code maxDelay}.
     *
     * <p>The SDK-wide default policy ({@code DEFAULT_RETRY_POLICY} on the Python side) is {@code
     * Exponential(delay=1, factor=2, max_delay=(1<<63)-1, max_retries=30)} — 1s base, doubling,
     * effectively unbounded cap, 30 retries.
     *
     * <p>Python computes {@code delay * factor**attempt} on arbitrary-precision {@code int}; in
     * Java we use {@code long} and clamp during the iterative multiplication so an overflow
     * saturates to {@code maxDelay} (matching Python's {@code min(..., max_delay)} behavior even at
     * extreme attempt counts) rather than wrapping.
     */
    public record Exponential(long delay, int maxRetries, long factor, long maxDelay) implements RetryPolicy {
        @Override
        public Long next(int attempt) {
            if (attempt > maxRetries) return null;
            // Iterative pow with saturation: equivalent to min(delay * factor^attempt, maxDelay),
            // but never overflows long. Once any intermediate value would exceed maxDelay, we
            // return maxDelay -- the same value Python's min(...) would produce.
            long result = delay;
            for (int i = 0; i < attempt; i++) {
                if (result > maxDelay / factor) return maxDelay;
                result *= factor;
            }
            return Math.min(result, maxDelay);
        }
    }

    /** Linear backoff: {@code delay * attempt}, until {@code attempt > maxRetries}. */
    public record Linear(int maxRetries, long delay) implements RetryPolicy {
        @Override
        public Long next(int attempt) {
            if (attempt > maxRetries) return null;
            return delay * attempt;
        }
    }

    /** Constant delay between retries until {@code attempt > maxRetries}. */
    public record Constant(int maxRetries, long delay) implements RetryPolicy {
        @Override
        public Long next(int attempt) {
            if (attempt > maxRetries) return null;
            return delay;
        }
    }

    /** Never retry: {@link #next(int)} is {@code null} for every {@code attempt}. */
    public record Never() implements RetryPolicy {
        @Override
        public Long next(int attempt) {
            return null;
        }
    }
}
