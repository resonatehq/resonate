package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.resonatehq.resonate.Retry.Constant;
import io.resonatehq.resonate.Retry.Exponential;
import io.resonatehq.resonate.Retry.Linear;
import io.resonatehq.resonate.Retry.Never;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Mirrors {@code resonate-sdk-py/tests/test_retry.py}.
 *
 * <p>Each Python {@code TestX} class maps to a {@link Nested} Java class; each {@code @pytest.mark.
 * parametrize} maps to a {@link ParameterizedTest} with {@link CsvSource} so the parameter tuples
 * remain visible at the test-name level.
 */
class RetryTest {

    @Nested
    class TestExponential {
        @Test
        void defaultScheduleAndCutoff() {
            Exponential policy = new Exponential(1, 5, 2, 60);

            assertEquals(2L, policy.next(1));
            assertEquals(4L, policy.next(2));
            assertEquals(32L, policy.next(5));
            assertNull(policy.next(6));
        }

        @Test
        void capsAtMaxDelay() {
            Exponential policy = new Exponential(10, 5, 3, 25);

            assertEquals(25L, policy.next(1));
            assertEquals(25L, policy.next(2));
            assertEquals(25L, policy.next(5));
            assertNull(policy.next(6));
        }

        @ParameterizedTest
        @CsvSource({"0, 1", "1, 2", "3, 8"})
        void returnsExpectedDelayForAttemptsWithinLimit(int attempt, long expected) {
            Exponential policy = new Exponential(1, 5, 2, 60);
            assertEquals(expected, policy.next(attempt));
        }
    }

    @Nested
    class TestLinear {
        @ParameterizedTest
        @CsvSource({
            "3, 5, 0, 0",
            "3, 5, 1, 5",
            "3, 5, 2, 10",
            "3, 5, 3, 15",
        })
        void returnsMultiplesUpToMaxRetries(int maxRetries, long delay, int attempt, long expected) {
            Linear policy = new Linear(maxRetries, delay);
            assertEquals(expected, policy.next(attempt));
        }

        @Test
        void returnsNoneAfterMaxRetries() {
            Linear policy = new Linear(3, 5);

            assertNull(policy.next(4));
            assertNull(policy.next(10));
        }
    }

    @Nested
    class TestConstant {
        @ParameterizedTest
        @CsvSource({
            "0, 7, 0", "3, 7, 1", "3, 7, 3",
        })
        void returnsConstantDelayUpToMaxRetries(int maxRetries, long delay, int attempt) {
            Constant policy = new Constant(maxRetries, delay);
            assertEquals(delay, policy.next(attempt));
        }

        @Test
        void returnsNoneAfterMaxRetries() {
            Constant policy = new Constant(3, 7);

            assertNull(policy.next(4));
            assertNull(policy.next(100));
        }
    }

    @Nested
    class TestNever {
        @ParameterizedTest
        @ValueSource(ints = {-1, 0, 1, 10, 999})
        void alwaysReturnsNone(int attempt) {
            assertNull(new Never().next(attempt));
        }
    }
}
