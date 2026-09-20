from __future__ import annotations

from typing import Protocol

import msgspec


class RetryPolicy(Protocol):
    def next(self, attempt: int) -> int | None:
        """Return seconds to sleep before ``attempt``, or ``None`` to stop retrying.

        ``attempt`` is the *upcoming* attempt number: the initial execution is
        attempt 0 and never consults the policy, the first retry is attempt 1,
        the second retry is attempt 2, and so on. A policy that wants to allow
        ``N`` retries (so ``1 + N`` total executions) returns a delay for
        ``attempt`` in ``1..=N`` and ``None`` for ``attempt > N``.

        A returned delay of ``0`` means "retry immediately, no sleep".
        """


class Exponential(msgspec.Struct, frozen=True, kw_only=True):
    # Exponential backoff: sleep ``delay * factor**attempt`` seconds before
    # each retry, capped at ``max_delay``, stopping after ``max_retries``.
    delay: int
    max_retries: int
    factor: int
    max_delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None

        return min(self.delay * self.factor**attempt, self.max_delay)


class Linear(msgspec.Struct, frozen=True, kw_only=True):
    max_retries: int
    delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None
        return self.delay * attempt


class Constant(msgspec.Struct, frozen=True, kw_only=True):
    max_retries: int
    delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None
        return self.delay


class Never(msgspec.Struct, frozen=True, kw_only=True):
    def next(self, attempt: int) -> int | None:
        return None


# ═══════════════════════════════════════════════════════════════
#  Connection backoff -- reconnect/resend, never gives up
# ═══════════════════════════════════════════════════════════════


class Backoff(Protocol):
    def delay(self, attempt: int) -> float:
        """Return seconds to wait before ``attempt``.

        Distinct from :class:`RetryPolicy`: a connection retries *forever* (a
        server being down is not a reason to abandon a durable workflow), so
        there is no ``None`` stop signal and the delay is a ``float``.
        ``attempt`` is the number of consecutive failures so far, starting at
        ``0`` for the first retry.
        """


class ExponentialBackoff(msgspec.Struct, frozen=True, kw_only=True):
    """Doubling backoff, capped -- the connection layer's reconnect ladder.

    Shared by :class:`~resonate.connections.HttpConnection` (resend) and
    :class:`~resonate.connections.SSEConnection` (reconnect) so the two cannot
    drift, and injectable so a test can pin every delay to zero instead of
    waiting out a real ``1s -> 60s`` ladder.
    """

    initial: float = 1.0
    factor: float = 2.0
    max_delay: float = 60.0

    def delay(self, attempt: int) -> float:
        if attempt < 0:
            return self.initial
        return min(self.initial * self.factor**attempt, self.max_delay)


class NoBackoff(msgspec.Struct, frozen=True, kw_only=True):
    """Retry immediately, forever. For tests that assert attempt counts."""

    def delay(self, attempt: int) -> float:
        return 0.0
