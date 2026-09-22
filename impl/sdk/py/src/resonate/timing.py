"""Injectable time: the SDK's one seam onto the wall clock and the event loop.

Wall-clock reads and sleeps are the two pieces of ambient global state the SDK
cannot avoid depending on, and both are hostile to tests: a real clock forces
sentinel deadlines (``timeout_at=1 << 63``) into every fixture, and a real
sleep turns a retry-policy assertion into a multi-second wait.

Neither is banned -- they are turned into *configuration options that default
to the global value*. :data:`Clock` and :data:`Sleeper` are the seams;
:func:`now_ms` and :func:`sleep` are the process-wide defaults every
constructor falls back to. A test passes its own and observes exactly which
deadlines were computed and which delays were requested.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

#: Reads the current time in milliseconds since the UNIX epoch. Defaults to
#: :func:`now_ms` everywhere it is accepted.
type Clock = Callable[[], int]

#: Suspends the caller for a number of *seconds*. Defaults to :func:`sleep`
#: everywhere it is accepted. Tests inject a recorder to assert the exact
#: delay sequence a retry or backoff policy produced without waiting for it.
type Sleeper = Callable[[float], Awaitable[None]]


def now_ms() -> int:
    """Return the current time in milliseconds since the UNIX epoch."""
    return time.time_ns() // 1_000_000


async def sleep(secs: float) -> None:
    """Suspend for ``secs`` seconds on the running event loop.

    The default :data:`Sleeper`. A thin wrapper over :func:`asyncio.sleep` so
    the injection point is a named function rather than a stdlib reference
    tests must monkeypatch.
    """
    await asyncio.sleep(secs)
