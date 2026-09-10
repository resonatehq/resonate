"""Fixtures shared by the whole suite.

Every fixture here exists to keep tests isolated and fast: a frozen clock, a
sleeper that never waits, an observer that records instead of logging, and a
client that tears itself down. Nothing reads process-wide state, so tests are
order-independent and safe to parallelize.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from resonate_testing import golden

from resonate.codec import Codec, NoopEncryptor
from resonate.registry import Registry
from resonate.testing import (
    FakeClock,
    RecordingObserver,
    RecordingSleeper,
    local_resonate,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resonate.resonate import Resonate

# Golden files for this suite live in tests/golden; resonate_testing.golden has
# no opinion on where, so each suite points it there once, before any test runs.
golden.GOLDEN_DIR = Path(__file__).parent / "golden"


@pytest.fixture
def clock() -> FakeClock:
    """Return a clock the test moves by hand, starting at 1ms rather than 0."""
    return FakeClock(start=1)


@pytest.fixture
def sleeper() -> RecordingSleeper:
    """Return a sleeper that records requested delays instead of waiting."""
    return RecordingSleeper()


@pytest.fixture
def observer() -> RecordingObserver:
    """Return an observer that captures the SDK's non-raising events."""
    return RecordingObserver()


@pytest.fixture
def codec() -> Codec:
    """Return the plaintext codec every non-encryption test wants."""
    return Codec(NoopEncryptor())


@pytest.fixture
def registry() -> Registry:
    """Return an empty function registry."""
    return Registry()


@pytest_asyncio.fixture
async def resonate(observer: RecordingObserver) -> AsyncIterator[Resonate]:
    """Yield a started, local-only ``Resonate`` that stops itself at teardown.

    Isolated by construction: ``env={}`` so a ``RESONATE_URL`` in the
    developer's shell cannot redirect it, retries off, and no real sleeping.

    The real clock is kept on purpose. A frozen clock is the right tool for
    asserting a *computed* deadline, but a workflow that actually runs needs
    time to advance -- so a test that wants one passes its own via
    ``local_resonate(clock=...)``, which threads it into the in-process server
    as well.
    """
    client = local_resonate(observer=observer)
    try:
        yield client
    finally:
        await client.stop()
