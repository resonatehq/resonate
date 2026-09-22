"""The psycopg seam.

``PsycopgSessions`` is the one adapter that touches a real driver, so the only
thing worth asserting without a database is that it *is* a ``PgSessions`` and
that it does not connect until asked.
"""

from __future__ import annotations

import pytest
from resonate_pg.client import PgSessions, PsycopgSessions


def test_the_psycopg_adapter_satisfies_the_seam() -> None:
    assert isinstance(PsycopgSessions("postgresql:///nonexistent"), PgSessions)


def test_constructing_the_adapter_opens_no_connection() -> None:
    """Construction must be free: ``Resonate`` builds connectors eagerly."""
    sessions = PsycopgSessions("postgresql:///nonexistent")
    assert sessions._pool is None


@pytest.mark.asyncio
async def test_closing_an_unopened_adapter_is_a_no_op() -> None:
    """``stop`` runs on the shutdown path and must never raise."""
    await PsycopgSessions("postgresql:///nonexistent").close()
