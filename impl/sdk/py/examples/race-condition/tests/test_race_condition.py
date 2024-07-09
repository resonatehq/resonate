from __future__ import annotations

import sqlite3
from functools import partial
from typing import TYPE_CHECKING

import pytest
import race_condition
import resonate

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.scheduler.dst import DSTScheduler


@pytest.fixture()
def setup_and_teardown() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect("test.db")
    ans = conn.execute("SELECT 1").fetchone()
    assert ans == (1,)
    conn.execute("DROP TABLE IF EXISTS accounts")
    conn.execute("CREATE TABLE accounts(account_id, balance)")
    conn.execute(
        """
        INSERT INTO accounts VALUES
        (1, 100),
        (2, 0)
        """
    )
    conn.commit()
    yield conn


@pytest.mark.parametrize("scheduler", resonate.testing.dst([range(20)]))
def test_race_condition(
    scheduler: DSTScheduler,
    setup_and_teardown: sqlite3.Connection,
) -> None:
    conn = setup_and_teardown

    _ = scheduler.run(
        [
            partial(
                race_condition.transaction,
                conn=conn,
                source=1,
                target=2,
                amount=100,
            ),
            partial(
                race_condition.transaction,
                conn=conn,
                source=1,
                target=2,
                amount=70,
            ),
        ]
    )
    conn.commit()

    source_balance: int = conn.execute(
        "SELECT balance FROM accounts WHERE account_id = 1"
    ).fetchone()[0]
    target_balance: int = conn.execute(
        "SELECT balance FROM accounts WHERE account_id = 2"
    ).fetchone()[0]

    assert (
        source_balance == 0 and target_balance == 100
    ), f"Seed {scheduler.seed} causes a failure"
