from __future__ import annotations

from sqlite3 import Connection
from typing import TYPE_CHECKING, Any

from resonate.typing import Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context


class NotEnoughFundsError(Exception):
    def __init__(self, account_id: int) -> None:
        super().__init__(f"Account {account_id} does not have enough money")


def current_balance(ctx: Context, account_id: int) -> int:  # noqa: ARG001
    conn: Connection = ctx.get_dependency("conn")
    balance: int = conn.execute(
        "SELECT balance FROM accounts WHERE account_id = ?", (account_id,)
    ).fetchone()[0]
    return balance


def update_balance(
    ctx: Context,
    account_id: int,
    amount: int,
) -> None:
    conn: Connection = ctx.get_dependency("conn")
    cur = conn.execute(
        """
        UPDATE accounts
        SET balance = balance + ?
        WHERE account_id = ?
        """,
        (amount, account_id),
    )

    ctx.assert_statement(cur.rowcount == 1, msg="More that one row was affected")


def transaction(
    ctx: Context, conn: Connection, source: int, target: int, amount: int
) -> Generator[Yieldable, Any, None]:
    ctx.set_dependency(key="conn", obj=conn)

    source_balance: int = yield ctx.call(current_balance, account_id=source)

    if source_balance - amount < 0:
        raise NotEnoughFundsError(account_id=source)

    yield ctx.call(
        update_balance,
        account_id=source,
        amount=amount * -1,
    )

    yield ctx.call(
        update_balance,
        account_id=target,
        amount=amount,
    )
