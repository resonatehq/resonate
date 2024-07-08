from __future__ import annotations

from sqlite3 import Connection
from typing import TYPE_CHECKING, Any

from resonate_sdk_py.typing import Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate_sdk_py.context import Context


class NotEnoughMoneyError(Exception):
    def __init__(self, account_id: int) -> None:
        super().__init__(f"Account {account_id} does not have enough money")


def get_current_balance(ctx: Context, conn: Connection, account_id: int) -> int:  # noqa: ARG001
    balance: int = conn.execute(
        "SELECT balance FROM accounts WHERE account_id = ?", (account_id,)
    ).fetchone()[0]
    return balance


def modify_balance(
    ctx: Context,
    conn: Connection,
    account_id: int,
    amount: int,
) -> None:
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
    source_balance: int = yield ctx.call(
        get_current_balance, conn=conn, account_id=source
    )

    if source_balance - amount < 0:
        raise NotEnoughMoneyError(account_id=source)

    yield ctx.call(
        modify_balance,
        conn=conn,
        account_id=source,
        amount=amount * -1,
    )

    yield ctx.call(
        modify_balance,
        conn=conn,
        account_id=target,
        amount=amount,
    )
    conn.commit()
