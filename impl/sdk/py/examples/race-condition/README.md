# Race Conditions

Transitioning from synchronous to asynchronous programming introduces concurrency. Concurrency, in turn, introduces the possibility of a *race condition*.

A race condition occurs when the concurrent execution of a program results in unexpected behavior due to the timing of events. This bug does not appear during a single execution but manifests when specific sequences of steps occur concurrently.

Consider the following code:

```py
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
```

At first glance, this code appears to be free of bugs during a single execution. However, consider what happens if two concurrent executions read the balance in quick succession before any updates are made to the database. In such a scenario, the following invariant check:

```py
if source_balance - amount < 0:
    raise NotEnoughFundsError(account_id=source)
```

would fail to prevent the system from reaching an undesirable state. This is because both executions might read the same balance and proceed to make updates based on an outdated state, leading to inconsistent or incorrect results.


### Using Resonate DST to Prevent Race Conditions

To effectively test for race conditions, we can use the Resonate SDK. This library facilitates testing concurrent executions, which is crucial for cloud applications but often overlooked. It also allows for deterministic randomization of possible combinations of concurrent executions, helping to ensure that your application does not end up in an undesired state due to race conditions.

Here's an example of how to use Resonate DST to test for race conditions:

```py
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
```

### Benefits of Using Resonate DST
- Simplified Concurrent Testing: Resonate DST simplifies the process of testing concurrent executions, making it more accessible and routine for cloud application testing.
- Deterministic Simulation Testing: By randomizing the possible combinations of concurrent executions deterministically, Resonate DST helps identify race conditions that might otherwise be missed, ensuring the robustness of your application.