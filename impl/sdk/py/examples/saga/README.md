# saga — booking saga with compensation

A travel-booking workflow that reserves a flight, reserves a hotel, and
charges a card. If any step fails, the workflow runs the inverse
compensations in reverse order. Every step — including each compensation —
is its own durable promise, so the saga survives worker crashes.

## What this demonstrates

- **Sequential `ctx.rpc` steps** with `await` for synchronization.
- **Compensating transactions**: on failure, the orchestrator dispatches the
  inverse operations for every step that already settled, in reverse order.
- **Idempotent recovery**: if the worker crashes mid-saga, restarting it
  picks up the workflow from the next unsettled step rather than re-running
  the settled ones.

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
# Happy path — all three steps succeed.
uv run python examples/saga/main.py

# Force charge_card to fail. The orchestrator runs release_hotel and
# release_flight in reverse order before returning the error.
uv run python examples/saga/main.py --fail charge

# Force reserve_hotel to fail. Only release_flight runs (no hotel ref yet).
uv run python examples/saga/main.py --fail hotel
```

Expected output for `--fail charge`:

```
[book_trip] starting workflow id=saga-... fail_at='charge'
  [reserve_flight] reserved FL-alice-SFO-JFK
  [reserve_hotel] reserved HT-alice-JFK
  [charge_card] FAILED for alice ($850)
  [release_hotel] released HT-alice-JFK
  [release_flight] released FL-alice-SFO-JFK
[book_trip] FAILED: ... card declined for $850
```

## A note on replay

The SDK runs an orchestrator with a
**suspend-and-replay** model: `book_trip` re-executes from the top each time it
awaits a not-yet-settled future. So all side effects (the log lines here) live
in the leaf step functions, which settle exactly once and are never re-run. If
you moved a `print` into `book_trip` or `compensate`, you would see it repeat on
every replay.

## Try killing the worker

In a long-running saga the durable-execution guarantee is most visible when a
crash splits the workflow. To see it:

1. Add an `await asyncio.sleep(5)` at the top of `charge_card`.
2. Run the example — wait until you see `[reserve_hotel] reserved ...`.
3. Hit Ctrl-C to kill the worker mid-saga.
4. Re-run with the **same** workflow id (hard-code one for the test). The
   output will skip the two reservation lines (their promises are already
   settled) and resume at `charge_card`.

## Code map

| Function         | Role                                              |
|------------------|---------------------------------------------------|
| `book_trip`      | Saga orchestrator: sequences steps + compensation |
| `reserve_flight` | Step 1                                            |
| `reserve_hotel`  | Step 2 (fails when `--fail hotel`)               |
| `charge_card`    | Step 3 (fails when `--fail charge`)              |
| `release_flight` | Compensation for step 1                           |
| `release_hotel`  | Compensation for step 2                           |
