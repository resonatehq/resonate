# saga — booking saga with compensation

A travel-booking workflow that reserves a flight, reserves a hotel, and
charges a card. If any step fails, the workflow runs the inverse
compensations in reverse order. Every step — including each compensation —
is its own durable promise, so the saga survives worker crashes.

## What this demonstrates

- **Sequential `ctx.RPC` steps** with `Await` for synchronization.
- **Compensating transactions**: on failure, the orchestrator dispatches the
  inverse operations for every step that already settled, in reverse order.
- **Idempotent recovery**: if the worker crashes mid-saga, restarting it
  picks up the workflow from the next unsettled step rather than re-running
  the settled ones.

## Run it

Start a Resonate server on `localhost:8001`, then:

```sh
# Happy path — all three steps succeed.
go run .

# Force chargeCard to fail. The orchestrator runs releaseHotel and
# releaseFlight in reverse order before returning the error.
go run . -fail=charge

# Force reserveHotel to fail. Only releaseFlight runs (no hotel ref yet).
go run . -fail=hotel
```

Expected output for `-fail=charge`:

```
[bookTrip] starting workflow id=saga-... fail_at="charge"
  [reserveFlight] reserved FL-alice-SFO-JFK
  [reserveHotel] reserved HT-alice-JFK
  [chargeCard] FAILED for alice ($850)
  [bookTrip] running compensations...
  [releaseHotel] released HT-alice-JFK
  [releaseFlight] released FL-alice-SFO-JFK
[bookTrip] FAILED: chargeCard: card declined for $850
```

## Try killing the worker

In a long-running saga the durable-execution guarantee is most visible when
a crash splits the workflow. To see it:

1. Add a `time.Sleep` (e.g. `5 * time.Second`) at the top of `chargeCard`.
2. Run `go run .` — wait until you see `[reserveHotel] reserved ...` in the
   logs.
3. Hit Ctrl-C to kill the worker mid-saga.
4. Re-run `go run .` with the **same** workflow id (export it as an env
   var or hard-code one for the test). The output will skip the two
   reservation lines (their promises are already settled) and resume at
   `chargeCard`.

## Code map

| Function        | Role                                              |
|-----------------|---------------------------------------------------|
| `bookTrip`      | Saga orchestrator: sequences steps + compensation |
| `reserveFlight` | Step 1                                            |
| `reserveHotel`  | Step 2 (fails when `-fail=hotel`)                 |
| `chargeCard`    | Step 3 (fails when `-fail=charge`)                |
| `releaseFlight` | Compensation for step 1                           |
| `releaseHotel`  | Compensation for step 2                           |
