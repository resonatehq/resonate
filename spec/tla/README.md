# The TLA+ machine

`spec/02-abstract` makes one request one transition. That is the right
altitude for saying what a request *means*, and the wrong one for asking what
two concurrent requests do to each other — at that altitude they cannot do
anything to each other.

`Resonate.tla` keeps the Lean alphabet and takes the atomicity out. It splits
every event into the phases the Verus executor uses
(`resonate-verus/src/concrete_spec/executor.rs`), so that the gap between a
step's decision and its effects is a place other steps can get into.

**The premise: the objects and the wheel are two stores, and nothing writes
them together.** A step that settles a promise and clears its timeout does two
writes, at two moments. `Process` reads and decides and writes nothing;
`Perform` applies one effect at a time. So the *order* of a handler's effects
is protocol — Verus emits `sched + put + ack + emits + respond`, arm before
the write and disarm after, and a crash between any two effects is what makes
the difference visible.

**There is no fence.** Two steps may read the same object and both write it;
the second wins. That is the experiment, not an oversight — put a
compare-and-swap in first and the model can only confirm that a fence is
sufficient. Left out, it has to say what goes wrong without one.

| | |
|---|---|
| `Resonate.tla` | the machine — four variables, six transitions, Apalache-typed |
| `MCResonate.cfg` | a model small enough to check and large enough to interleave |

## Running it

Apalache ≥ 0.61.

```
apalache-mc typecheck Resonate.tla
JVM_ARGS="-Xss512m" apalache-mc check --config=MCResonate.cfg --length=3 Resonate.tla
```

**`-Xss512m` is not optional.** Apalache's default JVM stack overflows while
rewriting this module — deep in `scalaz`, before the solver is reached, with a
`StackOverflowError` that looks like a bug in the spec and is not one. The
launcher sets `-Xmx` for you and leaves `-Xss` alone.

TLC can also run the module; `Spec` carries the fairness conditions, which
Apalache ignores. Under TLC, declare `Rid` a symmetry set — it is what makes
the permutations of which step got which identity collapse. Expect to drop
symmetry when checking the liveness properties, where TLC's reduction is not
reliable.

## The knob

`|Rid|` is how many steps may be in flight at once, and it is the whole
experiment:

- `|Rid| = 1` — no two steps overlap. This is the Lean machine, and the 95
  catalogue properties should hold here.
- `|Rid| = 2` — one step can Process while another is between its Process and
  its last Perform. This is where the questions are.

`Origin` and `Rest` are the two halves of an identifier. One origin with two
rests gives two objects that share an origin; a second origin is what tests
the other half of `SameOrigin`.

## What is checked, and what is not

`Handle` is a **stub** — `[effects |-> <<>>, res |-> Silent]`. The module
type-checks and runs today, and `TypeOK` is genuinely exercised: steps are
minted, phased, drained and retired.

`WheelSound`, `WheelComplete` and `UnitCoherent` still pass **vacuously** —
they quantify over `objects`, and a handler that emits no effects never writes
one. The frame is in place; the first handler is what makes them say
something.

The three invariants are expected to behave differently, and that is the
point of having all three:

| | expected to survive because |
|---|---|
| `UnitCoherent` | the **state shape**. A promise and its task are one object, so one `PutObject` moves both. No interleaving can catch them disagreeing. |
| `WheelComplete` | the **effect order**, and only that. The wheel is a second store; arm-before-write leaves noise on a crash, write-before-arm leaves silence. |
| *(a lost update)* | **nothing here** — this is the candidate for the finding. Two steps read one object, both write, one update vanishes. |

`promiseCreate` is the cheapest first handler: it writes an object *and* arms
a timeout, so it makes `WheelComplete` non-vacuous immediately and puts the
ordering question in front of the checker.

## The catalogue

`spec/02-abstract/properties.lean` already carries **95 entries** — 45 `.state`
(check at every state) and 50 `.trans` (check at every pair of consecutive
states) — plus the known gaps, the two provenance properties and the liveness
file. Those are the invariants. The three defined here are not competitors;
two of them are obligations the catalogue cannot state.

Measured against this model:

**The catalogue has no wheel.** Deadlines live *on the task record* —
`well_formed_task_acquired_iff_has_expires_at`,
`well_formed_task_pending_iff_has_retry_at`. There is no second store to drift
from them, so nothing in the 95 says an index agrees with the fields it
indexes. `WheelSound` and `WheelComplete` are therefore **not ports of
anything** — they are the price of the two-store premise, and they have to be
invented here.

**11 `.trans` properties reference `now`, and are predicted to break.** The
clearest is `consistent_task_pending_entry_arms_retry`: if a task became
pending, then `retryAt = now`. But `Process` decides at instant T and the
`PutObject` lands at T+k, and the property is evaluated where the write lands.
This is exactly what Verus freezes in `Phase::Perform { at: clamp(now, held) }`
— the *decision instant*, carried to every effect. Note that is a different
thing from `Workflow.clock`, which was the store artifact we cut: which
instant a decision was made at is protocol.

Also: `consistent_new_promise_born_clean`, `consistent_promise_settlement_stamp`,
`consistent_task_acquisition_is_atomic`, `consistent_task_lease_deadline_is_now_plus_ttl`,
`consistent_task_retry_rearm_only_when_due`, `consistent_task_wake_records_resume`,
`monotone_task_retry_rearm_advances`, `preserved_execute_only_for_live_task`,
`preserved_no_dead_dispatch`, `preserved_timedout_is_server_owned`.

**Fusing promise and task pays, and the payment is countable.** 12 properties
span both records. 8 of them are same-id, so one `PutObject` moves both and
they hold by representation:

    consistent_new_execute_matches_task_and_target
    consistent_settled_promise_has_fulfilled_task
    consistent_settled_task_promise_settled
    consistent_settlement_fulfils_task
    consistent_task_birth_couples_promise_birth
    consistent_task_fulfilment_needs_settlement
    consistent_task_iff_targeted_promise
    preserved_no_dead_dispatch

The other 4 are cross-id and stay at risk — and they are precisely the
multi-unit writes named above:

    consistent_callback_consumption_resumes_awaiter
    consistent_suspended_task_holds_rung
    consistent_suspension_registers_callback
    consistent_wake_follows_callback_consumption

**6 are unportable by our own choice** — the schedule properties, gone with
schedules.

### What "one step" means for a `.trans` property

In the Lean, `a → b` is one whole request. Here a request is `Process` plus N
`Perform`s, so a `.trans` property gets evaluated at a granularity it was
never tested at.

That is not a distortion to correct — it is the question. The walk is defined
over "every pair of consecutive states your server passes through", and a
server that does not write in one transaction passes through more of them. A
trace checker watching a real such server would see the same states. So
`.trans` is checked at every `Next`, and the reds are the answer.

### Predictions, recorded before the run

So that a confirmed prediction can be told from a surprise:

| | predicted | why |
|---|---|---|
| `consistent_settled_promise_has_fulfilled_task` | green | fusion — one write moves both |
| `WheelComplete` | green iff arm precedes write | the ordering is the whole content |
| `consistent_task_pending_entry_arms_retry` | **red** | decide at T, write at T+k |
| `preserved_no_dead_dispatch` | **red** | stale dispatch — one of the four defects the Verus port found |

That last one is the calibration: it is a known real defect, it is in the
catalogue, and this model is built to exhibit it. If it does not go red, the
model is not yet measuring what it claims to.

## Results

Checked exhaustively with TLC over the whole reachable state space of
`MCResonate.cfg` — 79 369 distinct states, complete graph depth 14, three
seconds.

| | | |
|---|---|---|
| 9 ported `.state` catalogue entries | **hold** | |
| `T_consistent_new_promise_born_clean` | **holds** | |
| `T_consistent_settlement_fulfils_task` | **holds** | |
| `UnitCoherent` | **holds** | predicted — fusion, one write moves promise and task |
| `WheelSound` | **fails** | predicted — arming before writing admits an entry for an object not yet there. Noise |
| `T_consistent_promise_settlement_stamp` | **fails** | predicted — decide at T, write at T+k |
| `T_preserved_settled_promise_record` | **fails** | a settled promise reverts to pending |
| `WheelComplete` | **fails** | **predicted to hold. It does not.** |

### The counterexample, and why the prediction was wrong

Two `promiseCreate`s for one id, and a `promiseSettle`:

```
S2,S3  two creates submitted, both in flight
S4     A decides [Arm, Put]     -- reads: object absent
S5     A arms      promise@1
S6     B decides [Arm, Put]     -- still reads absent, so B decides to create too
S7     A puts      -> pending@1
S8     A retires
S9,S10 settle submitted, decides [Put(resolved), Disarm]
S11    settle puts -> resolved
S12    B arms      promise@1     (already present; no change)
S13    settle disarms            -> wheel empty
S14    B puts      -> pending@1  -- B's stale decision lands
```

End state: a **pending promise with a deadline at 1 and nothing on the
wheel**. It will never time out, and nothing downstream recovers.

The reasoning that produced the wrong prediction was crash-local: arm before
write, and a crash between them leaves a spurious entry rather than a missing
one. That is true, and it is not enough. Ordering protects a step against
being *interrupted*; it does nothing about another step's **disarm landing
between this step's arm and its put**. Against interleaving, the effect order
is not the mechanism — a fence is.

So the module answered the question it was built for, and the answer was not
the one being guessed:

- effect order is necessary and **not sufficient**;
- what actually fails is the pair `WheelComplete` +
  `preserved_settled_promise_record`, and they fail in the same trace, from
  the same cause: a decision made against a state that has since moved;
- fusing promise and task genuinely bought `UnitCoherent` and the 8 same-id
  entries, and no interleaving touches them.

### Which checker

**TLC**, for running the experiment. Apalache could not reach depth 6 in 25
minutes — cost roughly triples per step (11s, 15s, 47s at lengths 1–3) and a
create-then-settle is about ten transitions. TLC explored the *entire*
reachable state space to depth 14 in three seconds, which is also a stronger
result than "no error up to length 6".

**Apalache, for the types.** The annotation pass is what forced `None` apart
into four different questions, forced the alphabet into a variant, and caught
a function-versus-sequence confusion in `Process`. Worth keeping both: one
checks the shape, the other runs the machine.

    apalache-mc typecheck Resonate.tla
    java -cp tla2tools.jar tlc2.TLC -config MCResonate_TLC.cfg -workers 4 MCResonate.tla

TLC needs `Variants.tla` and `Apalache.tla` (from the Apalache distribution's
`src/tla`) beside the specs. Both are ordinary TLA+ and evaluate fine.

## What Apalache forced

Three encodings the plain-TLA+ draft left vague, all of them improvements:

- **There is no `None`.** One value was standing for absence in four different
  questions — absent settlement, absent lease, absent worker, absent task.
  Absent instants are `NoTime = -1`; an absent worker is `NoPid`, ASSUMEd
  outside `Pid`; an absent task is a task in state `"none"`, which is the
  honest reading — whether a promise is targeted is part of the unit's state,
  not a hole in it.
- **The alphabet is a variant.** 20 events with 20 payloads is a tagged union.
  Written as records with a `kind` field it only worked because nothing
  checked the fields lined up.
- **`taskFence` carries a variant too** — `TaskFenceAction` in the Lean, an
  untagged union of two record types here.

One thing it did *not* force, but did clarify: the response is a field of the
step, not the last element of its pending list. The Verus executor makes it an
effect (`Effect::Respond`) so that its pending list is never empty and one
rule retires the step. Here retiring **is** answering, so the effect kind is
unnecessary and `pending` means exactly what it says — what is still unsaid.
