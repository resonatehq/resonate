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
