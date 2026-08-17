# The TLA+ machine

`spec/02-abstract` makes one request one transition. That is the right
altitude for saying what a request *means*, and the wrong one for asking what
two concurrent requests do to each other — at that altitude they cannot do
anything to each other.

`Resonate.tla` keeps the Lean alphabet and takes the atomicity out. It splits
every event into the phases the Verus executor uses
(`resonate-verus/src/concrete_spec/executor.rs`), so that the gap between a
step's decision and its effects is a place other steps can get into.

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

`Handle` is a **stub** — the handler that does nothing. The module type-checks
and runs today, and `TypeOK` is genuinely exercised: steps are minted, phased,
drained and retired, and the outbox keeps one entry per key.

`WheelSound`, `WheelComplete` and `UnitCoherent` currently pass **vacuously**.
They quantify over `objects`, and a no-op handler never writes one. They are
here so that the day the handlers land, the frame is already in place and the
first run says something.

Replacing the stub body with `Gen(3)` gives the adversarial handler: every
outcome the type admits. Expect it to refute nearly everything — that is the
measurement, and the point of it is to separate what the *machine* maintains
from what only the *handlers* do.

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
