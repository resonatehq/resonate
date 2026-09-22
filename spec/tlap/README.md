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
`Perform` lands the document once and works the wheel one entry at a time. So
the *order* of those effects matters — arm before the write and disarm after,
which is Verus's `sched + put + ack` — and a crash between any two of them is
what makes the difference visible.

**That ordering is a property of executors that keep an index, not protocol**,
and it took a while to say so. `Abstract` used to derive the arms and disarms
itself, inside `Commit`, by diffing deadlines across old and new — which put
wheel machinery in the specification, made a handler unreadable without
chasing four operators, and produced effects `Apply` then discarded. The Lean
does none of that: a deadline is a FIELD, `internal.lean` writes
`setTask { t with retryTimeoutAt := ... }` and stops, and `02-abstract` has no arm or
disarm anywhere. The derivation now lives in `Concrete!Process`, from the
document before against the objects written — which is where Verus computes
`sched`, from `w0` and `w2`.

**There is no fence.** Two steps may read the same object and both write it;
the second wins. That is the experiment, not an oversight — put a
compare-and-swap in first and the model can only confirm that a fence is
sufficient. Left out, it has to say what goes wrong without one.

| | |
|---|---|
| `Abstract.tla` | the machine: no fence, effects applied one at a time |
| `Concrete.tla` | the same protocol under one-document-per-origin and a CAS |
| `*.cfg` | models small enough to check and large enough to interleave |

`Implemented` is the knob that keeps them comparable: a CONSTANT naming which
event constructors the alphabet offers, read by both, so the two machines
always quantify over exactly the same events. It moves as handlers land.

**Grouping the alphabet is sound for a pass and not for a failure.** The full
alphabet does not fit, so the checks run in groups — but `Implemented` narrows
*both* machines, and in principle a concrete step can need an abstract event
the group left out to cover it. A group that passes has genuinely checked the
behaviours it can reach; a group that fails has to be re-run with the
candidate covering events added before its counterexample means anything.

No measured finding has yet been an artifact of this. It is recorded because
one nearly looked like it: the `CallbackDrain` counterexample below first
turned up in a group without `Timeout`, and `Timeout` settles the same promise
by the same projection, so grouping was the obvious explanation. Restoring it
did not change the answer — `Settle` keeps `callbacks` and the drain strikes
them — and the counterexample was real, though not what it first appeared to
be either. Re-running is what tells the two apart, and guessing is what does
not.

## Running it

```
./check.sh                  # every invariant expected to hold
./check.sh WheelComplete    # one name, to watch it fail
```

TLC is the checker; `check.sh` runs Apalache first on types only if it is on
the PATH. Set `TLA_TOOLS` to your `tla2tools.jar`, or fetch it:

```
curl -sSLO https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
```

The default sweep covers the whole reachable state space — ~474 000 distinct
states, graph depth 30 — in about ten seconds.

`Variants.tla` and `Apalache.tla` sit beside the specs, vendored from the
Apalache distribution (`src/tla`, Apache-2.0). TLC needs them on the module
path and does not ship them: `Variants` is what `$event` and `$effect` are
built from, and `Apalache` supplies `SetAsFun`, which is how `Init` names an
empty function whose type is not ambiguous. Both are ordinary TLA+ — TLC
evaluates them directly — so the dependency is on the definitions, not the
tool.

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
| `WheelComplete` | the **effect order** in `Concrete`, and only that. The wheel is a second store; arm-before-write leaves noise on a crash, write-before-arm leaves silence. |
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
`well_formed_task_acquired_iff_has_lease_timeout_at`,
`well_formed_task_pending_iff_has_retry_timeout_at`. There is no second store to drift
from them, so nothing in the 95 says an index agrees with the fields it
indexes. `WheelSound` and `WheelComplete` are therefore **not ports of
anything** — they are the price of the two-store premise, and they have to be
invented here.

**11 `.trans` properties reference `now`, and are predicted to break.** The
clearest is `consistent_task_pending_entry_arms_retry`: if a task became
pending, then `retryTimeoutAt = now`. But `Process` decides at instant T and the
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

Two machines, same protocol. `Abstract` has no fence; `Concrete` chunks the
store by origin and fences every write on the etag its decision was read
under. The handlers, effects, alphabet and invariants come from `Abstract`
through the instance, so a difference in behaviour is a difference in the
executor and can be nothing else.

Checked exhaustively with TLC over the whole reachable state space of each.

| | Abstract | Concrete |
|---|---|---|
| 9 ported `.state` catalogue entries | hold | hold |
| `T_consistent_new_promise_born_clean` | holds | — |
| `T_consistent_settlement_fulfils_task` | holds | — |
| `UnitCoherent` | holds | holds |
| `WheelSound` | n/a — no wheel | **fails** |
| `WheelComplete` | n/a — no wheel | holds |
| `T_consistent_promise_settlement_stamp` | **fails** | — |
| `preserved_settled_promise_record` | **fails** unfenced | holds |
| `Spec => A!Safety` | — | **holds** |
| `SpecStable => A!Spec` | — | **holds** |

`Concrete` closes over 425 506 distinct states for the liveness form and
716 353 for the invariants, complete graph in both, a few minutes each.

So the fence is sufficient for exactly the two failures that were real, and
`WheelSound` still fails on both — arming before writing admits an entry for
an object not yet there, which is noise no fence removes and none should.

### What the abstract machine gets wrong without a fence

Two `promiseCreate`s for one id, and a `promiseSettle`:

```
S4     A decides [Arm, Put]     -- reads: object absent
S5     A arms      promise@1
S6     B decides [Arm, Put]     -- still reads absent, so B decides to create too
S7     A puts      -> pending@1
S11    settle puts -> resolved
S12    B arms      promise@1     (already present; no change)
S13    settle disarms            -> wheel empty
S14    B puts      -> pending@1  -- B's stale decision lands
```

A **pending promise with a deadline at 1 and nothing on the wheel**. It will
never time out. `WheelComplete` and `preserved_settled_promise_record` fall in
the same trace from the same cause, so they are one bug wearing two hats.

`WheelComplete` was predicted to hold. The reasoning was crash-local: arm
before write, and a crash between them leaves a spurious entry rather than a
missing one. True, and not enough. **Ordering protects a step against being
interrupted; it does nothing about another step's disarm landing between this
step's arm and its put.** Against interleaving the effect order is necessary
and not sufficient.

### The refinement found three things, none of them in the concrete model

**A hole in the specification.** The first check failed on a refused
compare-and-swap sending a step `perform -> process`. `Abstract` had no action
that went backwards, so every behaviour containing a retry was outside the
spec. That is gone now for a better reason than the patch I first applied:
with `steps` out of the abstract machine entirely, a retry is not a smaller
abstract step, it is **no abstract step at all**.

**A missing fence on the clock.** With only the etag compared: decide at
`now = 0`, `Clock` ticks, the write lands at `now = 1`. Upstairs the write is
the whole step, so the handler runs at the later instant and gives a different
answer. A decision is only valid at the instant it was made, and a store etag
says nothing about the clock. Verus takes the other road, freezing `at` at
prepare time — which works against a Lean where `now` is an argument to each
step rather than a variable running underneath it.

**A scheduling obligation, and then a limit.** Liveness failed twice. First to
slot starvation: `SubmitInternal` needs a free `Rid`, and weak fairness bites
only when an action is *continuously* enabled, so a scheduler that always
prefers the next client request starves a due timeout forever. `SF_vars` fixes
that, and it is a real requirement — an executor must not indefinitely prefer
client work to work that has come due.

Then it failed again, on something no fairness can fix: submit the due
timeout, kill it while it is still deciding, repeat. `Crash` is unconstrained,
and an executor whose work may be destroyed arbitrarily often cannot promise
that anything finishes. Hence `Crashing` as a constant — safety is claimed
with crashes, liveness without them, which is the only way liveness is ever
claimed.

### And then it found two things that were mine

The next two counterexamples were not findings about Resonate. They were
findings about `Concrete` having drifted away from the executor it claims to
model, and they are recorded here because the drift was invisible until the
refinement went looking, and because both were written up as discoveries
before they were understood.

**`Concrete` was performing several document writes per step.** It consumes
`Abstract!Handle`'s effect list verbatim -- which is the right instinct, the
protocol should not know which executor is running it -- but `Abstract!Commit`
emits a `PutObject` PER OBJECT. Upstairs that granularity carries no meaning,
because `Apply` folds the whole list at one instant. Walking the same list one
entry at a time turned a per-object *description* into a sequence of per-object
*store operations*, and `Write2` and `CommitAll` therefore became two and N
document writes.

The executor emits exactly one:

```rust
let put = seq![Effect::PutDocument { key, expect: etag_of(held), body: w2 }];
...
sched + put + ack(key, completing) + emits(w0, cs, nowc) + respond
```

`body` is the whole `Workflow`. There is no way to write half of one, and the
per-origin `etags` in this model already said as much.

**`Concrete` was recording messages outside the document.** Same cause. It
applied each `Send` effect to the `outbox` variable after the put, so objects
and messages moved at different instants. In the executor the outbox is a
FIELD of the workflow --

```rust
pub proof fn lemma_sends_outbox(w: Workflow, cs: Input, now: nat)
    ensures step(w, cs, now).outbox == fold_send(w.outbox, sends_step(w, cs, now)),
```

-- and `w2` is what `step` returns, so the messages are already inside the body
the single put writes. `Effect::Send` moves them from that record onto the
wire, `s.sent`, which this model does not have at all.

So the transactional outbox is not a requirement the refinement discovered.
The implementation already had it; the model had lost it. Both repairs restore
fidelity rather than demand anything, and the counterexamples that led to them
were reported here as protocol findings first -- at length, twice -- which is
the part worth remembering.

**What the refinement actually earned is still real, and it is a different
claim.** A refinement against a hand-written abstraction function is a fidelity
check on the lower machine: it fails when the executor model stops being the
executor. Nothing else in this repository would have caught either drift.
`C_TypeOK` passes on an unfaithful executor, so does `C_UnitCoherent`, and so
do the catalogue properties -- they are all statements about states, and both
of these were errors about *when* states change.

A consequence worth stating for anyone extending `Concrete`: an effect list
written for an atomic machine does not carry execution granularity, and
reading it as though it did is silent. `Abstract`'s list says WHAT a step
decided. It does not say in how many store operations an executor lands it,
and it cannot, because upstairs there is only ever one.

### Liveness needs crashes to stop, said without a counter

Crashes are always in the machine. What separates the two claims is a formula:

```tla
EventuallyStable == <>[][ \A r \in Rid : ~Crash(r) ]_vars

Spec       == Init /\ [][Next]_vars /\ Fairness
SpecStable == Init /\ [][Next]_vars /\ Fairness /\ EventuallyStable
```

`[][A]_vars` holds of a step when it satisfies `A` or leaves `vars` alone; a
crash does neither, so this forbids crash steps, and `<>[]` says from some
point on, forever. No bound, no counter, no constant. There WAS a `Crashing`
constant, and switching it off was how liveness got claimed — which says
something about an executor that never fails, and that is not an executor
anyone has.

Two things about the form. It only type-checks in the SPECIFICATION: as a
property hypothesis, `EventuallyStable => A!Spec`, TLC refuses —
*temporal formulas containing actions must be of forms `<>[]A` or `[]<>A`*,
and an implication with one as antecedent is not among them. And it is
stronger than "not always": `<>[]` means crashes stop entirely.

The weaker reading — infinitely many crashes, never always the attempt that
matters — is what fairness would express, and it CANNOT be written here.
Three attempts, all refuted:

| fairness added | verdict | why |
|---|---|---|
| `SF` on `Perform(r)` | fails | **vacuous** — the adversary crashes every step while still in `"process"`, so `Perform` is never enabled |
| `SF` on `Process(r)` and `Perform(r)` | fails | **slot recycling** — below |
| anything else over these actions | — | same shape |

At the lasso of the second, every step is at rid `q2`:

```
S9   q2 settle    "perform", 2 pending   <- Process fired, SF discharged
S10  crashed
S12  q2 Timeout   "perform", 2 pending   <- Process fired
S13  crashed
S15  q2 settle    "perform", 0 pending   <- a NoOp outcome
     back to S7                          <- Perform retires it, SF discharged
```

`SF_vars(Perform(q2))` obliges the *rid* to perform infinitely often, and it
does — using a settle of an already-settled promise, whose handler returns
`NoOp`. It reaches `"perform"` with nothing pending and retires. Every step
that would actually write is crashed.

**Fairness names a slot, not an attempt.** No formula quantified over `Rid`
can say "this attempt completes", because attempts have no names and a rid is
recycled.

Note which fairness does NOT have this hole: `SF_vars(SubmitInternal(ev))`
names an abstract EVENT — a specific object and deadline — so no throwaway
request can discharge it on that event's behalf. That one is load-bearing and
sound. The difference is naming the work rather than the worker.

### A harness bug worth remembering

The first two attempts at the table above were run as

    /\ (StrongSteps => \A r \in Rid : SF_vars(..))

and measured nothing. TLC recognises fairness only in a restricted syntactic
form -- WF/SF terms and quantifications over them -- and an implication is not
in it, so the conjunct was never assumed. The tell was a self-contradicting
counterexample: `Perform(q2)` enabled twice in the cycle and never taken,
which breaks the very condition the run was supposed to assume. A behaviour
that violates its own assumption is a broken harness, not a finding.
Switchable fairness belongs in a second SPECIFICATION, not behind a constant
guard.

### What `Crash` actually contributes

If crashes were never modelled the theorem would be one line:

```tla
THEOREM Spec => A!Spec
```

no constant, no guard, no `A!Safety` variant. Measured with
`Crashing = FALSE`, every hazard finding survives anyway:

| | |
|---|---|
| `C_WheelSound` | still fails |
| `C_WheelComplete`, unfenced | still fails |
| `Spec => A!Safety`, unfenced | still fails |

Because interleaving alone already produces partial application: A arms, B
runs, A puts. A crash makes a partial application PERMANENT; it does not make
it possible. So the arm/put window, the lost update, the wheel going silent
and the fence's necessity are all interleaving results, and none of them
needed a crash to show up.

What `Crash` does buy is two things that cannot be said without it:

- `Crashing => (Spec => A!Safety)` — safety survives a step dying mid-list,
  which is a real claim about an executor and the reason `effects` is a
  sequence rather than a set;
- the negative liveness result, which is only interesting because crashes are
  in the model at all.

Worth knowing when deciding what to keep: the crash is cheap to model, buys
one genuine safety claim, and costs the one-line theorem.

### A door the Lean is missing

`promiseRegisterCallback` in `spec/02-abstract/external.lean` checks four
things — awaited is not awaiter, both exist, the awaiter is targeted, the
awaited is external — and says nothing about where either lives. So a client
may register a waiter whose two ends are in different origins, and the
specification admits it. TLC found it in three steps:

```
create o1/a  external
create o2/a  targeted
promiseRegisterCallback(awaited |-> o1/a, awaiter |-> o2/a)

  o1/a.callbacks = { o2/a }
```

That is a gap rather than a choice, and it prices more than a status code: an
await spanning origins is a write whose two ends can be in different
partitions, which is the difference between an implementation that may shard
and one that may not. The unit of serialisation is open: per-tree keeps
"most" operations local; this door is what would make it all of them.

Added here to both machines — `promiseRegisterCallback` and `taskSuspend`,
which registers the same kind of waiter on every promise in its action list.
With it, `SameOrigin` holds over two origins (3 025 distinct states, complete
graph) where it was refuted in a second before.

And it changes what `Concrete` is. Without the door, the one-document-per-
origin executor was merely SOUND: it declined cross-origin registrations by
reading a document that did not contain the awaiter, which is a stutter and
therefore a legal refinement. With the door it is COMPLETE — it serves
everything the protocol admits.

**Two handlers had no origin door at all**, and neither is an awaiter.
`taskFence` required only that its target be a DIFFERENT ID -- `if
req.action.targetId == req.id then 400` -- which says nothing about origins;
a parent fencing a child in the same tree is a different id at the same
origin, and is the ordinary case. `taskHeartbeat` names a set of task refs,
and P3 says outright they "need not share a partition". Both are
multi-object operations whose objects MAY span origins, not operations that
necessarily do, so whether they got the callback's door was a protocol
question rather than a modelling one.

`taskFence` has since been answered: the Lean spec refuses a cross-origin
target with a `400`, decided lexically before any read, so a backend can
answer it without asking its store whether the id exists -- which is what
makes the refusal implementable where "different origin" and "no such
promise" are otherwise indistinguishable. `taskHeartbeat` is still open,
and still contradicts P3 if it closes: `CheckPartitionable` already refuses
to partition a heartbeat that spans origins, on the grounds that belonging
to two partitions at once is not representable.

The doors here in `Abstract.tla` are still the two awaiter ones. Carrying
the fence door across, so TLC checks it too, is the obvious next step.

### Which checker

**TLC**, for running anything. Apalache could not reach depth 6 in 25 minutes —
cost roughly triples per step — and a create-then-settle is about ten
transitions.

**Apalache, for the types.** That pass is what split `None` into four different
questions, forced the alphabet into a variant, and caught a function-versus-
sequence confusion in `Process`. One checks the shape, the other runs the
machine.

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
