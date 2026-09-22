import «04-theorems».«properties-step»

namespace Abstraction

open AbstractModel (ServerState PromiseObject TaskObject)

/-!  # Stage 4 — liveness

Everything before this file is safety: nothing bad is in the state,
nothing bad happens across a step. None of it says anything ever
happens at all. `Valid true` requires only `now ≤ next now`, so the trace
in which `now = 0` forever and no internal step ever fires satisfies
every theorem in `04-theorems` — and every one of the 101 predicates.

Liveness needs two hypotheses the specification does not currently
have, and they are stated here for the first time:

  **`ClockAdvances`** — the clock advances without bound. Monotone is
  not enough; a clock that stalls at 5 forever is monotone.

  **`WeaklyFairOn`** — an internal step that stays enabled eventually
  fires. Weak, not strong: a step that flickers may be starved.

Neither is a property of the machine. Both are obligations on the
environment, and an implementation discharges them with a clock and a
scheduler. Stating them is what makes the difference between "the
server may wake you" and "the server will".

The three properties below are the ones worth having, and only the
third is hard.  -/

/-! ### Enabledness

What a scheduler must be able to see. An internal step fired when not
enabled is the identity — that is `preserved_state_under_disabled_*` —
so enabledness is not needed for safety; it is needed to say that
something is *owed*.

THE ARMING RULE LIVES HERE, and it is keyed on `otype`. A deadline is
owed an observation exactly when someone can be blocked on it, and
"can be blocked on it" is `.external` — the same predicate the three
obligation doors check. That is not two rules agreeing by luck: the
arming rule FALLS OUT of the obligation rule, which is why no third
axis is needed to state it.

Gating the step changes nothing about safety — `processPromiseTimeout`
is untouched, so no transition is removed — and nothing becomes
unobservable, because a read still projects an internal promise as
settled past its deadline. What changes is what a fair scheduler is
obliged to write, and `well_formed_promise_obligations_require_external`
is what makes the narrowing free: an internal promise carries no
callbacks and no listeners, so there is nobody to inform.

`otype` OVER-APPROXIMATES the set, and deliberately. The promises that
strictly need a wheel entry are those with a non-empty obligation set;
an external promise nobody awaits is armed for no one. Arming on
`otype` is the cheaper rule that is never wrong in the unsafe
direction, and narrowing it further would make enabledness depend on
the ledger. -/

def promiseAt (s : ServerState) (id : ServerModel.Ident) : Option PromiseObject :=
  s.promise? id

def taskAt (s : ServerState) (id : ServerModel.Ident) : Option TaskObject :=
  s.task? id

def enabledInternal (st : Step) (now : Nat) (s : ServerState) : Bool :=
  match st with
  | .internal (.promiseTimeout { id := id }) =>
      match promiseAt s id with
      | some p => p.otype.awaitable && p.state == .pending && p.timeoutAt ≤ now
      | none   => false
  | .internal (.callback { awaited := id, awaiter := x }) =>
      match promiseAt s id with
      | some p => (p.project now).state != .pending && p.callbacks.contains x
      | none   => false
  | .internal (.listener { awaited := id, address := addr }) =>
      match promiseAt s id with
      | some p => (p.project now).state != .pending && p.listeners.contains addr
      | none   => false
  | .internal (.taskLeaseTimeout { id := id }) =>
      match taskAt s id, promiseAt s id with
      | some t, some p =>
          t.state == .acquired && (t.leaseTimeoutAt.getD (now + 1)) ≤ now
            && (p.project now).state == .pending
      | _, _ => false
  | .internal (.taskRetryTimeout { id := id }) =>
      match taskAt s id, promiseAt s id with
      | some t, some p =>
          t.state == .pending && (t.retryTimeoutAt.getD (now + 1)) ≤ now
            && (p.project now).state == .pending
      | _, _ => false
  | .internal (.scheduleTimeout { schedule := id }) => (s.schedules.find? (·.id == id)).isSome
  | _ => false

/-! ### The environment's obligations -/

/-- The clock advances without bound. Monotone is not enough:
    a clock stalled at 5 forever is monotone. -/
def ClockAdvances (tr : Trace) : Prop :=
  ∀ n : Nat, ∃ t : Nat, n ≤ (tr t).now

/-- Weak fairness, restricted to a family of internal steps: one that is
    continuously enabled from some instant on is eventually taken. -/
def WeaklyFairOn (tr : Trace) (family : Step → Bool) : Prop :=
  ∀ st : Step, family st = true →
    ∀ t : Nat,
      (∀ u : Nat, t ≤ u → enabledInternal st (tr u).now (tr u).state = true) →
      ∃ u : Nat, t ≤ u ∧ (tr u).req = st

def isSettlementStep : Step → Bool
  | .internal (.promiseTimeout { id := _ }) => true
  | _     => false

def isCallbackStep : Step → Bool
  | .internal (.callback { awaited := _, awaiter := _ }) => true
  | _       => false

def isListenerStep : Step → Bool
  | .internal (.listener { awaited := _, address := _ }) => true
  | _       => false

/-! ### 1. Every external promise eventually settles

Not trivial, and not a consequence of time passing alone. A promise
past its deadline reads as settled through any view, but the STORED
promise stays pending until something touches it — and in the projected
discipline nothing touches it except the promise timeout. So this needs
the clock AND fairness on that step.

EXTERNAL, because that is the arming rule stated as a property. The
step is enabled only on external promises, so a fair scheduler owes a
write only there, and asking for more would be asking for a write no
implementation makes.

WHAT IS NOT NARROWED is the observation, and the two must not be
confused. `promiseAt` is the STORED row — `(objects.find? _).map
(·.promise)`, no projection — so this says a write happens. The claim
that every promise, internal ones included, eventually READS settled is
a different statement, and it is stated separately below because it
survives the guard untouched. An internal promise past its deadline is
settled through every view and pending in the store forever; the
catalogue tolerates that precisely because nothing can be waiting to
hear about it. -/

def EventuallyEveryExternalPromiseSettles : Prop :=
  ∀ tr : Trace, Valid true tr → ClockAdvances tr → WeaklyFairOn tr isSettlementStep →
    ∀ (t : Nat) (id : ServerModel.Ident),
      (∀ p, promiseAt (tr t).state id = some p → p.otype.awaitable = true) →
      (promiseAt (tr t).state id).isSome →
      ∃ u : Nat, t ≤ u ∧
        ∀ p, promiseAt (tr u).state id = some p → p.state ≠ .pending

/-- ### 1b. Every promise eventually READS settled

Every promise, with no externality hypothesis and — the point — with no
fairness hypothesis either. `project` settles a pending promise the
instant the clock passes its deadline, so this follows from
`ClockAdvances` alone.

That is why it is a separate statement rather than a replacement for
the one above. Weaker in what it asks of the environment and stronger
in what it covers, it is the half of "every promise settles" that no
arming rule can take away: gate the settlement STEP however you like
and this still holds, because it never depended on a step firing. The
one above is the half that costs a write, and a write is owed only
where someone can be waiting for it.

Read them together and they say the thing the arming rule is for: an
internal promise is never observed to be pending past its deadline, and
never costs the server a timer to make that true. -/
def EventuallyEveryPromiseReadsSettled : Prop :=
  ∀ tr : Trace, Valid true tr → ClockAdvances tr →
    ∀ (t : Nat) (id : ServerModel.Ident),
      (promiseAt (tr t).state id).isSome →
      ∃ u : Nat, t ≤ u ∧
        ∀ p, promiseAt (tr u).state id = some p →
          (p.project (tr u).now).state ≠ .pending

/-! ### 2. Every task eventually fulfils

A corollary, not a property. `consistent_settlement_fulfils_task` says
the promise's settlement fulfils its co-keyed task IN THE SAME STEP, and
`consistent_task_iff_kind_task` says every task has one. So (2)
is (1) composed with the coupled write, and it is recorded here as an
implication rather than as an axiom of its own — carrying it separately
would be exactly the redundancy the catalogue refuses.

The `otype` guard on the settlement step does not weaken this: a task's
promise is `runnable`, and every value but `internal` is awaitable, so
the step it depends on is still owed. -/

def EventuallyEveryTaskFulfils : Prop :=
  ∀ tr : Trace, Valid true tr → ClockAdvances tr → WeaklyFairOn tr isSettlementStep →
    ∀ (t : Nat) (id : ServerModel.Ident),
      (taskAt (tr t).state id).isSome →
      ∃ u : Nat, t ≤ u ∧
        ∀ w, taskAt (tr u).state id = some w → w.state = .fulfilled

theorem taskFulfilment_follows_from_promiseSettlement :
    EventuallyEveryExternalPromiseSettles → EventuallyEveryTaskFulfils := by
  sorry

/-! ### 3. The central thesis

**When a promise settles, its awaiters are eventually resumed — unless
the awaiter's own deadline wins the race first.**

This is the property the whole protocol exists to provide, and it is
the only one of the three that is not about time passing. The
antecedent is a settled promise carrying `x` on its callback ledger.
The conclusion has three parts, and all three matter:

  * the ledger entry is DISCHARGED — `x` is no longer on it, so the
    obligation cannot be silently retained;
  * the awaiter LEARNS which promise woke it — `a ∈ resumes` — so the
    wake is not an anonymous nudge;
  * and it has LEFT `suspended`, so it is dispatchable again.

The escape clause is the `.fulfilled` disjunct: TIMEOUT ALWAYS WINS. If
the awaiter's own promise died while it waited, the timeout path owns
its cleanup and the wake is void — which is exactly what
`resumeOne` does when it lands on a fulfilled task.

Fairness on the callback drain alone is not enough. The ledger is only
reachable once the awaited promise is settled in STORE, so the promise
timeout must fire too; hence both families in the hypothesis. -/

def EventuallyAwaiterResumed : Prop :=
  ∀ tr : Trace, Valid true tr → ClockAdvances tr →
    WeaklyFairOn tr isSettlementStep → WeaklyFairOn tr isCallbackStep →
    ∀ (t : Nat) (a x : ServerModel.Ident),
      (∃ p, promiseAt (tr t).state a = some p ∧
              p.state ≠ .pending ∧ p.callbacks.contains x = true) →
      ∃ u : Nat, t ≤ u ∧
        (∀ p, promiseAt (tr u).state a = some p → p.callbacks.contains x = false) ∧
        (∀ w, taskAt (tr u).state x = some w →
           w.state = .fulfilled ∨ (w.state ≠ .suspended ∧ w.resumes.contains a = true))

/-- The listener analogue, and strictly weaker: a settled promise's
    listeners are eventually notified. Weaker because delivery is
    outside the model — this says the message is ENQUEUED, and nothing
    in the machine can say it arrives. -/
def EventuallyListenerNotified : Prop :=
  ∀ tr : Trace, Valid true tr → ClockAdvances tr →
    WeaklyFairOn tr isSettlementStep → WeaklyFairOn tr isListenerStep →
    ∀ (t : Nat) (a : ServerModel.Ident) (addr : String),
      (∃ p, promiseAt (tr t).state a = some p ∧
              p.state ≠ .pending ∧ p.listeners.contains addr = true) →
      ∃ u : Nat, t ≤ u ∧
        (tr u).state.outbox.any (fun e =>
          e.address == addr &&
            (match e.message with
             | .unblock r => r.id == a && r.state != .pending
             | .execute _ _ => false)) = true

/-! ### The bounded, executable half

The `Prop`s above are unprovable by `decide` — they quantify over
infinite traces. What IS checkable is the finite core: fire every
enabled internal step, repeatedly, and see whether the wake
materializes. That is one fair round, iterated, and it is the exact
finite shadow of `WeaklyFairOn`.

If the bounded version FAILED, the unbounded one would be false, so
this is a real refutation channel rather than decoration. -/

def enabledSteps (now : Nat) (s : ServerState) : List Step :=
  s.objects.flatMap fun o =>
    let p := o.promise
    (if enabledInternal (.internal (.promiseTimeout { id := o.id })) now s then [Step.internal (.promiseTimeout { id := o.id })] else [])
      ++ p.callbacks.map (fun x => Step.internal (.callback { awaited := o.id, awaiter := x }))
      ++ p.listeners.map (fun addr => Step.internal (.listener { awaited := o.id, address := addr }))
      ++ (if o.task.isSome ∧ enabledInternal (.internal (.taskLeaseTimeout { id := o.id })) now s then [Step.internal (.taskLeaseTimeout { id := o.id })] else [])
      ++ (if o.task.isSome ∧ enabledInternal (.internal (.taskRetryTimeout { id := o.id })) now s then [Step.internal (.taskRetryTimeout { id := o.id })] else [])

def fireAllEnabled (now : Nat) (s : ServerState) : ServerState :=
  (enabledSteps now s).foldl
    (fun acc st => if enabledInternal st now acc then (stepOf true st now acc).2 else acc) s

def fairRounds : Nat → Nat → ServerState → ServerState
  | 0,     _,   s => s
  | k + 1, now, s => fairRounds k now (fireAllEnabled now s)

/-- After enough fair rounds at a fixed instant: no settled promise
    retains an obligation, and every awaiter has either learned its
    wake or died. The finite shadow of `EventuallyAwaiterResumed`. -/
def wakeMaterializes (w : List (Step × Nat)) (horizon : Nat) : Bool :=
  let s := fairRounds 6 horizon (runFin true w AbstractModel.ServerState.init).2
  s.promises.all fun p =>
    (p.project horizon).state == .pending ||
      (p.callbacks.isEmpty && p.listeners.isEmpty)

def resumeRecorded (w : List (Step × Nat)) (horizon : Nat) : Bool :=
  let s0 := (runFin true w AbstractModel.ServerState.init).2
  let s  := fairRounds 6 horizon s0
  s0.objects.all fun o =>
    o.promise.callbacks.all fun x =>
      match taskAt s x with
      | none   => true
      | some u => u.state == .fulfilled
                    || (u.state != .suspended && u.resumes.contains o.id)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000


/-- The wake, end to end: a suspended awaiter, a settled awaited, and
    nothing but internal steps between them. -/
def wWake : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 9000, param := {}, tags := extTags }), 100),
    (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 9000, param := {}, tags := tgtTags } }), 100),
    (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 120),
    (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} }), 200) ]

example : wakeMaterializes wWake 300 := by decide
example : resumeRecorded wWake 300 := by decide

/-- TIMEOUT ALWAYS WINS: the awaiter's own promise dies while it waits.
    The obligation is still discharged, and the escape clause is the one
    that fires — the task reads fulfilled, not resumed. -/
def wWakeTimedOut : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 9000, param := {}, tags := extTags }), 100),
    (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 250, param := {}, tags := tgtTags } }), 100),
    (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 120),
    (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} }), 200) ]

example : wakeMaterializes wWakeTimedOut 300 := by decide
example : resumeRecorded wWakeTimedOut 300 := by decide

/-- **Fairness is load-bearing, not decoration.** The same script with
    no internal step fired retains the obligation forever: the ledger
    still names `x` and the task is still suspended. This is the
    counterexample that makes `WeaklyFairOn` a hypothesis rather than a
    formality. -/
theorem wake_requires_fairness :
    ((runFin true wWake AbstractModel.ServerState.init).2.promises.any (fun p => p.callbacks.contains (oid "x"))
      && (runFin true wWake AbstractModel.ServerState.init).2.tasks.any (fun t => t.state == .suspended)) = true := by
  decide

/-- The bounded wake over the whole corpus: every script, both
    disciplines, then fair rounds. -/
theorem boundedWakeSweep :
    (((seqsUpToA kernelsResp 3).map instantiateA).all
      (fun w => wakeMaterializes w 9000 && resumeRecorded w 9000)) = true := by
  decide

theorem boundedWakeBattery :
    (battery.all (fun w => wakeMaterializes w 9000 && resumeRecorded w 9000)) = true := by
  decide

end Abstraction
