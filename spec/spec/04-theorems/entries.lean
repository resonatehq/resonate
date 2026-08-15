import «04-theorems».«handlers»

/-!  # Catalogue entries that are inductive

One section per entry. Each is a `Hereditary` instance — eight facts
about a `PromiseObject`, no monad and no handler in sight — and then
two lines: it holds at `init`, and every step preserves it.

That is the whole cost of a per-promise property now. `induction.lean`
built the reduction and `handlers.lean` paid for it once, generically
in `q`; what is left here is arithmetic on record fields.

## What "inductive" buys

The sweep checks 1 464 scripts. These theorems check every script, of
any length, under both read disciplines, at every instant. For an entry
that appears here, a `Legal` violation in an implementation trace is
unambiguously an implementation bug: the property cannot fail on the
model.

## What is NOT here, and why

The catalogue's remaining promise entries fail at a NAMED obligation,
which is more informative than a failed tactic.

Three fail at the two births, because the birth takes the client's tags
verbatim: `timer_not_targeted`, `target_is_nonempty` and
`delay_before_deadline` are true of every reachable state but false of
an arbitrary birth. They hold because `promise.create`, `task.create`
and `schedule.create` all 400 first — a HANDLER fact, not a promise
fact, and so outside this tier.

Two fail at `addCallback`/`addListener` for the same reason:
`obligations_require_external` and `awaiter_is_not_self` are enforced by
the 422 guards in `promise.registerCallback`, not by the ledger
operation.

Two — `callbacks_unique` and `listeners_unique` — are per-promise and
true at every obligation, but need `List.eraseDups` lemmas that core
does not ship.

And three (`settled_at_lte_timeout_at`,
`deadline_verdict_matches_timer_tag`,
`deadline_settlement_has_no_value`) need one more fact at the settle:
that a promise read as still pending is not yet past its deadline. That
is Fact P, and it is a property of `readPromise` rather than of a
`PromiseObject` — see `writesGood_afterReadPromise`, which is where it
would have to enter. -/

namespace Abstraction
namespace Induction

open AbstractModel

/-! ## `created_at ≤ timeout_at`

The first one through, and the simplest: no transformation touches
either field. That immutability IS the reason the property is
inductive. -/

def qCreatedLeTimeout (p : PromiseObject) : Bool := p.createdAt ≤ p.timeoutAt

theorem hereditary_createdLeTimeout : Hereditary qCreatedLeTimeout where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simpa [qCreatedLeTimeout] using h
    · simpa [qCreatedLeTimeout] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qCreatedLeTimeout] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qCreatedLeTimeout] using h
  settle p st v t _ h := by simpa [qCreatedLeTimeout] using h
  dropListener p a h := by simpa [qCreatedLeTimeout] using h
  dropCallback p a h := by simpa [qCreatedLeTimeout] using h
  live id param tags timeoutAt createdAt h := by simp [qCreatedLeTimeout]; omega
  dead id st param tags timeoutAt _ := by simp [qCreatedLeTimeout]

/-! ## A pending promise is born before its deadline

Strictly, where the previous one is non-strict. The difference is the
born-dead branch, where the two coincide — and that promise is not
pending, so this says nothing about it. -/

def qPendingBeforeDeadline (p : PromiseObject) : Bool :=
  p.state != .pending || p.createdAt < p.timeoutAt

theorem hereditary_pendingBeforeDeadline : Hereditary qPendingBeforeDeadline where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qPendingBeforeDeadline]
    · simpa [qPendingBeforeDeadline] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qPendingBeforeDeadline] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qPendingBeforeDeadline] using h
  settle p st v t hst _ := by
    cases st <;> simp_all [qPendingBeforeDeadline, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qPendingBeforeDeadline] using h
  dropCallback p a h := by simpa [qPendingBeforeDeadline] using h
  live id param tags timeoutAt createdAt h := by simp [qPendingBeforeDeadline]; omega
  dead id st param tags timeoutAt hst := by
    cases st <;> simp_all [qPendingBeforeDeadline]

/-! ## Settled exactly when stamped

The biconditional, which is why it needs every obligation to agree:
the two births disagree with each other (one pending and unstamped, one
settled and stamped) and both have to satisfy it. -/

def qSettledIffStamped (p : PromiseObject) : Bool :=
  (p.state != .pending) == p.settledAt.isSome

theorem hereditary_settledIffStamped : Hereditary qSettledIffStamped where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qSettledIffStamped]
    · simpa [qSettledIffStamped] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qSettledIffStamped] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qSettledIffStamped] using h
  settle p st v t hst _ := by
    cases st <;> simp_all [qSettledIffStamped, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qSettledIffStamped] using h
  dropCallback p a h := by simpa [qSettledIffStamped] using h
  live id param tags timeoutAt createdAt h := by simp [qSettledIffStamped]
  dead id st param tags timeoutAt hst := by cases st <;> simp_all [qSettledIffStamped]

/-! ## A pending promise carries no verdict

The value field is written only by a settle, so a promise still pending
has the empty value it was born with. -/

def qPendingHasNoValue (p : PromiseObject) : Bool :=
  p.state != .pending || (p.value.data.isNone && p.value.headers.isEmpty)

theorem hereditary_pendingHasNoValue : Hereditary qPendingHasNoValue where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qPendingHasNoValue]
    · simpa [qPendingHasNoValue] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qPendingHasNoValue] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qPendingHasNoValue] using h
  settle p st v t hst _ := by
    cases st <;> simp_all [qPendingHasNoValue, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qPendingHasNoValue] using h
  dropCallback p a h := by simpa [qPendingHasNoValue] using h
  live id param tags timeoutAt createdAt h := by simp [qPendingHasNoValue]
  dead id st param tags timeoutAt hst := by cases st <;> simp_all [qPendingHasNoValue]

/-! ## `rejectedTimedout` is server-owned

The one that turns a `settable` check into a state invariant. A client
cannot forge this verdict, so a row carrying it was written by the
deadline path and is stamped at the deadline. Note where it is
discharged: at `settle`, from `st.settable`, which is exactly the
precheck `promise.settle` and `task.fulfill` perform. Delete either
precheck and this obligation is the thing that fails. -/

def qTimedoutIsServerOwned (p : PromiseObject) : Bool :=
  p.state != .rejectedTimedout || p.settledAt == some p.timeoutAt

theorem hereditary_timedoutIsServerOwned : Hereditary qTimedoutIsServerOwned where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qTimedoutIsServerOwned]
    · simpa [qTimedoutIsServerOwned] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qTimedoutIsServerOwned] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qTimedoutIsServerOwned] using h
  settle p st v t hst _ := by
    cases st <;> simp_all [qTimedoutIsServerOwned, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qTimedoutIsServerOwned] using h
  dropCallback p a h := by simpa [qTimedoutIsServerOwned] using h
  live id param tags timeoutAt createdAt h := by simp [qTimedoutIsServerOwned]
  dead id st param tags timeoutAt hst := by
    cases st <;> simp_all [qTimedoutIsServerOwned]

/-! ## The catalogue statements

Each entry is `PerPromise` of its predicate by definition, so `init`
and the step law follow with nothing in between. `n'` is free of `now`
in every one of these — none of them reads the clock — which is why the
clock obligation does not appear. -/

section Entries

open Properties

theorem created_at_lte_timeout_at_init (now : Nat) :
    well_formed_promise_created_at_lte_timeout_at now ServerState.init = true := rfl

theorem created_at_lte_timeout_at_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_created_at_lte_timeout_at now s = true →
    well_formed_promise_created_at_lte_timeout_at n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_createdLeTimeout mat st now s

theorem pending_created_before_deadline_init (now : Nat) :
    well_formed_promise_pending_created_before_deadline now ServerState.init = true := rfl

theorem pending_created_before_deadline_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_pending_created_before_deadline now s = true →
    well_formed_promise_pending_created_before_deadline n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_pendingBeforeDeadline mat st now s

theorem settled_at_iff_not_pending_init (now : Nat) :
    well_formed_promise_settled_at_iff_not_pending now ServerState.init = true := rfl

theorem settled_at_iff_not_pending_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_settled_at_iff_not_pending now s = true →
    well_formed_promise_settled_at_iff_not_pending n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_settledIffStamped mat st now s

theorem pending_has_no_value_init (now : Nat) :
    well_formed_promise_pending_has_no_value now ServerState.init = true := rfl

theorem pending_has_no_value_step (mat : Bool) (st : Step) (now n' : Nat) (s : ServerState) :
    well_formed_promise_pending_has_no_value now s = true →
    well_formed_promise_pending_has_no_value n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_pendingHasNoValue mat st now s

theorem timedout_is_server_owned_init (now : Nat) :
    well_formed_promise_timedout_is_server_owned now ServerState.init = true := rfl

theorem timedout_is_server_owned_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_timedout_is_server_owned now s = true →
    well_formed_promise_timedout_is_server_owned n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_timedoutIsServerOwned mat st now s

end Entries

end Induction
end Abstraction
