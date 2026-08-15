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

And two mention the clock (`created_at_lte_now`, `settled_at_lte_now`),
which this tier cannot see: `Hereditary` is a predicate on a
`PromiseObject`, and those are predicates on a promise AND an instant.
They need the `now`-indexed variant, which is also what
`stateHolds_clock` is about.

## The two facts that had to be added

The last three entries here did not fit at first, and what they needed
is worth naming.

`NotDue` — a promise that reads pending is not yet past its deadline —
is Fact P, and it is a property of the READ rather than of a promise,
so it travels with the read combinator and is consumed at the `settle`
obligation. Without it, nothing says the stamp a settle writes is below
the deadline.

And `Hereditary.dead` had to name the born-dead verdict exactly
(`resolved` for a timer, `rejectedTimedout` otherwise) rather than
merely "not pending". Discharging that at `task.create` — which inlines
its own birth with an unconditional `rejectedTimedout` — is what turned
the standing claim that `task.create` faces no timers into a proof. -/

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
  settle p st v t _ _ h := by simpa [qCreatedLeTimeout] using h
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
  settle p st v t hst _ _ := by
    cases st <;> simp_all [qPendingBeforeDeadline, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qPendingBeforeDeadline] using h
  dropCallback p a h := by simpa [qPendingBeforeDeadline] using h
  live id param tags timeoutAt createdAt h := by simp [qPendingBeforeDeadline]; omega
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qPendingBeforeDeadline]

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
  settle p st v t hst _ _ := by
    cases st <;> simp_all [qSettledIffStamped, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qSettledIffStamped] using h
  dropCallback p a h := by simpa [qSettledIffStamped] using h
  live id param tags timeoutAt createdAt h := by simp [qSettledIffStamped]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qSettledIffStamped]

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
  settle p st v t hst _ _ := by
    cases st <;> simp_all [qPendingHasNoValue, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qPendingHasNoValue] using h
  dropCallback p a h := by simpa [qPendingHasNoValue] using h
  live id param tags timeoutAt createdAt h := by simp [qPendingHasNoValue]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qPendingHasNoValue]

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
  settle p st v t hst _ _ := by
    cases st <;> simp_all [qTimedoutIsServerOwned, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qTimedoutIsServerOwned] using h
  dropCallback p a h := by simpa [qTimedoutIsServerOwned] using h
  live id param tags timeoutAt createdAt h := by simp [qTimedoutIsServerOwned]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qTimedoutIsServerOwned]


/-! ## The settlement stamp is never past the deadline

The first entry that needs Fact P. A settle writes `some now`, and
nothing about a `PromiseObject` says `now` is below its deadline —
what says so is that the handler read the promise as PENDING, and a
promise reads pending only while it is not yet due. That fact arrives
as `NotDue`, carried by the read combinator, and is consumed at the
`settle` obligation. -/

def qSettledAtLeTimeout (p : PromiseObject) : Bool :=
  match p.settledAt with
  | none => true
  | some x => x ≤ p.timeoutAt

theorem hereditary_settledAtLeTimeout : Hereditary qSettledAtLeTimeout where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qSettledAtLeTimeout]
    · simpa [qSettledAtLeTimeout] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qSettledAtLeTimeout] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qSettledAtLeTimeout] using h
  settle p st v t _ hdue _ := by simp [qSettledAtLeTimeout]; omega
  dropListener p a h := by simpa [qSettledAtLeTimeout] using h
  dropCallback p a h := by simpa [qSettledAtLeTimeout] using h
  live id param tags timeoutAt createdAt h := by simp [qSettledAtLeTimeout]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qSettledAtLeTimeout]

/-! ## A promise stamped AT its deadline carries the deadline's verdict

The converse direction to `timedout_is_server_owned`, and the one that
pins the timer split: `resolved` for a timer, `rejectedTimedout`
otherwise. It needs BOTH new facts. Fact P at the settle — a client
settling a pending promise stamps strictly below the deadline, so it
can never produce this shape — and the strengthened birth obligation,
because the born-dead promise IS stamped at its deadline and has to
carry the matching verdict.

That birth obligation is what turned `task.create` faces no timers into
a proof: it inlines its own birth with an unconditional
`rejectedTimedout`, which is only correct because the handler requires
`resonate:target` and refuses `timerTargeted`. See
`writesGood_taskCreate`. -/

def qDeadlineVerdict (p : PromiseObject) : Bool :=
  p.settledAt != some p.timeoutAt
    || p.state == (if p.tags.isTimer then .resolved else .rejectedTimedout)

theorem hereditary_deadlineVerdict : Hereditary qDeadlineVerdict where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> rename_i ht <;>
        simp_all [qDeadlineVerdict, PromiseObject.isTimer]
    · simpa [qDeadlineVerdict] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qDeadlineVerdict] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qDeadlineVerdict] using h
  settle p st v t _ hdue _ := by simp [qDeadlineVerdict]; omega
  dropListener p a h := by simpa [qDeadlineVerdict] using h
  dropCallback p a h := by simpa [qDeadlineVerdict] using h
  live id param tags timeoutAt createdAt h := by simp [qDeadlineVerdict]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp_all [qDeadlineVerdict]

/-! ## A deadline settlement carries no value

The one entry here that is NOT inductive on its own, and the reason is
worth stating. `project` settles a pending promise and leaves `value`
alone, so the new row is stamped at its deadline with whatever value it
already had — and nothing in the entry itself says a PENDING promise
has no value. The catalogue knows that separately
(`pending_has_no_value`), which is exactly the situation an inductive
invariant is for: the entry is closed under stepping only in
conjunction with another entry.

So the strengthening is the conjunction, and the catalogue entries fall
out of it by weakening. This is the shape the remaining catalogue work
will mostly take. -/

def qNoValueUnlessSettled (p : PromiseObject) : Bool :=
  (p.state != .pending || (p.value.data.isNone && p.value.headers.isEmpty))
    && (p.settledAt != some p.timeoutAt || (p.value.data.isNone && p.value.headers.isEmpty))

theorem hereditary_noValueUnlessSettled : Hereditary qNoValueUnlessSettled where
  project p n h := by
    unfold PromiseObject.project
    split
    · rename_i hc
      have hv : p.value.data.isNone && p.value.headers.isEmpty = true := by
        simp only [qNoValueUnlessSettled, Bool.and_eq_true, Bool.or_eq_true, bne_iff_ne] at h
        rcases h.1 with h1 | h1
        · exact absurd hc.1 (by simp_all)
        · simpa using h1
      split <;> simp_all [qNoValueUnlessSettled]
    · simpa [qNoValueUnlessSettled] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qNoValueUnlessSettled] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qNoValueUnlessSettled] using h
  settle p st v t hst hdue _ := by
    cases st <;>
      simp_all [qNoValueUnlessSettled, ServerModel.PromiseState.settable] <;> omega
  dropListener p a h := by simpa [qNoValueUnlessSettled] using h
  dropCallback p a h := by simpa [qNoValueUnlessSettled] using h
  live id param tags timeoutAt createdAt h := by simp [qNoValueUnlessSettled]
  dead id st param tags timeoutAt hst := by
    subst hst; split <;> simp [qNoValueUnlessSettled]

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

theorem settled_at_lte_timeout_at_init (now : Nat) :
    well_formed_promise_settled_at_lte_timeout_at now ServerState.init = true := rfl

theorem settled_at_lte_timeout_at_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_settled_at_lte_timeout_at now s = true →
    well_formed_promise_settled_at_lte_timeout_at n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_settledAtLeTimeout mat st now s

theorem deadline_verdict_matches_timer_tag_init (now : Nat) :
    well_formed_promise_deadline_verdict_matches_timer_tag now ServerState.init = true := rfl

theorem deadline_verdict_matches_timer_tag_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_deadline_verdict_matches_timer_tag now s = true →
    well_formed_promise_deadline_verdict_matches_timer_tag n' (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_deadlineVerdict mat st now s

/-- The strengthened form. Note the hypothesis: not the catalogue entry
    but the conjunction, because the entry alone is not closed under
    stepping. -/
theorem deadline_settlement_has_no_value_step (mat : Bool) (st : Step) (now : Nat)
    (s : ServerState) :
    PerPromise qNoValueUnlessSettled s = true →
    PerPromise qNoValueUnlessSettled (stepOf mat st now s).2 = true :=
  perPromise_step hereditary_noValueUnlessSettled mat st now s

theorem deadline_settlement_has_no_value_of_strengthening (now : Nat) (s : ServerState) :
    PerPromise qNoValueUnlessSettled s = true →
    well_formed_promise_deadline_settlement_has_no_value now s = true :=
  perPromise_mono (fun _ h => (Bool.and_eq_true _ _ |>.mp h).2) s

theorem no_value_unless_settled_init : PerPromise qNoValueUnlessSettled ServerState.init = true :=
  rfl

end Entries

end Induction
end Abstraction
