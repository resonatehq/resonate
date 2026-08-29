import «04-theorems».«corpus»
import «02-abstract».«properties»

/-!  # Is `Valid → Legal` worth proving?

An implication is worthless if its hypothesis is unsatisfiable, and
almost worthless if its conclusion is trivially true. `valid_implies_legal`
is stated over an INFINITE trace and quantifies over all instants, so
neither question is decidable outright — but both split into a part
that is decidable and a part that is a construction, and it is worth
being exact about which is which.

## The conclusion is not trivially true

`Legal`'s body is a fold over the 95-entry catalogue. If that fold were
constantly `true` the theorem would say nothing. `legal_body_is_falsifiable`
exhibits a state the fold REFUSES, by `decide`. (The stronger version
lives in `properties-check.lean`: 47 hand-built mutants, each one
required to fail.)

## The hypothesis is satisfiable — and that part is a construction

`Valid mat tr` quantifies over every `t : Nat`, so no `decide` can
settle it. What settles it is building one: `traceOf` turns any finite
script into a trace by running the script and idling forever after, and
`valid_traceOf` proves the result `Valid` — with the first two conjuncts
`rfl`, because the trace is DEFINED as the run, and the third from the
clock being monotone by construction.

That is the honest division. `∃ tr, Valid mat tr` is a theorem, not a
computation, and it has to be: the object it asserts is infinite.

## But the witness being INTERESTING is decidable

A `Valid` trace that idles forever from the empty store is satisfiable
and useless — it would make the theorem true and vacuous in the way
that matters. So the witness runs `b1`, and the checks below confirm by
`decide` that it actually reaches settled promises, an acquired-then-
fulfilled task, a non-empty outbox, and a clock that moves. Those are
the states the catalogue has something to say about.

`legal_holds_along_witness` closes the loop: the catalogue fold — the
literal body of `Legal` — is `true` at every index of that witness. The
theorem's conclusion is therefore neither trivially true nor false
here; it is true for a reason. -/

namespace Abstraction
namespace Vacuity

open AbstractModel

/-! ## The conclusion is refusable

A promise born after its own deadline. Nothing in the machine writes
one; the fold rejects it, so `Legal` is not the constant-true
predicate. -/

def badState : ServerState :=
  { objects := [{ id := oid "p",
                  promise := { state := .pending, param := {}, tags := [],
                               timeoutAt := 1, createdAt := 5 } }] }

theorem legal_body_is_falsifiable :
    (AbstractModel.Properties.catalogue.all fun l =>
      match l.property with
      | .state f => f 0 badState
      | .trans f => f 0 badState badState) = false := by decide

/-! ## Building a `Valid` trace

The clock first, because it is the only conjunct that is not `rfl`. A
running maximum over the script's instants is monotone whatever the
script does, so no sortedness hypothesis is needed — and for a script
whose instants are already non-decreasing (every script in the corpus)
it is just the script's own clock. -/

theorem le_foldl_max : ∀ (m : List Nat) (b : Nat), b ≤ m.foldl Nat.max b
  | [],     b => Nat.le_refl b
  | x :: m, b => Nat.le_trans (Nat.le_max_left b x) (le_foldl_max m (Nat.max b x))

def clockOf (w : List (Step × Nat)) (t : Nat) : Nat :=
  ((w.map Prod.snd).take (t + 1)).foldl Nat.max 0

theorem clockOf_mono (w : List (Step × Nat)) (t : Nat) :
    clockOf w t ≤ clockOf w (t + 1) := by
  unfold clockOf
  have h : (w.map Prod.snd).take (t + 1 + 1)
      = (w.map Prod.snd).take (t + 1) ++ ((w.map Prod.snd)[t + 1]?).toList :=
    List.take_add_one
  rw [h, List.foldl_append]
  exact le_foldl_max _ _

def stepAt (w : List (Step × Nat)) (t : Nat) : Step :=
  match w[t]? with
  | some x => x.1
  | none   => .idle

def stateAt (mat : Bool) (w : List (Step × Nat)) (s₀ : ServerState) : Nat → ServerState
  | 0     => s₀
  | t + 1 => (stepOf mat (stepAt w t) (clockOf w t) (stateAt mat w s₀ t)).2

/-- The trace a finite script denotes: run it, then idle forever. -/
def traceOf (mat : Bool) (w : List (Step × Nat)) (s₀ : ServerState) : Trace := fun t =>
  { state := stateAt mat w s₀ t
  , req   := stepAt w t
  , res   := (stepOf mat (stepAt w t) (clockOf w t) (stateAt mat w s₀ t)).1
  , now   := clockOf w t }

/-- Two of the three conjuncts are `rfl`: the trace IS the run. The
    third is the clock, and it is monotone by construction. -/
theorem valid_traceOf (mat : Bool) (w : List (Step × Nat)) (s₀ : ServerState) :
    Valid mat (traceOf mat w s₀) :=
  fun t => ⟨rfl, rfl, clockOf_mono w t⟩

theorem traceOf_starts_at_init (mat : Bool) (w : List (Step × Nat)) :
    (traceOf mat w ServerState.init 0).state = ServerState.init := rfl

/-- The hypothesis of `valid_implies_legal` is satisfiable, under either
    read discipline, from the initial store. -/
theorem valid_is_satisfiable (mat : Bool) (w : List (Step × Nat)) :
    ∃ tr : Trace, Valid mat tr ∧ (tr 0).state = ServerState.init :=
  ⟨traceOf mat w ServerState.init, valid_traceOf mat w ServerState.init, rfl⟩

/-! ## And the witness is not the idle trace

`b1` creates an external promise and a targeted task, suspends the task
on the promise, settles the promise, drains the callback, redispatches,
re-acquires and fulfils. Everything below is `decide` on the trace
`b1` denotes. -/

abbrev wit : Trace := traceOf true b1 ServerState.init

theorem witness_clock_moves : (wit 0).now = 100 ∧ (wit 8).now = 230 := by decide

theorem witness_has_two_promises : (wit 9).state.promises.length = 2 := by decide

theorem witness_settles_a_promise :
    (wit 9).state.promises.any (·.state != .pending) = true := by decide

theorem witness_fulfils_a_task :
    (wit 9).state.tasks.any (·.state == .fulfilled) = true := by decide

theorem witness_acquires_a_task :
    (wit 8).state.tasks.any (·.state == .acquired) = true := by decide

theorem witness_registers_a_callback :
    (wit 3).state.promises.any (fun p => !p.callbacks.isEmpty) = true := by decide

theorem witness_suspends_a_task :
    (wit 3).state.tasks.any (·.state == .suspended) = true := by decide

theorem witness_writes_the_outbox :
    (wit 7).state.outbox.isEmpty = false := by decide

/-! ## The same-origin doors answer, and only when they should

An entry says no reachable state HOLDS a cross-origin awaits-edge. That
is the claim worth stating, but on its own it is satisfiable by a
machine that refuses everything, so these fix the other side: the door
answers `400` to a registration whose ends are in different origins, and
does not answer it to one whose ends agree. Decided rather than argued —
each is one handler run against the empty store, and the refusal comes
before any read, which is what makes it decidable there at all. -/

open AbstractModel in
theorem cross_origin_callback_is_refused :
    (run true (promiseRegisterCallback
        { awaited := { origin := "o1", suffix := "a" },
          awaiter := { origin := "o2", suffix := "x" } } 100)
      ServerState.init).1.status = 400 := by decide

open AbstractModel in
theorem same_origin_callback_passes_the_door :
    (run true (promiseRegisterCallback
        { awaited := { origin := "o1", suffix := "a" },
          awaiter := { origin := "o1", suffix := "x" } } 100)
      ServerState.init).1.status ≠ 400 := by decide

open AbstractModel in
theorem cross_origin_fence_is_refused :
    (run true (taskFence
        { id := { origin := "o1", suffix := "x" }, version := 1,
          action := .settle { id := { origin := "o2", suffix := "a" },
                              state := .resolved, value := {} } } 100)
      ServerState.init).1.status = 400 := by decide

open AbstractModel in
theorem cross_origin_suspend_is_refused :
    (run true (taskSuspend
        { id := { origin := "o1", suffix := "x" }, version := 1,
          actions := [{ awaited := { origin := "o2", suffix := "a" },
                        awaiter := { origin := "o1", suffix := "x" } }] } 100)
      ServerState.init).1.status = 400 := by decide

/-! ## The conclusion holds along it, for a reason

`Legal`'s body, verbatim, at every index the script covers. Together
with `legal_body_is_falsifiable` this says the fold discriminates: it
refuses `badState` and accepts every state this run passes through. -/

theorem legal_holds_along_witness :
    (List.range 12).all (fun t =>
      AbstractModel.Properties.catalogue.all fun l =>
        match l.property with
        | .state f => f (wit t).now (wit t).state
        | .trans f => f (wit t).now (wit t).state (wit (t + 1)).state) = true := by decide

/-! ## And `Legal` on the whole trace, not just the prefix

The prefix is decided; the tail is a fixed point. Past the end of the
script every step is `.idle`, an idle step writes nothing, and
`clockOf` has already saturated — so state and clock are constant from
index 9 on and the fold takes the value it had there, forever.

That closes the vacuity question completely: there is an infinite trace
that is `Valid`, starts at `init`, reaches settled promises and a
fulfilled task, and satisfies `Legal`. All of it proved, none of it
assumed. -/

theorem b1_length : b1.length = 9 := rfl

theorem stepAt_idle (t : Nat) (h : 9 ≤ t) : stepAt b1 t = Step.idle := by
  unfold stepAt
  rw [List.getElem?_eq_none (by rw [b1_length]; exact h)]

theorem clock_const (t : Nat) (h : 8 ≤ t) : clockOf b1 t = clockOf b1 8 := by
  unfold clockOf
  rw [List.take_of_length_le (by simp [b1_length]; omega),
      List.take_of_length_le (by simp [b1_length])]

theorem state_const (t : Nat) :
    stateAt true b1 ServerState.init (9 + t) = stateAt true b1 ServerState.init 9 := by
  induction t with
  | zero => rfl
  | succ k ih =>
      show (stepOf true (stepAt b1 (9 + k)) (clockOf b1 (9 + k))
              (stateAt true b1 ServerState.init (9 + k))).2 = _
      rw [stepAt_idle (9 + k) (by omega)]
      exact ih

theorem state_const' (t : Nat) (h : 9 ≤ t) :
    stateAt true b1 ServerState.init t = stateAt true b1 ServerState.init 9 := by
  have := state_const (t - 9)
  rwa [Nat.add_sub_cancel' h] at this

theorem legal_wit : Legal wit := by
  intro t
  by_cases h : t < 12
  · exact List.all_eq_true.mp legal_holds_along_witness t (by simp [List.mem_range]; omega)
  · have h9 : 9 ≤ t := by omega
    show (AbstractModel.Properties.catalogue.all fun l =>
      match l.property with
      | .state f => f (clockOf b1 t) (stateAt true b1 ServerState.init t)
      | .trans f => f (clockOf b1 t) (stateAt true b1 ServerState.init t)
                      (stateAt true b1 ServerState.init (t + 1))) = true
    rw [clock_const t (by omega), state_const' t h9, state_const' (t + 1) (by omega)]
    exact List.all_eq_true.mp legal_holds_along_witness 9 (by simp)

/-- The whole non-vacuity statement in one place. -/
theorem valid_implies_legal_is_not_vacuous :
    ∃ tr : Trace, Valid true tr ∧ (tr 0).state = ServerState.init ∧ Legal tr
      ∧ (tr 9).state.promises.any (·.state != .pending) = true
      ∧ (tr 9).state.tasks.any (·.state == .fulfilled) = true :=
  ⟨wit, valid_traceOf true b1 ServerState.init, rfl, legal_wit,
   witness_settles_a_promise, witness_fulfils_a_task⟩

/-! ## What is left un-decided, and why

Two things, and neither is a gap that `decide` could close.

`Valid mat tr → Legal tr` for ARBITRARY `tr` quantifies over infinitely
many traces and, for each, infinitely many instants. It is the theorem;
`decide` can only ever supply witnesses for it.

And the catalogue is 95 entries, of which 32 are currently proved
inductive. The rest are backed by `properties-check.lean` and
`properties-step.lean` — 1 464 scripts, both readings — which is a
finite fact, exactly like everything in this file. -/

end Vacuity
end Abstraction
