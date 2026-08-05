import «06-trace».validator

/-!  # What the checker is supposed to be

`validate` answers a question. This file says what the question MEANS,
independently of how the checker computes it, and states the two theorems
that connect the two. The proofs are `sorry` — the point of writing the
statements first is that they name the obligations, and two of them are
already known to be delicate because the implementation got them wrong
once.

## The semantics

A recorded run is admissible when SOME schedule of internal steps
explains it. Two things make that definition as short as it is.

`Tau` is a type, so "a schedule contains only internal steps" is not a
hypothesis to carry around — it is what `List Tau` MEANS. The predicate
used to read `∃ σ : List Request, (∀ t ∈ σ, t.isExternal = false) ∧ …`,
which put the invariant in a place every lemma had to re-establish.

Internal steps are also self-guarding here — `handleM` refuses a τ whose
obligation is not on the books and returns `.τ` — so the definition
quantifies over ARBITRARY schedules and needs no enabledness
side-condition either. That is a property of the machine, not a
convenience: the trace framework calls it being "OBLIGATION-GUARDED".

## Why there are three verdicts, not two

`.inconclusive` is not squeamishness. Completeness is only true when the
search actually ran to saturation, so the checker must be able to say "I
stopped early" as a distinct answer.

Writing that hypothesis down is what exposed a real gap: `tauClosureBy`
used to return its partial `seen` set when fuel ran out, indistinguishably
from having reached a fixpoint, so a REFUTED verdict could rest on a
closure that was simply cut short. It now reports saturation and
`validate` downgrades a truncated step to `.inconclusive`, which is why
`refuted_implies_not_conformant` below needs no fuel side-condition. -/

namespace TraceCheck.Correctness

open ServerModel Equivalence TraceCheck

/-! ## Firing a schedule -/

/-- Run a schedule of internal steps at one instant. Nothing external can
    appear in `σ`: the type forbids it. -/
def fireAll (σ : List Tau) (now : Nat) (s : ServerState) : ServerState :=
  σ.foldl (fun st t => t.step now st) s

/-- One observed event is EXPLAINED from `s`, landing in `s'`, when some
    internal schedule fired at `o.now` makes the request produce exactly
    the response that was observed. -/
def Explains (o : Observation) (s s' : ServerState) : Prop :=
  ∃ σ : List Tau,
    stepOf handleM o.req o.now (fireAll σ o.now s) = (o.res, s')

/-- The whole run is explained, event by event, from a starting state. -/
inductive Admissible : ServerState → List Observation → Prop
  | nil  {s} : Admissible s []
  | cons {s s' o rest} : Explains o s s' → Admissible s' rest → Admissible s (o :: rest)

/-- The claim a conformance suite actually wants to make about a server. -/
def Conformant (t : List Observation) : Prop :=
  Admissible ServerState.init t

/-! ## The two theorems

The pair one WANTS to write is

```
theorem admissible_implies_conformant :  verdict = admissible → Conformant t
theorem not_admissible_implies_not_conformant : verdict ≠ admissible → ¬ Conformant t
```

The first is right. The second is **false**, and not by a technicality:
`.inconclusive` is not `.admissible`, but a trace that exhausted the fuel
bound may be perfectly conformant — the checker simply did not finish
looking. Stating completeness against "not admissible" would demand that
running out of budget proves nonconformance.

So completeness is stated against `.refuted` specifically. That is why
the verdict type has three constructors and not a `Bool`: the checker
needs a way to decline, and the theorems need a name for it. Together the
two say: **whenever the verdict is not `.inconclusive`, it is right.**

The equivalence the pair adds up to is `conformant_iff` below, which is
the two-sided statement — and it is conditional exactly where it must be.

`admissible_implies_conformant` is the one that matters for trusting a
green suite: it says a pass is not vacuous. `refuted_implies_not_conformant`
is the one that matters for trusting a bug report: it says a refutation is
not a false alarm, which is exactly the direction the cone got wrong
before `affects` was introduced. -/

/-- **SOUNDNESS.** An `admissible` verdict is not a false pass: some
    schedule really does explain the run. -/
theorem admissible_implies_conformant {t : List Observation} {fuel cap : Nat}
    {w : List (Tau × Nat)} {f n : Nat}
    (h : validate t fuel cap = .admissible w f n) :
    Conformant t := by
  sorry

/-- **COMPLETENESS.** A `refuted` verdict is not a false alarm: no
    schedule explains the run.

    Note the hypothesis is `.refuted`, not `≠ .admissible`. The weaker
    form is what one reaches for first and it does not hold — see the
    section comment.

    Unconditional otherwise, but only because `validate` now refuses to
    return `.refuted` from a truncated closure — a step that hit the fuel
    bound yields `.inconclusive` instead. Without that, even this
    statement would be false, and the checker would report false alarms
    whenever a τ chain ran deeper than `fuel`. -/
theorem refuted_implies_not_conformant {t : List Observation} {fuel cap i k : Nat}
    (h : validate t fuel cap = .refuted i k) :
    ¬ Conformant t := by
  sorry

/-- **THE TWO SIDES TOGETHER.** Conformance and an `admissible` verdict
    coincide — given that the checker reached a decision at all.

    This is the statement "the checker is correct and complete", and the
    hypothesis is the honest price of a bounded search: it says nothing
    about traces the checker declined to decide, and it cannot, because
    for those the answer genuinely is unknown.

    It follows from the two theorems above plus the fact that the three
    verdicts are exhaustive and mutually exclusive. -/
theorem conformant_iff {t : List Observation} {fuel cap : Nat}
    (hdecided : ∀ i r, validate t fuel cap ≠ .inconclusive i r) :
    Conformant t ↔ ∃ w f n, validate t fuel cap = .admissible w f n := by
  sorry

/-- Stronger than `admissible_implies_conformant`, and the reason the
    checker carries a schedule at all: the witness it returns is itself an
    explanation, so a pass comes with a certificate rather than an
    assertion. -/
theorem witness_explains {t : List Observation} {fuel cap : Nat}
    {w : List (Tau × Nat)} {f n : Nat}
    (h : validate t fuel cap = .admissible w f n) :
    ∃ schedules : List (List Tau),
      schedules.flatten = w.map (·.fst) ∧ Conformant t := by
  sorry

/-! ## The three obligations the proofs rest on

Each is a real property of the machine, not bookkeeping, and each is a
place the implementation could be wrong without any test noticing. -/

/-- **1 · `canon` is a congruence for responses.**

    `dedup` discards a candidate whose canonical form matches one already
    kept. That is only sound if states with equal canonical forms answer
    every request identically — otherwise dedup throws away the branch
    that would have explained a later event.

    True because `canon` only sorts keyed collections, and every handler
    reaches them by key. -/
theorem canon_congruence {s s' : ServerState} (h : canon s = canon s')
    (req : Request) (now : Nat) :
    (stepOf handleM req now s).1 = (stepOf handleM req now s').1 := by
  sorry

/-- **2 · `canon` is preserved by stepping.**

    Needed to iterate obligation 1 across events: two candidates that
    dedup merged must stay merged after the next step, or the merge was
    only correct for one event. -/
theorem canon_step {s s' : ServerState} (h : canon s = canon s')
    (req : Request) (now : Nat) :
    canon (stepOf handleM req now s).2 = canon (stepOf handleM req now s').2 := by
  sorry

/-- **3 · INDEPENDENCE — the cone's whole justification.**

    A τ whose affected set misses everything the request reads cannot
    change the response, so declining to fire it now loses nothing: it
    stays armed and is available at the next event that does reach it.

    Note the two SIDES of the hypothesis are now different types —
    `affects` takes a `Tau`, `touches` a `Request` — which is the point:
    only a τ can be deferred, and the type says so.

    This is the statement `touches` violated. `touches (τPromiseTimeout a)`
    was `[a]`, but settling `a` defers resumes for its awaiters — so the
    hypothesis held while the conclusion did not, and the cone refuted a
    conforming trace. `affects` exists to make the hypothesis strong
    enough to be true. If this cannot be proved, `affects` is still
    wrong. -/
theorem cone_independence {s : ServerState} {now : Nat} {req : Request} {t : Tau}
    (hdisj : ∀ o ∈ affects s t, o ∉ touches req) :
    (stepOf handleM req now (t.step now s)).1
      = (stepOf handleM req now s).1 := by
  sorry

/-- **3b · and the states commute**, modulo `canon`, so deferring is a
    reordering rather than a loss. -/
theorem cone_commute {s : ServerState} {now : Nat} {req : Request} {t : Tau}
    (hdisj : ∀ o ∈ affects s t, o ∉ touches req) :
    canon (t.step now (stepOf handleM req now s).2)
      = canon (stepOf handleM req now (t.step now s)).2 := by
  sorry

end TraceCheck.Correctness
