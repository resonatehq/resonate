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
`rejected_trace_implies_not_valid_trace` below needs no fuel
side-condition. -/

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

/-- A trace is VALID when the specification can explain it. This is the
    claim a conformance suite actually wants to make about a server, and
    it is BINARY: a trace either is or is not explainable, with or
    without anyone running a checker.

    The three-valuedness below lives one level up, in what `validate`
    ESTABLISHED — never in the truth itself. -/
def Valid (t : List Observation) : Prop :=
  Admissible ServerState.init t

/-! ## Accepting, rejecting, and declining

`Valid` is binary. `validate`'s verdict is not: it ACCEPTS or REJECTS —
two positive judgements, not each other's negation — or it does neither.

So the map from verdict to truth is PARTIAL:

```
accepted   ↦  Valid
rejected   ↦  ¬ Valid
undecided  ↦  (nothing)
```

Three verdicts, two truths, one verdict with no image. Everything below
is bookkeeping on that picture, and it is why the first attempt at these
theorems fails. Written with a negation:

```
accepted  → valid          -- true
¬accepted → ¬valid         -- FALSE
```

`¬accepted` is total, so it swallows the declined case along with the
rejected one — and a declined trace may be perfectly valid, since the
search hit its bound and stopped looking, which is not evidence of
anything. Written with two positive judgements, both hold:

```
accepted → valid
rejected → ¬valid
```

Nothing about the logic changed between those two displays. The
vocabulary changed, and the vocabulary is what was wrong.

`Undecided` is therefore a first-class notion here rather than a leftover
`else`. `verdict_trichotomy` records that the three are exhaustive and
disjoint, which is what lets `valid_iff_accepted` recover the equivalence
on the traces the checker actually decided. -/

/-- The checker ACCEPTED the trace: it found a schedule. -/
def Accepted (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ w f n, validate t fuel cap = .admissible w f n

/-- The checker REJECTED the trace: it searched to saturation and found
    no schedule. A positive claim, not the failure of `Accepted`. -/
def Rejected (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ i k, validate t fuel cap = .refuted i k

/-- The checker DECLINED: it ran out of budget. Carries no claim about
    the trace at all, which is exactly why it needs its own name. -/
def Undecided (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ i r, validate t fuel cap = .inconclusive i r

/-! ## The two theorems -/

/-- **SOUNDNESS — an accepted trace is a valid trace.** A pass is not
    vacuous: some schedule really does explain the run. -/
theorem accepted_trace_implies_valid_trace {t : List Observation} {fuel cap : Nat}
    (h : Accepted t fuel cap) :
    Valid t := by
  sorry

/-- **COMPLETENESS — a rejected trace is not a valid trace.** A refutation
    is not a false alarm: no schedule explains the run. This is the
    direction the cone got wrong before `affects` was introduced.

    Unconditional, but only because `validate` refuses to REJECT from a
    truncated closure — a step that hit the fuel bound DECLINES instead.
    Without that separation this statement would be false, and the checker
    would raise false alarms whenever a τ chain ran deeper than `fuel`. -/
theorem rejected_trace_implies_not_valid_trace {t : List Observation} {fuel cap : Nat}
    (h : Rejected t fuel cap) :
    ¬ Valid t := by
  sorry

/-! ## What makes them add up -/

/-- The three judgements partition the outcomes: every run lands in
    exactly one. Without this, the two theorems above would not compose —
    "not rejected" would not narrow anything down. -/
theorem verdict_trichotomy (t : List Observation) (fuel cap : Nat) :
    (Accepted t fuel cap ∨ Rejected t fuel cap ∨ Undecided t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Rejected t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Undecided t fuel cap)
    ∧ ¬(Rejected t fuel cap ∧ Undecided t fuel cap) := by
  unfold Accepted Rejected Undecided
  cases validate t fuel cap <;> simp

/-- **THE TWO SIDES TOGETHER.** On the traces it decided, the checker's
    answer and the truth coincide — "correct and complete".

    The hypothesis is the honest price of a bounded search. It says
    nothing about declined traces and it cannot, because for those the
    answer genuinely is unknown. Follows from the two theorems plus
    `verdict_trichotomy`. -/
theorem valid_iff_accepted {t : List Observation} {fuel cap : Nat}
    (hdecided : ¬ Undecided t fuel cap) :
    Valid t ↔ Accepted t fuel cap := by
  constructor
  · intro hvalid
    rcases (verdict_trichotomy t fuel cap).1 with hacc | hrej | hund
    · exact hacc
    · exact absurd hvalid (rejected_trace_implies_not_valid_trace hrej)
    · exact absurd hund hdecided
  · exact accepted_trace_implies_valid_trace

/-- Stronger than `accepted_trace_implies_valid_trace`, and the reason the
    checker carries a schedule at all: the witness it returns is itself an
    explanation, so a pass comes with a certificate rather than an
    assertion. -/
theorem witness_explains {t : List Observation} {fuel cap : Nat}
    {w : List (Tau × Nat)} {f n : Nat}
    (h : validate t fuel cap = .admissible w f n) :
    ∃ schedules : List (List Tau),
      schedules.flatten = w.map (·.fst) ∧ Valid t := by
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
