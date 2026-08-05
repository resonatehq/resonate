import «06-trace».validator

/-!  # What the checker is supposed to be

`validate` answers a question. This file says what the question MEANS and
states the theorems that connect the two. Most proofs are `sorry`; the
point of writing the statements first is that they name the obligations,
and doing so has already caught three real defects — the last of them
being the reason this file no longer defines validity itself.

## Validity is the SPECIFICATION's notion, not ours

`04-theorems/trace.lean` already says what a valid run is:

```lean
abbrev Trace := Nat → StateAction        -- state, req, res, now
def Valid (handle) (tr : Trace) : Prop :=
  ∀ t, (tr t).res     = (stepOf handle (tr t).req (tr t).now (tr t).state).1
     ∧ (tr (t+1)).state = (stepOf handle (tr t).req (tr t).now (tr t).state).2
     ∧ (tr t).now    ≤ (tr (t+1)).now
def ValidM : Trace → Prop := Valid handleM
```

This file used to restate that for observation lists instead of reusing
it, and the restatement was quietly WEAKER — see the gap section below.
`Valid` here is now defined in terms of `ValidM`: a recorded run is valid
when some spec-valid execution from `init` has exactly these external
steps. The projection mirrors `SameObservation` from
`04-theorems/equivalence.lean`, which already expresses "same external
behaviour, internal steps unconstrained".

Reusing it also connects this checker to the equivalence proof: the two
machines are related through `SameObservation`, so a statement about
`ValidM` transports to `ValidP` rather than standing alone.

## The gap the restatement was hiding

`Explains` fires every hidden step at the OBSERVATION'S OWN INSTANT.
`ValidM` requires only `(tr t).now ≤ (tr (t+1)).now`, so a real execution
may fire internal steps at any instants BETWEEN observations. The pinned
notion is therefore strictly narrower, and that asymmetry is exactly what
the checker searches.

That is not academic. `onTaskRetryTimeout` re-arms at `now +
retryTimeout`, so WHEN a retry fired is visible at any later event that
straddles the re-armed deadline.

So the bridge `pinned_implies_valid` is deliberately ONE-DIRECTIONAL, and
the converse is `InstantsSuffice` — a named, unproved, possibly FALSE
proposition rather than a remark in a comment. Soundness survives
untouched. Completeness now carries `InstantsSuffice` as a hypothesis,
which is the honest statement: reject means "no schedule with τs at
observation instants", and upgrading that to "no schedule at all" is
precisely the missing lemma.

## The semantics of a schedule

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

/-! ## Validity — reusing the specification's own definition -/

/-- The external steps of `tr` are exactly `obs`, in order.

    Mirrors `SameObservation` from `04-theorems/equivalence.lean` with one
    side a recorded list: a strictly monotone `φ` places the i-th
    observation, matching request, instant AND response; and the last
    clause forbids `tr` from having external steps the recording missed.
    Internal steps are unconstrained — they may sit anywhere, at any
    instants, which is the whole point. -/
def ExternalsAre (tr : Trace) (obs : List Observation) : Prop :=
  ∃ φ : Nat → Nat,
    (∀ i j, i < j → φ i < φ j) ∧
    (∀ i, (h : i < obs.length) →
      (tr (φ i)).req.isExternal = true ∧
      (tr (φ i)).req = (obs[i]'h).req ∧
      (tr (φ i)).res = (obs[i]'h).res ∧
      (tr (φ i)).now = (obs[i]'h).now) ∧
    (∀ s, (tr s).req.isExternal = true → ∃ i, i < obs.length ∧ φ i = s)

/-- **VALIDITY.** A recorded run is valid when some execution the
    SPECIFICATION calls valid, started from `init`, has exactly these
    external steps.

    Note what is NOT here: no bound, no schedule shape, no instants
    restriction. This is the truth the checker is approximating, stated
    in the specification's vocabulary rather than the checker's. -/
def Valid (obs : List Observation) : Prop :=
  ∃ tr : Trace, ValidM tr ∧ (tr 0).state = ServerState.init ∧ ExternalsAre tr obs

/-! ## The restricted notion the checker actually searches -/

/-- Run a schedule of internal steps at one instant. Nothing external can
    appear in `σ`: the type forbids it. -/
def fireAll (σ : List Tau) (now : Nat) (s : ServerState) : ServerState :=
  σ.foldl (fun st t => t.step now st) s

/-- One observed event is EXPLAINED from `s`, landing in `s'`, when some
    internal schedule fired AT `o.now` makes the request produce exactly
    the response that was observed.

    The instant is pinned. That is the restriction; see `InstantsSuffice`. -/
def Explains (o : Observation) (s s' : ServerState) : Prop :=
  ∃ σ : List Tau,
    stepOf handleM o.req o.now (fireAll σ o.now s) = (o.res, s')

/-- The whole run is explained, event by event, from a starting state. -/
inductive Admissible : ServerState → List Observation → Prop
  | nil  {s} : Admissible s []
  | cons {s s' o rest} : Explains o s s' → Admissible s' rest → Admissible s (o :: rest)

/-- Validity RESTRICTED to schedules whose τs fire at observation
    instants. Proof-friendly — an inductive with one case per event —
    which is why the checker theorems are proved through it. -/
def ValidPinned (obs : List Observation) : Prop :=
  Admissible ServerState.init obs

/-! ## The bridge, and the gap -/

/-- **The bridge.** A pinned explanation IS an execution: interleave each
    event's schedule before it, all at that event's instant, and pad with
    `.idle`. Clock monotonicity is immediate because instants only ever
    come from the observations, which are ordered.

    ONE-DIRECTIONAL by design. -/
theorem pinned_implies_valid {obs : List Observation} (h : ValidPinned obs) :
    Valid obs := by
  sorry

/-- **The converse — NOT a theorem.** Naming it as a proposition instead
    of proving it is the point: it may well be FALSE.

    `ValidM` constrains instants only by `(tr t).now ≤ (tr (t+1)).now`, so
    an execution may fire a τ strictly between two observations. The
    checker never tries that. For the difference to be invisible, every
    internal step would have to be insensitive to WHICH instant in the gap
    it fired at — and `onTaskRetryTimeout` re-arms at `now + retryTimeout`,
    which is directly instant-sensitive and observable at any later event
    that straddles the re-armed deadline.

    Whether an actual counterexample trace exists is open. Until it is
    settled, every statement that needs "the checker searched everything"
    carries this as a hypothesis. -/
def InstantsSuffice : Prop :=
  ∀ obs : List Observation, Valid obs → ValidPinned obs

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

/-- The checker's core obligation, and the one place the ALGORITHM has to
    be right: if it accepted, the schedule it walked really does explain
    every event. Everything else in this section is bookkeeping on top.

    Rests on `canon_congruence` and `canon_step` (dedup must not discard a
    branch that would have explained a later event) and, when the cone is
    on, `cone_independence` and `cone_commute`. -/
theorem accepted_implies_pinned {t : List Observation} {fuel cap : Nat}
    (h : Accepted t fuel cap) :
    ValidPinned t := by
  sorry

/-- **SOUNDNESS — an accepted trace is a valid trace.** A pass is not
    vacuous: some execution the specification calls valid really does have
    exactly these external steps.

    Unaffected by the instants gap. The checker only ever exhibits pinned
    schedules, and pinned schedules ARE executions — that direction is
    `pinned_implies_valid`, and it is the direction that holds. -/
theorem accepted_trace_implies_valid_trace {t : List Observation} {fuel cap : Nat}
    (h : Accepted t fuel cap) :
    Valid t :=
  pinned_implies_valid (accepted_implies_pinned h)

/-- **COMPLETENESS, at the level the checker searches.** A rejection means
    no schedule with τs AT OBSERVATION INSTANTS explains the run.

    This is the honest unconditional statement, and it is the direction the
    cone got wrong before `affects` was introduced.

    Unconditional in `fuel` only because `validate` refuses to REJECT from
    a truncated closure — a step that hit the bound DECLINES instead. -/
theorem rejected_trace_implies_not_pinned_valid_trace
    {t : List Observation} {fuel cap : Nat}
    (h : Rejected t fuel cap) :
    ¬ ValidPinned t := by
  sorry

/-- **COMPLETENESS, at the level anyone actually cares about** — and it
    does not come for free.

    Rejecting means the checker found no PINNED schedule. Concluding that
    no execution whatsoever explains the run needs `InstantsSuffice`, which
    is unproved and may be false. The hypothesis is the gap, made
    impossible to overlook: anyone invoking this theorem must first
    discharge it.

    This is what the file previously claimed unconditionally, by defining
    validity to be the pinned notion. -/
theorem rejected_trace_implies_not_valid_trace
    {t : List Observation} {fuel cap : Nat}
    (hgap : InstantsSuffice)
    (h : Rejected t fuel cap) :
    ¬ Valid t :=
  fun hvalid => rejected_trace_implies_not_pinned_valid_trace h (hgap t hvalid)

/-! ## What makes them add up -/

/-- The three judgements partition the outcomes: every run lands in
    exactly one. Without this, the theorems above would not compose —
    "not rejected" would not narrow anything down. -/
theorem verdict_trichotomy (t : List Observation) (fuel cap : Nat) :
    (Accepted t fuel cap ∨ Rejected t fuel cap ∨ Undecided t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Rejected t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Undecided t fuel cap)
    ∧ ¬(Rejected t fuel cap ∧ Undecided t fuel cap) := by
  unfold Accepted Rejected Undecided
  cases validate t fuel cap <;> simp

/-- **CORRECT AND COMPLETE, unconditionally** — for the notion the checker
    actually decides. On the runs it did not decline, accepting and being
    pinned-valid are the same thing. No gap, no hypothesis. -/
theorem pinned_valid_iff_accepted {t : List Observation} {fuel cap : Nat}
    (hdecided : ¬ Undecided t fuel cap) :
    ValidPinned t ↔ Accepted t fuel cap := by
  constructor
  · intro hpinned
    rcases (verdict_trichotomy t fuel cap).1 with hacc | hrej | hund
    · exact hacc
    · exact absurd hpinned (rejected_trace_implies_not_pinned_valid_trace hrej)
    · exact absurd hund hdecided
  · exact accepted_implies_pinned

/-- **CORRECT AND COMPLETE, for real validity** — and the extra hypothesis
    is the price. `InstantsSuffice` is what closes the distance between
    "the checker searched everything it searches" and "the checker
    searched everything".

    Compare `pinned_valid_iff_accepted`: same proof, one fewer assumption,
    weaker conclusion. The pair is the clearest statement of what this
    checker does and does not establish. -/
theorem valid_iff_accepted {t : List Observation} {fuel cap : Nat}
    (hgap : InstantsSuffice)
    (hdecided : ¬ Undecided t fuel cap) :
    Valid t ↔ Accepted t fuel cap := by
  constructor
  · intro hvalid; exact (pinned_valid_iff_accepted hdecided).mp (hgap t hvalid)
  · exact accepted_trace_implies_valid_trace

/-- Stronger than `accepted_trace_implies_valid_trace`, and the reason the
    checker carries a schedule at all: the witness it returns is itself an
    explanation, so a pass comes with a certificate rather than an
    assertion. -/
theorem witness_explains {t : List Observation} {fuel cap : Nat}
    {w : List (Tau × Nat)} {f n : Nat}
    (h : validate t fuel cap = .admissible w f n) :
    ∃ schedules : List (List Tau),
      schedules.flatten = w.map (·.fst) ∧ ValidPinned t := by
  sorry

/-! ## The obligations the proofs rest on

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
