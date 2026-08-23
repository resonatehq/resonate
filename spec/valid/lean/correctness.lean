import «valid».«lean».validator

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

That is not academic. `processRetryTimeout` re-arms at `now +
retryTimeout`, so WHEN a retry fired is visible at any later event that
straddles the re-armed deadline.

So the bridge `pinned_implies_valid` is deliberately ONE-DIRECTIONAL, and
the converse is `InstantsSuffice` — a named, unproved, possibly FALSE
proposition rather than a remark in a comment. Soundness survives
untouched. Completeness now carries `InstantsSuffice` as a hypothesis,
which is the honest statement: reject means "no schedule with internal steps at
observation instants", and upgrading that to "no schedule at all" is
precisely the missing lemma.

## The semantics of a schedule

`InternalStep` is a type, so "a schedule contains only internal steps" is not a
hypothesis to carry around — it is what `List InternalStep` MEANS. The predicate
used to read `∃ σ : List Request, (∀ t ∈ σ, t.isExternal = false) ∧ …`,
which put the invariant in a place every lemma had to re-establish.

Internal steps are also self-guarding here — `handle` refuses an internal
step whose obligation is not on the books and returns `silent` — so the
definition
quantifies over ARBITRARY schedules and needs no enabledness
side-condition either. That is a property of the machine, not a
convenience: the trace framework calls it being "OBLIGATION-GUARDED".

## Why there are three verdicts, not two

`.inconclusive` is not squeamishness. Completeness is only true when the
search actually ran to saturation, so the checker must be able to say "I
stopped early" as a distinct answer.

Writing that hypothesis down is what exposed a real gap: `internalClosureBy`
used to return its partial `seen` set when fuel ran out, indistinguishably
from having reached a fixpoint, so a REFUTED verdict could rest on a
closure that was simply cut short. It now reports saturation and
`validate` downgrades a truncated step to `.inconclusive`, which is why
`rejected_trace_implies_not_valid_trace` below needs no fuel
side-condition. -/

namespace TraceCheck.Correctness

open ServerModel AbstractModel Abstraction Equivalence TraceCheck

/-! ## Validity — reusing the specification's own definition -/

/-- Validity of a run at the materialised reading. This used to come
    from `04-theorems/trace.lean` as `Equivalence.ValidM`, one of two
    notions; there is one machine now, so the discipline is an argument
    and this is a name for `Valid true`. Kept as a name because every
    theorem in `executions.lean` and below cites it. -/
abbrev ValidM : Trace → Prop := Abstraction.Valid true

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
      (tr (φ i)).req = .api (obs[i]'h).req ∧
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

/-! ## The abandoned notion, kept on purpose

`ValidPinned` is what an earlier checker searched: every hidden step fires
AT the instant of the observation it precedes. It is no longer anything's
definition of validity, and no checker computes it — but it is not dead
code either. `valid/lean/schedules.lean` proves that the gap between it and
`Valid` cannot be closed, and that result has to be stated against
something. Deleting these would delete the record of why the design
changed. -/

/-- Run a schedule of internal steps at ONE instant. -/
def fireAll (σ : List InternalStep) (now : Nat) (s : ServerState) : ServerState :=
  σ.foldl (fun st t => t.step now st) s

/-- One observed event explained by a schedule pinned to `o.now`. -/
def Explains (o : Observation) (s s' : ServerState) : Prop :=
  ∃ σ : List InternalStep,
    Abstraction.stepOf true (.api o.req) o.now (fireAll σ o.now s) = (o.res, s')

inductive Admissible : ServerState → List Observation → Prop
  | nil  {s} : Admissible s []
  | cons {s s' o rest} : Explains o s s' → Admissible s' rest → Admissible s (o :: rest)

def ValidPinned (obs : List Observation) : Prop :=
  Admissible ServerState.init obs

/-- The hypothesis the pinned checker needed and never got. Not merely
    unproved: `valid/lean/schedules.lean` shows it is INDEPENDENT, because
    `occurrences` is `opaque` with no value, so both it and its negation
    are consistent. No test could ever have settled it — which is why the
    checker was rebuilt to carry intervals instead of assuming this. -/
def InstantsSuffice : Prop :=
  ∀ obs : List Observation, Valid obs → ValidPinned obs

/-! ## Where the rest of the story lives

This file is the SEMANTICS only: what a valid run is, said in the
specification's vocabulary. It deliberately knows nothing about how the
checker computes.

* `valid/lean/executions.lean` refines `Valid` into `ValidExec`, which
  carries the INTERVAL between observations rather than a single instant,
  and proves `valid_implies_exec` outright.
* `valid/lean/intervals.lean` is the checker, and states the two theorems —
  `accepted_trace_implies_valid_trace` and
  `rejected_trace_implies_not_valid_trace` — against `Valid` as defined
  here.

An earlier version defined validity itself as "hidden steps fire at the
observation's own instant", which is what the checker searched. That made
completeness true by construction and hid a real gap. The definition above
is the specification's; the checker now has to meet it rather than the
other way round. -/

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
    (Abstraction.stepOf true (.api req) now s).1 = (Abstraction.stepOf true (.api req) now s').1 := by
  sorry

/-- **2 · `canon` is preserved by stepping.**

    Needed to iterate obligation 1 across events: two candidates that
    dedup merged must stay merged after the next step, or the merge was
    only correct for one event. -/
theorem canon_step {s s' : ServerState} (h : canon s = canon s')
    (req : Request) (now : Nat) :
    canon (Abstraction.stepOf true (.api req) now s).2 = canon (Abstraction.stepOf true (.api req) now s').2 := by
  sorry

/-- **3 · INDEPENDENCE — the cone's whole justification.**

    A internal step whose affected set misses everything the request reads cannot
    change the response, so declining to fire it now loses nothing: it
    stays armed and is available at the next event that does reach it.

    Note the two SIDES of the hypothesis are now different types —
    `affects` takes a `InternalStep`, `touches` a `Request` — which is the point:
    only an internal step can be deferred, and the type says so.

    This is the statement `touches` violated. `touches (promiseTimeout a)`
    was `[a]`, but settling `a` defers resumes for its awaiters — so the
    hypothesis held while the conclusion did not, and the cone refuted a
    conforming trace. `affects` exists to make the hypothesis strong
    enough to be true. If this cannot be proved, `affects` is still
    wrong. -/
theorem cone_independence {s : ServerState} {now : Nat} {req : Request} {t : InternalStep}
    (hdisj : ∀ o ∈ affects s t, o ∉ touches req) :
    (Abstraction.stepOf true (.api req) now (t.step now s)).1
      = (Abstraction.stepOf true (.api req) now s).1 := by
  sorry

/-- **3b · and the states commute**, modulo `canon`, so deferring is a
    reordering rather than a loss. -/
theorem cone_commute {s : ServerState} {now : Nat} {req : Request} {t : InternalStep}
    (hdisj : ∀ o ∈ affects s t, o ∉ touches req) :
    canon (t.step now (Abstraction.stepOf true (.api req) now s).2)
      = canon (Abstraction.stepOf true (.api req) now (t.step now s)).2 := by
  sorry

end TraceCheck.Correctness
