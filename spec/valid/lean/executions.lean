import «valid».«lean».correctness

/-!  # Deleting `InstantsSuffice` by making the search space exact

`valid/lean/correctness.lean` carries an unproved, possibly false
hypothesis:

```lean
def InstantsSuffice : Prop := ∀ obs, Valid obs → ValidPinned obs
```

It is there because `Explains` fires every hidden step AT the instant of
the observation it precedes, while `ValidM` lets a hidden step fire at any
instant with `now` monotone. The hypothesis is the distance between the
two.

The move made here is not to prove that hypothesis — it is FALSE, see
`valid/lean/schedules.lean` — but to make it unnecessary, by widening the
notion the checker searches until it EXACTLY characterises `Valid`. Then
there is no gap left to hypothesise about.

## What `ValidPinned` gets wrong, precisely

Three defects, of which only the first was previously named.

1. **Gap instants.** A hidden step may fire strictly between two
   observations. `Explains` cannot place it there.

2. **The instant is not even the RIGHT observation's.** `Admissible`
   fires each event's schedule BEFORE that event, so an internal step that a real
   execution fired at `o_i.now` — but after `o_i`'s request — is
   attributed by the checker to `o_{i+1}.now`. So `ValidPinned` is not
   the "internal steps at observation instants" fragment of `Valid`; it is strictly
   narrower than that, and the discrepancy is visible on a trace whose
   instants are ALL on the grid. This was not recorded anywhere.

3. **Nothing after the last observation.** internal steps that fire after the final
   event are not modelled. This one is harmless — such steps precede no
   observation, so deleting them from an execution changes no external
   step — but it has to be argued rather than assumed, and `Admissible`
   never made room to argue it.

`AdmissibleAt` below fixes all three by carrying the interval: each
event's schedule is a list of (internal step, instant) pairs whose instants are
NON-DECREASING and lie in `[a, o.now]`, where `a` is the previous event's
instant (`0` before the first). Defect 3 is fixed by the theorem, not the
definition: the reconstruction pads with `.idle`, so trailing internal steps simply
never need to appear.

## Why this is a restatement and not a redefinition

`Valid` is unchanged — it is still `ValidM` from the specification, with
the observation projection of `04-theorems/equivalence.lean`.
`valid_iff_exec` is a THEOREM relating the two, in both directions. That
is what makes this legitimate under "you may restate `Valid` only if the
restatement is provably equivalent": nothing here weakens the notion of
validity, and the equivalence is stated so that the equality of the two
search spaces is an obligation on this file rather than an assumption
about the machine.

The content of the equivalence is bookkeeping about traces — segment the
trace at the external steps, read the internal steps off each segment —
and involves NO handler semantics at all. That is exactly why it is the
right place to spend: `InstantsSuffice` is a claim about what the
handlers do, and it turned out to be false; `valid_iff_exec` is a claim
about what a trace IS, and it is true by construction. -/

namespace TraceCheck.Executions

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Correctness

/-! ## Internal requests, read back as internal steps

`InternalStep` was introduced so that "a schedule contains only internal steps" is
a typing fact. Recovering a schedule from a trace needs the other
direction: every non-external `Request` is either an internal step or `.idle`, and
`.idle` is a no-op that can be dropped. -/

def ofStep? : Step → Option InternalStep
  | .promiseTimeout id      => some (.promiseTimeout id)
  | .listener id addr => some (.listener id addr)
  | .callback id x    => some (.callback id x)
  | .taskLeaseTimeout id      => some (.taskLeaseTimeout id)
  | .taskRetryTimeout id      => some (.taskRetryTimeout id)
  | .scheduleTimeout id      => some (.scheduleTimeout id)
  | _           => none

theorem toStep_ofStep? {st : Step} {t : InternalStep} (h : ofStep? st = some t) :
    t.toStep = st := by
  cases st <;> simp [ofStep?] at h <;> subst h <;> rfl

/-- The only non-external step that is not an internal step. -/
theorem idle_of_internal {st : Step}
    (hx : st.isExternal = false) (h : ofStep? st = none) : st = .idle := by
  cases st <;> simp [ofStep?, Step.isExternal] at h hx <;> rfl

/-- `.idle` really is a stutter: same state, and the response is silent. -/
theorem step_idle (now : Nat) (s : ServerState) :
    Abstraction.stepOf true .idle now s = (.silent, s) := rfl

/-! ## Timed schedules

A schedule is now a list of (internal step, instant) pairs. `Timed a b σ` is the
clock discipline `ValidM` imposes on it: start no earlier than `a`, never
run backwards, finish no later than `b`. Written as a fold rather than as
`List.Chain` so that the recursion matches `fireAllAt`'s. -/

def Timed : Nat → Nat → List (InternalStep × Nat) → Prop
  | a, b, []             => a ≤ b
  | a, b, (_, n) :: rest => a ≤ n ∧ Timed n b rest

theorem Timed.weaken {a a' b : Nat} {σ : List (InternalStep × Nat)}
    (h : Timed a' b σ) (hle : a ≤ a') : Timed a b σ := by
  cases σ with
  | nil => exact Nat.le_trans hle h
  | cons hd tl => exact ⟨Nat.le_trans hle h.1, h.2⟩

/-- Fire a timed schedule: each internal step at ITS OWN instant. This is the one
    place `fireAll` from `correctness.lean` was too weak — there, one
    instant served the whole schedule. -/
def fireAllAt (σ : List (InternalStep × Nat)) (s : ServerState) : ServerState :=
  σ.foldl (fun st tn => tn.1.step tn.2 st) s

@[simp] theorem fireAllAt_nil (s : ServerState) : fireAllAt [] s = s := rfl

@[simp] theorem fireAllAt_cons (t : InternalStep) (n : Nat) (σ : List (InternalStep × Nat)) (s : ServerState) :
    fireAllAt ((t, n) :: σ) s = fireAllAt σ (t.step n s) := rfl

/-! ## Admissibility, with the interval -/

/-- One observed event is explained from `s`, no earlier than `a`: some
    schedule whose instants sit in `[a, o.now]` and never run backwards
    makes `o.req` produce exactly `o.res`.

    Compare `Explains`: identical except that the schedule is timed
    rather than pinned. -/
inductive AdmissibleAt : Nat → ServerState → List Observation → Prop
  | nil  {a s} : AdmissibleAt a s []
  | cons {a s s' o rest} (σ : List (InternalStep × Nat))
      (htime : Timed a o.now σ)
      (hstep : Abstraction.stepOf true (.api o.req) o.now (fireAllAt σ s) = (o.res, s'))
      (hrest : AdmissibleAt o.now s' rest) :
      AdmissibleAt a s (o :: rest)

/-- **The search space, stated so that it is exactly `Valid`.** Starts at
    instant `0` because `ValidM` says nothing about `(tr 0).now` beyond
    its being a `Nat`. -/
def ValidExec (obs : List Observation) : Prop :=
  AdmissibleAt 0 ServerState.init obs

/-! ## Pinned is a special case

Keeping this makes the relationship to the old file explicit: everything
`ValidPinned` accepted, `ValidExec` accepts. The converse is exactly the
three defects above, and is false. -/

/-- Instants have to be monotone for a pinned schedule to be timed: a
    recording whose instants run backwards is not an execution at all, so
    `ValidPinned` accepts traces that are not `Valid`. That is not a
    soundness bug in the old file only because `Explains` is applied
    through `pinned_implies_valid`, which needs the same fact. -/
def Monotone : List Observation → Prop
  | []             => True
  | _ :: []        => True
  | a :: b :: rest => a.now ≤ b.now ∧ Monotone (b :: rest)

theorem Monotone.tail {o : Observation} {rest : List Observation}
    (h : Monotone (o :: rest)) : Monotone rest := by
  cases rest with
  | nil => trivial
  | cons hd tl => exact h.2

theorem Monotone.head {o hd : Observation} {tl : List Observation}
    (h : Monotone (o :: hd :: tl)) : o.now ≤ hd.now := h.1

theorem timed_of_pinned (o : Observation) (σ : List InternalStep) :
    ∀ a : Nat, a ≤ o.now → Timed a o.now (σ.map (fun t => (t, o.now))) := by
  induction σ with
  | nil => intro a h; exact h
  | cons hd tl ih => intro a h; exact ⟨h, ih o.now (Nat.le_refl _)⟩

theorem fireAllAt_pinned (σ : List InternalStep) (now : Nat) (s : ServerState) :
    fireAllAt (σ.map (fun t => (t, now))) s = fireAll σ now s := by
  induction σ generalizing s with
  | nil => rfl
  | cons hd tl ih => simpa [fireAll, InternalStep.step] using ih (hd.step now s)

/-- Pinned admissibility is timed admissibility, on a monotone recording. -/
theorem admissibleAt_of_admissible {s : ServerState} {obs : List Observation}
    (h : Admissible s obs) :
    ∀ a : Nat, Monotone obs → (∀ o rest, obs = o :: rest → a ≤ o.now) →
      AdmissibleAt a s obs := by
  induction h with
  | nil => intro a _ _; exact .nil
  | @cons s s' o rest hex hrest ih =>
      intro a hmono hstart
      obtain ⟨σ, hσ⟩ := hex
      have hao : a ≤ o.now := hstart o rest rfl
      refine AdmissibleAt.cons (s' := s') (σ.map (fun t => (t, o.now)))
        (timed_of_pinned o σ a hao) ?_ ?_
      · rw [fireAllAt_pinned]; exact hσ
      · refine ih o.now hmono.tail ?_
        intro o' rest' heq
        cases rest with
        | nil => cases heq
        | cons hd tl =>
            cases heq
            exact hmono.head

theorem pinned_implies_exec {obs : List Observation}
    (hmono : Monotone obs) (h : ValidPinned obs) : ValidExec obs :=
  admissibleAt_of_admissible h 0 hmono (fun _ _ _ => Nat.zero_le _)

/-! ## The equivalence

Two directions, and they carry different weight.

`exec_implies_valid` is SOUNDNESS's dependency, and replaces
`pinned_implies_valid`: interleave each event's timed schedule before it
and pad with `.idle` at the last instant. It is the same construction as
before with the instants no longer forced equal, which if anything makes
it easier — clock monotonicity now follows from `Timed` directly instead
of from the observations being ordered.

`valid_implies_exec` is the one that DELETES `InstantsSuffice`. Its
content is `segment` below: between two consecutive external steps of a
valid trace sits a block of internal steps whose instants are monotone
and bracketed, which is precisely a timed schedule. -/

/-- **The heart of the elimination.** `m` consecutive internal steps
    starting at index `k` compose into one timed schedule bracketed by
    `[a, (tr (k+m)).now]`.

    This is the statement that `Explains` could not make, because a
    pinned schedule has one instant and this block has `m` of them. -/
theorem segment {tr : Trace} (hv : ValidM tr) :
    ∀ (m k a : Nat), a ≤ (tr k).now →
      (∀ j, k ≤ j → j < k + m → (tr j).req.isExternal = false) →
      ∃ σ, Timed a (tr (k + m)).now σ ∧ fireAllAt σ (tr k).state = (tr (k + m)).state := by
  intro m
  induction m with
  | zero =>
      intro k a ha _
      exact ⟨[], ha, rfl⟩
  | succ m ih =>
      intro k a ha hint
      -- the step at `k` is internal
      have hk : (tr k).req.isExternal = false := hint k (Nat.le_refl _) (by omega)
      -- the rest of the block starts at `k+1`
      have hint' : ∀ j, k + 1 ≤ j → j < (k + 1) + m → (tr j).req.isExternal = false := by
        intro j h1 h2; exact hint j (by omega) (by omega)
      have hstate : (tr (k + 1)).state
          = (Abstraction.stepOf true (tr k).req (tr k).now (tr k).state).2 := (hv k).2.1
      have hclock : (tr k).now ≤ (tr (k + 1)).now := (hv k).2.2
      obtain ⟨σ, htime, hfire⟩ :=
        ih (k + 1) (tr (k + 1)).now (Nat.le_refl _) hint'
      have hidx : (k + 1) + m = k + (m + 1) := by omega
      rw [hidx] at htime hfire
      cases hof : ofStep? (tr k).req with
      | none =>
          -- a stutter: the block is the same schedule, started earlier
          have : (tr k).req = .idle := idle_of_internal hk hof
          refine ⟨σ, htime.weaken (Nat.le_trans ha hclock), ?_⟩
          rw [← hfire, hstate, this, step_idle]
      | some t =>
          refine ⟨(t, (tr k).now) :: σ, ⟨ha, htime.weaken hclock⟩, ?_⟩
          rw [fireAllAt_cons, ← hfire, hstate]
          congr 1
          simp [InternalStep.step, toStep_ofStep? hof]

/-- **SOUNDNESS's half.** Every timed explanation is an execution the
    specification calls valid. -/
theorem exec_implies_valid {obs : List Observation} (h : ValidExec obs) : Valid obs := by
  sorry

/-! ### From the placement `φ` to a list induction

`ExternalsAre` places the i-th observation at trace index `φ i`. Three
facts turn that into an induction over the observation list. -/

theorem phi_mono_le {φ : Nat → Nat} (hφ : ∀ i j, i < j → φ i < φ j) {i j : Nat}
    (h : i ≤ j) : φ i ≤ φ j := by
  rcases Nat.lt_or_ge i j with hlt | hge
  · exact Nat.le_of_lt (hφ i j hlt)
  · have : i = j := Nat.le_antisymm h hge
    subst this; exact Nat.le_refl _

theorem phi_reflect {φ : Nat → Nat} (hφ : ∀ i j, i < j → φ i < φ j) {i j : Nat}
    (h : φ i < φ j) : i < j := by
  rcases Nat.lt_or_ge i j with hlt | hge
  · exact hlt
  · exact absurd (phi_mono_le hφ hge) (Nat.not_le_of_lt h)

/-- **No external step hides between two placements.** This is what the
    last clause of `ExternalsAre` buys: the recording missed nothing, so
    everything strictly between `φ i` and `φ (i+1)` is internal, and
    `segment` applies to it. -/
theorem no_external_between {tr : Trace} {φ : Nat → Nat}
    (hφ : ∀ i j, i < j → φ i < φ j)
    (hsurj : ∀ s, (tr s).req.isExternal = true → ∃ i, φ i = s)
    {i j : Nat} (h1 : φ i < j) (h2 : j < φ (i + 1)) :
    (tr j).req.isExternal = false := by
  cases hx : (tr j).req.isExternal with
  | false => rfl
  | true =>
      obtain ⟨m, hm⟩ := hsurj j hx
      subst hm
      have hlo : i < m := phi_reflect hφ h1
      have hhi : m < i + 1 := phi_reflect hφ h2
      exact absurd hlo (by omega)

/-- Before the first placement there is nothing external either. -/
theorem no_external_before {tr : Trace} {φ : Nat → Nat}
    (hφ : ∀ i j, i < j → φ i < φ j)
    (hsurj : ∀ s, (tr s).req.isExternal = true → ∃ i, φ i = s)
    {j : Nat} (h : j < φ 0) : (tr j).req.isExternal = false := by
  cases hx : (tr j).req.isExternal with
  | false => rfl
  | true =>
      obtain ⟨m, hm⟩ := hsurj j hx
      subst hm
      exact absurd (phi_reflect hφ h) (by omega)

/-- The induction that consumes one observation at a time. `k` is where
    the trace has got to, `a` the instant it may not fire before, and `i`
    the index of the next observation. -/
theorem walk {tr : Trace} (hv : ValidM tr) {φ : Nat → Nat}
    (hφ : ∀ i j, i < j → φ i < φ j)
    (hsurj : ∀ s, (tr s).req.isExternal = true → ∃ i, φ i = s) :
    ∀ (rest : List Observation) (i k a : Nat),
      k ≤ φ i → a ≤ (tr k).now →
      (∀ j, k ≤ j → j < φ i → (tr j).req.isExternal = false) →
      (∀ n, (hn : n < rest.length) →
         (tr (φ (i + n))).req = .api (rest[n]'hn).req ∧
         (tr (φ (i + n))).res = (rest[n]'hn).res ∧
         (tr (φ (i + n))).now = (rest[n]'hn).now) →
      AdmissibleAt a (tr k).state rest := by
  intro rest
  induction rest with
  | nil => intro _ _ _ _ _ _ _; exact .nil
  | cons o rest' ih =>
      intro i k a hki ha hint hmatch
      -- the block of internal steps from `k` up to the placement of `o`
      have hm : k + (φ i - k) = φ i := by omega
      obtain ⟨σ, htime, hfire⟩ :=
        segment hv (φ i - k) k a ha (by intro j h1 h2; exact hint j h1 (by omega))
      rw [hm] at htime hfire
      -- `o` sits at index `φ i`
      obtain ⟨hreq, hres, hnow⟩ := hmatch 0 (by simp)
      simp only [Nat.add_zero, List.getElem_cons_zero] at hreq hres hnow
      have hstep : Abstraction.stepOf true (.api o.req) o.now (fireAllAt σ (tr k).state)
          = (o.res, (tr (φ i + 1)).state) := by
        rw [hfire, ← hreq, ← hnow, ← hres, (hv (φ i)).1, (hv (φ i)).2.1]
      refine AdmissibleAt.cons σ (hnow ▸ htime) hstep ?_
      -- and the rest starts one step later, no earlier than `o.now`
      refine ih (i + 1) (φ i + 1) o.now
        (hφ i (i + 1) (by omega)) ?_ ?_ ?_
      · rw [← hnow]; exact (hv (φ i)).2.2
      · intro j h1 h2; exact no_external_between hφ hsurj (by omega) h2
      · intro n hn
        have hidx : i + 1 + n = i + (n + 1) := by omega
        rw [hidx]
        exact hmatch (n + 1) (by simpa using hn)

/-- **COMPLETENESS's half — and the whole point of this file.** Every
    execution the specification calls valid IS a timed explanation. No
    hypothesis: the schedule is read off the trace by `segment`, and the
    placement bookkeeping is `walk`.

    This is the theorem `InstantsSuffice` was standing in for, and the
    reason it can be deleted rather than discharged. -/
theorem valid_implies_exec {obs : List Observation} (h : Valid obs) : ValidExec obs := by
  obtain ⟨tr, hv, hinit, φ, hφ, hmatch, hsurj⟩ := h
  have hsurj' : ∀ s, (tr s).req.isExternal = true → ∃ i, φ i = s := by
    intro s hs; obtain ⟨i, _, hi⟩ := hsurj s hs; exact ⟨i, hi⟩
  have := walk hv hφ hsurj' obs 0 0 0 (Nat.zero_le _) (Nat.zero_le _)
    (fun j _ h2 => no_external_before hφ hsurj' h2)
    (fun n hn => by
      obtain ⟨_, hr, hs, hnw⟩ := hmatch n hn
      simpa using ⟨hr, hs, hnw⟩)
  rwa [hinit] at this

theorem valid_iff_exec (obs : List Observation) : Valid obs ↔ ValidExec obs :=
  ⟨valid_implies_exec, exec_implies_valid⟩

end TraceCheck.Executions
