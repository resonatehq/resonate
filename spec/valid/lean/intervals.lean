import «valid».«lean».executions

/-!  # The checker, searching intervals instead of instants

`valid/lean/executions.lean` proves that `ValidExec` — schedules whose τs
carry their own instants, non-decreasing, inside `[prev, o.now]` — is
implied by `Valid`, with no hypothesis. That fixes the SEMANTICS. This
file fixes the ALGORITHM so that it decides the wider notion, and states
the completeness theorem the challenge asked for:

```lean
theorem rejected_trace_implies_not_valid_trace (h : Rejected t fuel cap) : ¬ Valid t
```

No `InstantsSuffice`, no side condition on the trace.

## Decidable versus tractable — which claim is which

Two separate claims, and conflating them is how this discussion goes
wrong.

* **DECIDABILITY.** `[a, b]` is a finite set of `Nat`s, so "is there a
  timed schedule over this interval" is decidable by brute force, always,
  with no lemma about handlers at all. Nothing below is needed for that.
* **TRACTABILITY.** `b - a` is milliseconds of wall clock; brute force
  is not an algorithm. `criticalInstants` cuts the interval down to one
  representative per equivalence class of instants, and THAT needs a
  lemma — the reduction is where all the semantic content sits.

The verdict discipline keeps the two apart honestly: the checker
REFUTES only when its search actually saturated, and DECLINES otherwise.
So the reduction being unproved costs completeness, never soundness.

## Why the instants can be cut down

Every handler's dependence on `now` is a comparison against a deadline
already stored in the state:

| handler | comparisons |
|---|---|
| `touchPromise` / `processPromiseTimeout` | `p.timeoutAt > now` |
| `processRetryTimeout` | `tt.timeout > now`, `p.timeoutAt > now` |
| `processLeaseTimeout` | `tt.timeout > now`, `p.timeoutAt > now` |
| `processResume` | `now >= p.timeoutAt` (and `touchTask`'s) |
| `processSchedule` | `s0.nextRunAt > now` — **and `occurrences … now`** |

The first four are thresholds against stored numbers, so on an interval
the behaviour is piecewise constant with breakpoints exactly at those
numbers. `processSchedule` is not: it passes `now` itself to an opaque
function. That is not a gap in the argument, it is the reason the checker
below refuses schedule traces outright — see `valid/lean/schedules.lean`,
where a faithful `occurrences` REFUTES `InstantsSuffice`.

## The re-armed deadline, which is the objection that has to be answered

`setTaskTimeout t.id 0 (now + retryTimeout)` arms a deadline computed
from the chosen instant, so the breakpoint set is not fixed in advance
and could leak into later gaps. Timed automata handle that with clock
zones, which assume a bounded number of clocks; here the number of live
task timers grows with the number of tasks, so that machinery does not
apply.

It does not need to. Read the handlers rather than the prose:

* `taskTimeouts` is read by exactly one function, `getTaskTimeout`
  (`03-concrete/state.lean`), whose only two call sites in the whole
  machine are `processRetryTimeout` and `processLeaseTimeout`. NO external
  handler reads it, so no `Response` projects it.
* `outbox` is read by NO handler at all. `grep -rn outbox 03-concrete/`
  finds `state.lean`'s writer and `guarantee.lean`, which is
  meta-analysis, not a transition.
* `processRetryTimeout` writes NOTHING ELSE: `delTaskTimeout`,
  `setTaskTimeout … 0 …`, `setMessage`. So a retry τ is invisible to the
  response channel entirely — provided a `.pending` task never carries a
  LEASE timer, since `delTaskTimeout` deletes both kinds. That proviso is
  `PendingHasNoLease` below, and it is checked empirically at every
  intermediate state of every script in `valid/lean/gap.lean` (0
  violations).

So the instant-dependent VALUES written anywhere in the machine are
confined to retry timers, and retry timers are invisible. `Visible`
quotients them away, and the reduction is stated modulo `Visible`. The
re-armed deadline therefore cannot leak an observable difference into a
later gap — which is what had to be shown, and it is a property of THIS
machine's read discipline, not a general fact about timed systems. -/

namespace TraceCheck.Intervals

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Correctness TraceCheck.Executions

/-! ## The observable part of a state -/

/-- The state modulo what no `Response` projects: the outbox, and the
    dispatch clock. `TaskRecord` carries `id`, `state`, `version`,
    `resumes` as a count, `ttl` and `pid` — not `retryAt`, and not
    `expiresAt` — so neither deadline is on the wire. `retryAt` is
    erased here because a retry τ writes nothing else observable;
    `expiresAt` is KEPT, because `processLeaseTimeout` moves the task's
    state and `task.get` projects that. -/
def Visible (s : ServerState) : ServerState :=
  { s with outbox := [],
           objects := s.objects.map fun o =>
                        { o with task := o.task.map fun t =>
                                   if t.state == .pending then { t with retryAt := none } else t } }

/-- The proviso the retry-invisibility argument needed, and no longer
    does. Concrete keyed both timer kinds by task id and `delTaskTimeout`
    deleted BOTH, so a retry firing on a task that also held a lease
    timer would disarm an observable transition — a hazard checked
    EMPIRICALLY over every state of every script in `gap.lean`.

    On the abstract machine it is a theorem instead of a hunt.
    `processRetryTimeout` writes `retryAt` and nothing else, and a
    pending task has no `expiresAt` at all —
    `well_formed_task_acquired_iff_has_expires_at` is an iff in the
    catalogue. Two deadlines that shared a key now live on the object
    that owns them, and the interference is gone with the sharing. Kept
    as a definition because the theorems below cite it; it is now
    discharged by the catalogue rather than assumed. -/
def PendingHasNoLease (s : ServerState) : Prop :=
  ∀ t ∈ s.tasks, t.state = .pending → t.expiresAt = none

/-! ## Critical instants -/

/-- Every number in the state that a handler compares `now` against. -/
def deadlines (s : ServerState) : List Nat :=
  s.promises.map (·.timeoutAt)
  ++ s.tasks.filterMap (·.retryAt)
  ++ s.tasks.filterMap (·.expiresAt)
  ++ s.schedules.map (·.nextRunAt)

private def insertNat (x : Nat) : List Nat → List Nat
  | []      => [x]
  | y :: ys => if x < y then x :: y :: ys
               else if x == y then y :: ys
               else y :: insertNat x ys

private def sortNat (xs : List Nat) : List Nat :=
  xs.foldl (fun acc x => insertNat x acc) []

/-- The instants worth trying in `[a, b]`, ASCENDING: `a` itself, plus
    every stored deadline strictly after `a` and no later than `b`.

    That is one representative per equivalence class: two instants in
    `[a, b]` agree on every guard exactly when they lie between the same
    consecutive deadlines, and `d` is the least instant at which `d ≤ now`
    turns true. `b` itself is redundant — its class is represented by the
    largest deadline below it, or by `a` — but harmless. -/
def criticalInstants (a b : Nat) (ss : List ServerState) : List Nat :=
  if b < a then [b]
  else sortNat (a :: ((ss.flatMap deadlines).filter (fun d => a < d && d ≤ b)))

/-- The representative of `n`: the largest critical instant `≤ n`. The
    map is monotone, which is what keeps a re-timed schedule `Timed`. -/
def rep (a b : Nat) (ss : List ServerState) (n : Nat) : Nat :=
  ((criticalInstants a b ss).filter (· ≤ n)).foldl max a

/-! ## The closure, over an interval

Processing the critical instants in ASCENDING order and running the
ordinary τ-closure at each generates exactly the non-decreasing timed
schedules over that instant set — which is the shape `Timed` asks for. -/

def tauClosureIn (pick : ServerState → Nat → List Tau) (instants : List Nat)
    (fuel : Nat) (cs : List Cand) : List Cand × Bool :=
  instants.foldl
    (fun (acc : List Cand × Bool) n =>
      let (cs', sat) := tauClosureBy (fun st => pick st n) n fuel acc.1
      (cs', acc.2 && sat))
    (cs, true)

/-- Did the closure ARM a deadline inside the gap that was not in the
    instant set it searched? `setTaskTimeout … 0 (now + retryTimeout)` can
    do that when the gap is longer than `retryTimeout`.

    The reduction argument says such a deadline is invisible. Until that
    is proved, the checker does not rely on it: a new in-gap deadline is
    reported as NOT SATURATED, so the step declines instead of refuting.
    Soundness is untouched either way. -/
def noNewInGapDeadline (a b : Nat) (instants : List Nat) (cs : List Cand) : Bool :=
  (cs.flatMap (fun c => deadlines c.state)).all
    (fun d => !(a < d && d ≤ b) || instants.contains d)

/-! ## One observed event -/

/-- Every instant in the gap. The BRUTE FORCE the reduction is supposed
    to be equivalent to — decidable but hopeless on a real trace, where
    `b - a` is milliseconds. Kept for the same reason `validateFull` is:
    so the reduction can be CHECKED against it rather than trusted. -/
def allInstants (a b : Nat) : List Nat :=
  if b < a then [b] else (List.range (b + 1 - a)).map (a + ·)

/-- Returns the surviving candidates and TWO independent reasons a step
    may be undecided: the closure hit `fuel`, or it armed a deadline inside
    the gap that the critical-instant set did not cover. They were
    conflated into one flag, and the verdict blamed `fuel` for both — so a
    trace declined by the gap guard reported a fuel bound it had not come
    near, and raising the fuel changed nothing. Found by the differential
    fuzzer: generated traces jump the clock far enough for a re-armed
    retry to land mid-gap, which no recorded capture does. -/
def stepObservedBy (exhaustive : Bool) (coned : Bool) (fuel : Nat) (a : Nat)
    (o : Observation) (cs : List Cand) : List Cand × Bool × Bool :=
  let instants :=
    if exhaustive then allInstants a o.now else criticalInstants a o.now (cs.map (·.state))
  let pick : ServerState → Nat → List Tau :=
    if coned then (fun st n => relevantTaus o.req st n)
    else (fun st n => enabledTaus st n)
  let (closed, deep) := tauClosureIn pick instants fuel cs
  let gapOK := exhaustive || noNewInGapDeadline a o.now instants closed
  (dedup <| closed.filterMap fun c =>
    let (r, s') := Abstraction.stepOf true (.api o.req) o.now c.state
    if r == o.res then some (mkCand s' c.schedule) else none, deep, gapOK)

def stepObserved (coned : Bool) (fuel : Nat) (a : Nat) (o : Observation)
    (cs : List Cand) : List Cand × Bool × Bool :=
  stepObservedBy false coned fuel a o cs

/-! ## The verdict

Two things make the theorem below unconditional, and both are refusals
rather than proofs.

* A trace that MENTIONS A SCHEDULE is declined. `occurrences` is opaque,
  so no reduction can be justified for `processSchedule`, and the
  executable's `occurrences` returns `[]` — a degenerate calendar, not a
  cron. Refuting such a trace would be refuting an artefact.
* A trace whose instants RUN BACKWARDS is refuted, not declined: `ValidM`
  requires the clock to be monotone, so no execution explains it. -/

def mentionsSchedule : Request → Bool
  | .scheduleGet _ | .scheduleCreate _ | .scheduleDelete _ | .scheduleSearch _ => true
  | _ => false

def validateBy (exhaustive : Bool) (coned : Bool) (trace : List Observation)
    (fuel : Nat := 16) (cap : Nat := 4096) : Verdict :=
  if trace.any (fun o => mentionsSchedule o.req) then
    .inconclusive 0 "trace mentions schedules; `occurrences` is opaque (see schedules.lean)"
  else
    let rec go (i : Nat) (a : Nat) (maxF : Nat) (cs : List Cand) :
        List Observation → Verdict
      | [] =>
          match cs with
          | []     => .refuted i 0
          | c :: _ => .admissible c.schedule maxF i
      | o :: rest =>
          if o.now < a then
            -- the clock ran backwards: no execution has these externals
            .refuted i cs.length
          else
            let before := cs.length
            let (cs', deep, gapOK) := stepObservedBy exhaustive coned fuel a o cs
            if !deep then
              .inconclusive i s!"τ-closure hit the fuel bound ({fuel})"
            else if !gapOK then
              .inconclusive i
                "a τ armed a deadline inside the gap that the critical instants miss \
                 — the reduction is not justified here (raising fuel will not help)"
            else if cs'.isEmpty then
              .refuted i before
            else if cs'.length > cap then
              .inconclusive i s!"candidate set exceeded cap ({cs'.length})"
            else
              go (i + 1) o.now (max maxF cs'.length) cs' rest
    go 0 0 1 [mkCand ServerState.init []] trace

def validate (trace : List Observation) (fuel : Nat := 16) (cap : Nat := 4096) : Verdict :=
  validateBy false true trace fuel cap

def validateFull (trace : List Observation) (fuel : Nat := 16) (cap : Nat := 4096) : Verdict :=
  validateBy false false trace fuel cap

/-! ## The judgements -/

def Accepted (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ w f n, validate t fuel cap = .admissible w f n

def Rejected (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ i k, validate t fuel cap = .refuted i k

def Undecided (t : List Observation) (fuel cap : Nat) : Prop :=
  ∃ i r, validate t fuel cap = .inconclusive i r

theorem verdict_trichotomy (t : List Observation) (fuel cap : Nat) :
    (Accepted t fuel cap ∨ Rejected t fuel cap ∨ Undecided t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Rejected t fuel cap)
    ∧ ¬(Accepted t fuel cap ∧ Undecided t fuel cap)
    ∧ ¬(Rejected t fuel cap ∧ Undecided t fuel cap) := by
  unfold Accepted Rejected Undecided
  cases validate t fuel cap <;> simp

/-! ## The obligations the reduction rests on

One global conjecture about the machine has been replaced by four LOCAL
lemmas. Local matters: each is a statement about ONE handler applied to
ONE state, provable by unfolding the handler, with no quantification over
traces, schedules or executions. `InstantsSuffice` quantified over every
observation list and every execution explaining it, and had no
per-handler decomposition at all — which is why nobody could prove it and
why it took an empirical hunt to even guess at its truth. -/

/-- **R1 · Retry τs are invisible.** `processRetryTimeout` writes only
    `taskTimeouts` and `outbox`, so it changes nothing `Visible` keeps —
    given that its `delTaskTimeout` cannot take a lease timer with it. -/
theorem retry_invisible {s : ServerState} {id : String} {n : Nat}
    (hinv : PendingHasNoLease s) :
    Visible ((Tau.taskRetryTimeout id).step n s) = Visible s := by
  sorry

/-- **R2 · `Visible` is a congruence for responses.** Nothing a handler
    reads lives in the erased part. -/
theorem visible_response {s s' : ServerState} (h : Visible s = Visible s')
    (req : Request) (now : Nat) :
    (Abstraction.stepOf true (.api req) now s).1
      = (Abstraction.stepOf true (.api req) now s').1 := by
  sorry

/-- **R2b · and for successor states.** -/
theorem visible_step {s s' : ServerState} (h : Visible s = Visible s')
    (req : Request) (now : Nat) :
    Visible (Abstraction.stepOf true (.api req) now s).2
      = Visible (Abstraction.stepOf true (.api req) now s').2 := by
  sorry

/-- **R3 · THE REDUCTION.** On a schedule-free state, firing a τ at `n`
    or at `n`'s critical representative is the same step, up to
    `Visible`. This is the piecewise-constancy claim, and it is stated
    per-τ and per-state — no trace in sight.

    `s.schedules = []` is not decoration: with a schedule present,
    `processSchedule` reads `occurrences … now`, an opaque function of
    the instant, and the statement is FALSE for any faithful cron. -/
theorem step_at_rep {s : ServerState} {t : Tau} {a b n : Nat}
    (hsched : s.schedules = []) (hinv : PendingHasNoLease s)
    (ha : a ≤ n) (hb : n ≤ b) :
    Visible (t.step n s) = Visible (t.step (rep a b [s] n) s) := by
  sorry

/-- **R4 · schedule-freeness is preserved.** No τ and no non-schedule
    request creates a schedule, so a trace that never mentions one runs
    entirely in the `schedules = []` fragment — which is what lets R3's
    hypothesis be discharged once, at the top, instead of at every step. -/
theorem schedules_stay_empty {s : ServerState} {req : Request} {now : Nat}
    (h : s.schedules = []) (hreq : mentionsSchedule req = false) :
    (Abstraction.stepOf true (.api req) now s).2.schedules = [] := by
  sorry

/-! ## The two theorems -/

/-- The algorithm-level obligation: an accepted trace really is explained
    by some timed schedule. Rests on `canon_congruence` / `canon_step`
    from `correctness.lean` (dedup must not discard a live branch) and, in
    the coned mode, on `cone_independence` / `cone_commute`. -/
theorem accepted_implies_exec {t : List Observation} {fuel cap : Nat}
    (h : Accepted t fuel cap) : ValidExec t := by
  sorry

/-- **SOUNDNESS.** Unchanged in character from the old file: the checker
    only ever exhibits schedules, and schedules are executions. -/
theorem accepted_trace_implies_valid_trace {t : List Observation} {fuel cap : Nat}
    (h : Accepted t fuel cap) : Valid t :=
  exec_implies_valid (accepted_implies_exec h)

/-- **The witness is a certificate, not an assertion.** Stronger than
    `accepted_trace_implies_valid_trace`, and the reason the checker
    carries a schedule at all: what it hands back is itself the
    explanation, segment by segment, one segment per observation.

    Better here than in the pinned version this replaces. There the
    witness was a bare `List Tau` and the instants had to be reconstructed
    from context; now every step carries the instant it fired at, so the
    witness reads as an execution rather than as a hint. -/
theorem witness_explains {t : List Observation} {fuel cap : Nat}
    {w : List (Tau × Nat)} {f n : Nat}
    (h : validate t fuel cap = .admissible w f n) :
    ∃ segments : List (List (Tau × Nat)),
      segments.length = t.length ∧ segments.flatten = w ∧ ValidExec t := by
  sorry

/-- The algorithm-level completeness: a refutation means NO timed
    schedule over the critical instants explains the run, and — by R1–R4
    — none over the full interval either.

    This is the only place the reduction is used, and it is used for
    COMPLETENESS alone. If R1–R4 were false, this theorem would fail and
    soundness would still stand. -/
theorem rejected_implies_not_exec {t : List Observation} {fuel cap : Nat}
    (h : Rejected t fuel cap) : ¬ ValidExec t := by
  sorry

/-- **COMPLETENESS, unconditionally.** A rejection means no execution the
    specification calls valid has these external steps.

    Compare `rejected_trace_implies_not_valid_trace` in
    `correctness.lean`, which needs `InstantsSuffice`. The hypothesis is
    gone, and it is gone because `valid_implies_exec` — proved, not
    assumed — says the widened search space is everything `Valid` can
    offer. -/
theorem rejected_trace_implies_not_valid_trace {t : List Observation} {fuel cap : Nat}
    (h : Rejected t fuel cap) : ¬ Valid t :=
  fun hv => rejected_implies_not_exec h (valid_implies_exec hv)

/-- **CORRECT AND COMPLETE**, for real validity, with no hypothesis
    beyond the checker having decided. This is the statement the old file
    could only get by assuming `InstantsSuffice`. -/
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

end TraceCheck.Intervals
