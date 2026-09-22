import «valid».«lean».executions

/-!  # `InstantsSuffice` is not merely unproved — it is not provable

`valid/lean/correctness.lean` records `InstantsSuffice` as "unproved and
possibly FALSE", and `valid/instants.lean` hunted for a counterexample
among promise timeouts, leases and resumes and found none. That hunt was
looking in the wrong place, and the reason is structural rather than
empirical.

## Where the instant escapes

Every other handler's use of `now` is a COMPARISON against a number
already stored in the state, and every write it performs is stamped with
a stored number rather than with `now`. `touchPromise` is the clearest
case: a promise past its deadline is settled `at p.timeoutAt`, not at
`now` — "born settled, exactly as if it had been created on time". So the
instant at which the internal step fired leaves no trace in the state.

`processSchedule` breaks that discipline, in one line:

```lean
let ts := (occurrences s0.cron s0.nextRunAt now).filter (· ≤ now)
…
| some last => { s0 with lastRunAt := some last, nextRunAt := nextCron s0.cron last }
```

`now` is passed as an ARGUMENT to `occurrences`, and the result is
written into `lastRunAt` and `nextRunAt`. Those two fields are not
internal bookkeeping: `Schedule` is returned whole by `scheduleGet`,
`scheduleCreate` and `scheduleSearch` (`01-protocol/types.lean`:
`ScheduleGetRes.schedule : Option Schedule`). So the instant at which a
schedule timeout fired is DIRECTLY READABLE by a client.

That is the counterexample, and it is not subtle. Fire the timeout inside
a gap that contains one cron occurrence but not the next; the checker,
able to act only at the following observation, has by then swept up the
next occurrence too, and answers `scheduleGet` with a different
`lastRunAt`.

## Why it cannot be proved either way IN LEAN

`nextCron`, `occurrences` and `expand` are declared `opaque` with no
value (`01-protocol/validation.lean`). An opaque constant has no defining
equation, so in the logic they are ARBITRARY functions of their types.
Consequently:

* `InstantsSuffice` is NOT PROVABLE: instantiate `occurrences` with any
  faithful cron and `CronDiscriminates` below is satisfiable, and
  `instants_insufficient` refutes it.
* `¬ InstantsSuffice` is NOT PROVABLE EITHER: instantiate `occurrences`
  with `fun _ _ _ => []` — which is what the compiled executable
  actually uses, since an opaque constant compiles to the type's default
  — and every schedule timeout becomes instant-insensitive.

So `InstantsSuffice` is independent of the specification as written. No
amount of searching decides it. That is the verdict on Route B, and it is
why `valid/instants.lean`'s "0 counterexamples" was never going to be
evidence one way or the other: its own header says "(3) is untestable
here — `occurrences` and `nextCron` are `opaque`, so a schedule script
does not execute at all", and then it draws a conclusion as if the
untestable case did not exist.

(The header is also wrong on the mechanism: schedule scripts DO execute,
because `opaque` compiles to `default`. `nextCron` returns `0`,
`occurrences` returns `[]`, `expand` returns `""`. So the sweep was not
skipping schedules; it was silently testing a calendar in which nothing
ever happens.)

## What this file is for

Not to prove the specification wrong — a cron that fires is correct
behaviour. It is to fix the SCOPE of any completeness claim. Two
responses are available and both are honest:

1. **Restrict.** `valid/lean/intervals.lean` declines any trace that
   mentions a schedule. Completeness is then unconditional on the traces
   the checker judges, which is the discipline the file already applies
   to fuel and to the candidate cap.
2. **Axiomatise.** State a shell-side axiom about `occurrences`. The
   minimal one that rescues the reduction is stated as `CronPinned`
   below — and it says the calendar is blind to any instant between two
   observations, which is exactly what a cron is not. It is recorded here
   so that nobody proposes it a second time. -/

namespace TraceCheck.Schedules

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Correctness TraceCheck.Executions

/-- The occurrence a firing at `now` settles on: the last one in the
    window, which is what `processSchedule` writes into `lastRunAt`. -/
def lastOcc (cron : String) (since now : Nat) : Option Nat :=
  ((occurrences cron since now).filter (· ≤ now)).getLast?

/-! ## What a real calendar does

Every clause is a property of an ordinary cron expression with a period
shorter than the gap between two observations — "* * * * *" and a gap of
a few minutes will do. None of them is exotic; together they say only
that the calendar can tell two instants apart. -/

structure CronDiscriminates where
  cron : String
  /-- when the schedule is created, when the internal step really fires, and when the
      client next looks. -/
  t0 : Nat
  tg : Nat
  t1 : Nat
  hgap : t0 ≤ tg
  hlt  : tg < t1
  /-- the schedule is due by `tg` -/
  hdue : nextCron cron t0 ≤ tg
  /-- firing at `tg` lands on some occurrence … -/
  hsome : (lastOcc cron (nextCron cron t0) tg) ≠ none
  /-- … and firing at `t1` lands on a DIFFERENT one, because another
      occurrence fell in `(tg, t1]`. This is the whole content. -/
  hdiff : lastOcc cron (nextCron cron t0) tg ≠ lastOcc cron (nextCron cron t0) t1
  /-- having fired once at `t1`, the schedule is not due again at `t1`,
      so the checker cannot iterate its way to the missing state. -/
  hstop : ∀ u : Nat, lastOcc cron (nextCron cron t0) t1 = some u → t1 < nextCron cron u

/-! ## The counterexample

Three steps: create the schedule, let its timeout fire IN THE GAP, then
read the schedule back. Recording it keeps the two external events; the internal step
is thrown away, and the checker has to put it back. It cannot put it back
at `g`, because `Explains` fires everything at the observation's own
instant, and at `t1` the calendar has moved on. -/

def sid : ServerModel.Ident := { origin := "o", suffix := "s" }

def script (H : CronDiscriminates) : List (Step × Nat) :=
  [ (.external (.scheduleCreate { id := sid, cron := H.cron, promiseId := { origin := "o", suffix := "p" },
                                  promiseTimeout := 1000, promiseParam := {}, promiseTags := [] }), H.t0)
  , (.internal (.scheduleTimeout { schedule := sid }), H.tg)
  , (.external (.scheduleGet { id := sid }), H.t1) ]

def obs (H : CronDiscriminates) : List Observation := recordFrom (script H)

/-! ## Firing anything at `init` is a no-op

Needed to rule out the checker doing something clever before the first
observation, and true for a structural reason: every internal step is
OBLIGATION-GUARDED, and `init` carries no obligations. -/

theorem internal_step_init (t : InternalStep) (n : Nat) :
    t.step n ServerState.init = ServerState.init := by
  cases t <;> rfl

theorem fireAll_init (σ : List InternalStep) (n : Nat) :
    fireAll σ n ServerState.init = ServerState.init := by
  induction σ with
  | nil => rfl
  | cons hd tl ih => simpa [fireAll, internal_step_init hd n] using ih

theorem fireAllAt_init (σ : List (InternalStep × Nat)) :
    fireAllAt σ ServerState.init = ServerState.init := by
  induction σ with
  | nil => rfl
  | cons hd tl ih => simpa [fireAllAt, internal_step_init hd.1 hd.2] using ih

/-! ## The two halves -/

/-- **The recording IS valid** — the script is its own witness, and its
    instants are monotone by `hgap` and `hlt`. This is `recordFrom`'s
    contract, stated as a lemma instead of as a comment.

    Depends on `exec_implies_valid`, which is the soundness bridge in
    `executions.lean` and is still `sorry`. -/
theorem obs_valid (H : CronDiscriminates) : Valid (obs H) := by
  sorry

/-- **The pinned search cannot reproduce it.** The only schedule states a
    pinned schedule can reach at `t1` are the created one and the one
    that firing AT `t1` produces; `hdiff` says the recorded `lastRunAt` is
    neither.

    The remaining work is an invariant over an arbitrary `σ : List InternalStep`:
    no internal step other than `scheduleTimeout sid` writes `schedules` at all, and
    `scheduleTimeout sid` is idempotent at `t1` by `hstop`. -/
theorem obs_not_pinned (H : CronDiscriminates) : ¬ ValidPinned (obs H) := by
  sorry

/-- **Route B is dead.** Given any calendar that can tell two instants
    apart, `InstantsSuffice` is false. -/
theorem instants_insufficient (H : CronDiscriminates) : ¬ InstantsSuffice :=
  fun hgap => obs_not_pinned H (hgap (obs H) (obs_valid H))

/-! ## The axiom that would rescue it, and why not to adopt it

If one insisted on keeping schedules inside the checker's scope, the
minimal spec-side assumption is this. It is stated to be REJECTED, not
adopted. -/

/-- The calendar cannot distinguish two instants in the same gap: every
    firing in `[a, b]` settles on the same occurrence as a firing at `b`.

    This is not a weak assumption dressed up. Read it directly: it says
    that between any two consecutive client requests, at most one cron
    occurrence exists AND it is already visible at the later instant — in
    other words, that no cron ever fires strictly between two
    observations in a way the later observation cannot reconstruct. A
    once-a-minute schedule and a client that polls hourly violate it
    immediately.

    Adopting it would make the checker's verdicts true only of servers
    whose calendars are degenerate — which is worse than declining
    schedule traces, because the decline is visible in the verdict and
    the axiom is not. -/
def CronPinned : Prop :=
  ∀ (cron : String) (since a b n : Nat), a ≤ n → n ≤ b →
    lastOcc cron since n = lastOcc cron since b

theorem cronPinned_kills_the_counterexample (h : CronPinned) (H : CronDiscriminates) :
    False :=
  H.hdiff (h H.cron (nextCron H.cron H.t0) H.tg H.t1 H.tg (Nat.le_refl _)
            (Nat.le_of_lt H.hlt))

end TraceCheck.Schedules
