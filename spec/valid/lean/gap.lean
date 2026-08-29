import «valid».«lean».intervals

/-!  # The gap, measured properly

`valid/instants.lean` asked whether firing an internal step off the observation
grid can break the pinned checker, and answered "4112 scripts, 0
counterexamples". Two things are wrong with that answer as EVIDENCE, and
this file fixes both before re-running.

## Defect 1 — the coverage metric measures the wrong thing

`effectiveInternalSteps` counts off-grid internal steps that CHANGED THE STATE. That is not
the question. A internal step that changes the state identically whether it fires at
`n` or at the instant the checker would use is not a test of anything: the
checker reproduces it exactly. What matters is an internal step whose effect DIFFERS
from firing the same internal step, from the same state, at the PINNED instant.

`discriminating` below counts those. It is strictly smaller, and the
difference is the whole worth of the experiment.

## Defect 2 — the pinned instant is not the next grid point

`Admissible` fires each event's schedule BEFORE that event, at that
event's instant. So the instant the checker would use for an internal
step is the instant of the NEXT EXTERNAL STEP IN THE SCRIPT — not the
next larger grid value. Those differ exactly when an internal step fires at an
observation's own instant but AFTER it: instant `10` is on the grid, yet
an internal step written after the `@10` request is attributed by the checker to the
next observation, which may be at `60`.

So `ValidPinned` is not even the "internal steps at observation instants" fragment of
`Valid`. It is narrower than that. `pinnedInstant` computes the instant
the checker would actually have to use, and the sweeps below place internal steps AT
observation instants as well as strictly between them.

## What is invisible, and why that is the sharpest metric

No handler reads `outbox` — `grep -rn outbox 03-concrete/` finds only
writes plus `guarantee.lean`, which is meta-analysis, not a handler. And
`taskTimeouts` is read only by `getTaskTimeout`, whose only two call
sites are `processRetryTimeout` and `processLeaseTimeout`. So a difference
confined to `outbox` and to retry timers cannot reach a `Response`.

`visible` erases exactly that much. A internal step that discriminates only under
`canon` but not under `visible ∘ canon` is a difference the response
channel provably cannot see; an internal step that discriminates under `visible` is a
live lead. Counting both is what separates "0 counterexamples because
nothing was tested" from "0 counterexamples because the machine is
robust". -/

namespace TraceCheck.Gap

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Intervals

/-- The reduction's reference implementation: every instant in every gap,
    every enabled internal step. `validate` claims to be equivalent to this — that
    claim is R3 — so it lives here, in the harness that checks it, rather
    than in the checker's API. Only usable on toy instants. -/
def validateBrute (trace : List Observation)
    (fuel : Nat := 16) (cap : Nat := 4096) : Verdict :=
  validateBy true false trace fuel cap

/-! ## Projections -/

/-- The state modulo what no response can project: the outbox, and the
    RETRY timers (`kind = 0`), which only the retry internal step's own guard reads.
    Lease timers (`kind = 1`) are kept — `processLeaseTimeout` writes task
    state, which `taskGet` does project. -/
def visible (s : ServerState) : ServerState :=
  { s with outbox := [],
           objects := s.objects.map fun o =>
                        { o with task := o.task.map fun t =>
                                   if t.state == .pending then { t with retryTimeoutAt := none } else t } }

def sameCanon (a b : ServerState) : Bool := canon a == canon b
def sameVisible (a b : ServerState) : Bool := canon (visible a) == canon (visible b)

/-! ## The pinned instant, computed the way the checker computes it -/

/-- For every position in a script, the instant `Admissible` would fire it
    at: the instant of the next EXTERNAL step at a later position, or
    `none` if there is none — an internal step after the last observation is not
    modelled by the checker at all.

    Note this is a function of POSITION, not of instant. -/
def pinnedInstants : List (Step × Nat) → List (Option Nat)
  | [] => []
  | (rq, n) :: rest =>
      let tail := pinnedInstants rest
      let here : Option Nat :=
        (rest.find? (fun s => s.1.isExternal)).map (·.2)
      -- the head's own pinned instant is `here`; if the head is itself
      -- external it is its own instant and the field is unused
      (if rq.isExternal then some n else here) :: tail

/-! ## The corrected metric -/

structure Coverage where
  internalSteps          : Nat := 0   -- internal steps in the script
  offPinned     : Nat := 0   -- …fired at an instant ≠ the pinned one
  changed       : Nat := 0   -- …that changed the state at all (old metric)
  discriminating : Nat := 0  -- …whose effect DIFFERS from the pinned firing
  visibleDiscr  : Nat := 0   -- …and differs in a component a response can see
  unmodellable  : Nat := 0   -- …with no later observation at all
  deriving Repr

instance : Append Coverage where
  append a b :=
    { internalSteps := a.internalSteps + b.internalSteps, offPinned := a.offPinned + b.offPinned, changed := a.changed + b.changed, discriminating := a.discriminating + b.discriminating, visibleDiscr := a.visibleDiscr + b.visibleDiscr, unmodellable := a.unmodellable + b.unmodellable }

def coverage (w : List (Step × Nat)) : Coverage :=
  let pins := pinnedInstants w
  let rec go : List (Step × Nat) → List (Option Nat) → ServerState → Coverage
    | [],              _,          _  => {}
    | (rq, n) :: rest, p :: ps,    st =>
        let st' := (Abstraction.stepOf true rq n st).2
        let here : Coverage :=
          if rq.isExternal then {}
          else
            match p with
            | none      => { internalSteps := 1, unmodellable := 1 }
            | some b =>
              let stb := (Abstraction.stepOf true rq b st).2
              { internalSteps          := 1, offPinned     := if b == n then 0 else 1, changed       := if sameCanon st st' then 0 else 1, discriminating := if sameCanon st' stb then 0 else 1, visibleDiscr  := if sameVisible st' stb then 0 else 1 }
        here ++ go rest ps st'
    | _, [], _ => {}
  go w pins ServerState.init

/-! ## Scripts

Every script is sorted by instant — `ValidM` needs `now` monotone, and
`mergeSort` is stable, so an internal step written after a request at the same instant
STAYS after it. That is how an internal step gets placed on an observation instant yet
still off the checker's pinned instant. -/

def mono (w : List (Step × Nat)) : List (Step × Nat) :=
  w.mergeSort (fun a b => a.2 ≤ b.2)

def ext : Tags := [("resonate:external", "true")]
def tgt : Tags := [("resonate:target", "w1")]

/-- **The soundness check.** A recorded run is `Valid` BY CONSTRUCTION —
    the script that produced it is the execution that explains it. So if
    the checker refutes one, that is a soundness bug in `validate`, full
    stop. This used to be the counterexample hunt against the PINNED
    checker; with the interval checker it is a safety net instead, and it
    is the single most important number in this file. -/
def refutesValidRun (obs : List Observation) : Bool :=
  match validate obs 24 200000 with
  | .refuted _ _ => true
  | _            => false

/-- **The reduction, tested.** `validate` searches one representative
    per equivalence class of instants; `validateBrute` searches EVERY
    instant in every gap. R3 says they decide the same question. A
    disagreement here is a counterexample to the reduction — which is why
    the instant pools above are kept small enough for brute force to run
    at all. -/
def reductionDisagrees (obs : List Observation) : Bool :=
  verdictKind (validate obs 24 200000) != verdictKind (validateBrute obs 24 200000)

/-- Is the recording explainable with NO internal steps at all? If it is,
    the checker was never asked to recover anything and the script tests
    nothing about schedules — which is what the old sweep mostly produced.

    This is the honest version of "the checker had to invent a internal step": the old
    file read it off the witness length, but a witness may carry an internal step that
    a internal-step-free run would also have explained. -/
def internalFree (obs : List Observation) : Bool :=
  let rec go : List Observation → ServerState → Bool
    | [],     _ => true
    | o :: r, s =>
        let (res, s') := Abstraction.stepOf true (.external o.req) o.now s
        res == o.res && go r s'
  go obs ServerState.init

/-! ## An invariant the retry argument rests on

`processRetryTimeout` fires only on a `.pending` task, and its cleanup is
`delTaskTimeout t.id`, which deletes BOTH kinds. If a pending task could
ever carry a LEASE timer, the retry internal step would silently disarm an observable
transition, and the "retry internal steps are invisible" argument would be wrong.
`pendingHasLease` looks for that state; it is checked at every
intermediate state of every script below. -/

/-- The concrete hazard, asked of the abstract machine: a pending task
    still holding a lease deadline. There the two timers shared a key and
    a retry could disarm a lease; here each lives on the object that owns
    it, and the catalogue makes this an iff. The sweep below should now
    find zero by construction rather than by luck. -/
def pendingHasLease (s : ServerState) : Bool :=
  s.tasks.any fun t => t.state == .pending && t.leaseTimeoutAt.isSome

def anyPendingHasLease (w : List (Step × Nat)) : Bool :=
  let rec go : List (Step × Nat) → ServerState → Bool
    | [],              s => pendingHasLease s
    | (rq, n) :: rest, s =>
        pendingHasLease s || go rest (Abstraction.stepOf true rq n s).2
  go w ServerState.init

/-! ## A generator that actually exercises the recovery

The old sweep drew 1-in-5 steps from an internal step alphabet over two fixed object
pairs and got 4 scripts out of 4000 in which the checker had to invent a
single internal step. That is a sweep of nothing.

This one builds a suspend/settle/resume workflow — the shape in which a
hidden internal step is FORCED, because `taskGet` after a resume shows a different
task state than `taskGet` without one — and then randomises the instants
of everything: the promise deadlines, the lease, the internal step instants, and the
observation instants. Deadlines are drawn from the same pool as instants
so that thresholds land inside gaps rather than far outside them. -/

def lcg (x : Nat) : Nat := (x * 1103515245 + 12345) % 2147483648

def pick {α} (xs : List α) (d : α) (r : Nat) : α := (xs[r % xs.length]?).getD d

/-- Instants are deliberately dense and small so that deadlines, internal step
    instants and observation instants collide and straddle each other. -/
def pool : List Nat := [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,20,24,30]

/-- One workflow with every instant and every deadline randomised, and the
    internal steps placed at instants that need not be observed. -/
def workflow (seed : Nat) : List (Step × Nat) :=
  let r1 := lcg seed
  let r2 := lcg r1
  let r3 := lcg r2
  let r4 := lcg r3
  let r5 := lcg r4
  let r6 := lcg r5
  let r7 := lcg r6
  let r8 := lcg r7
  let r9 := lcg r8
  let ta := pick pool 5 r1          -- awaited deadline
  let tx := pick pool 5 r2          -- awaiter deadline
  let ttl := pick pool 5 r3         -- lease
  let t0 := 0
  let t1 := pick pool 5 r4          -- suspend instant
  let g1 := pick pool 5 r5          -- internal step promise timeout for a
  let g2 := pick pool 5 r6          -- internal step resume
  let g3 := pick pool 5 r7          -- internal step lease timeout
  let t2 := pick pool 5 r8          -- observation
  let t3 := pick pool 5 r9          -- observation
  mono
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := ta, param := {}, tags := ext }), t0)
  , (.external (.promiseCreate { id := oid "x", timeoutAt := tx, param := {}, tags := tgt }), t0)
  , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := ttl }), t0)
  , (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), t1)
  , (.internal (.promiseTimeout (oid "a")), g1)
  , (.internal (.callback (oid "a") (oid "x")), g2)
  , (.internal (.taskLeaseTimeout (oid "x")), g3)
  , (.internal (.taskRetryTimeout (oid "x")), g3)
  , (.external (.taskGet    { id := oid "x" }), t2)
  , (.external (.promiseGet { id := oid "x" }), t2)
  , (.external (.promiseGet { id := oid "a" }), t3)
  , (.external (.taskGet { id := oid "x" }), t3) ]

/-- A second shape: the awaited promise is SETTLED explicitly rather than
    timing out, so the resume is `.resumed`/`.buffered` rather than being
    erased by the awaiter's own timeout — the case where a difference has
    the best chance of surviving to an observation. -/
def workflowSettled (seed : Nat) : List (Step × Nat) :=
  let r1 := lcg seed
  let r2 := lcg r1
  let r3 := lcg r2
  let r4 := lcg r3
  let r5 := lcg r4
  let r6 := lcg r5
  let r7 := lcg r6
  let tx := pick pool 5 r1
  let ttl := pick pool 5 r2
  let t1 := pick pool 5 r3
  let ts := pick pool 5 r4          -- settle instant (external, observed)
  let g2 := pick pool 5 r5          -- internal step resume
  let t2 := pick pool 5 r6
  let t3 := pick pool 5 r7
  mono
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 100000, param := {}, tags := ext }), 0)
  , (.external (.promiseCreate { id := oid "x", timeoutAt := tx, param := {}, tags := tgt }), 0)
  , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := ttl }), 0)
  , (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), t1)
  , (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} }), ts)
  , (.internal (.callback (oid "a") (oid "x")), g2)
  , (.internal (.taskRetryTimeout (oid "x")), g2)
  , (.external (.taskGet    { id := oid "x" }), t2)
  , (.external (.promiseGet { id := oid "x" }), t2)
  , (.external (.taskGet { id := oid "x" }), t3) ]

/-- A third shape: a task released back to the pool, so a RETRY internal step is the
    only hidden step, and a lease timeout competes with it. This is the
    shape that exercises `delTaskTimeout` deleting a timer of the other
    kind. -/
def workflowRetry (seed : Nat) : List (Step × Nat) :=
  let r1 := lcg seed
  let r2 := lcg r1
  let r3 := lcg r2
  let r4 := lcg r3
  let r5 := lcg r4
  let r6 := lcg r5
  let tx := pick pool 5 r1
  let ttl := pick pool 5 r2
  let tr := pick pool 5 r3
  let g1 := pick pool 5 r4
  let g2 := pick pool 5 r5
  let t2 := pick pool 5 r6
  mono
  [ (.external (.promiseCreate { id := oid "x", timeoutAt := tx, param := {}, tags := tgt }), 0)
  , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := ttl }), 0)
  , (.external (.taskRelease   { id := oid "x", version := 1 }), tr)
  , (.internal (.taskLeaseTimeout (oid "x")), g1)
  , (.internal (.taskRetryTimeout (oid "x")), g2)
  , (.external (.taskGet    { id := oid "x" }), t2)
  , (.external (.promiseGet { id := oid "x" }), t2) ]

/-! ## Exhaustive, not random

The instant pools are small enough that the three workflow shapes can be
enumerated over a grid rather than sampled. Enumeration is what makes "no
counterexample" mean something: there is no seed that was not tried. -/

def grid : List Nat := [1, 2, 3, 4, 6, 8, 12]

/-- Every assignment of (awaited deadline, awaiter deadline, internal step instant,
    observation instant) from `grid`, with the suspend pinned at 1 and a
    second observation late enough to see everything. This is the
    resume-threshold shape, enumerated. -/
def resumeGrid : List (List (Step × Nat)) :=
  grid.flatMap fun ta => grid.flatMap fun tx => grid.flatMap fun g => grid.map fun t2 =>
    mono
    [ (.external (.promiseCreate { id := oid "a", timeoutAt := ta, param := {}, tags := ext }), 0)
    , (.external (.promiseCreate { id := oid "x", timeoutAt := tx, param := {}, tags := tgt }), 0)
    , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := 40 }), 0)
    , (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 1)
    , (.internal (.promiseTimeout (oid "a")), g)
    , (.internal (.callback (oid "a") (oid "x")), g)
    , (.external (.taskGet    { id := oid "x" }), t2)
    , (.external (.promiseGet { id := oid "x" }), t2)
    , (.external (.promiseGet { id := oid "a" }), t2)
    , (.external (.taskGet    { id := oid "x" }), 100)
    , (.external (.promiseGet { id := oid "x" }), 100) ]

/-- The lease/retry shape, enumerated: lease deadline, promise deadline,
    both internal step instants, observation instant. -/
def leaseGrid : List (List (Step × Nat)) :=
  grid.flatMap fun tx => grid.flatMap fun ttl => grid.flatMap fun g1 => grid.map fun t2 =>
    mono
    [ (.external (.promiseCreate { id := oid "x", timeoutAt := tx, param := {}, tags := tgt }), 0)
    , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := ttl }), 0)
    , (.internal (.taskLeaseTimeout (oid "x")), g1)
    , (.internal (.taskRetryTimeout (oid "x")), g1)
    , (.external (.taskGet    { id := oid "x" }), t2)
    , (.external (.promiseGet { id := oid "x" }), t2)
    , (.external (.taskGet { id := oid "x" }), 100) ]

/-- Two awaited promises on one awaiter, internal steps at two independent instants —
    the chain shape, enumerated over deadlines and both internal step instants. -/
def chainGrid : List (List (Step × Nat)) :=
  grid.flatMap fun ta => grid.flatMap fun tb => grid.flatMap fun g1 => grid.map fun g2 =>
    mono
    [ (.external (.promiseCreate { id := oid "a", timeoutAt := ta, param := {}, tags := ext }), 0)
    , (.external (.promiseCreate { id := oid "b", timeoutAt := tb, param := {}, tags := ext }), 0)
    , (.external (.promiseCreate { id := oid "x", timeoutAt := 100000, param := {}, tags := tgt }), 0)
    , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := 40 }), 0)
    , (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }, { awaited := oid "b", awaiter := oid "x" }] }), 1)
    , (.internal (.promiseTimeout (oid "a")), g1)
    , (.internal (.callback (oid "a") (oid "x")), g1)
    , (.internal (.promiseTimeout (oid "b")), g2)
    , (.internal (.callback (oid "b") (oid "x")), g2)
    , (.external (.taskGet    { id := oid "x" }), 20)
    , (.external (.promiseGet { id := oid "a" }), 20)
    , (.external (.promiseGet { id := oid "b" }), 20) ]

end TraceCheck.Gap

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Intervals TraceCheck.Gap in
def runFamily (name : String) (ws : List (List (Step × Nat))) : IO Nat := do
  let mut cov : Coverage := {}
  let mut broke := 0
  let mut shown := 0
  let mut withDiscr := 0
  let mut withVis := 0
  let mut forcedInternalStep := 0
  let mut badInv := 0
  let mut ivBroke := 0
  let mut diverged := 0
  let mut redDisagree := 0
  for w in ws do
    let c := coverage w
    cov := cov ++ c
    if c.discriminating > 0 then withDiscr := withDiscr + 1
    if c.visibleDiscr > 0 then withVis := withVis + 1
    if anyPendingHasLease w then badInv := badInv + 1
    let obs := recordFrom w
    if !internalFree obs then forcedInternalStep := forcedInternalStep + 1
    if reductionDisagrees obs then redDisagree := redDisagree + 1
    if refutesValidRun obs then
      broke := broke + 1
      if shown < 2 then
        shown := shown + 1
        IO.eprintln s!"  *** SOUNDNESS BUG in {name}: valid run refuted ***"
        for (rq, n) in w do IO.eprintln s!"      @{n}  {repr rq}"
  IO.eprintln s!"  {name}"
  IO.eprintln s!"    scripts                                  : {ws.length}"
  IO.eprintln s!"    internal steps                           : {cov.internalSteps}"
  IO.eprintln s!"      …fired off the observation's instant   : {cov.offPinned}"
  IO.eprintln s!"      …that changed state  (old metric)      : {cov.changed}"
  IO.eprintln s!"      …DISCRIMINATING vs firing at it        : {cov.discriminating}"
  IO.eprintln s!"      …discriminating in a VISIBLE component : {cov.visibleDiscr}"
  IO.eprintln s!"      …after the last observation (unmodelled): {cov.unmodellable}"
  IO.eprintln s!"    scripts with >=1 discriminating internal step : {withDiscr}/{ws.length}"
  IO.eprintln s!"    scripts with >=1 VISIBLY discriminating step  : {withVis}/{ws.length}"
  IO.eprintln s!"    recordings NOT explainable step-free         : {forcedInternalStep}/{ws.length}"
  IO.eprintln s!"    states with a pending task holding a lease: {badInv}"
  IO.eprintln s!"    valid run REFUTED (soundness bug)        : {broke}"
  IO.eprintln s!"    critical instants ≠ BRUTE FORCE (R3 fails): {redDisagree}"
  return broke

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Intervals TraceCheck.Gap in
def main : IO Unit := do
  IO.eprintln "══ the gap, measured properly ═════════════════════════════════"
  IO.eprintln ""
  let mut total := 0
  total := total + (← runFamily "resumeGrid (exhaustive)" resumeGrid)
  IO.eprintln ""
  total := total + (← runFamily "leaseGrid  (exhaustive)" leaseGrid)
  IO.eprintln ""
  total := total + (← runFamily "chainGrid  (exhaustive)" chainGrid)
  IO.eprintln ""
  total := total + (← runFamily "workflow        (random, 3000)"
                      ((List.range 3000).map fun i => workflow (i * 7919 + 1)))
  IO.eprintln ""
  total := total + (← runFamily "workflowSettled (random, 3000)"
                      ((List.range 3000).map fun i => workflowSettled (i * 7919 + 1)))
  IO.eprintln ""
  total := total + (← runFamily "workflowRetry   (random, 3000)"
                      ((List.range 3000).map fun i => workflowRetry (i * 7919 + 1)))
  IO.eprintln ""
  IO.eprintln s!"══ {total} counterexamples in total ══════════════════════════"

  IO.eprintln ""
  IO.eprintln "── sanity: the harness can still refute ───────────────────────"
  let good := recordFrom (workflow 1)
  let lied := match good with
              | []     => []
              | o :: r => ({ o with res := .promiseCreate { status := 409, promise := none } } : Observation) :: r
  IO.eprintln s!"  honest  {verdictLine (validate good 24 200000)}"
  IO.eprintln s!"  lied    {verdictLine (validate lied 24 200000)}"
