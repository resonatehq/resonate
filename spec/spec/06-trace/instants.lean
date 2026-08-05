import «06-trace».validator

/-!  # Hunting a counterexample to `InstantsSuffice`

`ValidPinned` fires every hidden step at an observation's own instant.
`ValidM` lets an execution fire them ANYWHERE in between. `InstantsSuffice`
says the difference is invisible. This file tries to break it.

## The oracle is exact

`recordFrom` runs a script — τs included, at whatever instants the script
names — through the specification and keeps only what an outside observer
sees. So for any script `w`:

* the recorded trace is `Valid` BY CONSTRUCTION, because `w` itself is the
  execution that explains it;
* `validate` decides `ValidPinned`.

A script whose τs sit at instants no observation names, and whose recording
is REFUTED, is therefore a counterexample — no search over executions
needed, because the witness is the script.

## Where instant-dependence can live

Reading the handlers, a τ's effect depends on `now` in only three places:

1. `onResume` — `if now >= p.timeoutAt then .expired`. A threshold.
2. every timeout — `setTaskTimeout t.id 0 (now + retryTimeout)`. The
   re-armed deadline is a function of when the τ fired.
3. `onScheduleTimeout` — `occurrences s0.cron s0.nextRunAt now`.

Everything else is deliberately instant-free. `touchPromise` settles at
`p.timeoutAt`, not at `now`: a promise is "born settled, exactly as if it
had been created on time". That is the discipline the trace framework
calls TIMEOUT ALWAYS WINS, and it is what makes this hard to break.

(3) is untestable here — `occurrences` and `nextCron` are `opaque`, so a
schedule script does not execute at all. This file covers (1) and (2). -/

namespace TraceCheck.Instants

open ServerModel Equivalence TraceCheck

def ext : Tags := [("resonate:external", "true")]
def tgt : Tags := [("resonate:target", "w1")]

/-! ## Instants

`obsAt` are the instants observations carry. `gapAt` are strictly between
them and are named by NO observation, so the checker can never fire a τ
there. Every scenario below places its τs on `gapAt`. -/

def obsAt : List Nat := [10, 20, 60, 200, 6000]
def gapAt : List Nat := [25, 30, 35, 40, 50, 100, 150, 5500]

/-! ## Scenario 1 — the resume threshold

`a` expires at 30 (a gap instant). Its timeout defers a resume for `x`,
and `onResume` answers `.resumed` before `x`'s own deadline and `.expired`
after. Put `x`'s deadline at 40, also in the gap, and the window in which
the resume behaves differently is `[30, 40)` — which contains no
observation at all.

An execution that fires both τs at 35 resumes `x`. The checker, able to
act only at 20 (τ not yet enabled) or 60 (past `x`'s deadline), cannot. -/
def resumeThreshold (tauAt : Nat) : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 30, param := {}, tags := ext }, 10)
  , (.promiseCreate { id := "x", timeoutAt := 40, param := {}, tags := tgt }, 10)
  , (.taskAcquire   { id := "x", version := 0, pid := "p", ttl := 50 }, 10)
  , (.taskSuspend   { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }, 20)
  , (.τPromiseTimeout "a", tauAt)
  , (.τResume { awaited := "a", awaiter := "x" }, tauAt)
  , (.taskGet  { id := "x" }, 60)
  , (.promiseGet { id := "x" }, 60)
  , (.promiseGet { id := "a" }, 60) ]

/-- Same, but `x` outlives the observation, so the difference — if any —
    is not erased by `x`'s own timeout fulfilling the task. -/
def resumeThresholdLive (tauAt : Nat) : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 30, param := {}, tags := ext }, 10)
  , (.promiseCreate { id := "x", timeoutAt := 100000, param := {}, tags := tgt }, 10)
  , (.taskAcquire   { id := "x", version := 0, pid := "p", ttl := 50 }, 10)
  , (.taskSuspend   { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }, 20)
  , (.τPromiseTimeout "a", tauAt)
  , (.τResume { awaited := "a", awaiter := "x" }, tauAt)
  , (.taskGet  { id := "x" }, 60)
  , (.promiseGet { id := "x" }, 60) ]

/-! ## Scenario 2 — the re-armed retry deadline

A lease timeout re-arms the retry at `now + retryTimeout`. Fire it at 35
and the retry is due at 5035; fire it at 50 and it is due at 5050. An
observation at 6000 straddles neither, but one at 5040 straddles both —
so this asks whether the re-armed deadline is observable at all. -/
def retryRearm (tauAt : Nat) (obsLate : Nat) : List (Request × Nat) :=
  [ (.promiseCreate { id := "x", timeoutAt := 100000, param := {}, tags := tgt }, 10)
  , (.taskAcquire   { id := "x", version := 0, pid := "p", ttl := 10 }, 10)
  , (.τTaskLeaseTimeout "x", tauAt)
  , (.taskGet  { id := "x" }, obsLate)
  , (.promiseGet { id := "x" }, obsLate) ]

/-! ## Scenario 3 — lease expiry inside the gap

The lease is armed at 20 and `x` dies at 40, so the lease timeout behaves
one way in `[20, 40)` and is a no-op after. Both endpoints are gap
instants. -/
def leaseWindow (tauAt : Nat) : List (Request × Nat) :=
  [ (.promiseCreate { id := "x", timeoutAt := 40, param := {}, tags := tgt }, 10)
  , (.taskAcquire   { id := "x", version := 0, pid := "p", ttl := 10 }, 10)
  , (.τTaskLeaseTimeout "x", tauAt)
  , (.τTaskRetryTimeout "x", tauAt)
  , (.taskGet  { id := "x" }, 60)
  , (.promiseGet { id := "x" }, 60) ]

/-! ## Scenario 4 — a chain fired entirely in the gap

Two independent objects whose timeouts and resumes all land between
observations, so the checker must reconstruct a four-step hidden chain at
instants it never sees. -/
def chainInGap (t1 t2 : Nat) : List (Request × Nat) :=
  [ (.promiseCreate { id := "a", timeoutAt := 30, param := {}, tags := ext }, 10)
  , (.promiseCreate { id := "b", timeoutAt := 35, param := {}, tags := ext }, 10)
  , (.promiseCreate { id := "x", timeoutAt := 100000, param := {}, tags := tgt }, 10)
  , (.taskAcquire   { id := "x", version := 0, pid := "p", ttl := 50 }, 10)
  , (.taskSuspend   { id := "x", version := 1,
                      actions := [{ awaited := "a", awaiter := "x" },
                                  { awaited := "b", awaiter := "x" }] }, 20)
  , (.τPromiseTimeout "a", t1)
  , (.τResume { awaited := "a", awaiter := "x" }, t1)
  , (.τPromiseTimeout "b", t2)
  , (.τResume { awaited := "b", awaiter := "x" }, t2)
  , (.taskGet  { id := "x" }, 200)
  , (.promiseGet { id := "a" }, 200)
  , (.promiseGet { id := "b" }, 200) ]

/-! ## Is a script actually testing anything?

A τ that fires as a no-op — obligation already discharged, guard false —
exercises nothing. "0 counterexamples" over scripts full of no-ops would
mean "0 tests". `effectiveTaus` counts the τs at OFF-GRID instants that
actually changed the state. -/

def effectiveTaus (w : List (Request × Nat)) (grid : List Nat) : Nat :=
  let rec go : List (Request × Nat) → ServerState → Nat
    | [],              _ => 0
    | (rq, n) :: rest, st =>
        let st' := (Equivalence.stepOf Equivalence.handleM rq n st).2
        let here :=
          if !rq.isExternal && !grid.contains n && !(TraceCheck.canon st == TraceCheck.canon st')
          then 1 else 0
        here + go rest st'
  go w ServerState.init

/-! ## Running the hunt

**Monotonicity matters.** `ValidM` requires `(tr t).now ≤ (tr (t+1)).now`,
so a script whose instants go backwards is not an execution and proves
nothing. Every script is sorted by instant before use — `mergeSort` is
stable, so requests sharing an instant keep their written order. -/

def mono (w : List (Request × Nat)) : List (Request × Nat) :=
  w.mergeSort (fun a b => a.2 ≤ b.2)

structure Case where
  label  : String
  script : List (Request × Nat)

def cases : List Case :=
  (gapAt.map fun t => { label := s!"resumeThreshold     τ@{t}", script := resumeThreshold t })
  ++ (gapAt.map fun t => { label := s!"resumeThresholdLive τ@{t}", script := resumeThresholdLive t })
  ++ (gapAt.flatMap fun t =>
        [200, 5040, 6000].map fun o =>
          { label := s!"retryRearm          τ@{t} obs@{o}", script := retryRearm t o })
  ++ (gapAt.map fun t => { label := s!"leaseWindow         τ@{t}", script := leaseWindow t })
  ++ (gapAt.flatMap fun a => gapAt.map fun b =>
        { label := s!"chainInGap          τ@{a},{b}", script := chainInGap a b })

/-- A case BREAKS `InstantsSuffice` when its recording is refuted: the
    script is a `Valid` witness the pinned search cannot reproduce. -/
def breaks (c : Case) : Bool :=
  match validate (recordFrom (mono c.script)) 24 200000 with
  | .refuted _ _ => true
  | _            => false

/-! ## A randomised sweep

Hand-built scenarios only test the mechanisms I thought of. This samples
scripts over a small alphabet, external requests on `obsAt` and τs on
`gapAt`, then sorts. Same oracle: a refutation is a counterexample. -/

def lcg (x : Nat) : Nat := (x * 1103515245 + 12345) % 2147483648

def pick {α} (xs : List α) (d : α) (r : Nat) : α :=
  (xs[r % xs.length]?).getD d

def extAlphabet : List Request :=
  [ .promiseCreate { id := "a", timeoutAt := 30,     param := {}, tags := ext }
  , .promiseCreate { id := "b", timeoutAt := 35,     param := {}, tags := ext }
  , .promiseCreate { id := "x", timeoutAt := 40,     param := {}, tags := tgt }
  , .promiseCreate { id := "y", timeoutAt := 100000, param := {}, tags := tgt }
  , .promiseSettle { id := "a", state := .resolved, value := {} }
  , .promiseSettle { id := "b", state := .rejected, value := {} }
  , .taskAcquire   { id := "x", version := 0, pid := "p", ttl := 10 }
  , .taskAcquire   { id := "y", version := 0, pid := "p", ttl := 10 }
  , .taskAcquire   { id := "y", version := 1, pid := "p", ttl := 10 }
  , .taskSuspend   { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }
  , .taskSuspend   { id := "y", version := 1, actions := [{ awaited := "b", awaiter := "y" }] }
  , .taskFulfill   { id := "y", version := 2, action := { id := "y", state := .resolved, value := {} } }
  , .taskGet       { id := "x" }
  , .taskGet       { id := "y" }
  , .promiseGet    { id := "a" }
  , .promiseGet    { id := "b" }
  , .promiseGet    { id := "x" }
  , .promiseGet    { id := "y" } ]

def tauAlphabet : List Request :=
  [ .τPromiseTimeout "a", .τPromiseTimeout "b"
  , .τPromiseTimeout "x", .τPromiseTimeout "y"
  , .τTaskLeaseTimeout "x", .τTaskLeaseTimeout "y"
  , .τTaskRetryTimeout "x", .τTaskRetryTimeout "y"
  , .τResume { awaited := "a", awaiter := "x" }
  , .τResume { awaited := "b", awaiter := "y" } ]

/-- One random script: `n` steps, each either external-on-grid or
    τ-off-grid, then sorted into a monotone execution. -/
def randScript (seed n : Nat) : List (Request × Nat) :=
  let rec go (r : Nat) : Nat → List (Request × Nat)
    | 0        => []
    | k + 1 =>
      let r1 := lcg r
      let r2 := lcg r1
      let r3 := lcg r2
      let step :=
        if r1 % 5 == 0 then
          (pick tauAlphabet (.idle) r2, pick gapAt 25 r3)
        else
          (pick extAlphabet (.idle) r2, pick obsAt 10 r3)
      step :: go r3 k
  mono (go seed n)

end TraceCheck.Instants

open TraceCheck TraceCheck.Instants in
def main : IO Unit := do
  IO.eprintln "── does firing a τ off-grid ever break the pinned checker? ────"
  IO.eprintln s!"observation instants : {obsAt}"
  IO.eprintln s!"τ instants (off-grid): {gapAt}"
  IO.eprintln ""
  let mut broken := 0
  for c in cases do
    let v := validate (recordFrom c.script) 24 200000
    let k := verdictKind v
    if breaks c then
      broken := broken + 1
      IO.eprintln s!"  {c.label}  {k}   *** COUNTEREXAMPLE ***"
  IO.eprintln s!"  {cases.length} scenarios, {broken} counterexamples"
  IO.eprintln ""
  IO.eprintln "── randomised sweep ───────────────────────────────────────────"
  let mut rbroken := 0
  let mut rrefuted := 0
  for i in [0:4000] do
    let w := randScript (i * 7919 + 1) 12
    let obs := recordFrom w
    match validate obs 24 200000 with
    | .refuted j _ =>
        rbroken := rbroken + 1
        if rrefuted < 3 then
          rrefuted := rrefuted + 1
          IO.eprintln s!"  seed {i} refuted at event {j}  *** COUNTEREXAMPLE ***"
          for (rq, n) in w do
            IO.eprintln s!"      @{n}  {repr rq}"
    | _ => pure ()
  IO.eprintln s!"  4000 random scripts, {rbroken} counterexamples"

  IO.eprintln ""
  IO.eprintln "── was any of that a real test? ───────────────────────────────"
  let mut withEff := 0
  let mut totalEff := 0
  let mut withWit := 0
  let mut events := 0
  for i in [0:4000] do
    let w := randScript (i * 7919 + 1) 12
    let e := effectiveTaus w obsAt
    if e > 0 then withEff := withEff + 1
    totalEff := totalEff + e
    let obs := recordFrom w
    events := events + obs.length
    match validate obs 24 200000 with
    | .admissible ws _ _ => if ws.length > 0 then withWit := withWit + 1
    | _ => pure ()
  IO.eprintln s!"  scripts with >=1 OFF-GRID τ that changed state : {withEff}/4000"
  IO.eprintln s!"  total such τ firings                           : {totalEff}"
  IO.eprintln s!"  scripts where the checker had to invent a τ     : {withWit}/4000"
  IO.eprintln s!"  observations checked                           : {events}"

  IO.eprintln ""
  IO.eprintln "── and the targeted scenarios? ────────────────────────────────"
  for c in [ ("resumeThreshold     τ@35", resumeThreshold 35)
           , ("resumeThresholdLive τ@35", resumeThresholdLive 35)
           , ("leaseWindow         τ@25", leaseWindow 25)
           , ("chainInGap          τ@30,40", chainInGap 30 40) ] do
    let w := mono c.2
    IO.eprintln s!"  {c.1}  off-grid effective τ = {effectiveTaus w obsAt}   {verdictKind (validate (recordFrom w) 24 200000)}"

  IO.eprintln ""
  IO.eprintln "── sanity: the harness CAN refute, so silence means something ──"
  let good := recordFrom (resumeThresholdLive 35)
  let lied := match good with
              | []      => []
              | o :: r  => ({ o with res := .promiseCreate { status := 409, promise := none } } : Observation) :: r
  IO.eprintln s!"  honest recording   {verdictLine (validate good 24 200000)}"
  IO.eprintln s!"  one response lied  {verdictLine (validate lied 24 200000)}"
