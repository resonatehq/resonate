import «05-sqlite».conformance

/-!  # The sweeps

Two, answering different questions.

* **Exhaustive at small scope.** Every script up to length three over a
  twenty-step alphabet — 8 420 scripts — run from the empty store on
  both machines. Bounded, but nothing inside the bound is missed.
* **Randomized at depth.** Scripts of length ten from a seeded LCG.
  Length ten reaches interleavings the exhaustive sweep cannot afford:
  create → acquire → suspend → settle → resume → re-acquire → fulfil is
  already seven steps, so the exhaustive bound cannot contain a single
  full workflow. This is where the protocol's actual shapes live.

Both are discharged by `native_decide`, which trades the kernel for the
compiler — `Lean.ofReduceBool` appears in the axiom list. That is a
deliberate departure from the specification's own discipline ("the
spec's string scans are structurally recursive precisely so no proof
needs `native_decide`"), and it is forced: each script step parses and
evaluates SQL text, so kernel reduction over thousands of scripts is not
affordable. `equiv-P-01.lean` remains kernel-only, so the repository
keeps one of each.

**Neither is a proof about SQLite.** They are proofs about the SQL
semantics in `sql.lean`. Validating THOSE against a real engine needs
`leansqlite` and an `IO` interpreter of the same handlers, which cannot
be a theorem because `native_decide` cannot run `IO`. -/

namespace Sqlite.Sweep

open Sql ServerModel Sqlite Sqlite.Conformance

set_option maxRecDepth 100000
set_option maxHeartbeats 10000000

/-! ## Alphabet

Two promise ids: `"x"` is targeted (so it has a task), `"a"` is external
(so it may be awaited). Two instants straddle `"a"`'s deadline of 30, so
both the live and the logically-dead branch of every guard is reachable. -/

def tgt : Tags := [("resonate:target", "w1")]
def ext : Tags := [("resonate:external", "true")]
def tmr : Tags := [("resonate:timer", "true")]

def alphabet : List (Request × Nat) :=
  [ (.promiseCreate { id := "x", timeoutAt := 100, param := {}, tags := tgt }, 10)
    -- a SHORT-lived targeted promise, so the task's own promise can be
    -- logically dead while its retry timer is due. Without this the
    -- TIMEOUT-ALWAYS-WINS guard in `onTaskRetryTimeout` is unreachable,
    -- and mutation testing showed the sweep could not see it removed.
  , (.promiseCreate { id := "x", timeoutAt := 30,  param := {}, tags := tgt }, 10)
  , (.promiseCreate { id := "a", timeoutAt := 30,  param := {}, tags := ext }, 10)
  , (.promiseCreate { id := "a", timeoutAt := 30,  param := {}, tags := tmr }, 10)
  , (.promiseSettle { id := "a", state := .resolved, value := {} }, 20)
  , (.promiseRegisterCallback { awaited := "a", awaiter := "x" }, 12)
  , (.promiseRegisterListener { awaited := "a", address := "poll://w@1" }, 12)
  , (.promiseGet { id := "a" }, 40)
  , (.taskCreate { pid := "p0", ttl := 50,
                   action := { id := "x", timeoutAt := 100, param := {}, tags := tgt } }, 10)
  , (.taskAcquire { id := "x", version := 0, pid := "p0", ttl := 50 }, 12)
  , (.taskAcquire { id := "x", version := 1, pid := "p1", ttl := 50 }, 20)
  , (.taskSuspend { id := "x", version := 1, actions := [{ awaited := "a", awaiter := "x" }] }, 12)
  , (.taskFulfill { id := "x", version := 1,
                    action := { id := "x", state := .resolved, value := {} } }, 20)
  , (.taskRelease { id := "x", version := 1 }, 20)
  , (.taskHalt { id := "x" }, 20)
  , (.taskContinue { id := "x" }, 21)
  , (.taskGet { id := "x" }, 40)
  , (.taskHeartbeat { pid := "p0", tasks := [{ id := "x", version := 1 }] }, 20)
  , (.τPromiseTimeout "a", 40)
  , (.τResume { awaited := "a", awaiter := "x" }, 40)
    -- LATE instants. `retryTimeout` is 5 000, so a kind-0 timer armed at
    -- `now + 5000` is not due at any of the instants above: with only
    -- those, `onTaskRetryTimeout`'s body never executed in any script and
    -- the sweep could not see its guard removed. Mutation testing is what
    -- surfaced that; `coverage` could not, because a τ reports no status.
  , (.τTaskRetryTimeout "x", 6000)
  , (.τTaskLeaseTimeout "x", 6000)
  , (.taskGet { id := "x" }, 6000) ]

def defaultStep : Request × Nat := (.promiseGet { id := "a" }, 40)

/-! ## Exhaustive to length three -/

def extend (scs : List Script) : List Script :=
  scs.flatMap fun sc => alphabet.map fun st => sc ++ [st]

def scripts1 : List Script := extend [[]]
def scripts2 : List Script := extend scripts1
def scripts3 : List Script := extend scripts2
def allScripts : List Script := scripts1 ++ scripts2 ++ scripts3

/-! ## Coverage

A sweep that only ever produced `400`/`404` would pass while testing
nothing — the failure mode `diff/differential.rs` guards against with its
"every operation kind must produce one 2xx" rule. Same idea: every step
of the alphabet must reach a `200`-class response somewhere in the
exhaustive sweep. -/

def statusOf : Response → Nat
  | .promiseGet r              => r.status
  | .promiseCreate r           => r.status
  | .promiseSettle r           => r.status
  | .promiseRegisterCallback r => r.status
  | .promiseRegisterListener r => r.status
  | .scheduleGet r             => r.status
  | .scheduleCreate r          => r.status
  | .scheduleDelete r          => r.status
  | .taskGet r                 => r.status
  | .taskCreate r              => r.status
  | .taskAcquire r             => r.status
  | .taskFence r               => r.status
  | .taskHeartbeat r           => r.status
  | .taskSuspend r             => r.status
  | .taskFulfill r             => r.status
  | .taskRelease r             => r.status
  | .taskHalt r                => r.status
  | .taskContinue r            => r.status
  | .resume _                  => 200
  | .τ                         => 200

/-- Did this alphabet step ever succeed, in any script that reaches it? -/
def stepSucceeds (i : Nat) : Bool :=
  allScripts.any fun sc =>
    let responses := (runSql sc emptyStore).1
    (sc.zip responses).any fun (st, res) =>
      alphabet[i]? == some st && statusOf res == 200

def coverage : Bool :=
  (List.range alphabet.length).all stepSucceeds

/-! ## Randomized, at depth -/

def lcg (s : Nat) : Nat := (s * 1103515245 + 12345) % 2147483648

def genScript : Nat → Nat → Script
  | 0,     _ => []
  | len+1, s =>
      let s' := lcg s
      (alphabet[s' % alphabet.length]?).getD defaultStep :: genScript len s'

def randomScripts (count len seed : Nat) : List Script :=
  (List.range count).map fun i => genScript len (lcg (seed + i * 7919))

/-! ## The theorems -/

theorem exhaustiveResponses :
    allScripts.all responsesAgree = true := by native_decide

theorem exhaustiveConformance :
    allScripts.all conforms = true := by native_decide

theorem exhaustiveCovers : coverage = true := by native_decide

theorem randomConformance :
    (randomScripts 2000 10 20260805).all conforms = true := by native_decide

end Sqlite.Sweep
