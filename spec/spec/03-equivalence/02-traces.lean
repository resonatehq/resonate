import «03-equivalence».«01-driver»

/-!  # Equivalence — the trace battery

Every trace starts from the empty state, builds its own objects through
the protocol, and drives both machines through the projection-sensitive
paths: reads of logically-settled promises, task operations across an
uncommitted deadline (the former D1), re-creation on dead promises (the
former D2), early τ firings (the former D3), timer promises, suspension
on settled awaiteds, lease and retry cycles, delayed dispatch.

Each `example` checks: responses identical, states closure-equal.  -/

namespace Equivalence

open ServerModel (PromiseState)

def extTags : ServerModel.Tags := [("resonate:external", "true")]
def tgtTags : ServerModel.Tags := [("resonate:target", "w1")]
def timerTags : ServerModel.Tags := [("resonate:timer", "true")]

/-! ### T1 — the D1 trace: observe a dead task, then halt it.
Both machines now answer `fulfilled` then `409`. -/

def t1 : List Op :=
  [ .promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags } 100,
    .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "x", timeoutAt := 300, param := {}, tags := tgtTags } } 100,
    .taskSuspend { id := "x", version := 1,
                   actions := [{ awaited := "a", awaiter := "x" }] } 120,
    .taskGet { id := "x" } 500,
    .taskHalt { id := "x" } 500 ]

example : equivalent t1 500 := by decide

/-! ### T2 — the D2 trace: task.create against a logically dead promise.
Both serve the settled pair; neither arms a lease. -/

def t2 : List Op :=
  [ .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } } 100,
    .taskRelease { id := "y", version := 1 } 150,
    .taskCreate { pid := "p1", ttl := 100,
                  action := { id := "y", timeoutAt := 300, param := {}, tags := tgtTags } } 500 ]

example : equivalent t2 500 := by decide

/-! ### T3 — the live settle pipeline: suspend, settle, drain, wake,
re-acquire, fulfill. No deadline is crossed; the machines run in
lockstep. -/

def t3 : List Op :=
  [ .promiseCreate { id := "a", timeoutAt := 1000, param := {}, tags := extTags } 100,
    .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } } 100,
    .taskSuspend { id := "x", version := 1,
                   actions := [{ awaited := "a", awaiter := "x" }] } 120,
    .promiseSettle { id := "a", state := .resolved, value := {} } 200,
    .tauDrain 200,
    .taskGet { id := "x" } 210,
    .taskAcquire { id := "x", version := 1, pid := "p2", ttl := 50 } 220,
    .taskFulfill { id := "x", version := 2,
                   action := { id := "x", state := .resolved, value := {} } } 230,
    .taskGet { id := "x" } 240 ]

example : equivalent t3 240 := by decide

/-! ### T4 — a timer promise past its deadline: read, late listener
registration, late settle, duplicate create. (`native_decide`: the
address validation's string scans do not reduce in the kernel.) -/

def t4 : List Op :=
  [ .promiseCreate { id := "tm", timeoutAt := 300, param := {}, tags := timerTags } 100,
    .promiseGet { id := "tm" } 500,
    .registerListener { awaited := "tm", address := "https://l" } 500,
    .promiseSettle { id := "tm", state := .rejected, value := {} } 500,
    .promiseCreate { id := "tm", timeoutAt := 9999, param := {}, tags := [] } 600 ]

example : equivalent t4 600 := by native_decide

/-! ### T5 — registration and suspension against an expired awaited:
callback refused-by-projection, suspend answers `300`. -/

def t5 : List Op :=
  [ .promiseCreate { id := "a", timeoutAt := 300, param := {}, tags := extTags } 100,
    .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } } 100,
    .registerCallback { awaited := "a", awaiter := "x" } 500,
    .taskSuspend { id := "x", version := 1,
                   actions := [{ awaited := "a", awaiter := "x" }] } 500 ]

example : equivalent t5 500 := by decide

/-! ### T6 — the material task lifecycle: heartbeat, early lease firing
(refused, NOT BEFORE), due lease firing, re-acquire, halt, continue,
early retry (refused), due retry against a by-then dead promise, final
read across the deadline. -/

def t6 : List Op :=
  [ .taskCreate { pid := "p0", ttl := 100,
                  action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } } 100,
    .taskHeartbeat { pid := "p0", tasks := [{ id := "x", version := 1 }] } 150,
    .tauLease "x" 200,
    .tauLease "x" 300,
    .taskAcquire { id := "x", version := 1, pid := "p1", ttl := 100 } 310,
    .taskHalt { id := "x" } 320,
    .taskContinue { id := "x" } 330,
    .tauRetry "x" 340,
    .tauRetry "x" 5400,
    .taskGet { id := "x" } 5500 ]

example : equivalent t6 5500 := by decide

/-! ### T7 — fencing: fenced create, fenced settle, self-target `400`,
fenced action after the fence's own promise dies. -/

def t7 : List Op :=
  [ .taskCreate { pid := "p0", ttl := 1000,
                  action := { id := "x", timeoutAt := 2000, param := {}, tags := tgtTags } } 100,
    .taskFence { id := "x", version := 1,
                 action := .create { id := "c", timeoutAt := 3000, param := {}, tags := extTags } } 200,
    .taskFence { id := "x", version := 1,
                 action := .settle { id := "c", state := .resolved, value := {} } } 300,
    .taskFence { id := "x", version := 1,
                 action := .settle { id := "x", state := .resolved, value := {} } } 400,
    .taskFence { id := "x", version := 1,
                 action := .settle { id := "c", state := .resolved, value := {} } } 2500 ]

example : equivalent t7 2500 := by decide

/-! ### T8 — delayed dispatch: no emission before the delay, retry-τ
refused early, fired at the delay. (`native_decide`: the delay tag's
`String.toNat!` does not reduce in the kernel.) -/

def t8 : List Op :=
  [ .promiseCreate { id := "d", timeoutAt := 9000, param := {},
                     tags := [("resonate:target", "wd"), ("resonate:delay", "800")] } 100,
    .tauRetry "d" 500,
    .tauRetry "d" 800,
    .taskGet { id := "d" } 900 ]

example : equivalent t8 900 := by native_decide

/-! ### T9 — absences and request-level validation. -/

def t9 : List Op :=
  [ .promiseGet { id := "z" } 100,
    .taskGet { id := "z" } 100,
    .taskHalt { id := "z" } 100,
    .promiseSettle { id := "z", state := .pending, value := {} } 100 ]

example : equivalent t9 100 := by decide

end Equivalence
