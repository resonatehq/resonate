import «04-theorems».«system»

/-!  # The corpus

The scripts every harness in this directory runs. Salvaged from
`abstract-twins.lean`, which built them to show the two StateM twins
locked step; with one machine there are no twins to compare, and what
survives is the alphabet itself — which was always the useful part.

`kernelsResp` is adversarial rather than representative: eleven steps
chosen so that every pair interferes. Length ≤ 3 over it is 1 + 11 +
121 + 1 331 = 1 464 scripts, and `instantiateA` stamps each step at
`100 · (i+1)` so the clock advances monotonically and never twice at
the same instant.

`b1`–`b6` and `wLag` are hand-built for shapes the alphabet misses:
`wLag` a task whose promise died under it, `b3` a timer, `b5` fencing,
`b6` a task request against an id that has no task.
They are the battery; the sweep is the alphabet. -/

namespace Abstraction

open Equivalence

/-! ## The battery -/

def wLag : List (Step × Nat) :=
  [ (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 250, param := {}, tags := tgtTags } }), 100),
    (.external (.taskGet { id := oid "x" }), 300),
    (.internal (.taskLeaseTimeout (oid "x")), 300),
    (.internal (.taskRetryTimeout (oid "x")), 300) ]

def b1 : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 100),
    (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 120),
    (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} }), 200),
    (.internal (.callback (oid "a") (oid "x")), 200),
    (.external (.taskGet { id := oid "x" }), 210),
    (.internal (.taskRetryTimeout (oid "x")), 210),
    (.external (.taskAcquire { id := oid "x", version := 1, pid := "p2", ttl := 50 }), 220),
    (.external (.taskFulfill { id := oid "x", version := 2, action := { id := oid "x", state := .resolved, value := {} } }), 230) ]

def b2 : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 1000, param := {}, tags := extTags }), 100),
    (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 300, param := {}, tags := tgtTags } }), 100),
    (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 120),
    (.external (.taskGet { id := oid "x" }), 500),
    (.external (.taskHalt { id := oid "x" }), 500) ]

def b3 : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "tm", timeoutAt := 300, param := {}, tags := timerTags }), 100),
    (.external (.promiseGet { id := oid "tm" }), 500),
    (.external (.promiseRegisterListener { awaited := oid "tm", address := "https://l" }), 500),
    (.external (.promiseSettle { id := oid "tm", state := .rejected, value := {} }), 500),
    (.external (.promiseCreate { id := oid "tm", timeoutAt := 9999, param := {}, tags := [] }), 600) ]

def b4 : List (Step × Nat) :=
  [ (.external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "y", timeoutAt := 300, param := {}, tags := tgtTags } }), 100),
    (.external (.taskRelease { id := oid "y", version := 1 }), 150),
    (.external (.taskCreate { pid := "p1", ttl := 100, action := { id := oid "y", timeoutAt := 300, param := {}, tags := tgtTags } }), 500) ]

def b5 : List (Step × Nat) :=
  [ (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 2000, param := {}, tags := tgtTags } }), 100),
    (.external (.taskFence { id := oid "x", version := 1, action := .create { id := oid "c", timeoutAt := 3000, param := {}, tags := extTags } }), 200),
    (.external (.taskFence { id := oid "x", version := 1, action := .settle { id := oid "c", state := .resolved, value := {} } }), 300),
    (.external (.taskFence { id := oid "x", version := 1, action := .settle { id := oid "x", state := .resolved, value := {} } }), 400),
    (.external (.taskFence { id := oid "x", version := 1, action := .settle { id := oid "c", state := .resolved, value := {} } }), 2500) ]

/-- A task request against an id that holds a promise with NO task. `"a"`
    is external and untargeted, and by 500 it is past its deadline. Both
    doors answer 404, and neither may settle `"a"` on the way out — the
    request was never about it, and `readTaskObject` is the guard that
    says so.

    The alphabet cannot reach this: every task request in `kernelsResp`
    names `"x"`, which is created targeted and therefore always has a
    task. The Go fuzzer is what noticed, and this is here so the Lean
    harnesses stop needing it to. -/
def b6 : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 250, param := {}, tags := extTags }), 100),
    (.external (.taskGet { id := oid "a" }), 500),
    (.external (.taskHalt { id := oid "a" }), 500),
    (.external (.promiseGet { id := oid "a" }), 500) ]

/-! ## The alphabet

`.internal (.taskRetryTimeout)` names only its task now — the next fire instant comes from
`Env.config.retryTimeout`, so there is no instant for a script to
choose. It used to read `.internal (.taskRetryTimeout (oid "x")) 9000`. -/

def kernelsResp : List Step :=
  [ .external (.promiseCreate { id := oid "a", timeoutAt := 250, param := {}, tags := extTags }),
    .external (.taskCreate { pid := "p0", ttl := 100, action := { id := oid "x", timeoutAt := 250, param := {}, tags := tgtTags } }),
    .external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }),
    .external (.promiseSettle { id := oid "a", state := .resolved, value := {} }),
    .external (.promiseGet { id := oid "a" }),
    .external (.taskGet { id := oid "x" }),
    .external (.taskHalt { id := oid "x" }),
    .internal (.promiseTimeout (oid "a")),
    .internal (.callback (oid "a") (oid "x")),
    .internal (.taskLeaseTimeout (oid "x")),
    .internal (.taskRetryTimeout (oid "x")) ]

def seqsLenA (ks : List Step) : Nat → List (List Step)
  | 0 => [[]]
  | n + 1 => (seqsLenA ks n).flatMap (fun s => ks.map (fun k => s ++ [k]))

def seqsUpToA (ks : List Step) (n : Nat) : List (List Step) :=
  (List.range (n + 1)).flatMap (seqsLenA ks)

def instantiateA (ks : List Step) : List (Step × Nat) :=
  ks.mapIdx (fun i st => (st, 100 * (i + 1)))


end Abstraction
