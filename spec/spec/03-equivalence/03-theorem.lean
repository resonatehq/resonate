import «03-equivalence».«01-driver»

/-!  # Equivalence — the theorem

Two theorem-shaped artifacts, honestly labeled.

**The claim**, stated formally as `EquivalenceTheorem` below: for every
trace with a monotone clock bounded by the closure horizon, the twin
machines return identical responses and closure-equal states. The
clock hypothesis is necessary, not decoration: with time running
backward, -m has already persisted a fact that -p's projection
un-observes at the earlier instant — the machines genuinely differ on
non-monotone traces, so the universal statement without the hypothesis
is false.

**What is proved here**, by kernel `decide`:

  * `smallScopeEquivalence` — the claim for EVERY trace up to length 4
    over an 8-operation alphabet on a fixed monotone clock that crosses
    the promise deadlines mid-trace (4 681 traces, exhaustive within
    scope). Unlike the hand-picked battery of `02-traces.lean`, no
    trace in this space was chosen; adversarial interleavings —
    early τ firings, operations on absent objects, suspension after
    settlement, reads astride the deadline — are all inside.
  * the battery of `02-traces.lean` — targeted deep traces beyond
    length 4 (leases, retries, fencing, delays).

The full `EquivalenceTheorem` is an open obligation. Its proof is an
induction over the trace, maintaining the simulation invariant
"`stateEq (close now sP) (close now sM)` for the current `now`", with
one commutation lemma per handler: each -p handler followed by closure
equals closure followed by the -m handler. The touch is by construction
the closure restricted to one object, so each lemma is a statement
about the closure's idempotence and locality — no new machinery, only
casework.  -/

namespace Equivalence

def opNow : Op → Nat
  | .promiseGet _ n       => n
  | .promiseCreate _ n    => n
  | .promiseSettle _ n    => n
  | .registerCallback _ n => n
  | .registerListener _ n => n
  | .taskGet _ n          => n
  | .taskCreate _ n       => n
  | .taskAcquire _ n      => n
  | .taskFence _ n        => n
  | .taskHeartbeat _ n    => n
  | .taskSuspend _ n      => n
  | .taskFulfill _ n      => n
  | .taskRelease _ n      => n
  | .taskHalt _ n         => n
  | .taskContinue _ n     => n
  | .tauPromise _ n       => n
  | .tauRetry _ n         => n
  | .tauLease _ n         => n
  | .tauDrain n           => n

/-- THE CLAIM. Every trace with a monotone clock, closed at any horizon
    past its last instant, drives the twin machines to identical
    responses and closure-equal states. -/
def EquivalenceTheorem : Prop :=
  ∀ (ops : List Op) (horizon : Nat),
    List.Pairwise (· ≤ ·) (ops.map opNow) →
    (∀ n ∈ ops.map opNow, n ≤ horizon) →
    equivalent ops horizon = true

/-! ### The small-scope theorem

Alphabet: two promises — `a` (external, awaited) and `x` (targeted,
with task) — both with deadline 250; creation, suspension, settlement,
read, halt, the promise-timeout τ, and the drain. Clock: position `i`
runs at `100·(i+1)`, so every trace of length ≥ 3 crosses the deadlines
between its second and third operations. -/

def kernels : List (Nat → Op) :=
  [ fun n => .promiseCreate { id := "a", timeoutAt := 250, param := {},
                              tags := [("resonate:external", "true")] } n,
    fun n => .taskCreate { pid := "p0", ttl := 100,
                           action := { id := "x", timeoutAt := 250, param := {},
                                       tags := [("resonate:target", "w1")] } } n,
    fun n => .taskSuspend { id := "x", version := 1,
                            actions := [{ awaited := "a", awaiter := "x" }] } n,
    fun n => .promiseSettle { id := "a", state := .resolved, value := {} } n,
    fun n => .taskGet { id := "x" } n,
    fun n => .taskHalt { id := "x" } n,
    fun n => .tauPromise "x" n,
    fun n => .tauDrain n ]

def seqsLen : Nat → List (List (Nat → Op))
  | 0 => [[]]
  | n + 1 => (seqsLen n).flatMap (fun s => kernels.map (fun k => s ++ [k]))

def seqsUpTo (n : Nat) : List (List (Nat → Op)) :=
  (List.range (n + 1)).flatMap seqsLen

def instantiate (ks : List (Nat → Op)) : List Op :=
  ks.mapIdx (fun i k => k (100 * (i + 1)))

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

/-- Exhaustive within scope: all 4 681 traces of length ≤ 4. -/
theorem smallScopeEquivalence :
    ((seqsUpTo 4).map instantiate).all (fun ops => equivalent ops 500) = true := by
  decide

end Equivalence
