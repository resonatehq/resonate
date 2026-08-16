import «04-theorems».«corpus»
import «02-abstract».«properties»

/-!  # What bounding the trace actually buys

`Legal tr` is `∀ t : Nat, …`, so the obvious experiment is to bound `t`
and see whether the theorem becomes decidable. It does not, and the
reason is worth having written down, because it says exactly what the
1 464-script sweep IS.

## Bounding `t` alone changes nothing

`valid_implies_legal` quantifies over two things: the instant `t`, and
the trace `tr : Nat → StateAction`. Bounding `t` leaves the second
alone, and `Trace` is a function space — uncountable, and with no
`Decidable` instance for any non-trivial predicate over it. `decide`
fails identically at `∀ t, …` and at `∀ t < 5, …`.

## But a `Valid` trace is not free

That is the useful half. `Valid mat tr` pins the state at every index to
the step taken at the one before, so a `Valid` trace starting at `init`
carries no information beyond its own SCHEDULE — the sequence of
`(request, instant)` pairs it performs. `valid_state_at` below proves
it: the state at index `n` is exactly `runFin` over the first `n`
actions.

So bounding `t` to `N` turns the conclusion into a statement about
finite schedules of length `N`. Still not decidable, because a schedule
ranges over all requests — every id is a `String`, every deadline a
`Nat`.

## Bound the alphabet too, and you have the sweep

Restrict the requests to a finite kernel set and the instants to a
finite set, and the space of schedules of length ≤ 3 becomes 1 464
elements. That is `properties-check.lean`, exactly — and
`valid_state_at` is what licenses calling it a bounded instance of the
theorem rather than a merely suggestive check. The sweep runs the
machine over schedules; the theorem quantifies over traces; this lemma
says those are the same thing once `Valid` and `init` are assumed.

So the honest ordering is: the sweep is `valid_implies_legal` with BOTH
quantifiers made finite — the instant by taking a prefix, the trace by
taking an 11-symbol alphabet. What the induction adds is removing both
bounds, one catalogue entry at a time. -/

namespace Abstraction
namespace Bounded

open AbstractModel

/-- The schedule a trace performs in its first `n` steps. -/
def sched (tr : Trace) (n : Nat) : List (Step × Nat) :=
  (List.range n).map (fun t => ((tr t).req, (tr t).now))

theorem sched_succ (tr : Trace) (n : Nat) :
    sched tr (n + 1) = sched tr n ++ [((tr n).req, (tr n).now)] := by
  unfold sched
  rw [List.range_succ, List.map_append]
  rfl

theorem runFin_snoc (mat : Bool) :
    ∀ (w : List (Step × Nat)) (x : Step × Nat) (s : ServerState),
      (runFin mat (w ++ [x]) s).2 = (stepOf mat x.1 x.2 (runFin mat w s).2).2
  | [],           _, _ => rfl
  | (st, n) :: w, x, s => runFin_snoc mat w x (stepOf mat st n s).2

/-- **A `Valid` trace is its own schedule, run.** The state at index `n`
    is what the machine reaches from the trace's own starting state by
    performing its own first `n` actions.

    This is the bridge between the two halves of the project. The
    theorem quantifies over TRACES; every harness in this directory
    enumerates SCHEDULES. Without this lemma those are two different
    subjects and the sweep is merely suggestive; with it the sweep is
    the theorem with both quantifiers bounded. -/
theorem valid_state_at (mat : Bool) (tr : Trace) (hv : Valid mat tr) :
    ∀ n, (tr n).state = (runFin mat (sched tr n) (tr 0).state).2
  | 0     => rfl
  | n + 1 => by
      rw [(hv n).2.1, sched_succ, runFin_snoc, ← valid_state_at mat tr hv n]

/-- And with the initial condition, the whole trace is determined by a
    list of `(request, instant)` pairs. -/
theorem valid_state_at_init (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) (n : Nat) :
    (tr n).state = (runFin mat (sched tr n) ServerState.init).2 := by
  rw [valid_state_at mat tr hv n, h0]

/-! ## The conclusion, bounded

`Legal`'s body at index `n`, rewritten to mention only the schedule.
Nothing here is a new fact — it is the same proposition with `tr`
eliminated — but that is the point: what is left quantifies over a LIST,
and a list over a finite alphabet is enumerable. -/

def foldAt (now : Nat) (a b : ServerState) : Bool :=
  AbstractModel.Properties.catalogue.all fun l =>
    match l.property with
    | .state f => f now a
    | .trans f => f now a b

theorem legal_at_is_about_the_schedule (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) (n : Nat) :
    foldAt (tr n).now (tr n).state (tr (n + 1)).state
      = foldAt (tr n).now (runFin mat (sched tr n) ServerState.init).2
                          (runFin mat (sched tr (n + 1)) ServerState.init).2 := by
  rw [valid_state_at_init mat tr hv h0 n, valid_state_at_init mat tr hv h0 (n + 1)]

/-- Bounded `Legal`: the conclusion restricted to the first `N`
    instants, stated over schedules. If every prefix of the trace's own
    schedule runs legally, the trace is legal that far. -/
theorem legal_below_of_schedule (mat : Bool) (tr : Trace) (hv : Valid mat tr)
    (h0 : (tr 0).state = ServerState.init) (N : Nat)
    (h : ∀ n < N, foldAt (tr n).now (runFin mat (sched tr n) ServerState.init).2
                         (runFin mat (sched tr (n + 1)) ServerState.init).2 = true) :
    ∀ n < N, foldAt (tr n).now (tr n).state (tr (n + 1)).state = true := by
  intro n hn
  rw [legal_at_is_about_the_schedule mat tr hv h0 n]
  exact h n hn

/-! ## What is still not decidable, and why

Everything above eliminates `tr` in favour of a list. What it does NOT
do is make anything decidable, and the reason is that `sched tr N` is
still an arbitrary list of arbitrary requests: `promise.create` carries
a `String` id and a `Nat` deadline, so even one step ranges over an
infinite set.

The step from here to a `decide` is to fix the alphabet — and that step
is not a proof technique, it is a modelling decision about which
requests are worth trying. `corpus.lean` makes it: 11 request kernels,
schedules of length ≤ 3, which is 1 + 11 + 121 + 1331 = 1 464 schedules,
each run under both read disciplines. That is `stage1_sweep`.

So the two artefacts are the same statement at different settings of two
dials:

    quantifier      theorem            sweep
    ------------    ---------------    -----------------------
    instant t       all of ℕ           prefix of length ≤ 3
    trace tr        all Valid traces   1 464 enumerated schedules
    catalogue       all 95 entries     all 95 entries

and the induction in `entries.lean` / `trans.lean` is the work of
turning the first column into the second's guarantees without the
bounds — 32 of the 95 entries so far, in `holds.lean`. -/

/-! ## Where it flips

The three experiments, in order.

BOUND `t` TO 5, trace still arbitrary — `decide +revert` reports

    failed to synthesize Decidable (∀ (mat : Bool) (tr : Trace), …)

identical to the unbounded case. Bounding the instant bought nothing,
because the trace was never the finite part.

BOUND `t` AND ELIMINATE THE TRACE via `valid_state_at`, leaving an
arbitrary schedule — the same failure. A schedule is a list of
requests, and one `promise.create` already ranges over every `String`
id and every `Nat` deadline.

BOUND THE ALPHABET, and it flips. Below: three request kernels,
schedules of length ≤ 2, so 1 + 3 + 9 = 13 schedules, every state and
every consecutive pair checked against all 95 entries — by `decide`, in
the kernel. `stage1_sweep` is this with 11 kernels and length ≤ 3.

That is the whole shape of the thing. The trace quantifier is what
resists, not the instant quantifier, and the only way past it by
computation is to choose an alphabet — which is a modelling decision
about which requests are worth trying, not a proof technique. The
induction is the alternative: it removes both bounds instead of
choosing them. -/

def statesOf (mat : Bool) :
    List (Step × Nat) → ServerState → List (Nat × ServerState × ServerState)
  | [],           _ => []
  | (st, n) :: w, s =>
      let s' := (stepOf mat st n s).2
      (n, s, s') :: statesOf mat w s'

def legalRunOf (mat : Bool) (w : List (Step × Nat)) : Bool :=
  (statesOf mat w ServerState.init).all (fun (n, a, b) => foldAt n a b)

/-- Three kernels, length ≤ 2, both read disciplines. The alphabet is
    what makes this a computation. -/
theorem tiny_sweep_materialized :
    ((seqsUpToA (kernelsResp.take 3) 2).map instantiateA).all (legalRunOf true) = true := by
  decide

theorem tiny_sweep_projected :
    ((seqsUpToA (kernelsResp.take 3) 2).map instantiateA).all (legalRunOf false) = true := by
  decide

/-! ## A demonstration

The witness from `vacuity.lean` is a `Valid` trace, so `valid_state_at`
applies to it: its state at index 9 is `runFin` over its own schedule.
Both sides `decide`, and they agree — which is the lemma made concrete
rather than a second proof of it. -/

def demoSched : List (Step × Nat) := b1

theorem demo_runFin_reaches_fulfilment :
    ((runFin true demoSched ServerState.init).2.tasks.any (·.state == .fulfilled)) = true := by
  decide

theorem demo_runFin_settles :
    ((runFin true demoSched ServerState.init).2.promises.any (·.state != .pending)) = true := by
  decide

end Bounded
end Abstraction
