import «02-abstract».«system»

/-!  # Admissibility checking — a shell around the specification

Nothing in `spec/01-` … `spec/04-` is touched. This file imports the
specification's own trace framework (`Request`, `Response`, `handle`)
and wraps it.

## The question

An implementation is observed. You see its EXTERNAL events — a request,
the response it gave, and when. You do NOT see its internal steps:
when a timeout fired, when a resume drained. The specification does not
fix those either; the τ schedule is deliberately unspecified, which is
exactly what `Env.mat` is: the read discipline as a parameter.

So "does the specification reproduce this trace" is the wrong question —
there is no single trace to reproduce. The right one is:

> Does there EXIST a schedule of internal steps under which the
> specification produces exactly these external events?

That is an existential over the unobserved. The retired TLA+ trace
validation used the same technique: recover what the harness never
recorded by existential quantification, and let post-state agreement
select the witness.

## The method

Carry a SET of candidate states rather than one — subset construction,
the standard way to run a nondeterministic machine against a word. At
each observed event: close each candidate under enabled τs, apply the
observed request, and keep only those whose response matches what was
observed. Empty set means no schedule explains the trace.

Three things make this tractable here, and none is an accident:

* handlers are total deterministic functions of `(req, now, state)`, so
  the τ schedule is the ONLY nondeterminism;
* τ enabledness is explicit in the state — the obligation records ARE
  the enabledness, which the trace framework calls being
  "OBLIGATION-GUARDED";
* every τ discharges its obligation or is a silent no-op, so closures
  terminate rather than cycling.

## Canonical form

Without it this does not work at all. Two τs on disjoint objects commute
but leave the component lists in different orders, and `BEq ServerState`
is list equality — so the candidate set would double on every pair of
independent obligations instead of collapsing. `canon` sorts every
component by a key, which is sound because every component is a keyed
collection (`setPromise`, `setTask`, `setMessage`, `defer` all
filter-then-prepend on a key). -/

namespace TraceCheck

open ServerModel AbstractModel Abstraction Equivalence

/-! ## Canonicalisation -/

private def insertBy {α} (key : α → String) (x : α) : List α → List α
  | []      => [x]
  | y :: ys => if key x ≤ key y then x :: y :: ys else y :: insertBy key x ys

private def sortBy {α} (key : α → String) : List α → List α
  | []      => []
  | x :: xs => insertBy key x (sortBy key xs)

private def pad (n : Nat) : String :=
  let s := toString n
  "".pushn '0' (20 - min 20 s.length) ++ s

def canon (s : ServerState) : ServerState :=
  { s with
      objects          := sortBy (·.id) s.objects |>.map fun o =>
                            { o with
                                promise := { o.promise with
                                             callbacks := sortBy id o.promise.callbacks
                                             listeners := sortBy id o.promise.listeners }
                                task := o.task.map fun t =>
                                          { t with resumes := sortBy id t.resumes } }
      schedules        := sortBy (·.id) s.schedules
      outbox           := sortBy (·.key) s.outbox }

/-! ## Internal steps, as their own type

`Request` mixes the client-visible actions with the machine's own. A
schedule may only contain the latter, and saying so with a side-condition
(`∀ t ∈ σ, t.isExternal = false`) leaves the invariant to be re-checked
everywhere it matters — and leaves `affects` with a catch-all case that a
new τ would fall into silently. That catch-all is how the cone went
unsound.

`Tau` makes it structural: a schedule is a `List Tau`, the side-condition
disappears, and every function over τs is an exhaustive match. Adding a
constructor here breaks `affects` until someone says what it reaches. -/

inductive Tau
  | promiseTimeout   (id : String)
  | listener         (id address : String)
  | callback         (id awaiter : String)
  | taskLeaseTimeout (id : String)
  | taskRetryTimeout (id : String)
  | scheduleTimeout  (id : String)
  deriving Repr, DecidableEq

/-- Six now, not five, and the shapes changed. The concrete machine
    drained listeners inline inside a touch and pushed callbacks onto a
    `deferred` queue, so a schedule named a `ResumeReq` and never named
    a listener at all. The abstract machine drains both explicitly, one
    obligation at a time, so both are steps an observer's explanation
    may have to include — and each names the obligation it discharges
    rather than a queue entry. -/
def Tau.toStep : Tau → Step
  | .promiseTimeout id   => .r1 id
  | .listener id addr    => .r3 id addr
  | .callback id awaiter => .r4 id awaiter
  | .taskLeaseTimeout id => .r5 id
  | .taskRetryTimeout id => .r6 id
  | .scheduleTimeout id  => .r7 id

/-- A witness is meant to be read, so it prints as the step rather than
    as its constructor. -/
def Tau.pretty : Tau → String
  | .promiseTimeout id   => s!"τ promise.timeout {id}"
  | .listener id addr    => s!"τ listener {id} → {addr}"
  | .callback id awaiter => s!"τ callback {id} → {awaiter}"
  | .taskLeaseTimeout id => s!"τ task.lease {id}"
  | .taskRetryTimeout id => s!"τ task.retry {id}"
  | .scheduleTimeout id  => s!"τ schedule.timeout {id}"

instance : ToString Tau := ⟨Tau.pretty⟩

/-- Firing a τ is firing its step. Materialised (`mat := true`); the
    abstract twins agree on responses, so the discipline is not
    observable through the channel this checker compares. -/
def Tau.step (t : Tau) (now : Nat) (s : ServerState) : ServerState :=
  (Abstraction.stepOf true t.toStep now s).2

/-! ## Enabled internal steps

Read off the OBJECTS. The concrete machine kept a table per timer kind,
so this was a filter over four indexes; the abstract machine carries the
deadline on the object it belongs to, so the same question is asked of
the promise and task lists directly.

This must be an over-approximation, never an under-one. A τ the machine
would refuse costs a candidate that dies at its own guard; a τ omitted
here costs a WITNESS, and the checker reports REFUTED for a trace that
conforms. Where the machine's guard is subtler than the shape — the
lease timeout also requires the promise to be live — the extra
conjunct is left out on purpose. -/

def enabledTaus (s : ServerState) (now : Nat) : List Tau :=
  -- r1: a pending promise whose deadline has passed
  (s.objects.filter (fun o => o.promise.state == .pending && o.promise.timeoutAt ≤ now)).map
      (fun o => .promiseTimeout o.id)
  -- r3/r4: obligations on a promise that READS settled, which is Fact P
  -- and why this consults `project` rather than the stored state
  ++ s.objects.flatMap (fun o =>
       if (o.promise.project now).state != .pending then
         o.promise.listeners.map (Tau.listener o.id) else [])
  ++ s.objects.flatMap (fun o =>
       if (o.promise.project now).state != .pending then
         o.promise.callbacks.map (Tau.callback o.id) else [])
  -- r5: an acquired task whose lease has lapsed
  ++ (s.objects.filter (fun o =>
        o.task.any fun t => t.state == .acquired && t.expiresAt.getD (now + 1) ≤ now)).map
       (fun o => .taskLeaseTimeout o.id)
  -- r6: a pending task whose dispatch clock is due
  ++ (s.objects.filter (fun o =>
        o.task.any fun t => t.state == .pending && t.retryAt.getD (now + 1) ≤ now)).map
       (fun o => .taskRetryTimeout o.id)
  -- r7: a schedule due to fire
  ++ (s.schedules.filter (·.nextRunAt ≤ now)).map (fun c => .scheduleTimeout c.id)

/-! ## Cone of influence

The closure above explores every subset of the enabled τs, which is
`2^n` and is the only thing that actually limits this checker. Most of
those subsets are irrelevant: a timeout on promise `a5` cannot change
the response to `promiseGet a0`.

So: at each observed event, fire only the τs whose objects the event's
request also touches. The rest are not discarded — they stay armed in
the state, because ENABLEDNESS LIVES IN THE STATE, not in the closure.
They will be explored at the first later event that names them. That is
partial-order reduction: independent steps commute, so firing them later
is a canonical choice rather than a lost one.

**Soundness, precisely.** This is sound for the RESPONSE channel, which
is what `validate` compares. Deferring a τ can change components no
response projects — `taskTimeouts` instants, `outbox` contents — because
a deferred retry may later find its promise settled and become a no-op
where firing it earlier would have emitted an execute. If a snapshot
channel is ever added, `touches` must widen to cover everything the
snapshot observes, or the reduction becomes unsound for it. -/

def touches : Request → List String
  | .promiseGet r              => [r.id]
  | .promiseCreate r           => [r.id]
  | .promiseSettle r           => [r.id]
  | .promiseRegisterCallback r => [r.awaited, r.awaiter]
  | .promiseRegisterListener r => [r.awaited]
  | .promiseSearch _           => []
  | .scheduleGet r             => ["sched:" ++ r.id]
  | .scheduleCreate r          => ["sched:" ++ r.id, r.promiseId]
  | .scheduleDelete r          => ["sched:" ++ r.id]
  | .scheduleSearch _          => []
  | .taskGet r                 => [r.id]
  | .taskCreate r              => [r.action.id]
  | .taskAcquire r             => [r.id]
  | .taskFence r               => [r.id, r.action.targetId]
  | .taskHeartbeat r           => r.tasks.map (·.id)
  | .taskSuspend r             => r.id :: r.actions.map (·.awaited)
  | .taskFulfill r             => [r.id]
  | .taskRelease r             => [r.id]
  | .taskHalt r                => [r.id]
  | .taskContinue r            => [r.id]
  | .taskSearch _              => []

/-- What firing this τ can AFFECT, which is not the same as the object it
    names — and getting that wrong is how the reduction went unsound the
    first time. `τPromiseTimeout a` names `a`, but settling `a` defers a
    resume for every awaiter registered on it, and those are different
    ids; a cone keyed on the name alone refuses to fire it when the
    observed request reads the awaiter, and then reports a conforming
    trace REFUTED.

    Schedule timeouts create promises whose ids come from `expand`, so
    there is no cheap bound on what they reach: they are marked `"*"` and
    treated as always relevant. Conservative, and schedules are rare in a
    trace. -/
def affects (s : ServerState) : Tau → List String
  | .promiseTimeout id =>
      id :: ((s.objects.filter (·.id == id)).flatMap (·.promise.callbacks))
  | .listener id _         => [id]
  | .callback id awaiter   => [id, awaiter]
  | .taskLeaseTimeout id   => [id]
  | .taskRetryTimeout id   => [id]
  | .scheduleTimeout _     => ["*"]

/-- The τs that could bear on this request — the CONE, closed
    transitively. Start from what the request reads; repeatedly pull in
    any enabled τ whose affected set meets it, and grow the set by what
    that τ affects. A chain like `τPromiseTimeout a` → defers `(a,x)` →
    `τResume` wakes `x` is only caught because the first step's affected
    set already mentions `x`. -/
partial def relevantTaus (req : Request) (s : ServerState) (now : Nat) : List Tau :=
  let enabled := enabledTaus s now
  let rec grow (fuel : Nat) (want : List String) : List String :=
    match fuel with
    | 0 => want
    | fuel + 1 =>
      let hit := enabled.filter fun t =>
        let a := affects s t
        a.contains "*" || a.any (want.contains ·)
      let want' := hit.foldl (init := want) fun acc t =>
        (affects s t).foldl (init := acc) fun acc' o =>
          if acc'.contains o then acc' else acc' ++ [o]
      if want'.length == want.length then want else grow fuel want'
  let want := grow 32 (touches req)
  enabled.filter fun t =>
    let a := affects s t
    a.contains "*" || a.any (want.contains ·)

/-- A candidate: a state the machine could be in, the internal steps that
    would have taken it there, and the canonical form CACHED. The cache
    matters — `canon` sorts the whole state, and recomputing it inside
    every pairwise comparison made dedup quadratic in states times
    quadratic in objects. -/
structure Cand where
  state    : ServerState
  schedule : List (Tau × Nat)
  key      : ServerState

def mkCand (state : ServerState) (schedule : List (Tau × Nat)) : Cand :=
  { state, schedule, key := canon state }

def dedup (cs : List Cand) : List Cand :=
  cs.foldl (init := []) fun acc c =>
    if acc.any (fun d => d.key == c.key) then acc else acc ++ [c]

/-- Every state reachable from `cs` by firing zero or more enabled τs at
    `now`. Fuelled: the closure does terminate — each τ discharges its
    own obligation, and a re-armed retry lands strictly in the future —
    but the bound turns a surprise into an INCONCLUSIVE verdict instead
    of a hang. -/
partial def tauClosureBy (pick : ServerState → List Tau)
    (now : Nat) (fuel : Nat) (cs : List Cand) : List Cand × Bool :=
  let rec go (fuel : Nat) (seen : List Cand) (frontier : List Cand) : List Cand × Bool :=
    match fuel with
    -- fuel exhausted with work outstanding: the closure is TRUNCATED, not
    -- saturated, and a refutation drawn from it would be unjustified
    | 0 => (seen, frontier.isEmpty)
    | fuel + 1 =>
      let next := frontier.flatMap fun c =>
        (pick c.state).map fun t =>
          mkCand (t.step now c.state) (c.schedule ++ [(t, now)])
      let fresh := next.filter fun c => !seen.any (fun d => d.key == c.key)
      if fresh.isEmpty then (seen, true)
      else
        let fresh := dedup fresh
        go fuel (seen ++ fresh) fresh
  go fuel (dedup cs) (dedup cs)

/-! ## Observations -/

structure Observation where
  req : Request
  res : Response
  now : Nat
  deriving Repr

/-! ## The verdict

Three constructors, not a `Bool`. A bounded search must be able to say "I
stopped early" as an answer distinct from "I found nothing", or a
refutation could rest on a closure that was merely cut short. -/

inductive Verdict
  | admissible   (witness : List (Tau × Nat)) (maxFanout : Nat) (steps : Nat)
  | refuted      (atEvent : Nat) (survivorsBefore : Nat)
  | inconclusive (atEvent : Nat) (reason : String)
  deriving Repr

/-! ## Producing traces

Run a script — internal steps included — through the specification and
keep only what an observer outside the server could have seen. The τs
are DISCARDED, which is the point: the validator has to rediscover a
schedule that explains the external events. -/

def record : List (Step × Nat) → ServerState → List Observation
  | [],              _ => []
  | (st, n) :: rest, s =>
      let (res, s') := Abstraction.stepOf true st n s
      let here := match st with
        | .api rq => [({ req := rq, res := res, now := n } : Observation)]
        | _       => []
      here ++ record rest s'

def recordFrom (script : List (Step × Nat)) : List Observation :=
  record script ServerState.init

/-! ## Reporting -/

/-- The verdict WITHOUT the cost metrics — what the reduction must
    preserve. `maxFanout` is expected to differ; the answer is not. -/
def verdictKind : Verdict → String
  | .admissible w _ n  => s!"ADMISSIBLE/{n}/{w.length}"
  | .refuted i _       => s!"REFUTED/{i}"
  | .inconclusive i _  => s!"INCONCLUSIVE/{i}"

def verdictLine : Verdict → String
  | .admissible w f n   => s!"ADMISSIBLE   events={n} maxFanout={f} witnessTaus={w.length}"
  | .refuted i n        => s!"REFUTED      at event {i} (survivors before: {n})"
  | .inconclusive i r   => s!"INCONCLUSIVE at event {i}: {r}"

end TraceCheck
