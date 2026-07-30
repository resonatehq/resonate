import «04-theorems».«alpha»
import «04-theorems».«eager»

/-!  # Refinement — the concrete machine into the abstract one

THE claim this repository has been driving at: the concrete machine —
timers, deferred queue, config, in either read discipline — refines the
coalesced abstract machine. Stated over `Valid` traces, in the shape of
`04-theorems`:

> **`ConcreteRefinesAbstract`**: for every valid concrete (-m) trace
> there is a valid abstract trace, from the same (empty) start, with
> the same externalized behavior — external steps matched exactly under
> an order-preserving re-indexing — and the same messages at
> quiescence. By the twins' bisimulation (`04-theorems`), the same
> holds for -p behaviors by composition.

This is precisely the refinement that FAILED against the unfixed base
machine — `taskHalt` branching on raw state, `taskCreate` serving
unprojected records, τs firing off-schedule were each machine-checked
obstructions — and the fixes in this PR are exactly what unblocked it.
(The abstract machine ported here carries the mirrored halt fix: its
`taskHalt` touches.)

The constructive schedule is the rule translation `translate`:
external requests pass through; each concrete internal step becomes
the abstract rules it discharges —

    τPromiseTimeout id     ↦  R1 id
    τResume (a, x)         ↦  R4 a x ; R6 x (n+retry)
    τTaskRetryTimeout id   ↦  R6 id (n+retry)
    τTaskLeaseTimeout id   ↦  R5 id ; R6 id (n+retry)
    τScheduleTimeout id    ↦  R7 id

— and the concrete handlers' inline `execute` emissions (create,
release, continue) get their R6 appended, since abstract handlers emit
nothing. What needs NO translation is the settlement cascade: concrete
settle flips the task and emits unblocks inline, the abstract machine
lets fact T self-heal at the next touch and drains listeners by rule —
reads on both sides serve the same bytes meanwhile, and quiescence
reconciles the lag. `translate` is syntactic (it reads requests, not
runs); it is the canonical schedule for these scripts, while the
theorem's ∃ admits run-dependent schedules in general.

Scoping notes, both flagged before: internal promises past their
deadline are settled by touches but have no armed τ concrete-side —
scripts keep expired promises external; and a listener on an internal
promise (unrefused by `promise.register_listener`, a base wrinkle)
would strand concrete-side, so scripts keep listeners external too.  -/

namespace Abstraction

open Equivalence (Request Response eqSet)

/-- The abstract machine's alphabet: the same external requests, plus
    its seven rules with their scheduler-chosen parameters. -/
inductive AStep
  | api (rq : Request)
  | r1 (id : String)
  | r2 (id : String)
  | r3 (id address : String)
  | r4 (id awaiter : String)
  | r5 (id : String)
  | r6 (id : String) (next : Nat)
  | r7 (id : String)
  | idle

def AStep.isExternal : AStep → Bool
  | .api rq => rq.isExternal
  | _ => false

/-- The abstract machine's step: dispatch to the coalesced handlers and
    rules. External requests answer with the SAME wire responses as the
    concrete machine — the protocol surface is shared. -/
def handleA (st : AStep) (now : Nat) : AbstractModel.M Response :=
  match st with
  | .api (.promiseGet req)              => Response.promiseGet <$> AbstractModel.promiseGet req now
  | .api (.promiseCreate req)           => Response.promiseCreate <$> AbstractModel.promiseCreate req now
  | .api (.promiseSettle req)           => Response.promiseSettle <$> AbstractModel.promiseSettle req now
  | .api (.promiseRegisterCallback req) => Response.promiseRegisterCallback <$> AbstractModel.promiseRegisterCallback req now
  | .api (.promiseRegisterListener req) => Response.promiseRegisterListener <$> AbstractModel.promiseRegisterListener req now
  | .api (.promiseSearch req)           => Response.promiseSearch <$> AbstractModel.promiseSearch req now
  | .api (.scheduleGet req)             => Response.scheduleGet <$> AbstractModel.scheduleGet req now
  | .api (.scheduleCreate req)          => Response.scheduleCreate <$> AbstractModel.scheduleCreate req now
  | .api (.scheduleDelete req)          => Response.scheduleDelete <$> AbstractModel.scheduleDelete req now
  | .api (.scheduleSearch req)          => Response.scheduleSearch <$> AbstractModel.scheduleSearch req now
  | .api (.taskGet req)                 => Response.taskGet <$> AbstractModel.taskGet req now
  | .api (.taskCreate req)              => Response.taskCreate <$> AbstractModel.taskCreate req now
  | .api (.taskAcquire req)             => Response.taskAcquire <$> AbstractModel.taskAcquire req now
  | .api (.taskFence req)               => Response.taskFence <$> AbstractModel.taskFence req now
  | .api (.taskHeartbeat req)           => Response.taskHeartbeat <$> AbstractModel.taskHeartbeat req now
  | .api (.taskSuspend req)             => Response.taskSuspend <$> AbstractModel.taskSuspend req now
  | .api (.taskFulfill req)             => Response.taskFulfill <$> AbstractModel.taskFulfill req now
  | .api (.taskRelease req)             => Response.taskRelease <$> AbstractModel.taskRelease req now
  | .api (.taskHalt req)                => Response.taskHalt <$> AbstractModel.taskHalt req now
  | .api (.taskContinue req)            => Response.taskContinue <$> AbstractModel.taskContinue req now
  | .api (.taskSearch req)              => Response.taskSearch <$> AbstractModel.taskSearch req now
  -- a concrete-internal request arriving as `.api` is a stutter; the
  -- translation never produces one
  | .api _                              => return .τ
  | .r1 id      => do AbstractModel.Rules.promiseTimeout id now; return .τ
  | .r2 id      => do AbstractModel.Rules.taskFulfillment id now; return .τ
  | .r3 id a    => do AbstractModel.Rules.notify id a now; return .τ
  | .r4 id x    => do AbstractModel.Rules.resume id x now; return .τ
  | .r5 id      => do AbstractModel.Rules.leaseExpiry id now; return .τ
  | .r6 id next => do AbstractModel.Rules.dispatch id next now; return .τ
  | .r7 id      => do AbstractModel.Rules.scheduleFire id now; return .τ
  | .idle       => return .τ

def stepOfA (st : AStep) (now : Nat) (s : AbstractModel.ServerState) :
    Response × AbstractModel.ServerState :=
  Id.run ((handleA st now).run s)

structure AStateAction where
  state : AbstractModel.ServerState
  req   : AStep
  res   : Response
  now   : Nat

abbrev ATrace := Nat → AStateAction

/-- Validity of an abstract trace: step-connectivity plus the monotone
    clock — `Valid`, at the abstract machine. -/
def ValidA (tr : ATrace) : Prop :=
  ∀ t : Nat,
    (tr t).res = (stepOfA (tr t).req (tr t).now (tr t).state).1 ∧
    (tr (t + 1)).state = (stepOfA (tr t).req (tr t).now (tr t).state).2 ∧
    (tr t).now ≤ (tr (t + 1)).now

/-- Same externalized behavior, across the machines: external concrete
    steps map, order-preservingly, onto external abstract steps with
    the SAME request (wrapped in `.api`), instant, and response — and
    every external abstract step is reached. -/
def SameObservationCA (tr : Equivalence.Trace) (tr' : ATrace) : Prop :=
  ∃ φ : Nat → Nat,
    (∀ s t, s < t → φ s < φ t) ∧
    (∀ t, (tr t).req.isExternal = true →
      (tr' (φ t)).req = .api (tr t).req ∧
      (tr' (φ t)).now = (tr t).now ∧
      (tr' (φ t)).res = (tr t).res) ∧
    (∀ s, (tr' s).req.isExternal = true →
      ∃ t, (tr t).req.isExternal = true ∧ φ t = s)

/-- Quiescence, abstract side: materialize every fact (R1, R2 over all
    objects), then drain every retained obligation (R3 per listener,
    R4 per awaiter — each wake immediately dispatched, mirroring the
    concrete drain's inline emission). -/
def absQuiesce (now : Nat) : AbstractModel.M Unit := do
  let pids := (← get).promises.map (·.id)
  for id in pids do
    AbstractModel.Rules.promiseTimeout id now
  let tids := (← get).tasks.map (·.id)
  for id in tids do
    AbstractModel.Rules.taskFulfillment id now
  let ps := (← get).promises
  for p in ps do
    if p.state != .pending then
      for a in p.listeners do
        AbstractModel.Rules.notify p.id a now
      for c in p.callbacks do
        AbstractModel.Rules.resume p.id c now
        AbstractModel.Rules.dispatch c (now + 5000) now

def absQuiesced (now : Nat) (s : AbstractModel.ServerState) : AbstractModel.ServerState :=
  (Id.run ((absQuiesce now).run s)).2

/-- The asynchronous channel across the machines: equal outboxes at
    matching quiescent points. -/
def SameMessagesCA (tr : Equivalence.Trace) (tr' : ATrace) : Prop :=
  ∀ N N',
    (∀ t, N ≤ t → (tr t).req = .idle) →
    (∀ t, N' ≤ t → (tr' t).req = .idle) →
    (tr N).now = (tr' N').now →
    eqSet (Equivalence.quiesced (tr N).now (tr N).state).outbox
          (absQuiesced (tr' N').now (tr' N').state).outbox = true

/-- **THE CLAIM: the concrete machine refines the abstract machine.**
    (Stated at -m; -p inherits it through the twins' bisimulation.) -/
def ConcreteRefinesAbstract : Prop :=
  ∀ tr, Equivalence.ValidM tr → (tr 0).state = ServerModel.ServerState.init →
    ∃ tr', ValidA tr' ∧ (tr' 0).state = AbstractModel.ServerState.init ∧
      SameObservationCA tr tr' ∧ SameMessagesCA tr tr'

/-! ### The constructive schedule, executed -/

/-- Rule translation: the abstract steps a concrete step discharges.
    Purely syntactic. -/
def translate (rq : Request) (n : Nat) : List (AStep × Nat) :=
  match rq with
  | .τPromiseTimeout id   => [(.r1 id, n)]
  | .τResume r            => [(.r4 r.awaited r.awaiter, n), (.r6 r.awaiter (n + 5000), n)]
  | .τTaskRetryTimeout id => [(.r6 id (n + 5000), n)]
  | .τTaskLeaseTimeout id => [(.r5 id, n), (.r6 id (n + 5000), n)]
  | .τScheduleTimeout id  => [(.r7 id, n)]
  | .idle                 => []
  | .promiseCreate r      =>
      (AStep.api rq, n) ::
        (if r.tags.has "resonate:target" then
          match r.tags.get? "resonate:delay" with
          | some d => if ServerModel.parseNat d > n then [] else [(.r6 r.id (n + 5000), n)]
          | none => [(.r6 r.id (n + 5000), n)]
        else [])
  | .taskRelease r        => [(.api rq, n), (.r6 r.id (n + 5000), n)]
  | .taskContinue r       => [(.api rq, n), (.r6 r.id (n + 5000), n)]
  | _                     => [(.api rq, n)]

def eagerScheduleA (w : List (Request × Nat)) : List (AStep × Nat) :=
  w.flatMap fun (rq, n) => translate rq n

def runFinA (w : List (AStep × Nat)) : List Response × AbstractModel.ServerState :=
  Id.run ((w.mapM (fun (st, n) => handleA st n)).run AbstractModel.ServerState.init)

def extResponsesA (w : List (AStep × Nat)) (rs : List Response) : List Response :=
  ((w.map (·.1)).zip rs).filterMap
    (fun (st, r) => if st.isExternal then some r else none)

/-- The executable refinement check for one concrete (-m) script: the
    abstract machine, under the translated schedule, exhibits the same
    external observation, and the quiesced abstract state equals alpha
    of the quiesced concrete state. -/
def refCheckCA (w : List (Request × Nat)) (horizon : Nat) : Bool :=
  let (rsC, sC) := Equivalence.runFin Equivalence.handleM w
  let wA := eagerScheduleA w
  let (rsA, sA) := runFinA wA
  Equivalence.extResponses w rsC == extResponsesA wA rsA
    && absStateEq (alpha (Equivalence.quiesced horizon sC)) (absQuiesced horizon sA)

/-! ### The battery, refined into the abstract machine -/

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

open Equivalence (lockstepCex t1 t2 t3 t4 t5 t6 t7 t8 t9 tgtTags)

example : refCheckCA lockstepCex 1200 := by decide
example : refCheckCA t1 500 := by decide
example : refCheckCA t2 500 := by decide
example : refCheckCA t3 240 := by decide
example : refCheckCA t4 600 := by decide
example : refCheckCA t5 500 := by decide

-- `t6` in one kernel `decide` exhausts memory (all four remaining
-- battery checks evaluate true; the 10-step trace with two quiescence
-- cascades is past the kernel evaluator's practical size). Its
-- coverage splits into two shorter scripts:
def t6a : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {},
                               tags := tgtTags } }, 100),
    (.taskHeartbeat { pid := "p0", tasks := [{ id := "x", version := 1 }] }, 150),
    (.τTaskLeaseTimeout "x", 200),
    (.τTaskLeaseTimeout "x", 300),
    (.taskAcquire { id := "x", version := 1, pid := "p1", ttl := 100 }, 310),
    (.taskHalt { id := "x" }, 320),
    (.taskContinue { id := "x" }, 330) ]

def t6b : List (Request × Nat) :=
  [ (.taskCreate { pid := "p0", ttl := 100,
                   action := { id := "x", timeoutAt := 2000, param := {},
                               tags := tgtTags } }, 100),
    (.τTaskLeaseTimeout "x", 2100),
    (.τTaskRetryTimeout "x", 7200),
    (.taskGet { id := "x" }, 7300) ]

example : refCheckCA t6a 400 := by decide
example : refCheckCA t6b 7300 := by decide
example : refCheckCA t7 2500 := by decide
example : refCheckCA t8 900 := by decide
example : refCheckCA t9 100 := by decide

/-- The sweep, chunked so each kernel proof term stays bounded. -/
def chunkOk (lo hi : Nat) : Bool :=
  ((((Equivalence.seqsLen 3).drop lo).take (hi - lo)).map Equivalence.instantiate).all
    (fun w => refCheckCA w 500)

theorem smallScopeAbstraction₀ :
    ((Equivalence.seqsUpTo 2).map Equivalence.instantiate).all
      (fun w => refCheckCA w 500) = true := by decide
theorem smallScopeAbstraction₁ : chunkOk 0 182 = true := by decide
theorem smallScopeAbstraction₂ : chunkOk 182 364 = true := by decide
theorem smallScopeAbstraction₃ : chunkOk 364 546 = true := by decide
theorem smallScopeAbstraction₄ : chunkOk 546 729 = true := by decide


/-! ### The small scope, exhausted

Same alphabet and clock as `04-theorems`, at length ≤ 3 — the
per-script check here is several times the twins' (two machines, two
quiescences, the abstraction map, set-comparison up to order), and the
kernel's proof term for the length-4 sweep exceeds memory. 820
scripts, none hand-picked; the twins' sweeps cover length 4. -/



end Abstraction
