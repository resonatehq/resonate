import «01-protocol».«validation»

/-!  # The abstract machine — state, and the shape of a transition

The coalesced state, and the monad every step is written in. There is
one machine now: reads come from the environment, writes accumulate as
`Effect`, and `run` folds them onto the pre-state at the end.

`bind` passes the SAME environment to its continuation, so a read
cannot observe a write of its own transition. That is not a habit to be
maintained — it is what the type says, and it is what lets a step's
writes be collected and committed together. One step is one
transaction.

MATERIALISATION IS A PARAMETER. `Env.mat` decides whether a read that
projects a settled promise also PERSISTS that settlement. `run true`
materialises; `run false` serves the same facts as views and persists
nothing. The two used to be two machines, 387 and 386 lines differing
in one word per read; here they are one body and a bit.

The parameter scopes over EXTERNAL steps only. Internal steps
materialise under both readings, and take the forced reads
(`touchPromise`, `touchTask`, `viewPromise`) rather than the parametric
ones. Getting that wrong is not theoretical: the first derivation of
this machine substituted the parametric read into the internal steps
too, and the sweep refuted it — 60 of 1 331 scripts. `withMat` is the
fix. -/

namespace AbstractModel

open ServerModel (Tags Value PromiseState TaskState PromiseRecord
                  TaskRecord Schedule Message OutboxEntry PromiseCreateReq)

structure PromiseObject where
  id        : String
  state     : PromiseState
  param     : Value
  value     : Value       := {}
  tags      : Tags
  timeoutAt : Nat
  createdAt : Nat
  settledAt : Option Nat  := none
  callbacks : List String := []
  listeners : List String := []
  deriving Repr

def PromiseObject.toRecord (p : PromiseObject) : PromiseRecord :=
  { id := p.id, state := p.state, param := p.param, value := p.value,
    tags := p.tags, timeoutAt := p.timeoutAt, createdAt := p.createdAt,
    settledAt := p.settledAt }

def PromiseObject.isTimer (p : PromiseObject) : Bool := p.tags.isTimer

def PromiseObject.external (p : PromiseObject) : Bool :=
  p.tags.get? "resonate:external" == some "true"
    || p.tags.has "resonate:target" || p.isTimer

def PromiseObject.addCallback (p : PromiseObject) (awaiterId : String) : PromiseObject :=
  if p.callbacks.contains awaiterId then
    p
  else
    { p with callbacks := p.callbacks ++ [awaiterId] }

def PromiseObject.addListener (p : PromiseObject) (address : String) : PromiseObject :=
  if p.listeners.contains address then
    p
  else
    { p with listeners := p.listeners ++ [address] }

def PromiseObject.project (p : PromiseObject) (now : Nat) : PromiseObject :=
  if p.state == .pending ∧ p.timeoutAt ≤ now then
    if p.isTimer then
      { p with state := .resolved, settledAt := some p.timeoutAt }
    else
      { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
  else
    p

structure TaskObject where
  id        : String
  state     : TaskState
  version   : Nat
  ttl       : Option Nat    := none
  pid       : Option String := none
  expiresAt : Option Nat    := none
  retryAt   : Option Nat    := none
  resumes   : List String   := []
  deriving Repr

def TaskObject.toRecord (t : TaskObject) : TaskRecord :=
  { id := t.id, state := t.state, version := t.version,
    resumes := t.resumes.length, ttl := t.ttl, pid := t.pid }

def TaskObject.fulfill (t : TaskObject) : TaskObject :=
  { t with state := .fulfilled, pid := none, ttl := none,
           expiresAt := none, retryAt := none, resumes := [] }

def TaskObject.view (t : TaskObject) (p : PromiseObject) : TaskObject :=
  if p.state != .pending ∧ t.state != .fulfilled then t.fulfill else t

structure ServerState where
  promises  : List PromiseObject := []
  tasks     : List TaskObject    := []
  schedules : List Schedule      := []
  outbox    : List OutboxEntry   := []
  deriving Repr

def ServerState.init : ServerState := {}

inductive Effect
  | setPromise  (p : PromiseObject)
  | setTask     (t : TaskObject)
  | setSchedule (s : Schedule)
  | delSchedule (id : String)
  | setMessage  (address : String) (msg : Message)
  deriving Repr

def Effect.apply (s : ServerState) : Effect → ServerState
  | .setPromise p  => { s with promises  := p :: s.promises.filter (·.id != p.id) }
  | .setTask t     => { s with tasks     := t :: s.tasks.filter (·.id != t.id) }
  | .setSchedule c => { s with schedules := c :: s.schedules.filter (·.id != c.id) }
  | .delSchedule i => { s with schedules := s.schedules.filter (·.id != i) }
  | .setMessage a m =>
      let entry := OutboxEntry.mk a m
      { s with outbox := entry :: s.outbox.filter (fun e => e.key != entry.key) }

def applyAll (s : ServerState) : List Effect → ServerState
  | []      => s
  | e :: es => applyAll (e.apply s) es

/-- Server configuration. NOT state: no step writes it, no response
    projects it, no trace records it. It is the operator's dial, and the
    machine only reads it — which is what puts it in the reader
    environment rather than in `ServerState`.

    `retryTimeout` is the redispatch cadence: how long the server waits
    before re-offering a task nobody has claimed. It belongs to the
    SERVER. `TaskObject.ttl` is a different quantity belonging to the
    WORKER — how long a holder asked to keep its lease — and using one
    as the other means a task whose worker died is re-offered on a
    schedule set by the worker that died. It is also absent exactly
    where retry matters most, on a task nobody has acquired yet. -/
structure ServerConfig where
  retryTimeout : Nat := 5000
  deriving Repr

structure Env where
  state  : ServerState
  mat    : Bool
  config : ServerConfig := {}

def H (α : Type) : Type := Env → α × List Effect

instance : Monad H where
  pure a   := fun _ => (a, [])
  bind x f := fun e =>
    let (a, w₁) := x e
    let (b, w₂) := f a e
    (b, w₁ ++ w₂)

def ask : H Env := fun e => (e, [])

def emit (f : Effect) : H Unit := fun _ => ((), [f])

def runWith (mat : Bool) (config : ServerConfig) (act : H α) (s : ServerState) :
    α × ServerState :=
  let (a, w) := act { state := s, mat := mat, config := config }
  (a, applyAll s w)

def run (mat : Bool) (act : H α) (s : ServerState) : α × ServerState :=
  runWith mat {} act s

def getPromise (id : String) : H (Option PromiseObject) :=
  return (← ask).state.promises.find? (·.id == id)

def getTask (id : String) : H (Option TaskObject) :=
  return (← ask).state.tasks.find? (·.id == id)

def getSchedule (id : String) : H (Option Schedule) :=
  return (← ask).state.schedules.find? (·.id == id)

def setPromise (p : PromiseObject) : H Unit := emit (.setPromise p)
def setTask (t : TaskObject) : H Unit := emit (.setTask t)
def setSchedule (c : Schedule) : H Unit := emit (.setSchedule c)
def delSchedule (id : String) : H Unit := emit (.delSchedule id)
def setMessage (a : String) (m : Message) : H Unit := emit (.setMessage a m)

def setSettled (p : PromiseObject) : H Unit := do
  setPromise p
  if p.state != .pending then
    match ← getTask p.id with
    | some t => if t.state != .fulfilled then setTask t.fulfill
    | none => pure ()

def createPromise (req : PromiseCreateReq) (now : Nat) : H PromiseObject := do
  if req.timeoutAt > now then
    let p : PromiseObject :=
      { id := req.id, state := .pending, param := req.param, tags := req.tags,
        timeoutAt := req.timeoutAt, createdAt := now }
    setPromise p
    if p.tags.has "resonate:target" then
      let due :=
        match p.tags.get? "resonate:delay" with
        | some d => max (ServerModel.parseNat d) now
        | none => now
      setTask { id := p.id, state := .pending, version := 0, retryAt := some due }
    return p
  else
    let state :=
      if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout
    let p : PromiseObject :=
      { id := req.id, state := state, param := req.param, tags := req.tags,
        timeoutAt := req.timeoutAt, createdAt := req.timeoutAt,
        settledAt := some req.timeoutAt }
    setPromise p
    if p.tags.has "resonate:target" then
      setTask { id := p.id, state := .fulfilled, version := 0 }
    return p

def readPromise (id : String) (now : Nat) : H (Option PromiseObject) := do
  match ← getPromise id with
  | none => return none
  | some p =>
      let p' := p.project now
      if (← ask).mat && p'.state != p.state then
        setSettled p'
      return some p'

def createIfAbsent (req : PromiseCreateReq) (now : Nat) : H Unit := do
  match ← readPromise req.id now with
  | some _ => pure ()
  | none   => let _ ← createPromise req now

def readTask (id : String) (now : Nat) :
    H (Option (TaskObject × Option PromiseObject)) := do
  match ← getTask id with
  | none => return none
  | some t =>
  match ← readPromise t.id now with
  | none => return some (t, none)
  | some p =>
      let u := t.view p
      if (← ask).mat && u.state != t.state then
        setTask u
      return some (u, some p)

def withMat (mat : Bool) (act : H α) : H α := fun e => act { e with mat := mat }

def touchPromise (id : String) (now : Nat) : H (Option PromiseObject) :=
  withMat true (readPromise id now)

def touchTask (id : String) (now : Nat) :
    H (Option (TaskObject × Option PromiseObject)) :=
  withMat true (readTask id now)

def viewPromise (id : String) (now : Nat) : H (Option PromiseObject) :=
  withMat false (readPromise id now)

end AbstractModel
