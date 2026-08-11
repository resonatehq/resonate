import «01-protocol».«validation»

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

abbrev H := StateM ServerState

def getPromise (id : String) : H (Option PromiseObject) :=
  return (← get).promises.find? (·.id == id)

def setPromise (p : PromiseObject) : H Unit :=
  modify fun s => { s with promises := p :: s.promises.filter (·.id != p.id) }

def getTask (id : String) : H (Option TaskObject) :=
  return (← get).tasks.find? (·.id == id)

def setTask (t : TaskObject) : H Unit :=
  modify fun s => { s with tasks := t :: s.tasks.filter (·.id != t.id) }

def getSchedule (id : String) : H (Option Schedule) :=
  return (← get).schedules.find? (·.id == id)

def setSchedule (sch : Schedule) : H Unit :=
  modify fun s => { s with schedules := sch :: s.schedules.filter (·.id != sch.id) }

def delSchedule (id : String) : H Unit :=
  modify fun s => { s with schedules := s.schedules.filter (·.id != id) }

def setMessage (address : String) (msg : Message) : H Unit :=
  modify fun s =>
    let entry := OutboxEntry.mk address msg
    let key   := entry.key
    { s with outbox := entry :: s.outbox.filter (fun e => e.key != key) }

def setSettled (p : PromiseObject) : H Unit := do
  setPromise p
  if p.state != .pending then
    match ← getTask p.id with
    | some t => if t.state != .fulfilled then setTask t.fulfill
    | none => pure ()

def createPromise (req : PromiseCreateReq) (now : Nat) : H PromiseObject := do
  if req.timeoutAt > now then
    let p : PromiseObject :=
      { id := req.id
        state := .pending
        param := req.param
        tags := req.tags
        timeoutAt := req.timeoutAt
        createdAt := now }
    setPromise p
    if p.tags.has "resonate:target" then
      let due :=
        match p.tags.get? "resonate:delay" with
        | some d => max (ServerModel.parseNat d) now
        | none => now
      setTask { id := p.id, state := .pending, version := 0,
                retryAt := some due }
    return p
  else
    let state :=
      if req.tags.isTimer then
        PromiseState.resolved
      else
        PromiseState.rejectedTimedout
    let p : PromiseObject :=
      { id := req.id
        state := state
        param := req.param
        tags := req.tags
        timeoutAt := req.timeoutAt
        createdAt := req.timeoutAt
        settledAt := some req.timeoutAt }
    setPromise p
    if p.tags.has "resonate:target" then
      setTask { id := p.id, state := .fulfilled, version := 0 }
    return p

def touchPromise (id : String) (now : Nat) : H (Option PromiseObject) := do
  match ← getPromise id with
  | none => return none
  | some p =>
      let p' := p.project now
      if p'.state != p.state then
        setSettled p'
      return some p'

def createIfAbsent (req : PromiseCreateReq) (now : Nat) : H Unit := do
  match ← touchPromise req.id now with
  | some _ => pure ()
  | none   => let _ ← createPromise req now

def viewPromise (id : String) (now : Nat) : H (Option PromiseObject) := do
  return (← getPromise id).map (·.project now)

def viewTask (id : String) (now : Nat) :
    H (Option (TaskObject × Option PromiseObject)) := do
  match ← getTask id with
  | none => return none
  | some t =>
  match ← getPromise t.id with
  | none => return some (t, none)
  | some p =>
      let p := p.project now
      return some (t.view p, some p)

def touchTask (id : String) (now : Nat) :
    H (Option (TaskObject × Option PromiseObject)) := do
  match ← getTask id with
  | none => return none
  | some t =>
  match ← touchPromise t.id now with
  | none => return some (t, none)
  | some p =>
      if p.state != .pending ∧ t.state != .fulfilled then
        let t := t.fulfill
        setTask t
        return some (t, some p)
      else
        return some (t, some p)

end AbstractModel
