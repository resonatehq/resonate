import «01-objects».«types»

namespace ServerModel

def Tags.get? (t : Tags) (k : String) : Option String :=
  (t.find? (·.fst == k)).map (·.snd)

def Tags.has (t : Tags) (k : String) : Bool :=
  (t.get? k).isSome

def Tags.isTimer (t : Tags) : Bool :=
  t.get? "resonate:timer" == some "true"

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
  p.tags.has "resonate:target" || p.isTimer

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

structure TaskObject where
  id      : String
  state   : TaskState
  version : Nat
  ttl     : Option Nat    := none
  pid     : Option String := none
  resumes : List String   := []
  deriving Repr

def TaskObject.toRecord (t : TaskObject) : TaskRecord :=
  { id := t.id, state := t.state, version := t.version,
    resumes := t.resumes.length, ttl := t.ttl, pid := t.pid }

structure PromiseTimeout where
  id      : String
  timeout : Nat
  deriving Repr

structure TaskTimeout where
  id      : String
  kind    : Nat   -- 0 = pending retry, 1 = lease expiration
  timeout : Nat
  deriving Repr

structure ScheduleTimeout where
  id      : String
  timeout : Nat
  deriving Repr

inductive Message
  | execute (taskId : String) (version : Nat)
  | unblock (promise : PromiseRecord)
  deriving Repr

structure OutboxEntry where
  address : String
  message : Message
  deriving Repr

def OutboxEntry.key : OutboxEntry → String
  | { message := .execute taskId _,  .. } => taskId
  | { address, message := .unblock p }    => s!"{p.id}:notify:{address}"

structure ServerConfig where
  retryTimeout : Nat := 5000
  deriving Repr

structure ServerState where
  config           : ServerConfig         := {}
  promises         : List PromiseObject   := []
  tasks            : List TaskObject       := []
  schedules        : List Schedule         := []
  promiseTimeouts  : List PromiseTimeout   := []
  taskTimeouts     : List TaskTimeout       := []
  scheduleTimeouts : List ScheduleTimeout  := []
  outbox           : List OutboxEntry       := []
  deriving Repr

def ServerState.init : ServerState := {}

abbrev M := StateM ServerState

def getPromise (id : String) : M (Option PromiseObject) :=
  return (← get).promises.find? (·.id == id)

def setPromise (p : PromiseObject) : M Unit :=
  modify fun s => { s with promises := p :: s.promises.filter (·.id != p.id) }

def getTask (id : String) : M (Option TaskObject) :=
  return (← get).tasks.find? (·.id == id)

def setTask (t : TaskObject) : M Unit :=
  modify fun s => { s with tasks := t :: s.tasks.filter (·.id != t.id) }

def getSchedule (id : String) : M (Option Schedule) :=
  return (← get).schedules.find? (·.id == id)

def setSchedule (sch : Schedule) : M Unit :=
  modify fun s => { s with schedules := sch :: s.schedules.filter (·.id != sch.id) }

def delSchedule (id : String) : M Unit :=
  modify fun s => { s with schedules := s.schedules.filter (·.id != id) }

/-- Next cron fire time strictly after the given instant. -/
opaque nextCron : (cron : String) → (after : Nat) → Nat

/-- Expand a schedule's promise-id template against one occurrence. -/
opaque expand : (template id : String) → (timestamp : Nat) → String

def setPromiseTimeout (id : String) (timeout : Nat) : M Unit :=
  modify fun s =>
    { s with promiseTimeouts :=
        { id, timeout } :: s.promiseTimeouts.filter (·.id != id) }

def delPromiseTimeout (id : String) : M Unit :=
  modify fun s =>
    { s with promiseTimeouts := s.promiseTimeouts.filter (·.id != id) }

def setTaskTimeout (id : String) (kind timeout : Nat) : M Unit :=
  modify fun s =>
    { s with taskTimeouts :=
        { id, kind, timeout } ::
        s.taskTimeouts.filter (fun t => !(t.id == id && t.kind == kind)) }

def delTaskTimeout (id : String) : M Unit :=
  modify fun s =>
    { s with taskTimeouts := s.taskTimeouts.filter (·.id != id) }

def setScheduleTimeout (id : String) (timeout : Nat) : M Unit :=
  modify fun s =>
    { s with scheduleTimeouts :=
        { id, timeout } :: s.scheduleTimeouts.filter (·.id != id) }

def delScheduleTimeout (id : String) : M Unit :=
  modify fun s =>
    { s with scheduleTimeouts := s.scheduleTimeouts.filter (·.id != id) }

def setMessage (address : String) (msg : Message) : M Unit :=
  modify fun s =>
    let entry := OutboxEntry.mk address msg
    let key   := entry.key
    { s with outbox := entry :: s.outbox.filter (fun e => e.key != key) }

def delMessage (id : String) : M Unit :=
  modify fun s =>
    { s with outbox := s.outbox.filter (fun e => e.key != id) }

end ServerModel
