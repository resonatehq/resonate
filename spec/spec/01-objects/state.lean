import «01-objects».«types»

namespace ServerModel

def Tags.get? (t : Tags) (k : String) : Option String :=
  (t.find? (·.fst == k)).map (·.snd)

def Tags.has (t : Tags) (k : String) : Bool :=
  (t.get? k).isSome

def Tags.isTimer (t : Tags) : Bool :=
  t.get? "resonate:timer" == some "true"

/-- The terminal states a client may settle into. `pending` is not a
    settlement, and `rejectedTimedout` is server-owned: only the timeout
    path writes it, so a client can never forge one. -/
def PromiseState.settable : PromiseState → Bool
  | .resolved | .rejected | .rejectedCanceled => true
  | _ => false

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

/-- THE PROJECTION. The logical view of a promise at instant `now`: a
    pending promise past its deadline is logically settled -- `resolved`
    for timers, `rejectedTimedout` otherwise -- stamped AT THE DEADLINE,
    so the projected record is byte-identical to the record the timeout
    τ-step (`onPromiseTimeout`) eventually writes. Materialization is
    memoization of this function.

    The projection is total over PROMISE state: every promise-bearing
    response and every guard that consults promise state consults the
    projected view (`promiseSettle`'s live-check, `taskSuspend`'s
    awaited-settled check, `taskGet`'s own-promise check). Stored-vs-
    projected divergence is therefore nowhere observable, which is what
    makes the materialization schedule unobservable, and hence
    unspecified.

    TASK state is deliberately NOT projected. Tasks are the material
    coordination layer -- claims, leases, fencing tokens -- and their
    guards (`taskAcquire`, `taskFence`, `taskFulfill`) must branch on
    material state; that is what fencing means. A projected task state
    would report an affordance (`.pending` = acquirable-now) that the
    material machine does not yet offer. Projection reports facts, not
    affordances; the one task projection that exists (`taskGet` serving
    `.fulfilled` when the own promise is logically settled) is safe
    precisely because `.fulfilled` is inert. -/
def PromiseObject.project (p : PromiseObject) (now : Nat) : PromiseObject :=
  if p.state == .pending ∧ p.timeoutAt ≤ now then
    if p.isTimer then
      { p with state := .resolved, settledAt := some p.timeoutAt }
    else
      { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
  else
    p

/-- External promises — explicitly tagged `resonate:external = "true"`,
    targeted, or timers — may have awaiters and carry an armed (durable)
    timeout; the timeout transition guarantees their awaiters are never
    stranded. Internal promises must not have awaiters; their deadlines
    are projection-only. -/
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
  deferred         : List ResumeReq        := []
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

/-- Record a resume obligation the server invokes on itself later. Keyed
    collapse-on-set like `setMessage` and `setTaskTimeout` -- but here the
    key is defensive rather than load-bearing: `addCallback` dedups, a
    promise settles at most once, and a settled awaited is never
    re-registered, so each pair is deferred at most once over any run.
    No time field: all call sites would pass `now`, and *later* is not
    *a time* -- the drain supplies its own clock to the deadline guard. -/
def defer (r : ResumeReq) : M Unit :=
  modify fun s =>
    { s with deferred :=
        r :: s.deferred.filter (fun e =>
          !(e.awaited == r.awaited && e.awaiter == r.awaiter)) }

def undefer (r : ResumeReq) : M Unit :=
  modify fun s =>
    { s with deferred :=
        s.deferred.filter (fun e =>
          !(e.awaited == r.awaited && e.awaiter == r.awaiter)) }

end ServerModel
