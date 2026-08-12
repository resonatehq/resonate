import «01-protocol».«validation»

/-!  # The concrete machine — state and effects

The machine's objects, its auxiliary obligation records (armed
timeouts, the deferred-resume queue, retry config), and the effects —
lookups, keyed upserts, deletes — through which handlers touch state.
Shared by both read disciplines (`p/`, `m/`).  -/

namespace ConcreteModel

open ServerModel

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
    τ-step (`processPromiseTimeout`) eventually writes. Materialization is
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
    stranded. Internal promises must not have awaiters — ENFORCED: both
    registration paths (`register_callback`, `register_listener`) refuse
    internal promises with `422` — their deadlines are projection-only,
    so an obligation recorded on one could never be discharged. -/
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

abbrev H := StateM ServerState

def getPromise (id : String) : H (Option PromiseObject) :=
  return (← get).promises.find? (·.id == id)

def setPromise (p : PromiseObject) : H Unit :=
  modify fun s => { s with promises := p :: s.promises.filter (·.id != p.id) }

def getTask (id : String) : H (Option TaskObject) :=
  return (← get).tasks.find? (·.id == id)

/-- Read an armed task timer. The timeout transitions consult their own
    due entry before acting: an armed timer means NOT BEFORE, and the
    machine enforces it rather than trusting the environment. -/
def getTaskTimeout (id : String) (kind : Nat) : H (Option TaskTimeout) :=
  return (← get).taskTimeouts.find? (fun t => t.id == id && t.kind == kind)

def setTask (t : TaskObject) : H Unit :=
  modify fun s => { s with tasks := t :: s.tasks.filter (·.id != t.id) }

def getSchedule (id : String) : H (Option Schedule) :=
  return (← get).schedules.find? (·.id == id)

def setSchedule (sch : Schedule) : H Unit :=
  modify fun s => { s with schedules := sch :: s.schedules.filter (·.id != sch.id) }

def delSchedule (id : String) : H Unit :=
  modify fun s => { s with schedules := s.schedules.filter (·.id != id) }

def setPromiseTimeout (id : String) (timeout : Nat) : H Unit :=
  modify fun s =>
    { s with promiseTimeouts :=
        { id, timeout } :: s.promiseTimeouts.filter (·.id != id) }

def delPromiseTimeout (id : String) : H Unit :=
  modify fun s =>
    { s with promiseTimeouts := s.promiseTimeouts.filter (·.id != id) }

def setTaskTimeout (id : String) (kind timeout : Nat) : H Unit :=
  modify fun s =>
    { s with taskTimeouts :=
        { id, kind, timeout } ::
        s.taskTimeouts.filter (fun t => !(t.id == id && t.kind == kind)) }

def delTaskTimeout (id : String) : H Unit :=
  modify fun s =>
    { s with taskTimeouts := s.taskTimeouts.filter (·.id != id) }

def setScheduleTimeout (id : String) (timeout : Nat) : H Unit :=
  modify fun s =>
    { s with scheduleTimeouts :=
        { id, timeout } :: s.scheduleTimeouts.filter (·.id != id) }

def delScheduleTimeout (id : String) : H Unit :=
  modify fun s =>
    { s with scheduleTimeouts := s.scheduleTimeouts.filter (·.id != id) }

/-- The coupled write: storing a settled promise disarms its deadline,
    fulfils its co-keyed task and disarms the task's deadlines, in one
    step. `setSettled` in `02-abstract/state.lean` is the same write
    without the timeout components. -/
def setSettled (p : PromiseObject) : H Unit := do
  setPromise p
  if p.state != .pending then
    delPromiseTimeout p.id
    match ← getTask p.id with
    | some t =>
        setTask { t with state := .fulfilled, pid := none, ttl := none, resumes := [] }
        delTaskTimeout t.id
    | none =>
        pure ()

def setMessage (address : String) (msg : Message) : H Unit :=
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
def defer (r : ResumeReq) : H Unit :=
  modify fun s =>
    { s with deferred :=
        r :: s.deferred.filter (fun e =>
          !(e.awaited == r.awaited && e.awaiter == r.awaiter)) }

def undefer (r : ResumeReq) : H Unit :=
  modify fun s =>
    { s with deferred :=
        s.deferred.filter (fun e =>
          !(e.awaited == r.awaited && e.awaiter == r.awaiter)) }

/-!  # The materialized machine (-m) — touch

The -m machine is the projected machine (-p) under one change of read
discipline: every read of an object MATERIALIZES what -p's projection
would have shown, by firing the anticipated timeout transition at the
moment of observation. `touchPromise` IS -p's `processPromiseTimeout`, run
eagerly; a handler reading through touch therefore sees stored state
that agrees, byte for byte, with -p's projected view.

Handlers keep -p's guards VERBATIM — a compound liveness check
`p.state == .pending ∧ p.timeoutAt > now` is still a true statement of
materialized state — and -p's projected-view expressions where they are
idempotent on materialized state. The line-level diff between the twin
machines is exactly: reads become touches, `project` disappears from
responses, and the extra writes hide inside the touch.  -/

open ServerModel


/-- Fire the anticipated promise-timeout transition, then read. The
    materialization body is -p's `processPromiseTimeout`, verbatim. -/
def touchPromise (id : String) (now : Nat) : H (Option PromiseObject) := do
  match ← getPromise id with
  | none =>
      return none
  | some p =>
      if p.state != .pending ∨ p.timeoutAt > now then
        return some p
      else
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p.project p.timeoutAt with callbacks := [], listeners := [] }
        setSettled p
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }
        return some p

/-- Read a task together with its promise, materializing first. The
    task is re-read after the touch: materialization may have
    fulfilled it. -/
def touchTask (id : String) (now : Nat) :
    H (Option (TaskObject × Option PromiseObject)) := do
  match ← getTask id with
  | none => return none
  | some t =>
  match ← touchPromise t.id now with
  | none => return some (t, none)
  | some p =>
  match ← getTask id with
  | none => return some (t, some p)
  | some t => return some (t, some p)

end ConcreteModel
