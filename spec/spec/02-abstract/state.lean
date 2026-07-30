import «01-protocol».«validation»

/-!  # The coalesced machine — state

A second, more abstract specification of the Resonate protocol. Same wire
types, same request/response behavior at the API — a different machine:

* **No timeout components.** The base machine mirrors every deadline into
  an armed-timeout set (`promiseTimeouts`, `taskTimeouts`,
  `scheduleTimeouts`). Here a deadline lives only on the object that owns
  it — `timeoutAt` on the promise, `expiresAt` on the task,
  `nextRunAt` on the schedule — and every internal transition is a
  guarded rule of the form *"if there is a promise past its deadline …"*.

* **No deferred set.** The base machine moves a settled promise's
  callbacks into a `deferred` queue and drains it. Here settlement writes
  the promise's state and nothing else: awaiters and listeners STAY ON
  THE PROMISE until batch rules (`Rules.resume`, `Rules.notify`) drain
  them, one to all at a time.

* **No projection.** The base machine serves a logical view of a
  timed-out promise without persisting it. Here there is no logical view:
  every time a handler touches an object it MATERIALIZES the facts that
  are forced at that instant, then reads the stored state.

Two facts are forced (monotone consequences of the state, no choice
involved), and touching applies them:

  fact P   a pending promise past `timeoutAt` is settled — `resolved`
           for timers, `rejectedTimedout` otherwise — stamped at the
           deadline.  Derivable from the promise alone; applied by
           `touchPromise`.
  fact T   the task of a settled promise is fulfilled.  Derivable from
           the task and its promise jointly; applied by `touchTask`.

Everything else — waking awaiters, notifying listeners, expiring leases,
dispatching executes, firing schedules — is a scheduling CHOICE, and
choices are never applied on touch: they are the internal rules of
`rules.lean`, fired by the environment at its own pace.

Retaining awaiters and listeners across settlement is what makes
materialization-on-touch sound: flipping a promise's state on a read
commits no notification atomically, because the obligations remain
recorded on the object and the batch rules discharge them later.

Wire records, request and response types, and the tag algebra are shared
with the base spec (`01-protocol/types.lean`) — the protocol surface is
identical, only the machine behind it differs.  -/

namespace AbstractModel

open ServerModel (Tags Value PromiseState TaskState PromiseRecord
                  TaskRecord Schedule Message OutboxEntry)

/-- Same shape as the base machine's promise object. The difference is
    dynamic, not structural: `callbacks` and `listeners` survive
    settlement here, drained later in batches. -/
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

/-- Fact P. The same function the base machine calls `project` — but here
    its result is always persisted (`touchPromise`), never merely served.
    Stamped AT THE DEADLINE, so the record is byte-identical to the one
    the base machine's timeout transition writes. Awaiters and listeners
    are untouched: settlement records the fact, the batch rules discharge
    the obligations. -/
def PromiseObject.materialize (p : PromiseObject) (now : Nat) : PromiseObject :=
  if p.state == .pending ∧ p.timeoutAt ≤ now then
    if p.isTimer then
      { p with state := .resolved, settledAt := some p.timeoutAt }
    else
      { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
  else
    p

/-- The base machine's task object plus its two deadlines, which the
    base machine keeps in its `taskTimeouts` component: `expiresAt`, the
    absolute lease deadline (kind 1), and `retryAt`, the instant the
    next dispatch of the task's `execute` is due (kind 0 — seeded by the
    `resonate:delay` tag at creation, reset to `now` by every transition
    that re-pends the task, re-armed by the dispatch rule itself). With
    the deadlines on the object, lease expiry and dispatch need no
    auxiliary state — their rules guard on the task alone.

    Both are guard data for CHOICES (R5, R6), never facts: neither is
    materialized on touch. -/
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

/-- Fact T, on the task side: the fulfilled form. Byte-identical to what
    the base machine's settle and timeout transitions write. -/
def TaskObject.fulfill (t : TaskObject) : TaskObject :=
  { t with state := .fulfilled, pid := none, ttl := none,
           expiresAt := none, retryAt := none, resumes := [] }

/-- Four components. No config (no retry cadence — dispatch is a rule the
    environment fires at will), no timeout sets, no deferred queue. -/
structure ServerState where
  promises  : List PromiseObject := []
  tasks     : List TaskObject    := []
  schedules : List Schedule      := []
  outbox    : List OutboxEntry   := []
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

def setMessage (address : String) (msg : Message) : M Unit :=
  modify fun s =>
    let entry := OutboxEntry.mk address msg
    let key   := entry.key
    { s with outbox := entry :: s.outbox.filter (fun e => e.key != key) }

/-- THE TOUCH, promise side: read a promise and materialize fact P.
    Handlers never call `getPromise` directly — every promise a handler
    consults arrives through here, so no guard and no response ever sees
    a pending promise past its deadline. -/
def touchPromise (id : String) (now : Nat) : M (Option PromiseObject) := do
  match ← getPromise id with
  | none => return none
  | some p =>
      let p' := p.materialize now
      if p'.state != p.state then
        setPromise p'
      return some p'

/-- THE TOUCH, task side: read a task together with its promise and
    materialize fact P then fact T. A task read WITHOUT its promise
    (`taskHalt` is the one case) sees raw state — fact T is a joint fact
    and cannot be derived from the task alone. -/
def touchTask (id : String) (now : Nat) :
    M (Option (TaskObject × Option PromiseObject)) := do
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
