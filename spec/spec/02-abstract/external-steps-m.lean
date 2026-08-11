import «02-abstract».«state»

/-!  # The coalesced machine — external steps, materialized

The machine's response to every external step, in one place: promises,
tasks, schedules. This is the MATERIALIZING read discipline — a handler
touches, persisting the facts forced at that instant; the projecting
twin is `external-steps-p.lean`, and the discipline is the only
difference between them.

Handlers write objects and return responses; they emit no messages and
record no auxiliary state — dispatching an `execute`, notifying a
listener, waking an awaiter is the internal steps' job
(`internal-steps.lean`).  -/

namespace AbstractModel

/-!  ## Promise handlers

* `promiseCreate` of a targeted promise creates the task and stops —
  the dispatch rule emits the `execute`. The `resonate:delay` tag is
  consumed at creation: it seeds the task's `retryAt`, so the
  create-side delay machinery of the base spec collapses to one field
  initialization.
* `promiseSettle` writes the promise AND its task pair — the coupled
  write: fact T is fact P seen through the task, so the same step makes
  both true. Awaiters and listeners stay on the promise for the batch
  rules.  -/


open ServerModel (PromiseState
                  PromiseGetReq PromiseGetRes
                  PromiseCreateReq PromiseCreateRes
                  PromiseSettleReq PromiseSettleRes
                  PromiseRegisterCallbackReq PromiseRegisterCallbackRes
                  PromiseRegisterListenerReq PromiseRegisterListenerRes
                  PromiseSearchReq PromiseSearchRes)

def promiseGet (req : PromiseGetReq) (now : Nat) : M PromiseGetRes := do
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

def promiseCreate (req : PromiseCreateReq) (now : Nat) : M PromiseCreateRes := do
  if req.tags.timerTargeted then
    return { status := 400, promise := none }
  match ← touchPromise req.id now with
  | some p =>
      return { status := 200, promise := some p.toRecord }
  | none =>
      let p ← createPromise req now
      return { status := 200, promise := some p.toRecord }

def promiseSettle (req : PromiseSettleReq) (now : Nat) : M PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending then
        let p := { p with state := req.state, value := req.value, settledAt := some now }
        setSettled p
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some p.toRecord }

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) :
    M PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← touchPromise req.awaiter now with
  | none =>
      return { status := 422 }
  | some pAwaiter =>
      if !(pAwaiter.tags.has "resonate:target") then
        return { status := 422 }
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending then
        if pAwaiter.state == .pending then
          setPromise (pAwaited.addCallback req.awaiter)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

/-- Registration on an already-settled promise returns the record without
    registering, exactly as in the base spec — even though this machine's
    retained-listener drain could naturally serve a late registration,
    admitting one would produce an `unblock` the base machine never
    sends. -/
def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) :
    M PromiseRegisterListenerRes := do
  if !ServerModel.addressValid req.address then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : M PromiseSearchRes := do
  return { status := 501 }

/-!  ## Task handlers

Every handler that consults a task's promise goes through `touchTask`,
so its guards branch on materialized state: the base spec's compound
liveness checks (`p.state != .pending ∨ p.timeoutAt ≤ now`) collapse to
`p.state != .pending`, and TIMEOUT ALWAYS WINS is automatic — touching
a task whose promise is past its deadline fulfills the task before any
guard looks at it.

`taskHalt` touches too: the concrete machine's halt was fixed to
consult the promise (halting a task whose own promise is settled is
`409` — its state is `.fulfilled`, which is what `taskGet` reports),
and the abstract halt materializes the same fact through the touch.  -/


open ServerModel (TaskGetReq TaskGetRes
                  TaskCreateReq TaskCreateRes
                  TaskAcquireReq TaskAcquireRes
                  TaskFenceAction TaskFenceReq TaskFenceRes
                  TaskHeartbeatReq TaskHeartbeatRes
                  TaskSuspendReq TaskSuspendRes
                  TaskFulfillReq TaskFulfillRes
                  TaskReleaseReq TaskReleaseRes
                  TaskHaltReq TaskHaltRes
                  TaskContinueReq TaskContinueRes
                  TaskSearchReq TaskSearchRes)

def taskGet (req : TaskGetReq) (now : Nat) : M TaskGetRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some _) =>
      return { status := 200, task := some t.toRecord }

def taskCreate (req : TaskCreateReq) (now : Nat) : M TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← touchPromise a.id now with
  | none =>
      if a.timeoutAt > now then
        let p : PromiseObject :=
          { id := a.id, state := .pending, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := now }
        setPromise p
        let t : TaskObject :=
          { id := p.id, state := .acquired, version := 1,
            ttl := some req.ttl, pid := some req.pid,
            expiresAt := some (now + req.ttl) }
        setTask t
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
      else
        let st := ServerModel.PromiseState.rejectedTimedout
        let p : PromiseObject :=
          { id := a.id, state := st, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := a.timeoutAt,
            settledAt := some a.timeoutAt }
        setPromise p
        let t : TaskObject := { id := p.id, state := .fulfilled, version := 0 }
        setTask t
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
  | some p =>
      if !(p.tags.has "resonate:target") then
        return { status := 422 }
      match ← touchTask p.id now with
      | none | some (_, none) =>
          return { status := 409 }
      | some (t, some p) =>
          -- Post-touch: a `.pending` task implies a pending promise —
          -- fact T would have fulfilled it otherwise — so re-acquisition
          -- needs no separate liveness guard.
          if t.state == .fulfilled then
            return { status := 200, task := some t.toRecord, promise := some p.toRecord }
          else if t.state == .pending then
            let t := { t with state := .acquired, version := t.version + 1,
                              ttl := some req.ttl, pid := some req.pid,
                              expiresAt := some (now + req.ttl),
                              retryAt := none, resumes := [] }
            setTask t
            return { status := 200, task := some t.toRecord, promise := some p.toRecord }
          else
            return { status := 409 }

def taskAcquire (req : TaskAcquireReq) (now : Nat) : M TaskAcquireRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .pending then
        return { status := 409 }
      if p.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let t := { t with state := .acquired, version := t.version + 1,
                        ttl := some req.ttl, pid := some req.pid,
                        expiresAt := some (now + req.ttl),
                        retryAt := none, resumes := [] }
      setTask t
      return { status := 200, task := some t.toRecord, promise := some p.toRecord }

def taskFence (req : TaskFenceReq) (now : Nat) : M TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      match req.action with
      | .create r =>
          let res ← promiseCreate r now
          return { status := 200, action := some (.create res) }
      | .settle r =>
          let res ← promiseSettle r now
          return { status := 200, action := some (.settle res) }

def heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (now : Nat) : M Unit := do
  match ← touchTask ref.id now with
  | some (t, some p) =>
      if t.state == .acquired ∧ t.version == ref.version
          ∧ t.pid == some pid ∧ p.state == .pending then
        setTask { t with expiresAt := some (now + t.ttl.getD 0) }
  | _ =>
      pure ()

def heartbeatAll (pid : String) (now : Nat) : List ServerModel.TaskRef → M Unit
  | [] => pure ()
  | ref :: refs => do
      heartbeatOne pid ref now
      heartbeatAll pid now refs

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : M TaskHeartbeatRes := do
  heartbeatAll req.pid now req.tasks
  return { status := 200 }

/-- Pass 1 over the awaited set, in order, stopping at the first
    undischargeable waiter: `none` is a 422 (missing or internal),
    `some settled` reports whether any awaited promise is already
    settled. -/
def checkAwaited (now : Nat) : List PromiseRegisterCallbackReq → M (Option Bool)
  | [] => return some false
  | action :: rest => do
      match ← touchPromise action.awaited now with
      | none => return none
      | some pa =>
          if !pa.external then
            return none
          else
            match ← checkAwaited now rest with
            | none => return none
            | some settled => return some (settled || pa.state != .pending)

/-- Pass 2: park the awaiter on every awaited promise. -/
def registerAwaited (awaiter : String) (now : Nat) :
    List PromiseRegisterCallbackReq → M Unit
  | [] => pure ()
  | action :: rest => do
      match ← touchPromise action.awaited now with
      | some pa => setPromise (pa.addCallback awaiter)
      | none => pure ()
      registerAwaited awaiter now rest

def taskSuspend (req : TaskSuspendReq) (now : Nat) : M TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some tp) =>
      if t.state != .acquired then
        return { status := 409 }
      if tp.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      match ← checkAwaited now req.actions with
      | none =>
          return { status := 422 }
      | some true =>
          setTask { t with resumes := [] }
          return { status := 300 }
      | some false =>
          registerAwaited req.id now req.actions
          setTask { t with state := .suspended, pid := none, ttl := none,
                           expiresAt := none, retryAt := none, resumes := [] }
          return { status := 200 }

/-- Settles the promise, and its task with it: fact T is fact P seen
    through the task, so `setSettled` writes the pair in one step —
    no separate fulfillment transition to lag behind. -/
def taskFulfill (req : TaskFulfillReq) (now : Nat) : M TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let p := { p with state := req.action.state, value := req.action.value,
                        settledAt := some now }
      setSettled p
      return { status := 200, promise := some p.toRecord }

def taskRelease (req : TaskReleaseReq) (now : Nat) : M TaskReleaseRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      setTask { t with state := .pending, pid := none, ttl := none,
                       expiresAt := none, retryAt := some now }
      return { status := 200 }

def taskHalt (req : TaskHaltReq) (now : Nat) : M TaskHaltRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some _) =>
      if t.state == .fulfilled then
        return { status := 409 }
      if t.state == .halted then
        return { status := 200 }
      setTask { t with state := .halted, pid := none, ttl := none,
                       expiresAt := none, retryAt := none }
      return { status := 200 }

def taskContinue (req : TaskContinueReq) (now : Nat) : M TaskContinueRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (t, pOpt) =>
      if t.state != .halted then
        return { status := 409 }
      match pOpt with
      | none =>
          return { status := 404 }
      | some p =>
          if p.state != .pending then
            return { status := 409 }
          setTask { t with state := .pending, retryAt := some now }
          return { status := 200 }

def taskSearch (_req : TaskSearchReq) (_now : Nat) : M TaskSearchRes := do
  return { status := 501 }

/-!  ## Schedule handlers

A schedule's `nextRunAt` is its alarm: `Internal.processSchedule` guards on it
directly, so creation arms nothing and deletion disarms nothing.  -/


open ServerModel (Schedule nextCron
                  ScheduleGetReq ScheduleGetRes
                  ScheduleCreateReq ScheduleCreateRes
                  ScheduleDeleteReq ScheduleDeleteRes
                  ScheduleSearchReq ScheduleSearchRes)

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : M ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }

def scheduleCreate (req : ScheduleCreateReq) (now : Nat) : M ScheduleCreateRes := do
  if req.promiseTags.timerTargeted then
    return { status := 400 }
  match ← getSchedule req.id with
  | some s =>
      return { status := 200, schedule := some s }
  | none =>
      let s : Schedule :=
        { id := req.id
          cron := req.cron
          promiseId := req.promiseId
          promiseTimeout := req.promiseTimeout
          promiseParam := req.promiseParam
          promiseTags := req.promiseTags
          createdAt := now
          nextRunAt := nextCron req.cron now
          lastRunAt := none }
      setSchedule s
      return { status := 200, schedule := some s }

def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : M ScheduleDeleteRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      delSchedule s.id
      return { status := 200 }

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : M ScheduleSearchRes := do
  return { status := 501 }

end AbstractModel
