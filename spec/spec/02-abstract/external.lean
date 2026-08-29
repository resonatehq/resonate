import «02-abstract».«state»

/-!  # External steps — the request handlers

The 21 handlers a client can reach. Each is a pure function from the
environment to a response and a list of writes; nothing here mutates.

These read through `readObject`, the DISCIPLINE-PARAMETRIC read:
whether a projected settlement is persisted is `Env.mat`, and that bit
is the whole of the difference between the two readings of this
machine.

One read, because there is one row. A task handler asks for the object
and then for its task; where it used to have a second arm for a task
whose promise was missing, it now has none, because the state cannot
hold one. -/

namespace AbstractModel

open ServerModel (Ident PromiseState
                  PromiseGetReq PromiseGetRes
                  PromiseCreateReq PromiseCreateRes
                  PromiseSettleReq PromiseSettleRes
                  PromiseRegisterCallbackReq PromiseRegisterCallbackRes
                  PromiseRegisterListenerReq PromiseRegisterListenerRes
                  PromiseSearchReq PromiseSearchRes)

def promiseGet (req : PromiseGetReq) (now : Nat) : H PromiseGetRes := do
  match ← readObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
      return { status := 200, promise := some (o.promise.toRecord o.id) }

def promiseCreate (req : PromiseCreateReq) (now : Nat) : H PromiseCreateRes := do
  if req.tags.timerTargeted then
    return { status := 400, promise := none }
  match ← readObject req.id now with
  | some o =>
      return { status := 200, promise := some (o.promise.toRecord o.id) }
  | none =>
      let o ← createPromise req now
      return { status := 200, promise := some (o.promise.toRecord o.id) }

def promiseSettle (req : PromiseSettleReq) (now : Nat) : H PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← readObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
      if o.promise.state == .pending then
        let p := { o.promise with state := req.state, value := req.value,
                                  settledAt := some now }
        setSettled o p
        return { status := 200, promise := some (p.toRecord o.id) }
      else
        return { status := 200, promise := some (o.promise.toRecord o.id) }

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) :
    H PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  if !req.awaited.sameOrigin req.awaiter then
    return { status := 400 }
  match ← readObject req.awaited now with
  | none =>
      return { status := 404 }
  | some oAwaited =>
  match ← readObject req.awaiter now with
  | none =>
      return { status := 422 }
  | some oAwaiter =>
      if oAwaiter.promise.otype != .runnable then
        return { status := 422 }
      if !oAwaited.promise.otype.awaitable then
        return { status := 422 }
      if oAwaited.promise.state == .pending then
        if oAwaiter.promise.state == .pending then
          setPromise oAwaited.id (oAwaited.promise.addCallback req.awaiter)
        return { status := 200, promise := some (oAwaited.promise.toRecord oAwaited.id) }
      else
        return { status := 200, promise := some (oAwaited.promise.toRecord oAwaited.id) }

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) :
    H PromiseRegisterListenerRes := do
  if !ServerModel.addressValid req.address then
    return { status := 400 }
  match ← readObject req.awaited now with
  | none =>
      return { status := 404 }
  | some oAwaited =>
      if !oAwaited.promise.otype.awaitable then
        return { status := 422 }
      if oAwaited.promise.state == .pending then
        setPromise oAwaited.id (oAwaited.promise.addListener req.address)
        return { status := 200, promise := some (oAwaited.promise.toRecord oAwaited.id) }
      else
        return { status := 200, promise := some (oAwaited.promise.toRecord oAwaited.id) }

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : H PromiseSearchRes := do
  return { status := 501 }

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

def taskGet (req : TaskGetReq) (now : Nat) : H TaskGetRes := do
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      return { status := 200, task := some (t.toRecord o.id) }

def taskCreate (req : TaskCreateReq) (now : Nat) : H TaskCreateRes := do
  let a := req.action
  if a.tags.otype != .runnable ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← readObject a.id now with
  | none =>
      if a.timeoutAt > now then
        let p : PromiseObject :=
          { state := .pending, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := now }
        setPromise a.id p
        let t : TaskObject :=
          { state := .acquired, version := 1,
            ttl := some req.ttl, pid := some req.pid,
            leaseTimeoutAt := some (now + req.ttl) }
        setTask a.id t
        return { status := 200, task := some (t.toRecord a.id),
                 promise := some (p.toRecord a.id) }
      else
        let st := ServerModel.PromiseState.rejectedTimedout
        let p : PromiseObject :=
          { state := st, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := a.timeoutAt,
            settledAt := some a.timeoutAt }
        setPromise a.id p
        let t : TaskObject := { state := .fulfilled, version := 0 }
        setTask a.id t
        return { status := 200, task := some (t.toRecord a.id),
                 promise := some (p.toRecord a.id) }
  | some o =>
      if o.promise.otype != .runnable then
        return { status := 422 }
      match o.task with
      | none =>
          return { status := 409 }
      | some t =>

          if t.state == .fulfilled then
            return { status := 200, task := some (t.toRecord o.id),
                     promise := some (o.promise.toRecord o.id) }
          else if t.state == .pending then
            let t := { t with state := .acquired, version := t.version + 1,
                              ttl := some req.ttl, pid := some req.pid,
                              leaseTimeoutAt := some (now + req.ttl),
                              retryTimeoutAt := none, resumes := [] }
            setTask o.id t
            return { status := 200, task := some (t.toRecord o.id),
                     promise := some (o.promise.toRecord o.id) }
          else
            return { status := 409 }

def taskAcquire (req : TaskAcquireReq) (now : Nat) : H TaskAcquireRes := do
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .pending then
        return { status := 409 }
      if o.promise.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let t := { t with state := .acquired, version := t.version + 1,
                        ttl := some req.ttl, pid := some req.pid,
                        leaseTimeoutAt := some (now + req.ttl),
                        retryTimeoutAt := none, resumes := [] }
      setTask o.id t
      return { status := 200, task := some (t.toRecord o.id),
               promise := some (o.promise.toRecord o.id) }

def taskFence (req : TaskFenceReq) (now : Nat) : H TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  if !req.action.targetId.sameOrigin req.id then
    return { status := 400 }
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .acquired then
        return { status := 409 }
      if o.promise.state != .pending then
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

def heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (now : Nat) : H Unit := do
  match ← readTaskObject ref.id now with
  | none =>
      pure ()
  | some o =>
  match o.task with
  | none =>
      pure ()
  | some t =>
      if t.state == .acquired ∧ t.version == ref.version
          ∧ t.pid == some pid ∧ o.promise.state == .pending then
        setTask o.id { t with leaseTimeoutAt := some (now + t.ttl.getD 0) }

def heartbeatAll (pid : String) (now : Nat) : List ServerModel.TaskRef → H Unit
  | [] => pure ()
  | ref :: refs => do
      heartbeatOne pid ref now
      heartbeatAll pid now refs

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : H TaskHeartbeatRes := do
  heartbeatAll req.pid now req.tasks
  return { status := 200 }

def checkAwaited (now : Nat) : List PromiseRegisterCallbackReq → H (Option Bool)
  | [] => return some false
  | action :: rest => do
      match ← readObject action.awaited now with
      | none => return none
      | some oa =>
          if !oa.promise.otype.awaitable then
            return none
          else
            match ← checkAwaited now rest with
            | none => return none
            | some settled => return some (settled || oa.promise.state != .pending)

def registerAwaited (awaiter : Ident) (now : Nat) :
    List PromiseRegisterCallbackReq → H Unit
  | [] => pure ()
  | action :: rest => do
      match ← readObject action.awaited now with
      | some oa => setPromise oa.id (oa.promise.addCallback awaiter)
      | none => pure ()
      registerAwaited awaiter now rest

def taskSuspend (req : TaskSuspendReq) (now : Nat) : H TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  if req.actions.any (fun a => !a.awaited.sameOrigin req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .acquired then
        return { status := 409 }
      if o.promise.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      match ← checkAwaited now req.actions with
      | none =>
          return { status := 422 }
      | some true =>
          setTask o.id { t with resumes := [] }
          return { status := 300 }
      | some false =>
          registerAwaited req.id now req.actions
          setTask o.id { t with state := .suspended, pid := none, ttl := none,
                                leaseTimeoutAt := none, retryTimeoutAt := none, resumes := [] }
          return { status := 200 }

def taskFulfill (req : TaskFulfillReq) (now : Nat) : H TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .acquired then
        return { status := 409 }
      if o.promise.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let p := { o.promise with state := req.action.state, value := req.action.value,
                                settledAt := some now }
      setSettled o p
      return { status := 200, promise := some (p.toRecord o.id) }

def taskRelease (req : TaskReleaseReq) (now : Nat) : H TaskReleaseRes := do
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .acquired then
        return { status := 409 }
      if o.promise.state != .pending then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      setTask o.id { t with state := .pending, pid := none, ttl := none,
                            leaseTimeoutAt := none, retryTimeoutAt := some now }
      return { status := 200 }

def taskHalt (req : TaskHaltReq) (now : Nat) : H TaskHaltRes := do
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state == .fulfilled then
        return { status := 409 }
      if t.state == .halted then
        return { status := 200 }
      setTask o.id { t with state := .halted, pid := none, ttl := none,
                            leaseTimeoutAt := none, retryTimeoutAt := none }
      return { status := 200 }

def taskContinue (req : TaskContinueReq) (now : Nat) : H TaskContinueRes := do
  match ← readTaskObject req.id now with
  | none =>
      return { status := 404 }
  | some o =>
  match o.task with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .halted then
        return { status := 409 }
      if o.promise.state != .pending then
        return { status := 409 }
      setTask o.id { t with state := .pending, retryTimeoutAt := some now }
      return { status := 200 }

def taskSearch (_req : TaskSearchReq) (_now : Nat) : H TaskSearchRes := do
  return { status := 501 }

open ServerModel (Schedule nextCron
                  ScheduleGetReq ScheduleGetRes
                  ScheduleCreateReq ScheduleCreateRes
                  ScheduleDeleteReq ScheduleDeleteRes
                  ScheduleSearchReq ScheduleSearchRes)

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : H ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }

def scheduleCreate (req : ScheduleCreateReq) (now : Nat) : H ScheduleCreateRes := do
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

def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : H ScheduleDeleteRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      delSchedule s.id
      return { status := 200 }

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : H ScheduleSearchRes := do
  return { status := 501 }

end AbstractModel
