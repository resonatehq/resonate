import «02-abstract».«state»

namespace AbstractModel
namespace E

open ServerModel (Tags Value PromiseState TaskState PromiseRecord TaskRecord
                  Schedule Message OutboxEntry PromiseCreateReq)

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

structure Env where
  state : ServerState
  mat   : Bool

def H (α : Type) : Type := Env → α × List Effect

instance : Monad H where
  pure a   := fun _ => (a, [])
  bind x f := fun e =>
    let (a, w₁) := x e
    let (b, w₂) := f a e
    (b, w₁ ++ w₂)

def ask : H Env := fun e => (e, [])

def emit (f : Effect) : H Unit := fun _ => ((), [f])

def run (mat : Bool) (act : H α) (s : ServerState) : α × ServerState :=
  let (a, w) := act { state := s, mat := mat }
  (a, applyAll s w)

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


open ServerModel (PromiseState
                  PromiseGetReq PromiseGetRes
                  PromiseCreateReq PromiseCreateRes
                  PromiseSettleReq PromiseSettleRes
                  PromiseRegisterCallbackReq PromiseRegisterCallbackRes
                  PromiseRegisterListenerReq PromiseRegisterListenerRes
                  PromiseSearchReq PromiseSearchRes)

def promiseGet (req : PromiseGetReq) (now : Nat) : H PromiseGetRes := do
  match ← readPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

def promiseCreate (req : PromiseCreateReq) (now : Nat) : H PromiseCreateRes := do
  if req.tags.timerTargeted then
    return { status := 400, promise := none }
  match ← readPromise req.id now with
  | some p =>
      return { status := 200, promise := some p.toRecord }
  | none =>
      let p ← createPromise req now
      return { status := 200, promise := some p.toRecord }

def promiseSettle (req : PromiseSettleReq) (now : Nat) : H PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← readPromise req.id now with
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
    H PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← readPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← readPromise req.awaiter now with
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

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) :
    H PromiseRegisterListenerRes := do
  if !ServerModel.addressValid req.address then
    return { status := 400 }
  match ← readPromise req.awaited now with
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
  match ← readTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some _) =>
      return { status := 200, task := some t.toRecord }

def taskCreate (req : TaskCreateReq) (now : Nat) : H TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← readPromise a.id now with
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
      match ← readTask p.id now with
      | none | some (_, none) =>
          return { status := 409 }
      | some (t, some p) =>

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

def taskAcquire (req : TaskAcquireReq) (now : Nat) : H TaskAcquireRes := do
  match ← readTask req.id now with
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

def taskFence (req : TaskFenceReq) (now : Nat) : H TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  match ← readTask req.id now with
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

def heartbeatOne (pid : String) (ref : ServerModel.TaskRef) (now : Nat) : H Unit := do
  match ← readTask ref.id now with
  | some (t, some p) =>
      if t.state == .acquired ∧ t.version == ref.version
          ∧ t.pid == some pid ∧ p.state == .pending then
        setTask { t with expiresAt := some (now + t.ttl.getD 0) }
  | _ =>
      pure ()

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
      match ← readPromise action.awaited now with
      | none => return none
      | some pa =>
          if !pa.external then
            return none
          else
            match ← checkAwaited now rest with
            | none => return none
            | some settled => return some (settled || pa.state != .pending)

def registerAwaited (awaiter : String) (now : Nat) :
    List PromiseRegisterCallbackReq → H Unit
  | [] => pure ()
  | action :: rest => do
      match ← readPromise action.awaited now with
      | some pa => setPromise (pa.addCallback awaiter)
      | none => pure ()
      registerAwaited awaiter now rest

def taskSuspend (req : TaskSuspendReq) (now : Nat) : H TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← readTask req.id now with
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

def taskFulfill (req : TaskFulfillReq) (now : Nat) : H TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← readTask req.id now with
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

def taskRelease (req : TaskReleaseReq) (now : Nat) : H TaskReleaseRes := do
  match ← readTask req.id now with
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

def taskHalt (req : TaskHaltReq) (now : Nat) : H TaskHaltRes := do
  match ← readTask req.id now with
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

def taskContinue (req : TaskContinueReq) (now : Nat) : H TaskContinueRes := do
  match ← readTask req.id now with
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

namespace Internal

open ServerModel (nextCron occurrences expand Schedule)

def processPromiseTimeout (id : String) (now : Nat) : H Unit := do
  let _ ← touchPromise id now

def processListener (id : String) (address : String) (now : Nat) : H Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.listeners.contains address then
        setPromise { p with listeners := p.listeners.filter (· != address) }
        setMessage address (.unblock p.toRecord)

def resumeOne (awaited awaiter : String) (now : Nat) : H Unit := do
  match ← touchTask awaiter now with
  | none => pure ()
  | some (_, none) => pure ()
  | some (t, some _) =>
      match t.state with
      | .suspended =>
          setTask { t with state := .pending, resumes := [awaited],
                           retryAt := some now }
      | .pending | .acquired | .halted =>
          if !(t.resumes.contains awaited) then
            setTask { t with resumes := t.resumes ++ [awaited] }
      | .fulfilled =>
          pure ()

def processCallback (id : String) (awaiter : String) (now : Nat) : H Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.callbacks.contains awaiter then
        setPromise { p with callbacks := p.callbacks.filter (· != awaiter) }
        resumeOne p.id awaiter now

def processLeaseTimeout (id : String) (now : Nat) : H Unit := do
  match ← getTask id with
  | none => pure ()
  | some t =>
      match t.expiresAt with
      | none => pure ()
      | some deadline =>
          if t.state == .acquired ∧ deadline ≤ now then
            match ← viewPromise t.id now with
            | none => pure ()
            | some p =>
                if p.state == .pending then
                  setTask { t with state := .pending, pid := none, ttl := none,
                                   expiresAt := none, retryAt := some now }

def processRetryTimeout (id : String) (next : Nat) (now : Nat) : H Unit := do
  match ← getTask id with
  | none => pure ()
  | some t =>
      match t.retryAt with
      | none => pure ()
      | some due =>
          if t.state == .pending ∧ due ≤ now then
            match ← viewPromise t.id now with
            | none => pure ()
            | some p =>
                if p.state == .pending then
                  setTask { t with retryAt := some next }
                  setMessage ((p.tags.get? "resonate:target").getD "")
                    (.execute t.id t.version)

def fireOccurrence (s : Schedule) (t : Nat) : H Unit :=
  createIfAbsent
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → H Unit
  | [] => pure ()
  | t :: ts => do
      fireOccurrence s t
      fireAll s ts

def processSchedule (id : String) (now : Nat) : H Unit := do
  match ← getSchedule id with
  | none => pure ()
  | some s =>
      let ts := (occurrences s.cron s.nextRunAt now).filter (· ≤ now)
      fireAll s ts
      match ts.getLast? with
      | some last =>
          setSchedule { s with lastRunAt := some last,
                               nextRunAt := nextCron s.cron last }
      | none => pure ()

end Internal

end E
end AbstractModel
