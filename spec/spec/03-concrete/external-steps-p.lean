import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

open ServerModel


def promiseGet (req : PromiseGetReq) (now : Nat) : H PromiseGetRes := do
  match ← getPromise req.id with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some (p.project now).toRecord }

open ServerModel


def promiseCreate (req : PromiseCreateReq) (now : Nat) : H PromiseCreateRes := do
  if req.tags.timerTargeted then
    return { status := 400, promise := none }
  let retryTimeout := (← get).config.retryTimeout
  match ← getPromise req.id with
  | none =>
      if req.timeoutAt > now then
        let p : PromiseObject :=
          { id := req.id
            state := .pending
            param := req.param
            tags := req.tags
            timeoutAt := req.timeoutAt
            createdAt := now }
        setPromise p
        if p.external then
          setPromiseTimeout p.id p.timeoutAt
        match p.tags.get? "resonate:target" with
        | none =>
            return { status := 200, promise := some p.toRecord }
        | some target =>
            let t : TaskObject := { id := p.id, state := .pending, version := 0 }
            setTask t
            match p.tags.get? "resonate:delay" with
            | none =>
                setTaskTimeout t.id 0 (now + retryTimeout)
                setMessage target (.execute t.id t.version)
                return { status := 200, promise := some p.toRecord }
            | some delayStr =>
                let delay := parseNat delayStr
                if delay > now then
                  setTaskTimeout t.id 0 delay
                  return { status := 200, promise := some p.toRecord }
                else
                  setTaskTimeout t.id 0 (now + retryTimeout)
                  setMessage target (.execute t.id t.version)
                  return { status := 200, promise := some p.toRecord }
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
          let t : TaskObject :=
            { id := p.id, state := .fulfilled, version := 0,
              ttl := none, pid := none, resumes := [] }
          setTask t
          return { status := 200, promise := some p.toRecord }
        else
          return { status := 200, promise := some p.toRecord }
  | some p =>
      return { status := 200, promise := some (p.project now).toRecord }

open ServerModel


def promiseSettle (req : PromiseSettleReq) (now : Nat) : H PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← getPromise req.id with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt > now then
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p with state := req.state, value := req.value, settledAt := some now, callbacks := [], listeners := [] }
        setSettled p
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some (p.project now).toRecord }

open ServerModel


def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) : H PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← getPromise req.awaiter with
  | none =>
      return { status := 422 }
  | some pAwaiter =>
      if !(pAwaiter.tags.has "resonate:target") then
        return { status := 422 }
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        if pAwaiter.state == .pending ∧ pAwaiter.timeoutAt > now then
          setPromise (pAwaited.addCallback req.awaiter)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some (pAwaited.project now).toRecord }

open ServerModel


/-- Listeners, like callbacks, attach only to EXTERNAL promises: only
    external promises carry an armed timeout, so only they can
    guarantee the notification is ever sent. An internal awaited is
    `422`, mirroring `promise.register_callback` — without this guard
    the machine would accept an obligation its transition relation
    cannot discharge (an internal promise that dies by deadline is
    settled by projection only; no τ ever emits the `unblock`). -/
def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) : H PromiseRegisterListenerRes := do
  if !addressValid req.address then
    return { status := 400 }
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some (pAwaited.project now).toRecord }

open ServerModel


def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : H PromiseSearchRes := do
  return { status := 501 }

open ServerModel


def scheduleGet (req : ScheduleGetReq) (_now : Nat) : H ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }

open ServerModel


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
      setScheduleTimeout s.id s.nextRunAt
      return { status := 200, schedule := some s }

open ServerModel


def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : H ScheduleDeleteRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      delSchedule s.id
      delScheduleTimeout s.id
      return { status := 200 }

open ServerModel


def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : H ScheduleSearchRes := do
  return { status := 501 }

open ServerModel


def taskGet (req : TaskGetReq) (now : Nat) : H TaskGetRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt > now then
        return { status := 200, task := some t.toRecord }
      else
        return { status := 200, task := some ({ t with state := .fulfilled, pid := none, ttl := none, resumes := [] }).toRecord }

open ServerModel


def taskCreate (req : TaskCreateReq) (now : Nat) : H TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← getPromise a.id with
  | none =>
      if a.timeoutAt > now then
        let p : PromiseObject :=
          { id := a.id, state := .pending, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := now }
        setPromise p
        setPromiseTimeout p.id p.timeoutAt
        let t : TaskObject :=
          { id := p.id, state := .acquired, version := 1,
            ttl := some req.ttl, pid := some req.pid, resumes := [] }
        setTask t
        setTaskTimeout t.id 1 (now + req.ttl)
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
      else
        let st := PromiseState.rejectedTimedout
        let p : PromiseObject :=
          { id := a.id, state := st, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := a.timeoutAt, settledAt := some a.timeoutAt }
        setPromise p
        let t : TaskObject :=
          { id := p.id, state := .fulfilled, version := 0,
            ttl := none, pid := none, resumes := [] }
        setTask t
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
  | some p =>
      if !(p.tags.has "resonate:target") then
        return { status := 422 }
      -- Re-acquisition is gated on the PROJECTED promise: a logically
      -- settled promise serves the projected pair — fulfilled task,
      -- settled record — exactly as the expired fresh-create path does.
      -- No lease is ever armed on a logically dead task.
      if p.state == .pending ∧ p.timeoutAt > now then
        match ← getTask p.id with
        | some t =>
            if t.state == .fulfilled then
              return { status := 200, task := some t.toRecord, promise := some p.toRecord }
            else if t.state == .pending then
              let t := { t with state := .acquired, version := t.version + 1, ttl := some req.ttl, pid := some req.pid, resumes := [] }
              setTask t
              delTaskTimeout t.id
              setTaskTimeout t.id 1 (now + req.ttl)
              return { status := 200, task := some t.toRecord, promise := some p.toRecord }
            else
              return { status := 409 }
        | none =>
            return { status := 409 }
      else
        match ← getTask p.id with
        | some t =>
            return { status := 200,
                     task := some ({ t with state := .fulfilled, pid := none, ttl := none, resumes := [] }).toRecord,
                     promise := some (p.project now).toRecord }
        | none =>
            return { status := 409 }

open ServerModel


def taskAcquire (req : TaskAcquireReq) (now : Nat) : H TaskAcquireRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .pending then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let t := { t with state := .acquired, version := t.version + 1, ttl := some req.ttl, pid := some req.pid, resumes := [] }
      setTask t
      delTaskTimeout t.id
      setTaskTimeout t.id 1 (now + req.ttl)
      return { status := 200, task := some t.toRecord, promise := some p.toRecord }

open ServerModel


def taskFence (req : TaskFenceReq) (now : Nat) : H TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
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

open ServerModel


def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : H TaskHeartbeatRes := do
  for ref in req.tasks do
    match ← getTask ref.id with
    | none =>
        pure ()
    | some t =>
        if t.state == .acquired ∧ t.version == ref.version ∧ t.pid == some req.pid then
          match ← getPromise t.id with
          | some p =>
              if p.state == .pending ∧ p.timeoutAt > now then
                delTaskTimeout t.id
                setTaskTimeout t.id 1 (now + t.ttl.getD 0)
          | none =>
              pure ()
  return { status := 200 }

open ServerModel


def taskSuspend (req : TaskSuspendReq) (now : Nat) : H TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some tp =>
      if t.state != .acquired then
        return { status := 409 }
      if tp.state != .pending ∨ tp.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let mut settled := false
      for action in req.actions do
        match ← getPromise action.awaited with
        | none =>
            return { status := 422 }
        | some pa =>
            if !pa.external then
              return { status := 422 }
            if pa.state != .pending ∨ pa.timeoutAt ≤ now then
              settled := true
      if settled then
        setTask { t with resumes := [] }
        return { status := 300 }
      else
        for action in req.actions do
          match ← getPromise action.awaited with
          | some pa =>
              setPromise (pa.addCallback req.id)
          | none =>
              pure ()
        setTask { t with state := .suspended, pid := none, ttl := none, resumes := [] }
        delTaskTimeout t.id
        return { status := 200 }

open ServerModel


def taskFulfill (req : TaskFulfillReq) (now : Nat) : H TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let listeners := p.listeners
      let callbacks := p.callbacks
      let p := { p with state := req.action.state, value := req.action.value, settledAt := some now, callbacks := [], listeners := [] }
      setSettled p
      for address in listeners do
        setMessage address (.unblock p.toRecord)
      for awaiterId in callbacks do
        defer { awaited := p.id, awaiter := awaiterId }
      return { status := 200, promise := some p.toRecord }

open ServerModel


def taskRelease (req : TaskReleaseReq) (now : Nat) : H TaskReleaseRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let t := { t with state := .pending, pid := none, ttl := none }
      setTask t
      delTaskTimeout t.id
      setTaskTimeout t.id 0 (now + retryTimeout)
      setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
      return { status := 200 }

open ServerModel


/-- Halting exists to take a LIVE task out of circulation. A task whose
    own promise is logically settled has no circulation left — its
    projected state is `.fulfilled` (exactly what `taskGet` reports),
    and halt-on-fulfilled is `409`. Branching on the raw stored task
    here would make the stored-vs-projected divergence observable — the
    one thing the projection discipline forbids. -/
def taskHalt (req : TaskHaltReq) (now : Nat) : H TaskHaltRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 404 }
  | some p =>
      if !(p.state == .pending ∧ p.timeoutAt > now) then
        return { status := 409 }
      if t.state == .fulfilled then
        return { status := 409 }
      if t.state == .halted then
        return { status := 200 }
      setTask { t with state := .halted, pid := none, ttl := none }
      delTaskTimeout t.id
      return { status := 200 }

open ServerModel


def taskContinue (req : TaskContinueReq) (now : Nat) : H TaskContinueRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .halted then
        return { status := 409 }
      match ← getPromise t.id with
      | none =>
          return { status := 404 }
      | some p =>
          if p.state != .pending || p.timeoutAt ≤ now then
            return { status := 409 }
          let t := { t with state := .pending }
          setTask t
          setTaskTimeout t.id 0 (now + retryTimeout)
          setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
          return { status := 200 }

open ServerModel


def taskSearch (_req : TaskSearchReq) (_now : Nat) : H TaskSearchRes := do
  return { status := 501 }

end P
end ConcreteModel
