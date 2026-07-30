import «02-abstract».«state»

/-!  # The coalesced machine — projected handlers

The abstract machine's second read discipline, mirroring the concrete
twins in the opposite direction: at the concrete level projection is
native and materialization is the twin; here materialization is native
(`m.lean`) and projection is the twin. A projected handler SERVES the
view — fact P via `PromiseObject.project`, fact T via
`TaskObject.view` — and writes no fact; the rules (`rules.lean`,
shared verbatim between the disciplines: rules are material
transitions, the read discipline concerns handlers) persist facts at
the environment's pace.

Line-aligned with `m.lean`: every `touch` becomes a `view`, nothing
else changes — every mutation site fires only on live views, where the
view IS the stored object, so the write sets are identical by
construction. Since every handler in this machine reads through the
view (the halt fix included), the two disciplines answer identically
even under a shared rule schedule; only the message channel can tell
them apart (`04-theorems/abstract-twins.lean`).  -/

namespace AbstractModel
namespace Projected

open ServerModel (PromiseState
                  PromiseGetReq PromiseGetRes
                  PromiseCreateReq PromiseCreateRes
                  PromiseSettleReq PromiseSettleRes
                  PromiseRegisterCallbackReq PromiseRegisterCallbackRes
                  PromiseRegisterListenerReq PromiseRegisterListenerRes
                  PromiseSearchReq PromiseSearchRes)

def promiseGet (req : PromiseGetReq) (now : Nat) : M PromiseGetRes := do
  match ← viewPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

def promiseCreate (req : PromiseCreateReq) (now : Nat) : M PromiseCreateRes := do
  match ← viewPromise req.id now with
  | some p =>
      return { status := 200, promise := some p.toRecord }
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
        if p.tags.has "resonate:target" then
          let due :=
            match p.tags.get? "resonate:delay" with
            | some d => max (ServerModel.parseNat d) now
            | none => now
          setTask { id := p.id, state := .pending, version := 0,
                    retryAt := some due }
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
          setTask { id := p.id, state := .fulfilled, version := 0 }
        return { status := 200, promise := some p.toRecord }

def promiseSettle (req : PromiseSettleReq) (now : Nat) : M PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← viewPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending then
        let p := { p with state := req.state, value := req.value, settledAt := some now }
        setPromise p
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some p.toRecord }

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) :
    M PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← viewPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← viewPromise req.awaiter now with
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
    M PromiseRegisterListenerRes := do
  if !ServerModel.addressValid req.address then
    return { status := 400 }
  match ← viewPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if pAwaited.state == .pending then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : M PromiseSearchRes := do
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

def taskGet (req : TaskGetReq) (now : Nat) : M TaskGetRes := do
  match ← viewTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some _) =>
      return { status := 200, task := some t.toRecord }

def taskCreate (req : TaskCreateReq) (now : Nat) : M TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") then
    return { status := 400 }
  match ← viewPromise a.id now with
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
        let st :=
          if a.tags.isTimer then
            ServerModel.PromiseState.resolved
          else
            ServerModel.PromiseState.rejectedTimedout
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
      match ← viewTask p.id now with
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

def taskAcquire (req : TaskAcquireReq) (now : Nat) : M TaskAcquireRes := do
  match ← viewTask req.id now with
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
  match ← viewTask req.id now with
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

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : M TaskHeartbeatRes := do
  for ref in req.tasks do
    match ← viewTask ref.id now with
    | some (t, some p) =>
        if t.state == .acquired ∧ t.version == ref.version
            ∧ t.pid == some req.pid ∧ p.state == .pending then
          setTask { t with expiresAt := some (now + t.ttl.getD 0) }
    | _ =>
        pure ()
  return { status := 200 }

def taskSuspend (req : TaskSuspendReq) (now : Nat) : M TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← viewTask req.id now with
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
      let mut settled := false
      for action in req.actions do
        match ← viewPromise action.awaited now with
        | none =>
            return { status := 422 }
        | some pa =>
            if !pa.external then
              return { status := 422 }
            if pa.state != .pending then
              settled := true
      if settled then
        setTask { t with resumes := [] }
        return { status := 300 }
      else
        for action in req.actions do
          match ← viewPromise action.awaited now with
          | some pa =>
              setPromise (pa.addCallback req.id)
          | none =>
              pure ()
        setTask { t with state := .suspended, pid := none, ttl := none,
                         expiresAt := none, retryAt := none, resumes := [] }
        return { status := 200 }

def taskFulfill (req : TaskFulfillReq) (now : Nat) : M TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← viewTask req.id now with
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
      setPromise p
      return { status := 200, promise := some p.toRecord }

def taskRelease (req : TaskReleaseReq) (now : Nat) : M TaskReleaseRes := do
  match ← viewTask req.id now with
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
  match ← viewTask req.id now with
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
  match ← viewTask req.id now with
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

end Projected
end AbstractModel
