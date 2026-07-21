import «02-actions».«P-02-promise.create»

open ServerModel

namespace Timeouts

def onPromiseTimeout (id : String) (_now : Nat) : M Unit := do
  match ← getPromise id with
  | none =>
      pure ()
  | some p =>
      if p.state != .pending then
        pure ()
      else
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p.project p.timeoutAt with callbacks := [], listeners := [] }
        setPromise p
        delPromiseTimeout p.id
        match ← getTask p.id with
        | some t =>
            setTask { t with state := .fulfilled, pid := none, ttl := none, resumes := [] }
            delTaskTimeout t.id
        | none =>
            pure ()
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }

def onTaskRetryTimeout (id : String) (now : Nat) : M Unit := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask id with
  | none =>
      pure ()
  | some t =>
      if t.state != .pending then
        pure ()
      else
        delTaskTimeout t.id
        setTaskTimeout t.id 0 (now + retryTimeout)
        match ← getPromise t.id with
        | none =>
            pure ()
        | some p =>
            setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)

def onTaskLeaseTimeout (id : String) (now : Nat) : M Unit := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask id with
  | none =>
      pure ()
  | some t =>
      if t.state != .acquired then
        pure ()
      else
        let t := { t with state := .pending, pid := none, ttl := none }
        setTask t
        delTaskTimeout t.id
        setTaskTimeout t.id 0 (now + retryTimeout)
        match ← getPromise t.id with
        | none =>
            pure ()
        | some p =>
            setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)

partial def catchUp (now : Nat) (s : Schedule) : M Schedule := do
  if s.nextRunAt ≤ now then
    let cronTime := s.nextRunAt
    let promiseId := expand s.promiseId s.id cronTime
    let _ ← promiseCreate
      { id := promiseId, timeoutAt := cronTime + s.promiseTimeout,
        param := s.promiseParam, tags := s.promiseTags } cronTime
    catchUp now { s with lastRunAt := some cronTime, nextRunAt := nextCron s.cron cronTime }
  else
    return s

def onScheduleTimeout (id : String) (now : Nat) : M Unit := do
  match ← getSchedule id with
  | none =>
      pure ()
  | some s0 =>
      let s ← catchUp now s0
      setSchedule s
      delScheduleTimeout s.id
      setScheduleTimeout s.id s.nextRunAt

end Timeouts
