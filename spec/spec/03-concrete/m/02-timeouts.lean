import «03-concrete».«m».«P-02-promise.create»

open ServerModel

namespace Materialized
namespace Timeouts

/-- Materialization IS the timeout transition: -m's promise timeout is
    a touch, discarded. (`touchPromise`'s body is -p's
    `onPromiseTimeout`, so the two τs are the same transition.) -/
def onPromiseTimeout (id : String) (now : Nat) : M Unit := do
  let _ ← touchPromise id now

/-- Retry and lease are material rules, verbatim from -p: the read
    discipline concerns projected facts, and these consult only their
    own timers, material task state, and immutable tags. -/
def onTaskRetryTimeout (id : String) (now : Nat) : M Unit := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTaskTimeout id 0 with
  | none =>
      pure ()
  | some tt =>
  if tt.timeout > now then
    pure ()
  else
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
  match ← getTaskTimeout id 1 with
  | none =>
      pure ()
  | some tt =>
  if tt.timeout > now then
    pure ()
  else
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
      if s0.nextRunAt > now then
        pure ()
      else
        let s ← catchUp now s0
        setSchedule s
        delScheduleTimeout s.id
        setScheduleTimeout s.id s.nextRunAt

end Timeouts
end Materialized
