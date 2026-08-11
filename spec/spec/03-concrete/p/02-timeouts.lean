import «03-concrete».«p».«P-02-promise.create»

open ServerModel

namespace Timeouts

/-- NOT BEFORE: every timeout transition re-checks its own due time and
    refuses an early firing — the machine enforces the timer contract
    rather than trusting the environment to fire on schedule. -/
def processPromiseTimeout (id : String) (now : Nat) : H Unit := do
  match ← getPromise id with
  | none =>
      pure ()
  | some p =>
      if p.state != .pending ∨ p.timeoutAt > now then
        pure ()
      else
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p.project p.timeoutAt with callbacks := [], listeners := [] }
        setSettled p
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }

def processRetryTimeout (id : String) (now : Nat) : H Unit := do
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
        match ← getPromise t.id with
        | none =>
            pure ()
        | some p =>
            -- TIMEOUT ALWAYS WINS, extended to redispatch: a retry
            -- consults the projected promise state and creates no new
            -- work for a logically dead task — its cleanup is owned by
            -- the promise-timeout transition.
            if p.state == .pending ∧ p.timeoutAt > now then
              delTaskTimeout t.id
              setTaskTimeout t.id 0 (now + retryTimeout)
              setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)

def processLeaseTimeout (id : String) (now : Nat) : H Unit := do
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
        match ← getPromise t.id with
        | none =>
            pure ()
        | some p =>
            -- TIMEOUT ALWAYS WINS, extended to reassignment: an
            -- expired lease of a logically dead task is not returned
            -- to circulation — there is no circulation left.
            if p.state == .pending ∧ p.timeoutAt > now then
              let t := { t with state := .pending, pid := none, ttl := none }
              setTask t
              delTaskTimeout t.id
              setTaskTimeout t.id 0 (now + retryTimeout)
              setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)

/-- One occurrence: create the promise AS OF ITS OWN CRON TIME — a
    backlogged occurrence is born settled, exactly as if it had been
    created on time. Idempotent: the expanded id is per-occurrence, so
    a re-fire finds the promise and writes nothing. -/
def fireOccurrence (s : Schedule) (t : Nat) : H Unit := do
  let _ ← promiseCreate
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → H Unit
  | [] => pure ()
  | t :: ts => do
      fireOccurrence s t
      fireAll s ts

def processSchedule (id : String) (now : Nat) : H Unit := do
  match ← getSchedule id with
  | none =>
      pure ()
  | some s0 =>
      if s0.nextRunAt > now then
        pure ()
      else
        let ts := (occurrences s0.cron s0.nextRunAt now).filter (· ≤ now)
        fireAll s0 ts
        let s :=
          match ts.getLast? with
          | some last => { s0 with lastRunAt := some last,
                                   nextRunAt := nextCron s0.cron last }
          | none => s0
        setSchedule s
        delScheduleTimeout s.id
        setScheduleTimeout s.id s.nextRunAt

end Timeouts
