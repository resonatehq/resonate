import «02-abstract».«external»

/-!  # Internal steps — what the server does on its own initiative

Six steps a background job may fire: the promise timeout, the callback
drain, the listener drain, the lease timeout, the retry dispatch, and
the schedule. No client asks for these.

They take the FORCED reads — `touchObject` where the step persists what
it projects, `viewObject` where it only needs to look. Neither is
parametric: internal steps materialise under both readings of the
machine, and `Env.mat` does not reach here. -/

namespace AbstractModel
namespace Internal


open ServerModel (Ident nextCron occurrences expand Schedule)

def processPromiseTimeout (id : Ident) (now : Nat) : H Unit := do
  let _ ← touchObject id now

def resumeOne (awaited awaiter : Ident) (now : Nat) : H Unit := do
  match ← touchTaskObject awaiter now with
  | none => pure ()
  | some o =>
  match o.task with
  | none => pure ()
  | some t =>
      match t.state with
      | .suspended =>
          setTask o.id { t with state := .pending, resumes := [awaited],
                                retryTimeoutAt := some now }
      | .pending | .acquired | .halted =>
          if !(t.resumes.contains awaited) then
            setTask o.id { t with resumes := t.resumes ++ [awaited] }
      | .fulfilled =>
          pure ()

def processCallback (id : Ident) (awaiter : Ident) (now : Nat) : H Unit := do
  match ← touchObject id now with
  | none => pure ()
  | some o =>
      if o.promise.state == .pending then
        pure ()
      else if o.promise.callbacks.contains awaiter then
        setPromise o.id { o.promise with
                          callbacks := o.promise.callbacks.filter (· != awaiter) }
        resumeOne o.id awaiter now

def processListener (id : Ident) (address : String) (now : Nat) : H Unit := do
  match ← touchObject id now with
  | none => pure ()
  | some o =>
      if o.promise.state == .pending then
        pure ()
      else if o.promise.listeners.contains address then
        setPromise o.id { o.promise with
                          listeners := o.promise.listeners.filter (· != address) }
        setMessage address (.unblock (o.promise.toRecord o.id))

def processLeaseTimeout (id : Ident) (now : Nat) : H Unit := do
  match ← viewTaskObject id now with
  | none => pure ()
  | some o =>
  match o.task with
  | none => pure ()
  | some t =>
      match t.leaseTimeoutAt with
      | none => pure ()
      | some deadline =>
          if t.state == .acquired ∧ deadline ≤ now then
            if o.promise.state == .pending then
              setTask o.id { t with state := .pending, pid := none, ttl := none,
                                    leaseTimeoutAt := none, retryTimeoutAt := some now }

/-- Redispatch a pending task whose dispatch clock is due.

    The next instant is COMPUTED, from the server's dial and the
    clock — it is not supplied by whoever fires the step. Every other
    arming site (`createPromise`, `taskRelease`, `taskContinue`,
    `resumeOne`, `processLeaseTimeout`) already computed it; this was
    the one exception, and the exception was the only place the
    environment could write a value into the store. It could write a
    past one, leaving the step enabled at the instant it fired.

    The interval is `config.retryTimeout`, read from the environment.
    It is the SERVER's dial — how long to wait before re-offering a task
    nobody has claimed — and it is present for every task, including one
    that has never been acquired, which is exactly where retry matters
    and where the task carries nothing of its own to read.

    It is deliberately not `TaskObject.ttl`. That is the WORKER's
    number, how long a holder asked to keep its lease; using it here
    would re-offer a dead worker's task on the dead worker's schedule,
    would be missing on the newborn path, and — since `TaskRecord`
    reports `ttl` — would be observable. A transcribed production trace
    refuses it: `valid/lean/real.lean` shows `ttl := none` on a task
    acquired with 60000, suspended and resumed. -/
def processRetryTimeout (id : Ident) (now : Nat) : H Unit := do
  match ← viewTaskObject id now with
  | none => pure ()
  | some o =>
  match o.task with
  | none => pure ()
  | some t =>
      match t.retryTimeoutAt with
      | none => pure ()
      | some due =>
          if t.state == .pending ∧ due ≤ now then
            if o.promise.state == .pending then
              setTask o.id { t with
                             retryTimeoutAt := some (now + (← ask).config.retryTimeout) }
              setMessage ((o.promise.tags.get? "resonate:target").getD "")
                (.execute o.id t.version)

def fireOccurrence (s : Schedule) (t : Nat) : H Unit :=
  createIfAbsent
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → H Unit
  | [] => pure ()
  | t :: ts => do
      fireOccurrence s t
      fireAll s ts

def processSchedule (id : Ident) (now : Nat) : H Unit := do
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
end AbstractModel
