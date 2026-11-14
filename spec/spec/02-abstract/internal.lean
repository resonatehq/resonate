import «02-abstract».«external»

/-!  # Internal steps — what the server does on its own initiative

Six steps a background job may fire: the promise timeout, the callback
drain, the listener drain, the lease timeout, the retry dispatch, and
the schedule.

They take the FORCED reads — `touchObject` where the step persists what
it projects, `viewObject` where it only needs to look. Neither is
parametric: internal steps materialise under both readings of the
machine, and `Env.mat` does not reach here. -/

namespace AbstractModel
namespace Internal


open ServerModel (Ident nextCron occurrences expand Schedule
                  PromiseTimeoutReq PromiseRegisterCallbackReq PromiseRegisterListenerReq
                  TaskLeaseTimeoutReq TaskRetryTimeoutReq ScheduleTimeoutReq)

def processPromiseTimeout (req : PromiseTimeoutReq) (now : Nat) : H Unit := do
  let _ ← touchObject req.id now

/-- `since` is the instant the awaited promise settled at: the resumed
    task's first offer, so that its grid is anchored at a fact of the state
    rather than at the instant the drain ran. -/
def resumeOne (awaited awaiter : Ident) (since now : Nat) : H Unit := do
  match ← touchTaskObject awaiter now with
  | none => pure ()
  | some o =>
  match o.task with
  | none => pure ()
  | some t =>
      match t.state with
      | .suspended =>
          setTask o.id { t with state := .pending, resumes := [awaited],
                                retryTimeoutAt := some since }
      | .pending | .acquired | .halted =>
          if !(t.resumes.contains awaited) then
            setTask o.id { t with resumes := t.resumes ++ [awaited] }
      | .fulfilled =>
          pure ()

def processCallback (req : PromiseRegisterCallbackReq) (now : Nat) : H Unit := do
  match ← touchObject req.awaited now with
  | none => pure ()
  | some o =>
      if o.promise.state == .pending then
        pure ()
      else if o.promise.callbacks.contains req.awaiter then
        setPromise o.id { o.promise with
                          callbacks := o.promise.callbacks.filter (· != req.awaiter) }
        resumeOne o.id req.awaiter (o.promise.settledAt.getD now) now

def processListener (req : PromiseRegisterListenerReq) (now : Nat) : H Unit := do
  match ← touchObject req.awaited now with
  | none => pure ()
  | some o =>
      if o.promise.state == .pending then
        pure ()
      else if o.promise.listeners.contains req.address then
        setPromise o.id { o.promise with
                          listeners := o.promise.listeners.filter (· != req.address) }
        setMessage req.address (.unblock (o.promise.toRecord o.id))

/-- The lease's end is a projection (`TaskObject.lapse`): a read already
    shows the task pending. This step persists it, as the promise timeout
    does. The re-offer is the retry step's: the task's first offer is due
    at the lease's end. -/
def processLeaseTimeout (req : TaskLeaseTimeoutReq) (now : Nat) : H Unit := do
  let _ ← touchTaskObject req.id now

/-- Redispatch a pending task whose dispatch clock is due.

    The next instant is COMPUTED, from the server's dial and the task's
    own grid (`nextOffer`) — it is not supplied by whoever fires the step,
    and it does not depend on when the step ran. Every other arming site
    (`createPromise`, `taskRelease`, `taskContinue`, `resumeOne`,
    `TaskObject.lapse`) anchors the grid at a fact of the state: the
    instant the task came to be pending. This step walks it.

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
def processRetryTimeout (req : TaskRetryTimeoutReq) (now : Nat) : H Unit := do
  match ← viewTaskObject req.id now with
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
                             retryTimeoutAt := some (nextOffer due now (← ask).config.retryTimeout) }
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

def processSchedule (req : ScheduleTimeoutReq) (now : Nat) : H Unit := do
  match ← getSchedule req.schedule with
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
