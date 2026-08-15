import «02-abstract».«external»

/-!  # Internal steps — what the server does on its own initiative

Six steps a background job may fire: the promise timeout, the listener
drain, the callback drain, the lease timeout, the retry dispatch, and
the schedule. No client asks for these.

They take the FORCED reads — `touchPromise`, `touchTask`,
`viewPromise` — not the parametric ones. Internal steps materialise
under both readings of the machine; `Env.mat` does not reach here. -/

namespace AbstractModel
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
                  setTask { t with state := .pending, pid := none,
                                   expiresAt := none, retryAt := some now }

/-- Redispatch a pending task whose dispatch clock is due.

    The next instant is COMPUTED, from the task's own `ttl` and the
    clock — it is not supplied by whoever fires the step. Every other
    arming site (`createPromise`, `taskRelease`, `taskContinue`,
    `resumeOne`, `processLeaseTimeout`) already computed it; this was
    the one exception, and the exception was the only place the
    environment could write a value into the store. It could write a
    past one, leaving the step enabled at the instant it fired.

    `ttl` is the interval because `ttl` is what a holder tells the
    server about its own cadence, and it survives release: it is task
    CONFIGURATION, not lease state. A task never yet acquired carries
    none, so it redispatches as fast as the environment fires — which
    is right, since nothing has picked it up, and harmless, because
    `execute` entries are keyed by task id alone and collapse. -/
def processRetryTimeout (id : String) (now : Nat) : H Unit := do
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
                  setTask { t with retryAt := some (now + t.ttl.getD 0) }
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
end AbstractModel
