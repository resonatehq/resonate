import «02-abstract».«m»

/-!  # The coalesced machine — internal rules

The machine's entire internal life is seven guarded rules, fired by the
environment in any order, at any pace, any number of times. Each rule
names its target (and, where there is a choice, the chosen listener,
awaiter, or re-arm instant) — the
parameters ARE the scheduler's nondeterminism, exactly as `now` is the
clock's. Every rule is total: if its guard does not hold it is a no-op,
so a stale or spurious firing is harmless.

  R1 `promiseTimeout`    if there is a promise past its deadline — fact P.
  R2 `taskFulfillment`   if there is a task whose promise is settled — fact T.
  R3 `notify`            if there is a settled promise with a listener:
                         deliver a chosen one its `unblock`.
  R4 `resume`            if there is a settled promise with an awaiter:
                         wake a chosen one.
  R5 `leaseExpiry`       if there is a task past its lease deadline.
  R6 `dispatch`          if there is a pending task past `retryAt`, emit
                         its `execute` and re-arm `retryAt` at a chosen
                         instant.
  R7 `scheduleFire`      if there is a schedule past `nextRunAt`.

R1 and R2 are the facts themselves — firing one is the environment
touching an object, nothing more. They exist as rules so that facts
materialize eventually even on objects no request ever touches again.

R5 and R6 read the TASK raw — lease expiry and dispatch are choices,
not facts: an expired lease is permission to redispatch, not
revocation, and neither may be forced by observation. But their
DECISIONS consult the promise through the view, like every decision in
the machine: TIMEOUT ALWAYS WINS extends to redispatch and
reassignment — no rule creates new work for a logically dead task.
R6's due time is state (`retryAt`) while its re-arm instant is a
parameter: the machine records WHEN the next attempt is owed, the
scheduler decides the cadence — repeated firing is at-least-once
delivery.  -/

namespace AbstractModel
namespace Rules

open ServerModel (nextCron occurrences expand Schedule)

/-- R1: materialize fact P on a promise of the environment's choosing. -/
def promiseTimeout (id : String) (now : Nat) : M Unit := do
  let _ ← touchPromise id now

/-- R2: materialize fact T on a task of the environment's choosing. -/
def taskFulfillment (id : String) (now : Nat) : M Unit := do
  let _ ← touchTask id now

/-- R3: deliver a chosen listener of a settled promise its `unblock`.
    A batch — one to all at a time — is consecutive firings; the
    selection, like the pace, is the scheduler's. -/
def notify (id : String) (address : String) (now : Nat) : M Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.listeners.contains address then
        setPromise { p with listeners := p.listeners.filter (· != address) }
        setMessage address (.unblock p.toRecord)

/-- Wake one awaiter of a settled promise. The touch materializes the
    awaiter's own deadline first, so TIMEOUT ALWAYS WINS falls out: an
    awaiter past its own deadline reads `.fulfilled` and is dropped —
    no explicit deadline guard, unlike the base machine's resume. -/
def resumeOne (awaited awaiter : String) (now : Nat) : M Unit := do
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

/-- R4: wake a chosen awaiter of a settled promise. A woken task is
    `.pending`; its `execute` is R6's job. -/
def resume (id : String) (awaiter : String) (now : Nat) : M Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.callbacks.contains awaiter then
        setPromise { p with callbacks := p.callbacks.filter (· != awaiter) }
        resumeOne p.id awaiter now

/-- R5: an acquired task past its lease deadline returns to `.pending`.
    The TASK is read raw — expiry is a choice, not a fact, and must
    never be forced by observation — but the DECISION consults the
    promise through the view: TIMEOUT ALWAYS WINS extends to
    reassignment, and no rule creates new work for a logically dead
    task. -/
def leaseExpiry (id : String) (now : Nat) : M Unit := do
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

/-- R6: emit the `execute` for a pending task whose dispatch is due
    (`retryAt ≤ now`), and re-arm `retryAt` at `next` — the rule's
    parameter, because cadence is the scheduler's choice, not the
    machine's state. Raw read; repeatable — the outbox's keyed upsert
    makes re-emission idempotent, so any `next` (including one already
    past) is sound. -/
def dispatch (id : String) (next : Nat) (now : Nat) : M Unit := do
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
                -- The view again: no dispatch for the dead.
                if p.state == .pending then
                  setTask { t with retryAt := some next }
                  setMessage ((p.tags.get? "resonate:target").getD "")
                    (.execute t.id t.version)

/-- One occurrence: create the promise AS OF ITS OWN CRON TIME — a
    backlogged occurrence is born settled, exactly as if it had been
    created on time. Idempotent: the expanded id is per-occurrence, so
    a re-fire finds the promise and writes nothing. -/
def fireOccurrence (s : Schedule) (t : Nat) : M Unit := do
  let _ ← promiseCreate
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → M Unit
  | [] => pure ()
  | t :: ts => do
      fireOccurrence s t
      fireAll s ts

/-- R7: fire a schedule past `nextRunAt` (with catch-up): create every
    occurrence due at or before `now`, then advance the schedule past
    the last one. The occurrence list is the opaque `occurrences` —
    calendar math, like `nextCron` itself — filtered by due-ness: the
    machine takes only what is due, whatever the calendar says. -/
def scheduleFire (id : String) (now : Nat) : M Unit := do
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

end Rules
end AbstractModel
