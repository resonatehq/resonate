import «02-abstract».«state»

/-!  # The coalesced machine — internal steps

The machine's steps come in two kinds. EXTERNAL steps are the protocol
handlers — one per request, in `external-steps-p.lean` and
`external-steps-m.lean`, which differ only in read discipline. INTERNAL
steps are these: the transitions the machine takes on its own, with no
request behind them, shared by both disciplines. `AStep.isExternal` in
`04-theorems` is the same distinction, drawn over the alphabet.

The machine's entire internal life is six guarded rules — "rule" for
the shape (a guard and a write), "internal step" for the role — fired
by the environment in any order, at any pace, any number of times. Each
rule names its target (and, where there is a choice, the chosen
listener, awaiter, or re-arm instant) — the parameters ARE the
scheduler's nondeterminism, exactly as `now` is the clock's. Every rule
is total: if its guard does not hold it is a no-op, so a stale or
spurious firing is harmless.

  R1 `processPromiseTimeout`  the promise deadline: settle to the
                              verdict and fulfill the task pair — the
                              coupled write.
  R3 `processListener`        one listener of a settled promise: emit
                              its `unblock` — the work continues
                              outside.
  R4 `processCallback`        one callback of a settled promise: wake
                              the awaiter task — the work continues in
                              the durable world.
  R5 `processLeaseTimeout`    the lease deadline: reclaim the task to
                              pending, arm the retry deadline.
  R6 `processRetryTimeout`    the retry deadline: emit the `execute`,
                              re-arm at a chosen instant.
  R7 `processSchedule`        the schedule's alarm: create every
                              occurrence now due, advance past the last.

EVERY RULE PROCESSES SOMETHING THE STATE OWES, and is named for what it
processes — its TRIGGER — never for what it emits. Three process a
deadline that has come due (R1, R5, R6); two process one entry of a
settled promise's fan-out ledger, remove-and-act in a single step (R3
over `listeners`, R4 over `callbacks`); R7 processes the schedule's
alarm. What a rule emits is an effect: exactly two write the outbox,
and neither takes its name from the message. R6 is the third member of
the timeout family, not a `dispatch` of its own — the retry deadline's
only consequence IS the `execute`, so the deadline and the message are
one event.

There is no sixth kind of obligation, and that is checkable: the only
things the state can owe work on are the three deadlines (`timeoutAt`,
the lease deadline, the retry deadline), the two ledgers, and the
schedule's `nextRunAt`. A task's queued resumes are CARRIED and
delivered with the `execute` rather than processed, and outbox entries
are transport's business, below the model.

FACT T IS NOT A RULE. There was once an R2 `taskFulfillment` — a
separate transition materializing fact T on a task whose promise is
settled. It is gone. Fact T is not an independent fact but fact P seen
through the task, made true by the same event, so settling a promise
fulfills its task pair in the SAME step: the coupled write of
`setSettled`, which R1 goes through like every other settling
transition. On any state satisfying that storage invariant a separate
fulfillment rule is the identity — the write that settled the promise
already fulfilled the task. The surviving rules keep their original
numbers so that cross-references elsewhere in the tree still point at
the same rules; the gap at R2 is the deletion, recorded.

The fan-out ledgers are still drained ASYNCHRONOUSLY, one entry at a
time, rather than inlined into the settling transitions. That keeps
this spec the liberal contract: an implementation that pages through a
large fan-out still refines it, and one that settles with the whole
fan-out inline in a single atomic write refines it trivially.

R5 and R6 read the TASK raw — lease and retry are choices, not facts:
an expired lease is permission to redispatch, not revocation, and
neither may be forced by observation. But their DECISIONS consult the
promise through the view, like every decision in the machine: TIMEOUT
ALWAYS WINS extends to redispatch and reassignment — no rule creates
new work for a logically dead task. R6's due time is state (`retryAt`)
while its re-arm instant is a parameter: the machine records WHEN the
next attempt is owed, the scheduler decides the cadence — repeated
firing is at-least-once delivery.  -/

namespace AbstractModel
namespace Internal

open ServerModel (nextCron occurrences expand Schedule)

/-- R1: process the promise deadline on a promise of the environment's
    choosing — settle it to its verdict and fulfill its task pair. Fact
    P and fact T are one event, so the rule that materializes one
    materializes both: `touchPromise` settles through the coupled write
    (`setSettled`). It exists as a rule so that the fact materializes
    eventually even on a promise no request ever touches again. -/
def processPromiseTimeout (id : String) (now : Nat) : M Unit := do
  let _ ← touchPromise id now

/-- R3: process one listener of a settled promise — remove it from the
    ledger and emit its `unblock`. A batch — one to all at a time — is
    consecutive firings; the selection, like the pace, is the
    scheduler's. -/
def processListener (id : String) (address : String) (now : Nat) : M Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.listeners.contains address then
        setPromise { p with listeners := p.listeners.filter (· != address) }
        setMessage address (.unblock p.toRecord)

/-- Wake one awaiter of a settled promise — R4's wake step, a helper
    rather than a rule of its own. The touch materializes the
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

/-- R4: process one callback of a settled promise — remove it from the
    ledger and wake the awaiter task. A woken task is `.pending`; its
    `execute` is R6's job. -/
def processCallback (id : String) (awaiter : String) (now : Nat) : M Unit := do
  match ← touchPromise id now with
  | none => pure ()
  | some p =>
      if p.state == .pending then
        pure ()
      else if p.callbacks.contains awaiter then
        setPromise { p with callbacks := p.callbacks.filter (· != awaiter) }
        resumeOne p.id awaiter now

/-- R5: process the lease deadline — an acquired task past it returns
    to `.pending` with the retry deadline armed. The TASK is read raw —
    expiry is a choice, not a fact, and must never be forced by
    observation — but the DECISION consults the promise through the
    view: TIMEOUT ALWAYS WINS extends to reassignment, and no rule
    creates new work for a logically dead task. -/
def processLeaseTimeout (id : String) (now : Nat) : M Unit := do
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

/-- R6: process the retry deadline — emit the `execute` for a pending
    task whose dispatch is due (`retryAt ≤ now`), and re-arm `retryAt`
    at `next` — the rule's parameter, because cadence is the
    scheduler's choice, not the machine's state. The deadline and the
    message are one event: emitting the `execute` is the deadline's
    only consequence. Raw read; repeatable — the outbox's keyed upsert
    makes re-emission idempotent, so any `next` (including one already
    past) is sound. -/
def processRetryTimeout (id : String) (next : Nat) (now : Nat) : M Unit := do
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

/-- One occurrence: create the promise AS OF ITS OWN CRON TIME — the
    occurrence instant is passed as `now`, so a backlogged occurrence
    is born exactly as one fired on time would have been. Born
    PENDING, therefore, dated at its cron instant with its deadline a
    promise-timeout later; if that window has already elapsed by the
    time anyone reads it, the touch settles it at the deadline, and the
    record is the one an on-time firing would have left. Idempotent:
    the expanded id is per-occurrence, so a re-fire finds the promise
    and writes nothing.

    The birth write is `createIfAbsent`, over the same `createPromise`
    both external twins call — an internal step reaches for the shared
    write, never for a handler. -/
def fireOccurrence (s : Schedule) (t : Nat) : M Unit :=
  createIfAbsent
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → M Unit
  | [] => pure ()
  | t :: ts => do
      fireOccurrence s t
      fireAll s ts

/-- R7: process a schedule past `nextRunAt` (with catch-up): create
    every occurrence due at or before `now`, then advance the schedule
    past the last one. The occurrence list is the opaque `occurrences` —
    calendar math, like `nextCron` itself — filtered by due-ness: the
    machine takes only what is due, whatever the calendar says. -/
def processSchedule (id : String) (now : Nat) : M Unit := do
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
