import «03-concrete».«state»

namespace ConcreteModel

open ServerModel

/-!  # The concrete machine's standing invariant

`alpha` (`04-theorems/alpha.lean`) maps this machine onto the coalesced
one, and it does three different things with the nine components:

  RECONSTRUCTS   `taskTimeouts` become the abstract task's `expiresAt`
                 and `retryAt`; `deferred` folds back into the awaited
                 promise's `callbacks`.
  DROPS          `promiseTimeouts`, `scheduleTimeouts`, `config`.
  COPIES         promises, tasks, schedules, outbox.

Each of those needs something to be true, and this is that something.
Without it `alpha` is not even a function on set-equal states — two
entries for one `(id, kind)` and `find?` picks whichever came first.

Stated as a `Bool` so it can be EVALUATED before anyone tries to prove
it preserved. -/

def uniqueTimeouts (s : ServerState) : Bool :=
  s.taskTimeouts.all fun e =>
    (s.taskTimeouts.filter (fun f => f.id == e.id && f.kind == e.kind)).length == 1

def timeoutsNameTasks (s : ServerState) : Bool :=
  s.taskTimeouts.all fun e => s.tasks.any (·.id == e.id)

/-- kind 1 is the lease, and a lease belongs to an acquired task. -/
def leaseDiscipline (s : ServerState) : Bool :=
  s.tasks.all fun t =>
    (s.taskTimeouts.any (fun e => e.id == t.id && e.kind == 1))
      == (t.state == .acquired)

/-- kind 0 is the retry deadline, armed while the task is pending. -/
def retryDiscipline (s : ServerState) : Bool :=
  s.tasks.all fun t =>
    (s.taskTimeouts.any (fun e => e.id == t.id && e.kind == 0))
      == (t.state == .pending)

/-- Settlement MOVED the callback into `deferred`; it did not copy it.
    `alpha` folds each entry back on, so a copy would double-count. -/
def deferredMoved (s : ServerState) : Bool :=
  s.deferred.all fun d =>
    (s.promises.any fun p =>
       p.id == d.awaited && p.state != .pending && !(p.callbacks.contains d.awaiter))
    && s.tasks.any (·.id == d.awaiter)

/-- Dropped, so redundant. The two directions are NOT symmetric: armed
    implies pending, but pending implies armed only for an EXTERNAL
    promise, because `promise.create` arms only those. That asymmetry is
    the fact lag — an internal promise past its deadline is unarmed, so
    this machine never settles it while the abstract one settles it on
    the next touch, which is why `square.lean` compares through `normEq`
    rather than equality. The weak conjunct and the up-to-normalisation
    relation are the same fact seen twice. -/
def promiseTimeoutsMirror (s : ServerState) : Bool :=
  s.promiseTimeouts.all (fun e =>
    s.promises.any fun p => p.id == e.id && p.timeoutAt == e.timeout && p.state == .pending)
  && s.promises.all (fun p =>
    !(p.state == .pending && p.external) ||
      s.promiseTimeouts.any (fun e => e.id == p.id && e.timeout == p.timeoutAt))

def scheduleTimeoutsMirror (s : ServerState) : Bool :=
  s.scheduleTimeouts.all (fun e =>
    s.schedules.any fun c => c.id == e.id && c.nextRunAt == e.timeout)
  && s.schedules.all (fun c =>
    s.scheduleTimeouts.any (fun e => e.id == c.id && e.timeout == c.nextRunAt))

/-- Every task shares its promise's id — issue #10, one rung down. -/
def coKeyed (s : ServerState) : Bool :=
  s.tasks.all fun t => s.promises.any (·.id == t.id)

/-- The storage invariant `setSettled` maintains — issue #9, one rung down. -/
def settledCoupled (s : ServerState) : Bool :=
  s.promises.all fun p =>
    !(p.state != .pending) ||
      s.tasks.all (fun t => !(t.id == p.id) || t.state == .fulfilled)

/-- A timer is never targeted, in storage and not only at the door. -/
def tagsWellFormed (s : ServerState) : Bool :=
  s.promises.all fun p => !p.tags.timerTargeted

def inv (s : ServerState) : Bool :=
  uniqueTimeouts s && timeoutsNameTasks s
    && leaseDiscipline s && retryDiscipline s
    && deferredMoved s
    && promiseTimeoutsMirror s && scheduleTimeoutsMirror s
    && coKeyed s && settledCoupled s && tagsWellFormed s

end ConcreteModel
