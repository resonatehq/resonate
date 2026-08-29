import «01-protocol».«validation»

/-!  # The abstract machine — state, and the shape of a transition

The coalesced state, and the monad every step is written in. There is
one machine now: reads come from the environment, writes accumulate as
`Effect`, and `run` folds them onto the pre-state at the end.

`bind` passes the SAME environment to its continuation, so a read
cannot observe a write of its own transition. That is not a habit to be
maintained — it is what the type says, and it is what lets a step's
writes be collected and committed together. One step is one
transaction.

MATERIALISATION IS A PARAMETER. `Env.mat` decides whether a read that
projects a settled promise also PERSISTS that settlement. `run true`
materialises; `run false` serves the same facts as views and persists
nothing. The two used to be two machines, 387 and 386 lines differing
in one word per read; here they are one body and a bit.

The parameter scopes over EXTERNAL steps only. Internal steps
materialise under both readings, and take the forced read
(`touchObject`) rather than the parametric one. Getting that wrong is
not theoretical: the first derivation of this machine substituted the
parametric read into the internal steps too, and the sweep refuted it —
60 of 1 331 scripts. `withMat` is the fix.

ONE OBJECT. A promise and its task are not two rows that happen to
share an id; they are one thing with two faces. Every task the machine
writes is written at a promise's id, and nothing deletes promises, so a
task without its promise was never reachable — but the state used to
say it was, and the cost showed up everywhere. `readTask` returned
`Option (TaskObject × Option PromiseObject)`, eleven handlers dismissed
the impossible `some (_, none)` with a status code no run can emit, and
the induction tier carried an `hbare` obligation for each one. Fusing
the row deletes all of that, and — the reason that matters — it turns
the promise/task joins in the catalogue into predicates on ONE row,
which is the only shape `PerStore` can reach.

What it does NOT delete is the catalogue's right to name the shape.
`consistent_task_iff_kind_task` survives, because the OTHER half
of it — an `okind = .task` promise must HAVE a task — is still a claim
an implementation with two tables can get wrong. The type stops the
machine from writing the illegal row; the entry stops a server from
being believed when it does.

EFFECTS STAY SPLIT, and asymmetric. `.setPromise` is an upsert;
`.setTask` is a map over the objects already there. Two reasons, both
load-bearing:

  * They COMMUTE. A read never sees a write of its own step, so every
    value a handler holds is a pre-state value. An effect that carried
    a whole `Object` would let a late task write clobber a promise a
    materialising read had already settled — the writes would have to
    be ordered, and the ordering argument would have to be re-made
    after every change. Naming only the field it touches makes each
    write immune to what the others did.
  * `.setTask` is a `map`, so it preserves length, order and ids
    definitionally. Every frame, uniqueness and monotonicity lemma has
    NOTHING to prove for a task write; only the promise side needs the
    upsert argument.

The price is one dead branch — `.setTask` at an id no object holds is a
no-op — against the eleven it removes. The three creation sites
(`createPromise`, and both arms of `taskCreate`) emit the promise
first, which is what makes the no-op unreachable. -/

namespace AbstractModel

open ServerModel (Ident Tags Value PromiseState TaskState PromiseRecord
                  TaskRecord Schedule Message OutboxEntry OutboxKey
                  PromiseCreateReq OType OKind)

structure PromiseObject where
  state     : PromiseState
  param     : Value
  value     : Value       := {}
  tags      : Tags
  timeoutAt : Nat
  createdAt : Nat
  settledAt : Option Nat  := none
  callbacks : List Ident     := []
  listeners : List String := []
  deriving Repr

def PromiseObject.toRecord (p : PromiseObject) (id : Ident) : PromiseRecord :=
  { id := id, state := p.state, param := p.param, value := p.value,
    tags := p.tags, timeoutAt := p.timeoutAt, createdAt := p.createdAt,
    settledAt := p.settledAt }

def PromiseObject.isTimer (p : PromiseObject) : Bool := p.tags.isTimer

/-- The two axes, lifted off the tags. Both DERIVED, both delegating to
    the protocol layer — there is no stored field to disagree with the
    tags, and no second place the disjunction is written.

    They replace the two booleans that used to live here. `external`
    and `targeted` were read at eight sites between them, and nothing
    in either name said which decision it was for; the arming rule and
    the task rule both read `targeted`, and only one of them was right
    to. Naming the axis makes the wrong reading a type error rather
    than a plausible line of code. -/
def PromiseObject.otype (p : PromiseObject) : OType := p.tags.otype

def PromiseObject.okind (p : PromiseObject) : OKind := p.tags.okind

def PromiseObject.addCallback (p : PromiseObject) (awaiterId : Ident) : PromiseObject :=
  if p.callbacks.contains awaiterId then
    p
  else
    { p with callbacks := p.callbacks ++ [awaiterId] }

def PromiseObject.addListener (p : PromiseObject) (address : String) : PromiseObject :=
  if p.listeners.contains address then
    p
  else
    { p with listeners := p.listeners ++ [address] }

def PromiseObject.project (p : PromiseObject) (now : Nat) : PromiseObject :=
  if p.state == .pending ∧ p.timeoutAt ≤ now then
    if p.isTimer then
      { p with state := .resolved, settledAt := some p.timeoutAt }
    else
      { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
  else
    p

structure TaskObject where
  state          : TaskState
  version        : Nat
  ttl            : Option Nat    := none
  pid            : Option String := none
  leaseTimeoutAt : Option Nat    := none
  retryTimeoutAt : Option Nat    := none
  resumes        : List Ident    := []
  deriving Repr

def TaskObject.toRecord (t : TaskObject) (id : Ident) : TaskRecord :=
  { id := id, state := t.state, version := t.version,
    resumes := t.resumes.length, ttl := t.ttl, pid := t.pid }

def TaskObject.fulfill (t : TaskObject) : TaskObject :=
  { t with state := .fulfilled, pid := none, ttl := none,
           leaseTimeoutAt := none, retryTimeoutAt := none, resumes := [] }

def TaskObject.view (t : TaskObject) (p : PromiseObject) : TaskObject :=
  if p.state != .pending ∧ t.state != .fulfilled then t.fulfill else t

/-- A promise, its id, and the task that executes it — if it has one.

    The `Option` is not free information: it is `promise.okind`, and
    `consistent_task_iff_kind_task` is the entry that says so.
    Keeping it an `Option` rather than an index on the tag is
    deliberate — the tag arrives from a client, and the catalogue has
    to be able to write down the state where the two disagree. -/
structure Object where
  id      : Ident
  promise : PromiseObject
  task    : Option TaskObject := none
  deriving Repr

/-- The whole object at an instant. Promise projection and the task's
    view of it were always the same fact read twice — the promise
    settles at its deadline, and a task whose promise is settled is
    fulfilled. One row, one function. -/
def Object.project (o : Object) (now : Nat) : Object :=
  let p := o.promise.project now
  { o with promise := p, task := o.task.map (·.view p) }

structure ServerState where
  objects   : List Object      := []
  schedules : List Schedule    := []
  outbox    : List OutboxEntry := []
  deriving Repr

def ServerState.init : ServerState := {}

/-- The two faces, as views of the one store.

    These are projections, not stores: `s.promises` is the promise of
    every object and `s.tasks` the task of every object that has one.
    The catalogue's per-row entries read through them and are written
    exactly as they were when there were two lists, which is the point —
    fusing the row was a change to the state, not to what is claimed
    about it. An entry that needs the id reads `s.objects` directly. -/
def ServerState.promises (s : ServerState) : List PromiseObject :=
  s.objects.map (·.promise)

def ServerState.tasks (s : ServerState) : List TaskObject :=
  s.objects.filterMap (·.task)

/-- Lookup by id, one face at a time. The catalogue reads through these;
    handlers take the whole object. `task?` is `none` for both an id no
    object holds and an object with no task — which is exactly the
    distinction the handlers stopped having to make. -/
def ServerState.promise? (s : ServerState) (id : Ident) : Option PromiseObject :=
  (s.objects.find? (·.id == id)).map (·.promise)

def ServerState.task? (s : ServerState) (id : Ident) : Option TaskObject :=
  (s.objects.find? (·.id == id)).bind (·.task)

def ServerState.hasTask (s : ServerState) (id : Ident) : Bool :=
  (s.task? id).isSome

inductive Effect
  | setPromise  (id : Ident) (p : PromiseObject)
  | setTask     (id : Ident) (t : TaskObject)
  | setSchedule (s : Schedule)
  | delSchedule (id : Ident)
  | setMessage  (address : String) (msg : Message)
  deriving Repr

/-- The row `.setPromise` writes: the promise it carries, onto the
    object already at that id, or a new object with no task. Named
    rather than inlined so the frame and induction tiers have a term to
    reason about instead of an anonymous matcher. -/
def Object.withPromise (id : Ident) (p : PromiseObject) : Option Object → Object
  | some o => { o with promise := p }
  | none   => { id := id, promise := p }

def Effect.apply (s : ServerState) : Effect → ServerState
  | .setPromise id p =>
      { s with objects := Object.withPromise id p (s.objects.find? (·.id == id))
                            :: s.objects.filter (·.id != id) }
  | .setTask id t =>
      { s with objects := s.objects.map fun o =>
                 if o.id == id then { o with task := some t } else o }
  | .setSchedule c => { s with schedules := c :: s.schedules.filter (·.id != c.id) }
  | .delSchedule i => { s with schedules := s.schedules.filter (·.id != i) }
  | .setMessage a m =>
      let entry := OutboxEntry.mk a m
      { s with outbox := entry :: s.outbox.filter (fun e => e.key != entry.key) }

def applyAll (s : ServerState) : List Effect → ServerState
  | []      => s
  | e :: es => applyAll (e.apply s) es

/-- Server configuration. NOT state: no step writes it, no response
    projects it, no trace records it. It is the operator's dial, and the
    machine only reads it — which is what puts it in the reader
    environment rather than in `ServerState`.

    `retryTimeout` is the redispatch cadence: how long the server waits
    before re-offering a task nobody has claimed. It belongs to the
    SERVER. `TaskObject.ttl` is a different quantity belonging to the
    WORKER — how long a holder asked to keep its lease — and using one
    as the other means a task whose worker died is re-offered on a
    schedule set by the worker that died. It is also absent exactly
    where retry matters most, on a task nobody has acquired yet. -/
structure ServerConfig where
  retryTimeout : Nat := 5000
  deriving Repr

structure Env where
  state  : ServerState
  mat    : Bool
  config : ServerConfig := {}

def H (α : Type) : Type := Env → α × List Effect

instance : Monad H where
  pure a   := fun _ => (a, [])
  bind x f := fun e =>
    let (a, w₁) := x e
    let (b, w₂) := f a e
    (b, w₁ ++ w₂)

def ask : H Env := fun e => (e, [])

def emit (f : Effect) : H Unit := fun _ => ((), [f])

def runWith (mat : Bool) (config : ServerConfig) (act : H α) (s : ServerState) :
    α × ServerState :=
  let (a, w) := act { state := s, mat := mat, config := config }
  (a, applyAll s w)

def run (mat : Bool) (act : H α) (s : ServerState) : α × ServerState :=
  runWith mat {} act s

def getObject (id : Ident) : H (Option Object) :=
  return (← ask).state.objects.find? (·.id == id)

def getSchedule (id : Ident) : H (Option Schedule) :=
  return (← ask).state.schedules.find? (·.id == id)

def setPromise (id : Ident) (p : PromiseObject) : H Unit := emit (.setPromise id p)
def setTask (id : Ident) (t : TaskObject) : H Unit := emit (.setTask id t)
def setSchedule (c : Schedule) : H Unit := emit (.setSchedule c)
def delSchedule (id : Ident) : H Unit := emit (.delSchedule id)
def setMessage (a : String) (m : Message) : H Unit := emit (.setMessage a m)

/-- Settle a promise, and fulfil the task executing it. The task no
    longer has to be looked up — it is in the object the caller already
    read. -/
def setSettled (o : Object) (p : PromiseObject) : H Unit := do
  setPromise o.id p
  if p.state != .pending then
    match o.task with
    | some t => if t.state != .fulfilled then setTask o.id t.fulfill
    | none   => pure ()

def createPromise (req : PromiseCreateReq) (now : Nat) : H Object := do
  if req.timeoutAt > now then
    let p : PromiseObject :=
      { state := .pending, param := req.param, tags := req.tags,
        timeoutAt := req.timeoutAt, createdAt := now }
    setPromise req.id p
    if p.okind == .task then
      let due :=
        match p.tags.get? "resonate:delay" with
        | some d => max (ServerModel.parseNat d) now
        | none => now
      let t : TaskObject := { state := .pending, version := 0, retryTimeoutAt := some due }
      setTask req.id t
      return { id := req.id, promise := p, task := some t }
    else
      return { id := req.id, promise := p }
  else
    let state :=
      if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout
    let p : PromiseObject :=
      { state := state, param := req.param, tags := req.tags,
        timeoutAt := req.timeoutAt, createdAt := req.timeoutAt,
        settledAt := some req.timeoutAt }
    setPromise req.id p
    if p.okind == .task then
      let t : TaskObject := { state := .fulfilled, version := 0 }
      setTask req.id t
      return { id := req.id, promise := p, task := some t }
    else
      return { id := req.id, promise := p }

/-- Persist whichever face the projection moved. Split out of
    `readObject` rather than inlined so that the read stays a single
    statement behind the `mat` test — and so the induction tier has a
    name for the only write a read performs. -/
def materialise (id : Ident) (o o' : Object) : H Unit :=
  (if o'.promise.state != o.promise.state then setPromise id o'.promise else pure ()) >>=
    fun _ =>
      match o.task, o'.task with
      | some t, some u => if u.state != t.state then setTask id u else pure ()
      | _, _ => pure ()

/-- The one read. Projects the object at `now`, and — when `mat` —
    writes back what the projection moved. -/
def readObject (id : Ident) (now : Nat) : H (Option Object) := do
  match ← getObject id with
  | none => return none
  | some o =>
      let o' := o.project now
      if (← ask).mat then materialise id o o'
      return some o'

/-- The read a TASK handler makes. Identical to `readObject` except that
    it looks at the task FIRST and declines the row when there is none.

    That guard is not decoration. A `task.*` request against an id holding
    an untargeted promise answers 404 because there is no task, and it has
    no business settling that promise on the way out — the request was
    never about it. The two-store machine got this from the order of its
    lookups (`getTask` before `readPromise`); fusing the row would have
    lost it silently, so it is stated here instead.

    Nothing observable turns on it: the response is 404 either way, and a
    settlement is stamped at the promise's deadline rather than at `now`,
    so persisting it later writes the same row. `b6` in the battery is the
    script that reaches the shape, and it is there so that this stays
    checked rather than argued. -/
def readTaskObject (id : Ident) (now : Nat) : H (Option Object) := do
  match ← getObject id with
  | none   => return none
  | some o => if o.task.isSome then readObject id now else return none

def createIfAbsent (req : PromiseCreateReq) (now : Nat) : H Unit := do
  match ← readObject req.id now with
  | some _ => pure ()
  | none   => let _ ← createPromise req now

def withMat (mat : Bool) (act : H α) : H α := fun e => act { e with mat := mat }

def touchObject (id : Ident) (now : Nat) : H (Option Object) :=
  withMat true (readObject id now)

def viewObject (id : Ident) (now : Nat) : H (Option Object) :=
  withMat false (readObject id now)

def touchTaskObject (id : Ident) (now : Nat) : H (Option Object) :=
  withMat true (readTaskObject id now)

def viewTaskObject (id : Ident) (now : Nat) : H (Option Object) :=
  withMat false (readTaskObject id now)

end AbstractModel
