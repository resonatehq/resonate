import «04-theorems».«handlers»

/-!  # Catalogue entries that are inductive

One section per table. Each is a `Hereditary` instance — facts about a
single row, no monad and no handler in sight — and then two lines per
catalogue entry: it holds at `init`, and every step preserves it.

That is the whole cost of a per-row property now. `induction.lean` built
the reduction and `handlers.lean` paid for it once, generically; what is
left here is arithmetic on record fields.

## What "inductive" buys

The sweep checks 1 464 scripts. These theorems check every script, of
any length, under both read disciplines, at every instant. For an entry
that appears here, a `Legal` violation in an implementation trace is
unambiguously an implementation bug: the property cannot fail on the
model.

## The shape of the task argument

The nine task entries are proved as ONE predicate and then weakened,
rather than nine times over. That is not economy — it is necessary.
`tContinue` re-pends a HALTED task and only clears `retryAt`; that the
result has no `pid` is inherited from the halted row, which is a
different catalogue entry. Several of the nine are inductive only in
each other's company, so the strengthening is their conjunction and the
entries fall out by weakening.

## What is NOT here, and why

Every remaining `.state` entry fails at a NAMED obligation, which is
more informative than a failed tactic.

Three promise entries fail at the two births, because the birth takes
the client's tags verbatim: `timer_not_targeted`, `target_is_nonempty`
and `delay_before_deadline` are true of every reachable state but false
of an arbitrary birth. They hold because `promise.create`,
`task.create` and `schedule.create` all 400 first — a HANDLER fact, not
a promise fact.

Two fail at `addCallback`/`addListener` for the same reason:
`obligations_require_external` and `awaiter_is_not_self` are enforced by
the 422 guards in `promise.registerCallback`, not by the ledger
operation.

Two promise entries and one task entry — `callbacks_unique`,
`listeners_unique`, `task_resumes_unique` — are per-row and true at
every obligation, but need `List.eraseDups` lemmas core does not ship.

Two mention the clock (`promise_created_at_lte_now`,
`settled_at_lte_now`), which this tier cannot see: `Hereditary` is a
predicate on a row, and those are predicates on a row AND an instant.

Three schedule entries are arithmetic about `nextCron` and
`occurrences`, which are `opaque`. They are not provable and should not
be: they need the calendar contract in `01-protocol/validation.lean`,
and that contract is a hypothesis about the outside world, not a lemma.

And the four uniqueness entries and the eleven cross-table `consistent_*`
entries are not per-row at all — they relate rows to each other, and
need a different argument. -/

namespace Abstraction
namespace Induction

open AbstractModel

/-! ## Instantiating one table at a time

A property about promises owes the task and schedule obligations too,
but trivially: take those predicates to be constantly true. These three
builders pay that once. -/

/-- `BEq` on the two state enums comes from `DecidableEq`, and `simp`
    will not rewrite `a == b` inside a HYPOTHESIS without being told
    that. Told once here. -/
theorem ts_beq (a b : ServerModel.TaskState) : (a == b) = decide (a = b) := rfl
theorem ps_beq (a b : ServerModel.PromiseState) : (a == b) = decide (a = b) := rfl

theorem all_const {α} : ∀ (l : List α), l.all (fun _ => true) = true
  | [] => rfl
  | _ :: xs => by simp [List.all_cons, all_const xs]

/-- `Q.promise` is keyed by the row's id now, and these three entries
    never look at it: a single-object property is a claim about the row,
    not about where it is filed. -/
def onlyPromise (f : PromiseObject → Bool) : Q :=
  { promise := fun _ => f, task := fun _ => true, schedule := fun _ => true }

def onlyTask (f : TaskObject → Bool) : Q :=
  { promise := fun _ _ => true, task := f, schedule := fun _ => true }

def onlySchedule (f : ServerModel.Schedule → Bool) : Q :=
  { promise := fun _ _ => true, task := fun _ => true, schedule := f }

theorem perStore_onlyPromise {f : PromiseObject → Bool} {s : ServerState} :
    PerStore (onlyPromise f) s = s.promises.all f := by
  rw [PerStore, allObj_split]
  simp [onlyPromise, all_const, ServerState.promises, List.all_map, Function.comp_def]

theorem perStore_onlyTask {f : TaskObject → Bool} {s : ServerState} :
    PerStore (onlyTask f) s = s.tasks.all f := by
  rw [PerStore, allObj_split]
  simp [onlyTask, all_const]

theorem perStore_onlySchedule {f : ServerModel.Schedule → Bool} {s : ServerState} :
    PerStore (onlySchedule f) s = s.schedules.all f := by
  rw [PerStore, allObj_split]
  simp [onlySchedule, all_const]

/-! ## The promise obligations, and the step law they buy -/

structure HPromise (f : PromiseObject → Bool) : Prop where
  project      : ∀ (p : PromiseObject) (n : Nat), f p = true → f (p.project n) = true
  addCallback  : ∀ (p : PromiseObject) (a : String), f p = true → f (p.addCallback a) = true
  addListener  : ∀ (p : PromiseObject) (a : String), f p = true → f (p.addListener a) = true
  settle       : ∀ (p : PromiseObject) (st : ServerModel.PromiseState)
                   (v : ServerModel.Value) (t : Nat),
                   st.settable = true → p.state = .pending → t < p.timeoutAt → f p = true →
                   f { p with state := st, value := v, settledAt := some t } = true
  dropListener : ∀ (p : PromiseObject) (a : String), f p = true →
                   f { p with listeners := p.listeners.filter (· != a) } = true
  dropCallback : ∀ (p : PromiseObject) (a : String), f p = true →
                   f { p with callbacks := p.callbacks.filter (· != a) } = true
  live         : ∀ (id : String) (param : ServerModel.Value) (tags : ServerModel.Tags)
                   (timeoutAt createdAt : Nat), createdAt < timeoutAt →
                   f { state := .pending, param := param, tags := tags,
                       timeoutAt := timeoutAt, createdAt := createdAt } = true
  dead         : ∀ (id : String) (st : ServerModel.PromiseState)
                   (param : ServerModel.Value) (tags : ServerModel.Tags) (timeoutAt : Nat),
                   st = (if tags.isTimer then .resolved else .rejectedTimedout) →
                   f { state := st, param := param, tags := tags,
                       timeoutAt := timeoutAt, createdAt := timeoutAt,
                       settledAt := some timeoutAt } = true

theorem hereditary_onlyPromise {f : PromiseObject → Bool} (h : HPromise f)
    (a : ServerState) : Hereditary (onlyPromise f) a where
  project _ p n _ := h.project p n
  addCallback _ p c _ := h.addCallback p c
  addListener _ p c _ := h.addListener p c
  settle _ p st v t _ := h.settle p st v t
  dropListener _ p c _ _ := h.dropListener p c
  dropCallback _ p c _ _ := h.dropCallback p c
  live := fun id param tags tAt cAt hlt _ => h.live id param tags tAt cAt hlt
  dead := fun id st param tags tAt hst _ => h.dead id st param tags tAt hst
  tFulfill _ _ := rfl
  tBornPending _ := rfl
  tBornDone := rfl
  tBornHeld _ _ _ := rfl
  tAcquire _ _ _ _ _ := rfl
  tHeartbeat _ _ _ _ := rfl
  tClearResumes _ _ := rfl
  tSuspend _ _ := rfl
  tRepend _ _ _ := rfl
  tHalt _ _ := rfl
  tContinue _ _ _ _ := rfl
  tResume _ _ _ _ _ := rfl
  tAddResume _ _ _ _ _ _ := rfl
  tRearm _ _ _ _ := rfl
  cBorn _ _ _ _ _ _ _ _ := rfl
  cAdvance _ _ _ := rfl

theorem promise_step {f : PromiseObject → Bool} (h : HPromise f)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    s.promises.all f = true → (stepOf mat st now s).2.promises.all f = true := by
  intro hf
  have := perStore_step mat st now s (hereditary_onlyPromise h s)
    (by rw [perStore_onlyPromise]; exact hf)
  rwa [perStore_onlyPromise] at this

/-! ## The task obligations, and the step law they buy -/

structure HTask (f : TaskObject → Bool) : Prop where
  fulfill    : ∀ (t : TaskObject), f t = true → f t.fulfill = true
  bornPending : ∀ (due : Nat),
                  f { state := .pending, version := 0, retryAt := some due } = true
  bornDone   : f { state := .fulfilled, version := 0 } = true
  bornHeld   : ∀ (pid : String) (ttl now : Nat),
                 f { state := .acquired, version := 1, ttl := some ttl, pid := some pid, expiresAt := some (now + ttl) } = true
  acquire    : ∀ (t : TaskObject) (pid : String) (ttl now : Nat), f t = true →
                 f { t with state := .acquired, version := t.version + 1, ttl := some ttl, pid := some pid, expiresAt := some (now + ttl), retryAt := none, resumes := [] } = true
  heartbeat  : ∀ (t : TaskObject) (x : Nat), (t.state == .acquired) = true →
                 f t = true → f { t with expiresAt := some x } = true
  clearResumes : ∀ (t : TaskObject), f t = true → f { t with resumes := [] } = true
  suspend    : ∀ (t : TaskObject), f t = true →
                 f { t with state := .suspended, pid := none, ttl := none, expiresAt := none, retryAt := none, resumes := [] } = true
  repend     : ∀ (t : TaskObject) (n : Nat), f t = true →
                 f { t with state := .pending, pid := none, ttl := none, expiresAt := none, retryAt := some n } = true
  halt       : ∀ (t : TaskObject), f t = true →
                 f { t with state := .halted, pid := none, ttl := none, expiresAt := none, retryAt := none } = true
  cont       : ∀ (t : TaskObject) (n : Nat), (t.state == .halted) = true → f t = true →
                 f { t with state := .pending, retryAt := some n } = true
  resume     : ∀ (t : TaskObject) (a : String) (n : Nat), t.state = .suspended → f t = true →
                 f { t with state := .pending, resumes := [a], retryAt := some n } = true
  addResume  : ∀ (t : TaskObject) (a : String), t.state ≠ .suspended → t.state ≠ .fulfilled →
                 (t.resumes.contains a) = false → f t = true →
                 f { t with resumes := t.resumes ++ [a] } = true
  rearm      : ∀ (t : TaskObject) (n : Nat), (t.state == .pending) = true → f t = true →
                 f { t with retryAt := some n } = true

theorem hereditary_onlyTask {f : TaskObject → Bool} (h : HTask f)
    (a : ServerState) : Hereditary (onlyTask f) a where
  project _ _ _ _ _ := rfl
  addCallback _ _ _ _ _ := rfl
  addListener _ _ _ _ _ := rfl
  settle _ _ _ _ _ _ _ _ _ _ := rfl
  dropListener _ _ _ _ _ _ := rfl
  dropCallback _ _ _ _ _ _ := rfl
  live _ _ _ _ _ _ _ := rfl
  dead _ _ _ _ _ _ _ := rfl
  tFulfill := h.fulfill
  tBornPending := h.bornPending
  tBornDone := h.bornDone
  tBornHeld := h.bornHeld
  tAcquire := h.acquire
  tHeartbeat := h.heartbeat
  tClearResumes := h.clearResumes
  tSuspend := h.suspend
  tRepend := h.repend
  tHalt := h.halt
  tContinue := h.cont
  tResume := h.resume
  tAddResume := h.addResume
  tRearm := h.rearm
  cBorn _ _ _ _ _ _ _ _ := rfl
  cAdvance _ _ _ := rfl

theorem task_step {f : TaskObject → Bool} (h : HTask f)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    s.tasks.all f = true → (stepOf mat st now s).2.tasks.all f = true := by
  intro hf
  have := perStore_step mat st now s (hereditary_onlyTask h s)
    (by rw [perStore_onlyTask]; exact hf)
  rwa [perStore_onlyTask] at this

/-! ## The schedule obligations, and the step law they buy -/

structure HSchedule (f : ServerModel.Schedule → Bool) : Prop where
  born    : ∀ (id cron promiseId : String) (promiseTimeout : Nat)
              (promiseParam : ServerModel.Value) (promiseTags : ServerModel.Tags)
              (now : Nat), promiseTags.timerTargeted = false →
              f { id := id, cron := cron, promiseId := promiseId,
                  promiseTimeout := promiseTimeout, promiseParam := promiseParam,
                  promiseTags := promiseTags, createdAt := now,
                  nextRunAt := ServerModel.nextCron cron now, lastRunAt := none } = true
  advance : ∀ (c : ServerModel.Schedule) (last : Nat), f c = true →
              f { c with lastRunAt := some last, nextRunAt := ServerModel.nextCron c.cron last } = true

theorem hereditary_onlySchedule {f : ServerModel.Schedule → Bool} (h : HSchedule f)
    (a : ServerState) : Hereditary (onlySchedule f) a where
  project _ _ _ _ _ := rfl
  addCallback _ _ _ _ _ := rfl
  addListener _ _ _ _ _ := rfl
  settle _ _ _ _ _ _ _ _ _ _ := rfl
  dropListener _ _ _ _ _ _ := rfl
  dropCallback _ _ _ _ _ _ := rfl
  live _ _ _ _ _ _ _ := rfl
  dead _ _ _ _ _ _ _ := rfl
  tFulfill _ _ := rfl
  tBornPending _ := rfl
  tBornDone := rfl
  tBornHeld _ _ _ := rfl
  tAcquire _ _ _ _ _ := rfl
  tHeartbeat _ _ _ _ := rfl
  tClearResumes _ _ := rfl
  tSuspend _ _ := rfl
  tRepend _ _ _ := rfl
  tHalt _ _ := rfl
  tContinue _ _ _ _ := rfl
  tResume _ _ _ _ _ := rfl
  tAddResume _ _ _ _ _ _ := rfl
  tRearm _ _ _ _ := rfl
  cBorn := h.born
  cAdvance := h.advance

theorem schedule_step {f : ServerModel.Schedule → Bool} (h : HSchedule f)
    (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    s.schedules.all f = true → (stepOf mat st now s).2.schedules.all f = true := by
  intro hf
  have := perStore_step mat st now s (hereditary_onlySchedule h s)
    (by rw [perStore_onlySchedule]; exact hf)
  rwa [perStore_onlySchedule] at this

/-! # The promise entries

## `created_at ≤ timeout_at`

The first one through, and the simplest: no transformation touches
either field. That immutability IS the reason the property is
inductive. -/

def qCreatedLeTimeout (p : PromiseObject) : Bool := p.createdAt ≤ p.timeoutAt

theorem hp_createdLeTimeout : HPromise qCreatedLeTimeout where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simpa [qCreatedLeTimeout] using h
    · simpa [qCreatedLeTimeout] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qCreatedLeTimeout] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qCreatedLeTimeout] using h
  settle p st v t _ _ _ h := by simpa [qCreatedLeTimeout] using h
  dropListener p a h := by simpa [qCreatedLeTimeout] using h
  dropCallback p a h := by simpa [qCreatedLeTimeout] using h
  live id param tags timeoutAt createdAt h := by simp [qCreatedLeTimeout]; omega
  dead id st param tags timeoutAt _ := by simp [qCreatedLeTimeout]

/-! ## A pending promise is born before its deadline

Strictly, where the previous one is non-strict. The difference is the
born-dead branch, where the two coincide — and that promise is not
pending, so this says nothing about it. -/

def qPendingBeforeDeadline (p : PromiseObject) : Bool :=
  p.state != .pending || p.createdAt < p.timeoutAt

theorem hp_pendingBeforeDeadline : HPromise qPendingBeforeDeadline where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qPendingBeforeDeadline]
    · simpa [qPendingBeforeDeadline] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qPendingBeforeDeadline] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qPendingBeforeDeadline] using h
  settle p st v t hst _ _ _ := by
    cases st <;> simp_all [qPendingBeforeDeadline, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qPendingBeforeDeadline] using h
  dropCallback p a h := by simpa [qPendingBeforeDeadline] using h
  live id param tags timeoutAt createdAt h := by simp [qPendingBeforeDeadline]; omega
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qPendingBeforeDeadline]

/-! ## Settled exactly when stamped

The biconditional, which is why it needs every obligation to agree: the
two births disagree with each other (one pending and unstamped, one
settled and stamped) and both have to satisfy it. -/

def qSettledIffStamped (p : PromiseObject) : Bool :=
  (p.state != .pending) == p.settledAt.isSome

theorem hp_settledIffStamped : HPromise qSettledIffStamped where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qSettledIffStamped]
    · simpa [qSettledIffStamped] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qSettledIffStamped] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qSettledIffStamped] using h
  settle p st v t hst _ _ _ := by
    cases st <;> simp_all [qSettledIffStamped, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qSettledIffStamped] using h
  dropCallback p a h := by simpa [qSettledIffStamped] using h
  live id param tags timeoutAt createdAt h := by simp [qSettledIffStamped]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qSettledIffStamped]

/-! ## `rejectedTimedout` is server-owned

The one that turns a `settable` check into a state invariant. A client
cannot forge this verdict, so a row carrying it was written by the
deadline path and is stamped at the deadline. Note where it is
discharged: at `settle`, from `st.settable`, which is exactly the
precheck `promise.settle` and `task.fulfill` perform. Delete either
precheck and this obligation is the thing that fails. -/

def qTimedoutIsServerOwned (p : PromiseObject) : Bool :=
  p.state != .rejectedTimedout || p.settledAt == some p.timeoutAt

theorem hp_timedoutIsServerOwned : HPromise qTimedoutIsServerOwned where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qTimedoutIsServerOwned]
    · simpa [qTimedoutIsServerOwned] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qTimedoutIsServerOwned] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qTimedoutIsServerOwned] using h
  settle p st v t hst _ _ _ := by
    cases st <;> simp_all [qTimedoutIsServerOwned, ServerModel.PromiseState.settable]
  dropListener p a h := by simpa [qTimedoutIsServerOwned] using h
  dropCallback p a h := by simpa [qTimedoutIsServerOwned] using h
  live id param tags timeoutAt createdAt h := by simp [qTimedoutIsServerOwned]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qTimedoutIsServerOwned]

/-! ## The settlement stamp is never past the deadline

The first entry that needs Fact P. A settle writes `some now`, and
nothing about a `PromiseObject` says `now` is below its deadline — what
says so is that the handler read the promise as PENDING, and a promise
reads pending only while it is not yet due. -/

def qSettledAtLeTimeout (p : PromiseObject) : Bool :=
  match p.settledAt with
  | none => true
  | some x => x ≤ p.timeoutAt

theorem hp_settledAtLeTimeout : HPromise qSettledAtLeTimeout where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> simp [qSettledAtLeTimeout]
    · simpa [qSettledAtLeTimeout] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qSettledAtLeTimeout] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qSettledAtLeTimeout] using h
  settle p st v t _ _ hdue _ := by simp [qSettledAtLeTimeout]; omega
  dropListener p a h := by simpa [qSettledAtLeTimeout] using h
  dropCallback p a h := by simpa [qSettledAtLeTimeout] using h
  live id param tags timeoutAt createdAt h := by simp [qSettledAtLeTimeout]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qSettledAtLeTimeout]

/-! ## A promise stamped AT its deadline carries the deadline's verdict

Needs BOTH new facts. Fact P at the settle — a client settling a
pending promise stamps strictly below the deadline, so it can never
produce this shape — and the strengthened birth obligation, because the
born-dead promise IS stamped at its deadline and has to carry the
matching verdict. That birth obligation is what turned "`task.create`
faces no timers" into a proof; see `writesGood_taskCreate`. -/

def qDeadlineVerdict (p : PromiseObject) : Bool :=
  p.settledAt != some p.timeoutAt
    || p.state == (if p.tags.isTimer then .resolved else .rejectedTimedout)

theorem hp_deadlineVerdict : HPromise qDeadlineVerdict where
  project p n h := by
    unfold PromiseObject.project
    split
    · split <;> rename_i ht <;> simp_all [qDeadlineVerdict, PromiseObject.isTimer]
    · simpa [qDeadlineVerdict] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qDeadlineVerdict] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qDeadlineVerdict] using h
  settle p st v t _ _ hdue _ := by simp [qDeadlineVerdict]; omega
  dropListener p a h := by simpa [qDeadlineVerdict] using h
  dropCallback p a h := by simpa [qDeadlineVerdict] using h
  live id param tags timeoutAt createdAt h := by simp [qDeadlineVerdict]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp_all [qDeadlineVerdict]

/-! ## A pending promise carries no verdict, and neither does a deadline settlement

Two entries, one predicate, because the second is not inductive on its
own: `project` settles a pending promise and leaves `value` alone, so
the new row is stamped at its deadline with whatever value it already
had — and nothing in that entry says a PENDING promise has no value.
The catalogue knows it separately. This is exactly the situation an
inductive invariant is for. -/

def qNoValueUnlessSettled (p : PromiseObject) : Bool :=
  (p.state != .pending || (p.value.data.isNone && p.value.headers.isEmpty))
    && (p.settledAt != some p.timeoutAt || (p.value.data.isNone && p.value.headers.isEmpty))

theorem hp_noValueUnlessSettled : HPromise qNoValueUnlessSettled where
  project p n h := by
    unfold PromiseObject.project
    split
    · rename_i hc
      have hv : p.value.data.isNone && p.value.headers.isEmpty = true := by
        simp only [qNoValueUnlessSettled, Bool.and_eq_true, Bool.or_eq_true, bne_iff_ne] at h
        rcases h.1 with h1 | h1
        · exact absurd hc.1 (by simp_all)
        · simpa using h1
      split <;> simp_all [qNoValueUnlessSettled]
    · simpa [qNoValueUnlessSettled] using h
  addCallback p a h := by
    unfold PromiseObject.addCallback
    split <;> simpa [qNoValueUnlessSettled] using h
  addListener p a h := by
    unfold PromiseObject.addListener
    split <;> simpa [qNoValueUnlessSettled] using h
  settle p st v t hst _ hdue _ := by
    cases st <;>
      simp_all [qNoValueUnlessSettled, ServerModel.PromiseState.settable] <;> omega
  dropListener p a h := by simpa [qNoValueUnlessSettled] using h
  dropCallback p a h := by simpa [qNoValueUnlessSettled] using h
  live id param tags timeoutAt createdAt h := by simp [qNoValueUnlessSettled]
  dead id st param tags timeoutAt hst := by subst hst; split <;> simp [qNoValueUnlessSettled]

/-! # The task entries

One predicate for all nine, because several are inductive only in each
other's company. `tContinue` re-pends a HALTED task and clears only
`retryAt`; that the result carries no `pid` comes from the halted row,
which is a different entry. Prove the conjunction, weaken to each. -/

def qTaskShape (t : TaskObject) : Bool :=
  ((t.state == .acquired) == t.pid.isSome)
    && ((t.state == .acquired) == t.ttl.isSome)
    && ((t.state == .acquired) == t.expiresAt.isSome)
    && ((t.state == .pending) == t.retryAt.isSome)
    && (t.state != .fulfilled
        || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone
            && t.resumes.isEmpty))
    && (t.state != .suspended
        || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone))
    && (t.state != .halted
        || (t.pid.isNone && t.ttl.isNone && t.expiresAt.isNone && t.retryAt.isNone))
    && (t.state != .suspended || t.resumes.isEmpty)
    && (t.state != .acquired || 1 ≤ t.version)

theorem ht_taskShape : HTask qTaskShape where
  fulfill t h := by simp [qTaskShape, TaskObject.fulfill]
  bornPending due := by simp [qTaskShape]
  bornDone := by simp [qTaskShape]
  bornHeld pid ttl now := by simp [qTaskShape]
  acquire t pid ttl now h := by simp [qTaskShape]
  heartbeat t x hacq h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  clearResumes t h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  suspend t h := by simp [qTaskShape]
  repend t n h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  halt t h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  cont t n hhalt h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  resume t a n hsusp h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  addResume t a hns hnf hc h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]
  rearm t n hpend h := by cases hst : t.state <;> simp_all [qTaskShape, ts_beq]

/-! # The schedule entry that does not need the calendar

`schedule.create` 400s on `timerTargeted`, and the only other write to
the table copies the tags across unchanged. So the tag invariant is
inductive with no arithmetic at all — unlike the three that compare
instants, which need `nextCron` to actually move forward. -/

def qScheduleTags (c : ServerModel.Schedule) : Bool := !c.promiseTags.timerTargeted

theorem hc_scheduleTags : HSchedule qScheduleTags where
  born id cron promiseId promiseTimeout promiseParam promiseTags now htt := by
    simp [qScheduleTags, htt]
  advance c last h := by simpa [qScheduleTags] using h

/-! # The catalogue statements

Each entry is an `.all` over one table by definition, so `init` and the
step law follow with nothing in between. `n'` is free of `now` in every
one of these — none of them reads the clock — which is why the clock
obligation does not appear. -/

section Entries

open Properties

theorem created_at_lte_timeout_at_init (now : Nat) :
    well_formed_promise_created_at_lte_timeout_at now ServerState.init = true := rfl

theorem created_at_lte_timeout_at_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_created_at_lte_timeout_at now s = true →
    well_formed_promise_created_at_lte_timeout_at n' (stepOf mat st now s).2 = true :=
  promise_step hp_createdLeTimeout mat st now s

theorem pending_created_before_deadline_init (now : Nat) :
    well_formed_promise_pending_created_before_deadline now ServerState.init = true := rfl

theorem pending_created_before_deadline_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_pending_created_before_deadline now s = true →
    well_formed_promise_pending_created_before_deadline n' (stepOf mat st now s).2 = true :=
  promise_step hp_pendingBeforeDeadline mat st now s

theorem settled_at_iff_not_pending_init (now : Nat) :
    well_formed_promise_settled_at_iff_not_pending now ServerState.init = true := rfl

theorem settled_at_iff_not_pending_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_settled_at_iff_not_pending now s = true →
    well_formed_promise_settled_at_iff_not_pending n' (stepOf mat st now s).2 = true :=
  promise_step hp_settledIffStamped mat st now s

theorem timedout_is_server_owned_init (now : Nat) :
    well_formed_promise_timedout_is_server_owned now ServerState.init = true := rfl

theorem timedout_is_server_owned_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_timedout_is_server_owned now s = true →
    well_formed_promise_timedout_is_server_owned n' (stepOf mat st now s).2 = true :=
  promise_step hp_timedoutIsServerOwned mat st now s

theorem settled_at_lte_timeout_at_init (now : Nat) :
    well_formed_promise_settled_at_lte_timeout_at now ServerState.init = true := rfl

theorem settled_at_lte_timeout_at_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_settled_at_lte_timeout_at now s = true →
    well_formed_promise_settled_at_lte_timeout_at n' (stepOf mat st now s).2 = true :=
  promise_step hp_settledAtLeTimeout mat st now s

theorem deadline_verdict_matches_timer_tag_init (now : Nat) :
    well_formed_promise_deadline_verdict_matches_timer_tag now ServerState.init = true := rfl

theorem deadline_verdict_matches_timer_tag_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_promise_deadline_verdict_matches_timer_tag now s = true →
    well_formed_promise_deadline_verdict_matches_timer_tag n' (stepOf mat st now s).2 = true :=
  promise_step hp_deadlineVerdict mat st now s

/-! ## The two value entries, from the one strengthening -/

theorem no_value_unless_settled_init :
    ServerState.init.promises.all qNoValueUnlessSettled = true := rfl

theorem no_value_unless_settled_step (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    s.promises.all qNoValueUnlessSettled = true →
    (stepOf mat st now s).2.promises.all qNoValueUnlessSettled = true :=
  promise_step hp_noValueUnlessSettled mat st now s

theorem pending_has_no_value_of_strengthening (now : Nat) (s : ServerState) :
    s.promises.all qNoValueUnlessSettled = true →
    well_formed_promise_pending_has_no_value now s = true :=
  all_mono (fun _ h => (Bool.and_eq_true _ _ |>.mp h).1) _

theorem deadline_settlement_has_no_value_of_strengthening (now : Nat) (s : ServerState) :
    s.promises.all qNoValueUnlessSettled = true →
    well_formed_promise_deadline_settlement_has_no_value now s = true :=
  all_mono (fun _ h => (Bool.and_eq_true _ _ |>.mp h).2) _

/-! ## The nine task entries, from the one strengthening

Nothing but weakening happens below — which is the point: the argument
was made once, and each entry is a projection out of it. -/

theorem task_shape_init : ServerState.init.tasks.all qTaskShape = true := rfl

theorem task_shape_step (mat : Bool) (st : Step) (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → (stepOf mat st now s).2.tasks.all qTaskShape = true :=
  task_step ht_taskShape mat st now s

theorem task_acquired_iff_has_pid_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_acquired_iff_has_pid now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.1.1.1.1.1) _

theorem task_acquired_iff_has_ttl_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_acquired_iff_has_ttl now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.1.1.1.1.2) _

theorem task_acquired_iff_has_expires_at_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_acquired_iff_has_expires_at now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.1.1.1.2) _

theorem task_pending_iff_has_retry_at_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_pending_iff_has_retry_at now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.1.1.2) _

theorem task_fulfilled_is_cleared_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_fulfilled_is_cleared now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.1.2) _

theorem task_suspended_is_cleared_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_suspended_is_cleared now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.1.2) _

theorem task_halted_is_cleared_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_halted_is_cleared now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.1.2) _

theorem task_suspended_has_no_resumes_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_suspended_has_no_resumes now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.1.2) _

theorem task_acquired_version_positive_of_shape (now : Nat) (s : ServerState) :
    s.tasks.all qTaskShape = true → well_formed_task_acquired_version_positive now s = true :=
  all_mono (fun _ h => by
    simp only [qTaskShape, Bool.and_eq_true] at h; exact h.2) _

/-! ## The schedule tag entry -/

theorem schedule_promise_tags_not_timer_targeted_init (now : Nat) :
    well_formed_schedule_promise_tags_not_timer_targeted now ServerState.init = true := rfl

theorem schedule_promise_tags_not_timer_targeted_step (mat : Bool) (st : Step) (now n' : Nat)
    (s : ServerState) :
    well_formed_schedule_promise_tags_not_timer_targeted now s = true →
    well_formed_schedule_promise_tags_not_timer_targeted n' (stepOf mat st now s).2 = true :=
  schedule_step hc_scheduleTags mat st now s

end Entries

end Induction
end Abstraction
