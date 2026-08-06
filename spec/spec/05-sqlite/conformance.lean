import «05-sqlite».handlers
import «03-concrete».«m».«P-01-promise.get»
import «03-concrete».«m».«P-02-promise.create»
import «03-concrete».«m».«P-03-promise.settle»
import «03-concrete».«m».«P-04-promise.register_callback»
import «03-concrete».«m».«P-05-promise.register_listener»
import «03-concrete».«m».«P-06-promise.search»
import «03-concrete».«m».«S-01-schedule.get»
import «03-concrete».«m».«S-02-schedule.create»
import «03-concrete».«m».«S-03-schedule.delete»
import «03-concrete».«m».«S-04-schedule.search»
import «03-concrete».«m».«T-01-task.get»
import «03-concrete».«m».«T-02-task.create»
import «03-concrete».«m».«T-03-task.acquire»
import «03-concrete».«m».«T-04-task.fence»
import «03-concrete».«m».«T-05-task.heartbeat»
import «03-concrete».«m».«T-06-task.suspend»
import «03-concrete».«m».«T-07-task.fulfill»
import «03-concrete».«m».«T-08-task.release»
import «03-concrete».«m».«T-09-task.halt»
import «03-concrete».«m».«T-10-task.continue»
import «03-concrete».«m».«T-11-task.search»
import «03-concrete».«m».«02-timeouts»
import «03-concrete».«m».«03-resume»

/-!  # Conformance of the SQL machine to `-m`, over scripts

Not "given an arbitrary state, does one action agree" — that requires
hand-building states and arguing they are reachable. Here both machines
start EMPTY and are driven by the same script of requests, so every
state compared is one the machines built themselves. Reachability comes
for free and there is no well-formedness side-condition to audit.

Two channels, following `alts/unified/TRACES.md`'s discipline of saying
exactly what is checked:

* **responses** — the full `Response` for every step, in order. The
  channel that document records as unobserved by every existing harness.
* **state at the end** — `alpha` of the final store against the final
  `ServerState`, compared as the keyed collections both represent.

`stateEquiv` compares components as bags, for the reason
`equiv-Q-01.lean` records: the specification's `setPromise`/`setTask`
filter-then-prepend, so the most recently written object floats to the
front, while a SQL table has insertion order. Both represent keyed maps
and neither machine's behaviour depends on the sequence. -/

namespace Sqlite.Conformance

open Sql ServerModel Sqlite

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

deriving instance DecidableEq for ServerModel.Value
deriving instance DecidableEq for ServerModel.PromiseRecord
deriving instance DecidableEq for ServerModel.TaskRecord
deriving instance DecidableEq for ServerModel.Schedule
deriving instance DecidableEq for ServerModel.PromiseGetRes
deriving instance DecidableEq for ServerModel.PromiseCreateRes
deriving instance DecidableEq for ServerModel.PromiseSettleRes
deriving instance DecidableEq for ServerModel.PromiseRegisterCallbackRes
deriving instance DecidableEq for ServerModel.PromiseRegisterListenerRes
deriving instance DecidableEq for ServerModel.PromiseSearchRes
deriving instance DecidableEq for ServerModel.ScheduleGetRes
deriving instance DecidableEq for ServerModel.ScheduleCreateRes
deriving instance DecidableEq for ServerModel.ScheduleDeleteRes
deriving instance DecidableEq for ServerModel.ScheduleSearchRes
deriving instance DecidableEq for ServerModel.TaskGetRes
deriving instance DecidableEq for ServerModel.TaskCreateRes
deriving instance DecidableEq for ServerModel.TaskAcquireRes
deriving instance DecidableEq for ServerModel.PromiseCreateReq
deriving instance DecidableEq for ServerModel.PromiseSettleReq
deriving instance DecidableEq for ServerModel.TaskFenceInnerRes
deriving instance DecidableEq for ServerModel.TaskFenceRes
deriving instance DecidableEq for ServerModel.TaskHeartbeatRes
deriving instance DecidableEq for ServerModel.TaskSuspendRes
deriving instance DecidableEq for ServerModel.TaskFulfillRes
deriving instance DecidableEq for ServerModel.TaskReleaseRes
deriving instance DecidableEq for ServerModel.TaskHaltRes
deriving instance DecidableEq for ServerModel.TaskContinueRes
deriving instance DecidableEq for ServerModel.TaskSearchRes
deriving instance DecidableEq for ServerModel.ResumeRes
deriving instance DecidableEq for ServerModel.PromiseObject
deriving instance DecidableEq for ServerModel.TaskObject
deriving instance DecidableEq for ServerModel.ResumeReq
deriving instance DecidableEq for ServerModel.PromiseTimeout
deriving instance DecidableEq for ServerModel.TaskTimeout
deriving instance DecidableEq for ServerModel.ScheduleTimeout
deriving instance DecidableEq for ServerModel.Message
deriving instance DecidableEq for ServerModel.OutboxEntry
deriving instance DecidableEq for ServerModel.ServerConfig
deriving instance DecidableEq for ServerModel.ServerState

-- request types, for the alphabet's own `DecidableEq`
deriving instance DecidableEq for ServerModel.PromiseGetReq
deriving instance DecidableEq for ServerModel.PromiseRegisterCallbackReq
deriving instance DecidableEq for ServerModel.PromiseRegisterListenerReq
deriving instance DecidableEq for ServerModel.ScheduleGetReq
deriving instance DecidableEq for ServerModel.ScheduleCreateReq
deriving instance DecidableEq for ServerModel.ScheduleDeleteReq
deriving instance DecidableEq for ServerModel.TaskGetReq
deriving instance DecidableEq for ServerModel.TaskCreateReq
deriving instance DecidableEq for ServerModel.TaskAcquireReq
deriving instance DecidableEq for ServerModel.TaskFenceAction
deriving instance DecidableEq for ServerModel.TaskFenceReq
deriving instance DecidableEq for ServerModel.TaskRef
deriving instance DecidableEq for ServerModel.TaskHeartbeatReq
deriving instance DecidableEq for ServerModel.TaskSuspendReq
deriving instance DecidableEq for ServerModel.TaskFulfillReq
deriving instance DecidableEq for ServerModel.TaskReleaseReq
deriving instance DecidableEq for ServerModel.TaskHaltReq
deriving instance DecidableEq for ServerModel.TaskContinueReq

/-! ## Alphabet -/

inductive Request
  | promiseGet              (req : PromiseGetReq)
  | promiseCreate           (req : PromiseCreateReq)
  | promiseSettle           (req : PromiseSettleReq)
  | promiseRegisterCallback (req : PromiseRegisterCallbackReq)
  | promiseRegisterListener (req : PromiseRegisterListenerReq)
  | scheduleGet             (req : ScheduleGetReq)
  | scheduleCreate          (req : ScheduleCreateReq)
  | scheduleDelete          (req : ScheduleDeleteReq)
  | taskGet                 (req : TaskGetReq)
  | taskCreate              (req : TaskCreateReq)
  | taskAcquire             (req : TaskAcquireReq)
  | taskFence               (req : TaskFenceReq)
  | taskHeartbeat           (req : TaskHeartbeatReq)
  | taskSuspend             (req : TaskSuspendReq)
  | taskFulfill             (req : TaskFulfillReq)
  | taskRelease             (req : TaskReleaseReq)
  | taskHalt                (req : TaskHaltReq)
  | taskContinue            (req : TaskContinueReq)
  | τPromiseTimeout         (id : String)
  | τTaskRetryTimeout       (id : String)
  | τTaskLeaseTimeout       (id : String)
  | τResume                 (req : ResumeReq)
  deriving Repr, DecidableEq

inductive Response
  | promiseGet              (res : PromiseGetRes)
  | promiseCreate           (res : PromiseCreateRes)
  | promiseSettle           (res : PromiseSettleRes)
  | promiseRegisterCallback (res : PromiseRegisterCallbackRes)
  | promiseRegisterListener (res : PromiseRegisterListenerRes)
  | scheduleGet             (res : ScheduleGetRes)
  | scheduleCreate          (res : ScheduleCreateRes)
  | scheduleDelete          (res : ScheduleDeleteRes)
  | taskGet                 (res : TaskGetRes)
  | taskCreate              (res : TaskCreateRes)
  | taskAcquire             (res : TaskAcquireRes)
  | taskFence               (res : TaskFenceRes)
  | taskHeartbeat           (res : TaskHeartbeatRes)
  | taskSuspend             (res : TaskSuspendRes)
  | taskFulfill             (res : TaskFulfillRes)
  | taskRelease             (res : TaskReleaseRes)
  | taskHalt                (res : TaskHaltRes)
  | taskContinue            (res : TaskContinueRes)
  | resume                  (res : ResumeRes)
  | τ
  deriving Repr, DecidableEq

/-! ## The SQL machine as one dispatcher

τs are OBLIGATION-GUARDED, exactly as `04-theorems/trace.lean` guards
them: a timeout may fire only when a due obligation is on the books, a
resume only for a pair in `deferred`. Otherwise the alphabet would let
a script fire a τ the machine never armed. -/

def handleSql (rq : Request) (now : Nat) : Sql.SqlM Response :=
  match rq with
  | .promiseGet r              => Response.promiseGet <$> promiseGet r now
  | .promiseCreate r           => Response.promiseCreate <$> promiseCreate r now
  | .promiseSettle r           => Response.promiseSettle <$> promiseSettle r now
  | .promiseRegisterCallback r => Response.promiseRegisterCallback <$> promiseRegisterCallback r now
  | .promiseRegisterListener r => Response.promiseRegisterListener <$> promiseRegisterListener r now
  | .scheduleGet r             => Response.scheduleGet <$> scheduleGet r now
  | .scheduleCreate r          => Response.scheduleCreate <$> scheduleCreate r now
  | .scheduleDelete r          => Response.scheduleDelete <$> scheduleDelete r now
  | .taskGet r                 => Response.taskGet <$> taskGet r now
  | .taskCreate r              => Response.taskCreate <$> taskCreate r now
  | .taskAcquire r             => Response.taskAcquire <$> taskAcquire r now
  | .taskFence r               => Response.taskFence <$> taskFence r now
  | .taskHeartbeat r           => Response.taskHeartbeat <$> taskHeartbeat r now
  | .taskSuspend r             => Response.taskSuspend <$> taskSuspend r now
  | .taskFulfill r             => Response.taskFulfill <$> taskFulfill r now
  | .taskRelease r             => Response.taskRelease <$> taskRelease r now
  | .taskHalt r                => Response.taskHalt <$> taskHalt r now
  | .taskContinue r            => Response.taskContinue <$> taskContinue r now
  | .τPromiseTimeout id => do
      let d ← query "SELECT id FROM promise_timeouts WHERE id = ?1 AND timeout_at <= ?2"
                    [.text id, .int (Int.ofNat now)]
      if !d.rows.isEmpty then onPromiseTimeout id now
      return .τ
  | .τTaskRetryTimeout id => do onTaskRetryTimeout id now; return .τ
  | .τTaskLeaseTimeout id => do onTaskLeaseTimeout id now; return .τ
  | .τResume r => do
      let d ← query "SELECT awaited_id FROM deferred WHERE awaited_id = ?1 AND awaiter_id = ?2"
                    [.text r.awaited, .text r.awaiter]
      if d.rows.isEmpty then
        return .τ
      else
        undefer r.awaited r.awaiter
        Response.resume <$> onResume r.awaited r.awaiter now

def handleSpec (rq : Request) (now : Nat) : ServerModel.M Response :=
  match rq with
  | .promiseGet r              => Response.promiseGet <$> Materialized.promiseGet r now
  | .promiseCreate r           => Response.promiseCreate <$> Materialized.promiseCreate r now
  | .promiseSettle r           => Response.promiseSettle <$> Materialized.promiseSettle r now
  | .promiseRegisterCallback r => Response.promiseRegisterCallback <$> Materialized.promiseRegisterCallback r now
  | .promiseRegisterListener r => Response.promiseRegisterListener <$> Materialized.promiseRegisterListener r now
  | .scheduleGet r             => Response.scheduleGet <$> Materialized.scheduleGet r now
  | .scheduleCreate r          => Response.scheduleCreate <$> Materialized.scheduleCreate r now
  | .scheduleDelete r          => Response.scheduleDelete <$> Materialized.scheduleDelete r now
  | .taskGet r                 => Response.taskGet <$> Materialized.taskGet r now
  | .taskCreate r              => Response.taskCreate <$> Materialized.taskCreate r now
  | .taskAcquire r             => Response.taskAcquire <$> Materialized.taskAcquire r now
  | .taskFence r               => Response.taskFence <$> Materialized.taskFence r now
  | .taskHeartbeat r           => Response.taskHeartbeat <$> Materialized.taskHeartbeat r now
  | .taskSuspend r             => Response.taskSuspend <$> Materialized.taskSuspend r now
  | .taskFulfill r             => Response.taskFulfill <$> Materialized.taskFulfill r now
  | .taskRelease r             => Response.taskRelease <$> Materialized.taskRelease r now
  | .taskHalt r                => Response.taskHalt <$> Materialized.taskHalt r now
  | .taskContinue r            => Response.taskContinue <$> Materialized.taskContinue r now
  | .τPromiseTimeout id => do
      if (← get).promiseTimeouts.any (fun e => e.id == id && decide (e.timeout ≤ now)) then
        Materialized.Timeouts.onPromiseTimeout id now
      return .τ
  | .τTaskRetryTimeout id => do Materialized.Timeouts.onTaskRetryTimeout id now; return .τ
  | .τTaskLeaseTimeout id => do Materialized.Timeouts.onTaskLeaseTimeout id now; return .τ
  | .τResume r => do
      if (← get).deferred.any (fun d => d.awaited == r.awaited && d.awaiter == r.awaiter) then
        ServerModel.undefer r
        Response.resume <$> Materialized.onResume r now
      else
        return .τ

/-! ## Running a script -/

abbrev Script := List (Request × Nat)

def runSql : Script → Store → List Response × Store
  | [],            st => ([], st)
  | (rq, n) :: rest, st =>
      let (res, st') := Id.run ((handleSql rq n).run st)
      let (rs, st'') := runSql rest st'
      (res :: rs, st'')

def runSpec : Script → ServerState → List Response × ServerState
  | [],            s => ([], s)
  | (rq, n) :: rest, s =>
      let (res, s') := Id.run ((handleSpec rq n).run s)
      let (rs, s'') := runSpec rest s'
      (res :: rs, s'')

/-! ## Comparison -/

def bagEq {α} [BEq α] (a b : List α) : Bool :=
  a.length == b.length && a.all fun x => a.countP (· == x) == b.countP (· == x)

def promiseEq (p q : PromiseObject) : Bool :=
  p.id == q.id && p.state == q.state && p.param == q.param && p.value == q.value
    && bagEq p.tags q.tags && p.timeoutAt == q.timeoutAt && p.createdAt == q.createdAt
    && p.settledAt == q.settledAt
    && bagEq p.callbacks q.callbacks && bagEq p.listeners q.listeners

def taskEq (t u : TaskObject) : Bool :=
  t.id == u.id && t.state == u.state && t.version == u.version
    && t.ttl == u.ttl && t.pid == u.pid && bagEq t.resumes u.resumes

def bagEqBy {α} (eq : α → α → Bool) (a b : List α) : Bool :=
  a.length == b.length && a.all fun x => b.any (eq x)

def stateEquiv (s t : ServerState) : Bool :=
  s.config == t.config
    && bagEqBy promiseEq s.promises t.promises
    && bagEqBy taskEq    s.tasks    t.tasks
    && bagEq s.schedules        t.schedules
    && bagEq s.deferred         t.deferred
    && bagEq s.promiseTimeouts  t.promiseTimeouts
    && bagEq s.taskTimeouts     t.taskTimeouts
    && bagEq s.scheduleTimeouts t.scheduleTimeouts
    && bagEq s.outbox           t.outbox

/-- Both channels for one script, from empty on both sides. -/
def conforms (sc : Script) : Bool :=
  let (rs, st) := runSql  sc emptyStore
  let (rt, s)  := runSpec sc ServerState.init
  decide (rs = rt) && stateEquiv (alpha st) s

/-- Responses only — reported separately so a state-shape disagreement
    cannot be mistaken for a wire-visible one. -/
def responsesAgree (sc : Script) : Bool :=
  decide ((runSql sc emptyStore).1 = (runSpec sc ServerState.init).1)

end Sqlite.Conformance
