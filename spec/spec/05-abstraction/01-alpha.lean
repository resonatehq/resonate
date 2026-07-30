import «03-equivalence».«01-trace»
import «04-abstract».«05-rules»
import «04-abstract».«03-tasks»
import «04-abstract».«04-schedules»

/-!  # Abstraction — the map from the concrete machine to the abstract one

The CONCRETE machine is the base machine of `01-objects`/`02-actions-*`
— timeout components, a deferred-resume queue, retry config — in either
read discipline (-p and -m are weakly bisimilar, `03-equivalence`). The
ABSTRACT machine is the coalesced machine of `04-abstract`: no timeout
components (deadlines live on the objects), no deferred queue (awaiters
and listeners stay on the settled promise, drained by chosen-element
rules), no config (cadence is a rule parameter).

`alpha` is purely structural and time-independent:

| concrete                        | abstract image                     |
|---------------------------------|------------------------------------|
| `deferred` entry `(a, x)`       | folded back into `a.callbacks`     |
| `taskTimeouts` kind 1 (lease)   | the task's `expiresAt`             |
| `taskTimeouts` kind 0 (retry)   | the task's `retryAt`               |
| `promiseTimeouts`,              | dropped — pure mirrors of          |
| `scheduleTimeouts`              | `timeoutAt` / `nextRunAt`          |
| `config`                        | dropped — the retry constant lives |
|                                 | in R6's re-arm parameter           |
| everything else                 | verbatim                           |

List order in `callbacks`/`listeners` is not preserved (the concrete
machine's LIFO deferred folds back in) and not observable, so abstract
state comparison is up to order.  -/

namespace Abstraction

open Equivalence (eqSet)

deriving instance BEq for AbstractModel.TaskObject

def absPromise (p : ServerModel.PromiseObject) : AbstractModel.PromiseObject :=
  { id := p.id, state := p.state, param := p.param, value := p.value,
    tags := p.tags, timeoutAt := p.timeoutAt, createdAt := p.createdAt,
    settledAt := p.settledAt, callbacks := p.callbacks,
    listeners := p.listeners }

def absTask (timeouts : List ServerModel.TaskTimeout)
    (t : ServerModel.TaskObject) : AbstractModel.TaskObject :=
  { id := t.id, state := t.state, version := t.version, ttl := t.ttl,
    pid := t.pid, resumes := t.resumes,
    expiresAt :=
      (timeouts.find? (fun e => e.id == t.id && e.kind == 1)).map (·.timeout),
    retryAt :=
      (timeouts.find? (fun e => e.id == t.id && e.kind == 0)).map (·.timeout) }

/-- Fold one deferred resume back onto its awaited promise. -/
def addDeferred (ps : List AbstractModel.PromiseObject)
    (d : ServerModel.ResumeReq) : List AbstractModel.PromiseObject :=
  ps.map fun p => if p.id == d.awaited then p.addCallback d.awaiter else p

/-- THE ABSTRACTION. -/
def alpha (s : ServerModel.ServerState) : AbstractModel.ServerState :=
  { promises := s.deferred.foldl addDeferred (s.promises.map absPromise)
    tasks := s.tasks.map (absTask s.taskTimeouts)
    schedules := s.schedules
    outbox := s.outbox }

/-- Promise equality with `callbacks`/`listeners` as sets. -/
def promiseEq (a b : AbstractModel.PromiseObject) : Bool :=
  a.id == b.id && a.state == b.state && a.param == b.param
    && a.value == b.value && a.tags == b.tags
    && a.timeoutAt == b.timeoutAt && a.createdAt == b.createdAt
    && a.settledAt == b.settledAt
    && eqSet a.callbacks b.callbacks && eqSet a.listeners b.listeners

def eqSetBy (eq : α → α → Bool) (a b : List α) : Bool :=
  a.all (fun x => b.any (eq x)) && b.all (fun x => a.any (eq x))

/-- Abstract-state equality up to list order. -/
def absStateEq (a b : AbstractModel.ServerState) : Bool :=
  eqSetBy promiseEq a.promises b.promises
    && eqSet a.tasks b.tasks
    && eqSet a.schedules b.schedules
    && eqSet a.outbox b.outbox

end Abstraction
