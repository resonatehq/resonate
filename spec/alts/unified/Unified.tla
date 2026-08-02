------------------------------- MODULE Unified -------------------------------
(***************************************************************************)
(* THE UNIFIED MODEL                                                        *)
(*                                                                          *)
(* One TLA+ machine derived from four artifacts:                            *)
(*                                                                          *)
(*   - the Lean specification in this repository (the semantics),           *)
(*   - resonate-pg's `base.tla`        (the skeleton: projection, history   *)
(*                                      variables, conformance switches),   *)
(*   - resonate's `Resonate.tla`       (the environment: workers, message   *)
(*                                      delivery, faults, fencing),         *)
(*   - resonate-on-convex's `Resonate.tla`                                  *)
(*                                      (time: unpunctual scheduler,        *)
(*                                       fairness, liveness).               *)
(*                                                                          *)
(* THIS MODEL IS THE SPECIFICATION.  One configuration, no profiles.        *)
(*                                                                          *)
(* An earlier version carried a boolean CONSTANT for every place an         *)
(* implementation diverged, so each implementation had a "profile".  That    *)
(* was self-defeating and the evidence showed it: `MC_convex.cfg` set        *)
(* CallbackExternalGuard = FALSE, which TELLS the model that suspending on   *)
(* an internal promise is expected -- so nothing fired, and the divergence   *)
(* was found only by replaying a real trace against the specification.  A    *)
(* switch that encodes a defect cannot detect that defect.                   *)
(*                                                                          *)
(* So the design is now:                                                    *)
(*                                                                          *)
(*   VALID BEHAVIOUR  is the transition relation.  Every guard the          *)
(*     specification requires is hard-wired.  Where the specification       *)
(*     genuinely PERMITS several behaviours -- the read discipline, whether *)
(*     there is a delivery stage, the order the environment fires its rules *)
(*     -- the model offers all of them as NONDETERMINISM, not as            *)
(*     configuration.  One model covers every valid implementation.         *)
(*                                                                          *)
(*   INVALID BEHAVIOUR  is anything the relation cannot produce.  A         *)
(*     recorded trace that will not replay IS the detection, and it names   *)
(*     the exact event.  See UTrace.tla.                                    *)
(*                                                                          *)
(* The invariants below are properties of the specification.  They hold by  *)
(* construction; their job is to catch errors in THIS MODEL, and to be the  *)
(* vocabulary a mutation experiment reports against (see MUTATIONS.md).     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Ids,          \* promise / task ids
    Addrs,        \* listener addresses
    Workers,      \* SDK worker processes
    MaxTime,      \* time horizon
    MaxVersion,   \* bound on task.version
    Retry,        \* pending-retry TTL
    Ttl,          \* default lease TTL
    TTLs,         \* the lease TTLs a worker may present on acquire.  A task
                  \* carries its OWN ttl -- the specification puts `ttl` on
                  \* TaskRecord and a heartbeat refreshes by it.  The unified
                  \* model lacked this until resonate-on-convex's `crash`
                  \* trace, which acquires with a short lease, could not be
                  \* replayed.  Model configs set TTLs = {Ttl}, so the state
                  \* space is unchanged.

    (***********************************************************************)
    (* There are no behaviour switches.  What remains are SCOPE parameters  *)
    (* and the fault toggle.                                                *)
    (***********************************************************************)
    FaultsOn               \* message loss and worker crashes are enabled

NoAddr   == "-"
NoWorker == "-"

ASSUME NoAddr   \notin Addrs
ASSUME NoWorker \notin Workers

ClientSettable == {"resolved", "rejected", "rejected_canceled"}

\* resonate:target | resonate:timer | resonate:external | (none)
Kinds  == {"target", "timer", "ext", "plain"}

PStates == {"pending", "resolved", "rejected", "rejected_canceled",
            "rejected_timedout"}
TStates == {"pending", "acquired", "suspended", "halted", "fulfilled"}

VARIABLES
    now,          \* wall clock
    promises,     \* [Ids -> promise record]
    tasks,        \* [Ids -> task record]
    callbacks,    \* SUBSET (Ids \X Ids) : <<awaited, awaiter>>
    listeners,    \* SUBSET (Ids \X Addrs)
    resumes,      \* SUBSET (Ids \X Ids) : <<task, awaited>> buffered triggers
    outbox,       \* messages awaiting delivery
    delivered,    \* execute messages handed to the transport (from `resonate`)
    claim,        \* [Workers -> [task, version]] what each worker believes
                  \* it holds                     (from `resonate`)

    (* history variables -- observation channel, from resonate-pg *)
    obs,          \* first non-pending projection ever observable per promise
    badDispatch,  \* ids given a lease or an execute while logically dead
    badHalt,      \* ids halted while task.get would already report fulfilled

    (* THE RESPONSE CHANNEL.  Everything above observes STATE.  These observe
       what a handler actually ANSWERED -- the one channel none of the three
       source models had, and the channel the specification's own stability
       theorem is stated on. *)
    obsRes,          \* first settled promise state ever RETURNED, per promise
    resRegress,      \* promises for which a later response answered `pending`
                     \* after some response had already answered settled
    resUnprojected,  \* promises answered with a record that is not the
                     \* projection at the answering instant
    obsResT,         \* first `fulfilled` ever RETURNED for a task
    resTRegress,     \* tasks answered with a live state after some response
                     \* had already answered `fulfilled`
    resTUnprojected  \* tasks answered with a state that is not the task
                     \* projection at the answering instant

core    == <<promises, tasks, callbacks, listeners, resumes, outbox,
             delivered, claim>>
\* `obs` is deliberately ABSENT from `ghosts` and from every action's
\* UNCHANGED list: it is written by `Next` alone.  An action that also
\* constrained it would be DISABLED exactly when that step newly exposes an
\* expiry -- silently pruning the states this model exists to reach.
ghosts  == <<badDispatch, badHalt>>
\* Unlike `obs`, these ARE written by the actions, so every action must
\* mention them -- either by responding or by leaving them alone.
resVars  == <<obsRes, resRegress, resUnprojected>>       \* promise channel
resTVars == <<obsResT, resTRegress, resTUnprojected>>    \* task channel
vars    == <<now, promises, tasks, callbacks, listeners, resumes, outbox,
             delivered, claim, obs, badDispatch, badHalt,
             obsRes, resRegress, resUnprojected,
             obsResT, resTRegress, resTUnprojected>>

NoPromise == [exists |-> FALSE, state |-> "pending", timeoutAt |-> 0,
              kind |-> "plain"]

NoTask == [exists |-> FALSE, state |-> "pending", version |-> 0,
           pid |-> NoWorker, timerKind |-> "none", timerAt |-> 0, ttl |-> 0]

NoClaim == [task |-> NoAddr, version |-> 0]

ExecMsg(i, v)    == [kind |-> "execute", id |-> i, version |-> v, addr |-> NoAddr]
UnblockMsg(i, a) == [kind |-> "unblock", id |-> i, version |-> 0, addr |-> a]

-----------------------------------------------------------------------------
(***************************************************************************)
(* PROJECTION.  A pending promise past its deadline is OBSERVED as settled  *)
(* before anything persists that fact.  All three implementation models     *)
(* derived this independently; it is the protocol's central concept.        *)
(***************************************************************************)

External(i) == promises[i].kind \in {"target", "timer", "ext"}
Targeted(i) == promises[i].kind = "target"
IsTimer(i)  == promises[i].kind = "timer"

ProjOf(pm, t, i) ==
    IF ~pm[i].exists THEN "none"
    ELSE IF pm[i].state = "pending" /\ pm[i].timeoutAt <= t
         THEN IF pm[i].kind = "timer" THEN "resolved" ELSE "rejected_timedout"
         ELSE pm[i].state

Proj(i) == ProjOf(promises, now, i)

\* "logically pending": the promise a worker may still do work for.
Live(i) == Proj(i) = "pending"

\* Which promises the promise-timeout rule is allowed to fire on.  This is
\* the structural root of the stranded-obligation defect: an obligation may
\* only be recorded where the timeout rule can discharge it.
Armed(i) ==
    /\ promises[i].exists
    \* EXTERNAL-ONLY.  An internal promise's deadline is projection-only;
    \* giving it a durable timeout would settle a promise the protocol says
    \* must merely be projected, and refusing one where an obligation was
    \* accepted would strand that obligation.
    /\ External(i)

\* task.get reports a task `fulfilled` the moment its promise is no longer
\* logically pending, whatever the stored row says (T-01).
ProjTask(i) ==
    IF ~tasks[i].exists THEN "none"
    ELSE IF ~Live(i)   THEN "fulfilled"
    ELSE tasks[i].state

(***************************************************************************)
(* THE RESPONSE CHANNEL.                                                    *)
(*                                                                          *)
(* `RespondP(i, ps)` records that a handler answered, about promise i, a    *)
(* record whose state field is `ps`.  Two things are then observable that   *)
(* no amount of state inspection can see:                                   *)
(*                                                                          *)
(*   resRegress      a response answered `pending` for a promise some       *)
(*                   earlier response had already answered settled.  This   *)
(*                   is settled-promise stability, ON THE WIRE -- the       *)
(*                   client-visible statement, which does not presuppose    *)
(*                   the projection discipline.                             *)
(*   resUnprojected  a response carried a record that is not the projection *)
(*                   at the answering instant -- the discipline itself.     *)
(*                                                                          *)
(* `ps` is evaluated in the PRE-state, so only handlers that do not         *)
(* themselves settle the promise may use this; settling handlers answer     *)
(* with the state they just wrote, which is projection-equal by             *)
(* construction.                                                            *)
(***************************************************************************)
RespondP(i, ps) ==
    /\ obsRes' = [j \in Ids |->
                    IF j = i /\ obsRes[j] = "none" /\ ps \notin {"none", "pending"}
                    THEN ps ELSE obsRes[j]]
    /\ resRegress' = IF obsRes[i] # "none" /\ ps = "pending"
                     THEN resRegress \cup {i} ELSE resRegress
    /\ resUnprojected' = IF ps # "none" /\ ps # Proj(i)
                         THEN resUnprojected \cup {i} ELSE resUnprojected

(***************************************************************************)
(* THE TASK RESPONSE CHANNEL.                                               *)
(*                                                                          *)
(* `ts` is the task state a handler answers with -- for a mutating handler, *)
(* the state it just wrote.  T-01 says a task reads `fulfilled` the moment  *)
(* its promise is no longer logically pending, so answering any live state  *)
(* at such an instant is an unprojected answer, and answering it after some *)
(* response already said `fulfilled` is a regression on the wire.           *)
(*                                                                          *)
(* This is what turns resonate-pg's BUG-4 from a ghost into a wire          *)
(* property: `task.halt` succeeds and answers `halted` at an instant when   *)
(* `task.get` answers `fulfilled`, and halt-on-fulfilled is 409.            *)
(***************************************************************************)
RespondT(i, ts) ==
    /\ obsResT' = [j \in Ids |->
                     IF j = i /\ obsResT[j] = "none" /\ ts = "fulfilled"
                     THEN "fulfilled" ELSE obsResT[j]]
    /\ resTRegress' = IF obsResT[i] = "fulfilled" /\ ts \notin {"none", "fulfilled"}
                      THEN resTRegress \cup {i} ELSE resTRegress
    /\ resTUnprojected' = IF ts \notin {"none", "fulfilled"} /\ ~Live(i)
                          THEN resTUnprojected \cup {i} ELSE resTUnprojected

NoResponse ==
    /\ obsRes' = obsRes
    /\ resRegress' = resRegress
    /\ resUnprojected' = resUnprojected

-----------------------------------------------------------------------------
(***************************************************************************)
(* Outbox: a keyed upsert, never an append.  Re-emission is idempotent,     *)
(* which is what makes at-least-once dispatch sound.                        *)
(***************************************************************************)

PutExec(ob, i, v) ==
    {m \in ob : ~(m.kind = "execute" /\ m.id = i)} \cup {ExecMsg(i, v)}

DropExec(ob, S) == {m \in ob : ~(m.kind = "execute" /\ m.id \in S)}

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE SETTLEMENT CASCADE.                                                  *)
(*                                                                          *)
(* Settle promise i, fulfil its task, notify its listeners, wake its        *)
(* awaiters.  ResumeLivenessGuard is the specification's TIMEOUT ALWAYS     *)
(* WINS rule at this site: an awaiter whose OWN promise is already          *)
(* logically dead is dead weight and must not be woken or dispatched.       *)
(*                                                                          *)
(* All three implementation models encode this cascade WITHOUT that guard,  *)
(* and only one of the three instrumented for the consequence.  Here it is  *)
(* a switch and the consequence is always recorded in `badDispatch`.        *)
(***************************************************************************)

Awaiters(i)     == {j \in Ids : <<i, j>> \in callbacks}
LAddrs(i)       == {a \in Addrs : <<i, a>> \in listeners}

\* awaiters eligible to be woken / to buffer the trigger
\* TIMEOUT ALWAYS WINS: an awaiter past its own deadline is dead weight.
Eligible(j)     == Live(j)

SuspAwaiters(i) == {j \in Awaiters(i) : tasks[j].exists
                                     /\ tasks[j].state = "suspended"
                                     /\ Eligible(j)}

BufAwaiters(i)  == {j \in Awaiters(i) : tasks[j].exists
                                     /\ tasks[j].state \in {"pending", "acquired", "halted"}
                                     /\ Eligible(j)}

\* the awaiters actually resumed that were already logically dead -- empty
\* whenever ResumeLivenessGuard holds
DeadResumed(i)  == {j \in SuspAwaiters(i) : ~Live(j)}

CascadeTasks(i) ==
    [j \in Ids |->
        IF j = i /\ tasks[j].exists
          THEN [tasks[j] EXCEPT !.state = "fulfilled", !.pid = NoWorker,
                                !.timerKind = "none", !.timerAt = 0, !.ttl = 0]
        ELSE IF j \in SuspAwaiters(i)
          THEN [tasks[j] EXCEPT !.state = "pending", !.pid = NoWorker,
                                !.timerKind = "retry", !.timerAt = now + Retry,
                                !.ttl = 0]
        ELSE tasks[j]]

CascadeResumes(i) ==
    {r \in resumes : r[1] # i /\ r[1] \notin SuspAwaiters(i)}
      \cup {<<j, i>> : j \in (SuspAwaiters(i) \cup BufAwaiters(i))}

\* drop registrations ON the settling promise, and registrations the settling
\* promise itself holds elsewhere (the specification's settlement scrub)
CascadeCallbacks(i) == {c \in callbacks : c[1] # i /\ c[2] # i}
CascadeListeners(i) == {l \in listeners : l[1] # i}

CascadeOutbox(i) ==
    LET resumeIds == {j \in SuspAwaiters(i) : Targeted(j)}
        cleaned   == DropExec(outbox, {i} \cup resumeIds)
    IN  cleaned
          \cup {UnblockMsg(i, a) : a \in LAddrs(i)}
          \cup {ExecMsg(j, tasks[j].version) : j \in resumeIds}

DoSettle(i, st) ==
    /\ promises'  = [promises EXCEPT ![i].state = st]
    /\ tasks'     = CascadeTasks(i)
    /\ resumes'   = CascadeResumes(i)
    /\ callbacks' = CascadeCallbacks(i)
    /\ listeners' = CascadeListeners(i)
    /\ outbox'    = CascadeOutbox(i)
    \* every claim on the now-fulfilled task is void
    /\ claim'     = [w \in Workers |->
                       IF claim[w].task = i THEN NoClaim ELSE claim[w]]
    /\ badDispatch' = badDispatch \cup DeadResumed(i)
    /\ UNCHANGED <<now, delivered, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* PROMISE HANDLERS                                                         *)
(***************************************************************************)

\* P-02 promise.create
PromiseCreate(i, toat, k) ==
    /\ ~promises[i].exists
    /\ LET rec == [exists |-> TRUE, state |-> "pending",
                   timeoutAt |-> toat, kind |-> k]
       IN IF toat > now
          THEN /\ promises' = [promises EXCEPT ![i] = rec]
               /\ IF k = "target"
                  THEN /\ tasks' = [tasks EXCEPT ![i] =
                                       [exists |-> TRUE, state |-> "pending",
                                        version |-> 0, pid |-> NoWorker,
                                        timerKind |-> "retry",
                                        timerAt |-> now + Retry, ttl |-> 0]]
                       /\ outbox' = PutExec(outbox, i, 0)
                  ELSE UNCHANGED <<tasks, outbox>>
          ELSE \* created already settled
               /\ promises' = [promises EXCEPT ![i] =
                     [rec EXCEPT !.state = IF k = "timer" THEN "resolved"
                                                          ELSE "rejected_timedout"]]
               /\ IF k = "target"
                  THEN tasks' = [tasks EXCEPT ![i] =
                                    [exists |-> TRUE, state |-> "fulfilled",
                                     version |-> 0, pid |-> NoWorker,
                                     timerKind |-> "none", timerAt |-> 0,
                                     ttl |-> 0]]
                  ELSE UNCHANGED tasks
               /\ UNCHANGED outbox
    /\ UNCHANGED <<now, callbacks, listeners, resumes, delivered, claim,
                   badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* P-03 promise.settle
PromiseSettle(i, st) ==
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt > now
    /\ DoSettle(i, st)

\* P-04 promise.register_callback
RegisterCallback(aw, ar) ==
    /\ aw # ar                                  \* 400 self-await
    /\ promises[aw].exists                      \* 404
    /\ promises[ar].exists                      \* 422
    /\ Targeted(ar)                             \* 422 awaiter must be routable
    /\ External(aw)                             \* 422 external-only waiters
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ promises[ar].state = "pending" /\ promises[ar].timeoutAt > now
    /\ <<aw, ar>> \notin callbacks
    /\ callbacks' = callbacks \cup {<<aw, ar>>}
    /\ UNCHANGED <<now, promises, tasks, listeners, resumes, outbox,
                   delivered, claim, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* P-05 promise.register_listener
RegisterListener(aw, ad) ==
    /\ promises[aw].exists                      \* 404
    /\ External(aw)                             \* 422 external-only waiters
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners
    /\ listeners' = listeners \cup {<<aw, ad>>}
    /\ UNCHANGED <<now, promises, tasks, callbacks, resumes, outbox,
                   delivered, claim, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* TASK HANDLERS                                                            *)
(***************************************************************************)

\* T-02 task.create, re-claim branch: an existing pending task is claimed
\* directly, without going through the outbox.
TaskClaim(i, w, ttl) ==
    /\ claim[w].task = NoAddr
    /\ promises[i].exists
    /\ Targeted(i)
    /\ tasks[i].exists
    /\ tasks[i].state = "pending"
    /\ Live(i)                                  \* 409 -- spec T-02
    /\ tasks[i].version < MaxVersion
    /\ tasks'   = [tasks EXCEPT ![i].state = "acquired",
                                ![i].version = tasks[i].version + 1,
                                ![i].pid = w,
                                ![i].timerKind = "lease",
                                ![i].timerAt = now + ttl, ![i].ttl = ttl]
    /\ resumes' = {r \in resumes : r[1] # i}
    /\ claim'   = [claim EXCEPT ![w] = [task |-> i,
                                        version |-> tasks[i].version + 1]]
    /\ badDispatch' = IF Live(i) THEN badDispatch ELSE badDispatch \cup {i}
    \* T-02's response.  The specification serves `(p.project now).toRecord`;
    \* resonate.sql serves `_promise_json_raw(p)`, deliberately unprojected --
    \* that is resonate-pg BUG-2's response half, and it is NOT what the
    \* specification does.
    /\ RespondP(i, Proj(i))
    \* T-02 also answers a task record: the task it just acquired.
    /\ RespondT(i, "acquired")
    /\ UNCHANGED <<now, promises, callbacks, listeners, outbox, delivered,
                   badHalt>>

\* T-03 task.acquire.  The worker presents a message it was handed; the
\* version CAS is the fencing token.
TaskAcquire(w, m, ttl) ==
    /\ claim[w].task = NoAddr
    \* NONDETERMINISM, not configuration: an implementation may or may not
    \* have a delivery stage between the outbox and acquire.  Both are
    \* valid, so the model permits either source.
    /\ m \in outbox \cup delivered
    /\ m.kind = "execute"
    /\ LET i == m.id IN
       /\ tasks[i].exists
       /\ tasks[i].state = "pending"
       /\ tasks[i].version = m.version
       /\ tasks[i].version < MaxVersion
       /\ promises[i].exists
       /\ Live(i)                               \* 409 -- spec T-03, all impls
       /\ tasks'   = [tasks EXCEPT ![i].state = "acquired",
                                   ![i].version = m.version + 1,
                                   ![i].pid = w,
                                   ![i].timerKind = "lease",
                                   ![i].timerAt = now + ttl, ![i].ttl = ttl]
       /\ resumes' = {r \in resumes : r[1] # i}
       /\ claim'   = [claim EXCEPT ![w] = [task |-> i, version |-> m.version + 1]]
       /\ delivered' = delivered \ {m}
    \* T-03 answers the task it just acquired.
    /\ RespondT(m.id, "acquired")
    /\ UNCHANGED <<now, promises, callbacks, listeners, outbox,
                   badDispatch, badHalt, obsRes, resRegress, resUnprojected>>

\* T-06 task.suspend
TaskSuspend(i, S) ==
    /\ S # {}                                   \* 400
    /\ i \notin S                               \* 400 self-await
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ promises[i].exists
    /\ Live(i)                                  \* 409
    /\ \A j \in S : promises[j].exists           \* 422
    /\ \A j \in S : External(j)                  \* 422 external-only waiters
    \* T-06 answers the task: still acquired on the 300 re-check path,
    \* parked on the parking path.
    /\ RespondT(i, IF \E j \in S : ~Live(j) THEN "acquired" ELSE "suspended")
    /\ IF \E j \in S : ~Live(j)
       THEN \* 300: re-check instead of parking
            /\ resumes' = {r \in resumes : r[1] # i}
            /\ UNCHANGED <<tasks, callbacks, claim>>
       ELSE /\ callbacks' = callbacks \cup {<<j, i>> : j \in S}
            /\ resumes'   = {r \in resumes : r[1] # i}
            /\ tasks'     = [tasks EXCEPT ![i].state = "suspended",
                                          ![i].pid = NoWorker,
                                          ![i].timerKind = "none",
                                          ![i].timerAt = 0, ![i].ttl = 0]
            /\ claim'     = [w \in Workers |->
                               IF claim[w].task = i THEN NoClaim ELSE claim[w]]
    /\ UNCHANGED <<now, promises, listeners, outbox, delivered,
                   badDispatch, badHalt, obsRes, resRegress, resUnprojected>>

\* T-07 task.fulfill
TaskFulfill(i, st) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ promises[i].exists
    /\ Live(i)                                  \* 409 -- the fence guard
    /\ DoSettle(i, st)

\* T-08 task.release
TaskRelease(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ promises[i].exists
    /\ Live(i)                                  \* 409
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending", ![i].pid = NoWorker,
                               ![i].timerKind = "retry", ![i].timerAt = now + Retry,
                               ![i].ttl = 0]
    /\ outbox' = IF Targeted(i) THEN PutExec(outbox, i, tasks[i].version)
                                ELSE outbox
    /\ claim'  = [w \in Workers |->
                    IF claim[w].task = i THEN NoClaim ELSE claim[w]]
    \* T-08 answers the task returned to pending.
    /\ RespondT(i, "pending")
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, delivered,
                   badDispatch, badHalt, obsRes, resRegress, resUnprojected>>

\* T-05 task.heartbeat
TaskHeartbeat(w) ==
    /\ claim[w].task # NoAddr
    /\ LET i == claim[w].task IN
       /\ tasks[i].exists
       /\ tasks[i].state = "acquired"
       /\ tasks[i].version = claim[w].version
       /\ Live(i)                               \* spec T-05
       /\ tasks' = [tasks EXCEPT ![i].timerKind = "lease",
                                 ![i].timerAt = now + tasks[i].ttl]
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, outbox,
                   delivered, claim, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* T-09 task.halt
TaskHalt(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state \notin {"fulfilled", "halted"}
    /\ Live(i)                                  \* spec T-09
    /\ tasks' = [tasks EXCEPT ![i].state = "halted", ![i].pid = NoWorker,
                              ![i].timerKind = "none", ![i].timerAt = 0,
                              ![i].ttl = 0]
    /\ claim' = [w \in Workers |->
                   IF claim[w].task = i THEN NoClaim ELSE claim[w]]
    \* task.get already reports this task `fulfilled`; halt-on-fulfilled is 409
    /\ badHalt' = IF Live(i) THEN badHalt ELSE badHalt \cup {i}
    \* T-09 answers 200 with the task now `halted`.  If the promise is
    \* already logically dead, task.get answers `fulfilled` at this same
    \* instant and halt-on-fulfilled is 409: the wire contradicts itself.
    /\ RespondT(i, "halted")
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, outbox,
                   delivered, badDispatch, obsRes, resRegress, resUnprojected>>

\* T-10 task.continue
TaskContinue(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "halted"
    /\ promises[i].exists
    /\ Live(i)                                  \* spec T-10
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending",
                               ![i].timerKind = "retry", ![i].timerAt = now + Retry]
    /\ outbox' = IF Targeted(i) THEN PutExec(outbox, i, tasks[i].version)
                                ELSE outbox
    /\ badDispatch' = IF Live(i) \/ ~Targeted(i) THEN badDispatch
                                                 ELSE badDispatch \cup {i}
    \* T-10 answers the task returned to pending.
    /\ RespondT(i, "pending")
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, delivered,
                   claim, badHalt, obsRes, resRegress, resUnprojected>>

\* P-01 promise.get -- a pure observation.  The reference endpoint: it always
\* serves the projection, so it is what every other answer is measured against.
PromiseGet(i) ==
    /\ promises[i].exists
    /\ RespondP(i, Proj(i))
    /\ UNCHANGED resTVars
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes,
                   outbox, delivered, claim, badDispatch, badHalt>>

\* T-01 task.get -- a pure observation, and the reference endpoint for the
\* task channel: it always answers the projection, so every other answer is
\* measured against it.
TaskGet(i) ==
    /\ tasks[i].exists
    /\ RespondT(i, ProjTask(i))
    /\ NoResponse
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes,
                   outbox, delivered, claim, badDispatch, badHalt>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* INTERNAL RULES (R1, R4, R5, R6 of the specification).                    *)
(*                                                                          *)
(* Each is enabled from its deadline onwards and may fire arbitrarily late  *)
(* or spuriously -- the unpunctual scheduler of the Convex model.  A stale  *)
(* firing is harmless: the rule re-checks its own due time.                 *)
(***************************************************************************)

(***************************************************************************)
(* MATERIALISATION vs. ARMING -- two different things, and conflating them  *)
(* was a real defect in this model until resonate's `timer_gate` trace      *)
(* could not be replayed.                                                   *)
(*                                                                          *)
(* `TouchPromise` is the specification's fact P: a pending promise past its *)
(* deadline IS settled, and touching it writes that down.  The `-m` machine *)
(* touches ANY promise a request names, armed or not.                       *)
(*                                                                          *)
(* `Armed` gates only the UNATTENDED rule R1 -- which promises the          *)
(* environment will settle with nobody asking.  A promise that is not armed *)
(* can still be settled by a read that names it, which is precisely why     *)
(* resonate's BUG-1 is so sharp: the API reports the promise timed out, and *)
(* the read is what unblocks the awaiter.                                   *)
(***************************************************************************)
TouchPromise(i) ==
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt <= now
    /\ DoSettle(i, IF IsTimer(i) THEN "resolved" ELSE "rejected_timedout")

\* R1 -- the unattended path.  `Armed` decides which promises have one.
OnPromiseTimeout(i) == Armed(i) /\ TouchPromise(i)

\* P-01 / T-01 on an implementation that MATERIALISES rather than projects.
\* The specification proves the two read disciplines indistinguishable (the
\* square), so this is latitude, not a defect: resonate's `try_timeout` and
\* convex's `tryEagerTimeout` materialise; resonate-pg projects and writes
\* nothing.
\* NONDETERMINISM, not configuration: the specification's twins.  A read may
\* materialise the timeout it observes (`-m`) or merely project it (`-p`).
\* Both are valid -- the square theorem proves them indistinguishable -- so
\* the model simply permits the materialising step at any time.
ClientTouch(i) == TouchPromise(i)

\* R6 -- dispatch
OnTaskRetryTimeout(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "pending"
    /\ tasks[i].timerKind = "retry"
    /\ tasks[i].timerAt <= now
    /\ Live(i)                    \* no new work for the dead (R6)
    /\ tasks'  = [tasks EXCEPT ![i].timerAt = now + Retry]
    /\ outbox' = IF promises[i].exists /\ Targeted(i)
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ badDispatch' = IF promises[i].exists /\ Targeted(i) /\ ~Live(i)
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, delivered,
                   claim, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* R5 -- lease expiry.  The version is NOT bumped: the fence token survives
\* the lease, in the specification and in all three implementations.
OnTaskLeaseTimeout(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ tasks[i].timerKind = "lease"
    /\ tasks[i].timerAt <= now
    /\ Live(i)                    \* no new work for the dead (R6)
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending", ![i].pid = NoWorker,
                               ![i].timerKind = "retry", ![i].timerAt = now + Retry,
                               ![i].ttl = 0]
    /\ outbox' = IF promises[i].exists /\ Targeted(i)
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ badDispatch' = IF promises[i].exists /\ Targeted(i) /\ ~Live(i)
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, delivered,
                   claim, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ENVIRONMENT: delivery, faults, time.                                 *)
(***************************************************************************)

\* enqueue -> deliver -> acquire.  The dispatcher is fire-and-forget: the
\* row is gone before the send is attempted.
Deliver(m) ==
    /\ m \in outbox
    /\ outbox' = outbox \ {m}
    /\ delivered' = IF m.kind = "execute" THEN delivered \cup {m} ELSE delivered
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes,
                   claim, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* fault: the message is lost between the delete and the send
DropMsg(m) ==
    /\ FaultsOn
    /\ m \in outbox
    /\ outbox' = outbox \ {m}
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes,
                   delivered, claim, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

\* fault: a worker dies holding a claim.  Server state is untouched; the
\* lease timer is what eventually reclaims the task.
WorkerCrash(w) ==
    /\ FaultsOn
    /\ claim[w].task # NoAddr
    /\ claim' = [claim EXCEPT ![w] = NoClaim]
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes,
                   outbox, delivered, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<core, badDispatch, badHalt, obsRes, resRegress, resUnprojected, resTVars>>

-----------------------------------------------------------------------------
Init ==
    /\ now         = 0
    /\ promises    = [i \in Ids |-> NoPromise]
    /\ tasks       = [i \in Ids |-> NoTask]
    /\ callbacks   = {}
    /\ listeners   = {}
    /\ resumes     = {}
    /\ outbox      = {}
    /\ delivered   = {}
    /\ claim       = [w \in Workers |-> NoClaim]
    /\ obs         = [i \in Ids |-> "none"]
    /\ badDispatch = {}
    /\ badHalt     = {}
    /\ obsRes      = [i \in Ids |-> "none"]
    /\ resRegress  = {}
    /\ resUnprojected = {}
    /\ obsResT     = [i \in Ids |-> "none"]
    /\ resTRegress = {}
    /\ resTUnprojected = {}

(***************************************************************************)
(* Each action is a NAMED top-level operator, and `Next` is their bare      *)
(* disjunction.  That is not cosmetic: with `Next == Step /\ ObsUpdate`     *)
(* TLC attributed every step of every counterexample to the line of the     *)
(* conjunction, so error traces read "line 736" throughout and said nothing *)
(* about WHICH action fired.  Named disjuncts make TLC print the action's   *)
(* name, which is what turns a counterexample into a scenario.              *)
(***************************************************************************)

\* `obs` records, for each promise, the first non-pending state any client
\* could have read through promise.get.  History variable only.
ObsUpdate ==
    obs' = [i \in Ids |->
              IF obs[i] = "none"
                 /\ ProjOf(promises', now', i) \notin {"none", "pending"}
              THEN ProjOf(promises', now', i)
              ELSE obs[i]]

APromiseCreate    == (\E i \in Ids, toat \in 1..MaxTime, k \in Kinds : PromiseCreate(i, toat, k)) /\ ObsUpdate
APromiseSettle    == (\E i \in Ids, st \in ClientSettable : PromiseSettle(i, st)) /\ ObsUpdate
ARegisterCallback == (\E aw, ar \in Ids : RegisterCallback(aw, ar)) /\ ObsUpdate
ARegisterListener == (\E aw \in Ids, ad \in Addrs : RegisterListener(aw, ad)) /\ ObsUpdate
APromiseGet       == (\E i \in Ids : PromiseGet(i)) /\ ObsUpdate
ATaskGet          == (\E i \in Ids : TaskGet(i)) /\ ObsUpdate
ATaskCreate       == (\E i \in Ids, w \in Workers, t \in TTLs : TaskClaim(i, w, t)) /\ ObsUpdate
ATaskAcquire      == (\E w \in Workers, m \in delivered, t \in TTLs : TaskAcquire(w, m, t)) /\ ObsUpdate
ATaskSuspend      == (\E i \in Ids, S \in SUBSET Ids : TaskSuspend(i, S)) /\ ObsUpdate
ATaskFulfill      == (\E i \in Ids, st \in ClientSettable : TaskFulfill(i, st)) /\ ObsUpdate
ATaskRelease      == (\E i \in Ids : TaskRelease(i)) /\ ObsUpdate
ATaskHeartbeat    == (\E w \in Workers : TaskHeartbeat(w)) /\ ObsUpdate
ATaskHalt         == (\E i \in Ids : TaskHalt(i)) /\ ObsUpdate
ATaskContinue     == (\E i \in Ids : TaskContinue(i)) /\ ObsUpdate
AOnPromiseTimeout == (\E i \in Ids : OnPromiseTimeout(i)) /\ ObsUpdate
AClientTouch      == (\E i \in Ids : ClientTouch(i)) /\ ObsUpdate
AOnTaskRetry      == (\E i \in Ids : OnTaskRetryTimeout(i)) /\ ObsUpdate
AOnTaskLease      == (\E i \in Ids : OnTaskLeaseTimeout(i)) /\ ObsUpdate
ADeliver          == (\E m \in outbox : Deliver(m)) /\ ObsUpdate
ADropMsg          == (\E m \in outbox : DropMsg(m)) /\ ObsUpdate
AWorkerCrash      == (\E w \in Workers : WorkerCrash(w)) /\ ObsUpdate
ATick             == Tick /\ ObsUpdate

Next ==
    \/ APromiseCreate  \/ APromiseSettle  \/ ARegisterCallback \/ ARegisterListener
    \/ APromiseGet     \/ ATaskGet
    \/ ATaskCreate     \/ ATaskAcquire    \/ ATaskSuspend      \/ ATaskFulfill
    \/ ATaskRelease    \/ ATaskHeartbeat  \/ ATaskHalt         \/ ATaskContinue
    \/ AOnPromiseTimeout \/ AClientTouch  \/ AOnTaskRetry      \/ AOnTaskLease
    \/ ADeliver        \/ ADropMsg        \/ AWorkerCrash      \/ ATick

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Productive sub-actions, for fairness.  Weak fairness on an action whose  *)
(* effect is stuttering is vacuous, so liveness is attached to the guarded  *)
(* forms: "a rule that actually has work to do eventually does it".         *)
(***************************************************************************)

PromiseTimeoutFires(i) == OnPromiseTimeout(i) /\ ObsUpdate
RetryFires(i)          == OnTaskRetryTimeout(i) /\ ObsUpdate
LeaseFires(i)          == OnTaskLeaseTimeout(i) /\ ObsUpdate
DeliverFires           == (\E m \in outbox : Deliver(m)) /\ ObsUpdate
TickFires              == Tick /\ ObsUpdate

FairSpec ==
    /\ Spec
    /\ WF_vars(TickFires)
    /\ WF_vars(DeliverFires)
    /\ \A i \in Ids : /\ WF_vars(PromiseTimeoutFires(i))
                      /\ WF_vars(RetryFires(i))
                      /\ WF_vars(LeaseFires(i))

-----------------------------------------------------------------------------
(***************************************************************************)
(* PROPERTIES.                                                              *)
(*                                                                          *)
(* The union of four vocabularies, in four classes.  The class a property   *)
(* belongs to says what kind of evidence it is.                             *)
(***************************************************************************)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ \A i \in Ids : promises[i].state \in PStates
    /\ \A i \in Ids : promises[i].kind  \in Kinds
    /\ \A i \in Ids : tasks[i].state    \in TStates
    /\ \A i \in Ids : tasks[i].version  \in 0..MaxVersion
    /\ \A i \in Ids : tasks[i].timerKind \in {"none", "retry", "lease"}
    /\ callbacks \subseteq (Ids \X Ids)
    /\ listeners \subseteq (Ids \X Addrs)
    /\ resumes   \subseteq (Ids \X Ids)
    /\ \A w \in Workers : claim[w].task \in Ids \cup {NoAddr}

(*-------------------------------------------------------------------------
  CLASS 1 -- STRUCTURAL WELL-FORMEDNESS.  From the specification's own
  invariant set.  These fail fast and localize well.
 -------------------------------------------------------------------------*)

TaskHasPromise ==
    \A i \in Ids : tasks[i].exists => promises[i].exists

TaskHasAtMostOneTimer ==
    \A i \in Ids : tasks[i].exists =>
        /\ (tasks[i].state = "pending"  => tasks[i].timerKind = "retry")
        /\ (tasks[i].state = "acquired" => tasks[i].timerKind = "lease")
        /\ (tasks[i].state \in {"suspended", "halted", "fulfilled"}
              => tasks[i].timerKind = "none")

NonAcquiredTaskHasNoPid ==
    \A i \in Ids : tasks[i].state # "acquired" => tasks[i].pid = NoWorker

CallbackNotSelfReferential ==
    \A c \in callbacks : c[1] # c[2]

\* A suspended task that is still logically ALIVE must have something that
\* can wake it.  The liveness qualifier is not a weakening for convenience:
\* under TIMEOUT ALWAYS WINS a settling promise scrubs its callback rows
\* while deliberately NOT resuming an awaiter that is itself past its
\* deadline.  Such a task is dead weight owned by the promise-timeout rule,
\* which will fulfil it -- so it legitimately sits suspended with no
\* callback.  Stating this without the qualifier makes the SPECIFICATION
\* profile fail, at depth 7.
SuspendedTaskHasCallback ==
    \A i \in Ids :
        (tasks[i].state = "suspended" /\ Live(i))
            => \E j \in Ids : <<j, i>> \in callbacks

SettledPromiseHasNoSubscriptions ==
    \A i \in Ids :
        (promises[i].exists /\ promises[i].state # "pending")
            => /\ \A c \in callbacks : c[1] # i
               /\ \A l \in listeners : l[1] # i

(*-------------------------------------------------------------------------
  THE MERGED STRUCTURAL PROPERTY.  An obligation may only be recorded
  against a promise whose deadline the machine will actually fire.  This is
  the specification's `NonExternalPromiseHasNoTimeout` and its external-only
  waiter rule, stated as one structural fact -- and it is the root of the
  stranded-obligation defect in two of the three implementations.
 -------------------------------------------------------------------------*)
ObligationsAreDischargeable ==
    /\ \A c \in callbacks : Armed(c[1])
    /\ \A l \in listeners : Armed(l[1])

(*-------------------------------------------------------------------------
  THE OTHER HALF OF ARMING.  `ObligationsAreDischargeable` catches
  UNDER-arming -- an obligation recorded where no timeout will fire.  It
  says nothing about OVER-arming, because an over-armed machine discharges
  every obligation it accepts.

  Arming is external-only: an internal promise's deadline is
  projection-only by design, and giving it a durable timeout makes the
  server settle a promise the protocol says it must merely project.  This
  is the specification's `NonExternalPromiseHasNoTimeout`.

  Note what this property can and cannot do.  Under `ArmPolicy =
  "external"` it is TRUE BY THE DEFINITION of `Armed`, so it cannot fail in
  the specification profile and is not evidence about it.  Its entire job is
  to fail for the other two settings -- and since `target` is a subset of
  `external` it fails only for `"all"`.  That is the intended division of
  labour with the property above, not an oversight.
 -------------------------------------------------------------------------*)
ArmingIsExternalOnly ==
    \A i \in Ids : Armed(i) => External(i)

(*-------------------------------------------------------------------------
  CLASS 2 -- NO NEW WORK FOR THE DEAD.  TIMEOUT ALWAYS WINS, at every site.
 -------------------------------------------------------------------------*)

NoDeadDispatch == badDispatch = {}
NoHaltOnDead   == badHalt = {}

(*-------------------------------------------------------------------------
  CLASS 3 -- OBSERVATION.  What a client can actually see.
 -------------------------------------------------------------------------*)

\* Once a client has read a promise as settled, it never reads anything else.
Stickiness == \A i \in Ids : obs[i] # "none" => Proj(i) = obs[i]

\* The environment has nothing left to do at this instant.
DriverIdle ==
    /\ \A i \in Ids : ~(Armed(i) /\ promises[i].state = "pending"
                        /\ promises[i].timeoutAt <= now)
    /\ \A i \in Ids : ~(tasks[i].exists
                        /\ tasks[i].state \in {"pending", "acquired"}
                        /\ tasks[i].timerAt <= now)

Quiesced == now = MaxTime /\ DriverIdle

NoStrandedListener ==
    Quiesced => \A l \in listeners : Live(l[1])

NoStrandedTask ==
    Quiesced => \A i \in Ids :
        tasks[i].state = "suspended" =>
            \E j \in Ids : <<j, i>> \in callbacks /\ Live(j)

TaskPromiseCoherence ==
    DriverIdle => \A i \in Ids :
        (promises[i].exists /\ promises[i].state # "pending" /\ tasks[i].exists)
            => tasks[i].state = "fulfilled"

(*-------------------------------------------------------------------------
  CLASS 3b -- THE WIRE.  Stated on what handlers ANSWERED, not on state.
  These are the properties no state-only model can express, and the ones
  the specification's own stability theorem is about.
 -------------------------------------------------------------------------*)

\* Settled-promise stability on the wire: once any response has answered
\* settled for a promise, no later response may answer `pending` for it.
ResponsesNeverRegress == resRegress = {}

\* The projection discipline: every response carries the projection at the
\* answering instant.  Strictly stronger than the above, and the form the
\* specification states.
ResponsesAreProjected == resUnprojected = {}

\* The same two properties on the TASK channel.  `TaskResponsesNeverRegress`
\* is resonate-pg's BUG-4 as a wire property: task.get answers `fulfilled`,
\* then task.halt answers `halted` for the same task.
TaskResponsesNeverRegress == resTRegress = {}
TaskResponsesAreProjected == resTUnprojected = {}

(*-------------------------------------------------------------------------
  CLASS 4 -- FENCING.  At-most-once execution.  Only expressible because
  the model carries worker-side state; no other artifact in this family
  states it.
 -------------------------------------------------------------------------*)

ValidClaim(w) ==
    /\ claim[w].task \in Ids
    /\ tasks[claim[w].task].state = "acquired"
    /\ tasks[claim[w].task].version = claim[w].version

AtMostOneValidClaim ==
    \A i \in Ids :
        Cardinality({w \in Workers : ValidClaim(w) /\ claim[w].task = i}) <= 1

\* The server never dispatches a version it has not issued.
OutboxNeverAhead ==
    /\ \A m \in outbox   : m.kind = "execute" => m.version <= tasks[m.id].version
    /\ \A m \in delivered: m.version <= tasks[m.id].version

(*-------------------------------------------------------------------------
  LIVENESS.  No stranded workflow: every task that exists eventually
  reaches its terminal state.
 -------------------------------------------------------------------------*)

\* `halted` is terminal by design -- T-09 takes a task out of circulation --
\* so it counts as converged; only `continue` puts it back in play.
TasksConverge ==
    \A i \in Ids : tasks[i].exists
                     ~> (tasks[i].state \in {"fulfilled", "halted"})

-----------------------------------------------------------------------------

Structural ==
    /\ TypeOK
    /\ TaskHasPromise
    /\ TaskHasAtMostOneTimer
    /\ NonAcquiredTaskHasNoPid
    /\ CallbackNotSelfReferential
    /\ SuspendedTaskHasCallback
    /\ SettledPromiseHasNoSubscriptions

Safety ==
    /\ Structural
    /\ ObligationsAreDischargeable
    /\ ArmingIsExternalOnly
    /\ NoDeadDispatch
    /\ NoHaltOnDead
    /\ Stickiness
    /\ NoStrandedListener
    /\ NoStrandedTask
    /\ TaskPromiseCoherence
    /\ AtMostOneValidClaim
    /\ OutboxNeverAhead
    /\ ResponsesNeverRegress
    /\ ResponsesAreProjected
    /\ TaskResponsesNeverRegress
    /\ TaskResponsesAreProjected

=============================================================================
