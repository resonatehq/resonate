------------------------------- MODULE UTrace -------------------------------
(***************************************************************************)
(* ONE TRACE VALIDATOR FOR EVERY IMPLEMENTATION.                            *)
(*                                                                          *)
(* Drives TLC through a recorded NDJSON trace and checks that the unified   *)
(* model can reproduce every observed transition, and that the model's      *)
(* state agrees with the state the real system actually held after each     *)
(* one.                                                                     *)
(*                                                                          *)
(* The trace is in the CANONICAL shape produced by `adapters/from_*.py`.    *)
(* Each adapter normalises one implementation's harness output; the         *)
(* validator itself is implementation-agnostic.  Adding an implementation   *)
(* means writing an adapter, not touching this file.                        *)
(*                                                                          *)
(* WHICH PROFILE TO REPLAY UNDER.  Two useful runs per trace:               *)
(*                                                                          *)
(*   under that implementation's own profile  - does the model describe the *)
(*     implementation?  A failure means the MODEL is wrong (or the code has *)
(*     changed).                                                            *)
(*   under the SPECIFICATION profile          - was this execution          *)
(*     conformant?  A failure means the TRACE exhibits a divergence.  This  *)
(*     is a conformance test driven by real executions rather than by the   *)
(*     model's own search.                                                  *)
(*                                                                          *)
(* NO ACTION ARGUMENTS ARE INVENTED.  The harnesses record none, so each    *)
(* step existentially quantifies over the candidate ids and lets the        *)
(* post-state agreement pick the witness -- the technique resonate-pg's     *)
(* Trace.tla uses.  If no witness reproduces the recorded state, the step   *)
(* is unsatisfiable and TLC stops exactly there.                            *)
(***************************************************************************)
EXTENDS Unified, Sequences, Naturals, FiniteSets, TLC, Json

CONSTANT TraceFile

RawLog   == ndJsonDeserialize(TraceFile)
TraceLog == SelectSeq(RawLog, LAMBDA e : e.tag = "trace")
Config   == (CHOOSE e \in {RawLog[i] : i \in DOMAIN RawLog} : e.tag = "config").config

VARIABLE l                  \* cursor: currently validating TraceLog[l]
tvars == <<vars, l>>

logline == TraceLog[l].event

ToSet(sq) == { sq[i] : i \in DOMAIN sq }

(***************************************************************************)
(* WHAT THIS TRACE CONSTRAINS.                                              *)
(*                                                                          *)
(* No harness records everything the unified model carries -- none records  *)
(* responses, and only `resonate` records worker claims and the delivery    *)
(* stage.  The adapter declares the components its implementation actually  *)
(* observes, and the validator checks exactly those.  Stating the set is    *)
(* the point: a validator that silently compares a subset is indistinguish- *)
(* able from one that compares everything, and worth much less.             *)
(***************************************************************************)
Observed(c) == c \in ToSet(Config.observed)

RecordedIds == DOMAIN logline.state.promises
RecordedTaskIds == DOMAIN logline.state.tasks

PromiseOK ==
    \A i \in RecordedIds :
        LET r == logline.state.promises[i] IN
        /\ promises'[i].exists
        /\ promises'[i].state     = r.state
        /\ promises'[i].timeoutAt = r.timeoutAt
        /\ promises'[i].kind      = r.kind

\* `pid` is not compared: only `resonate` tracks task ownership.
\*
\* `timerAt` is compared ONLY when the task actually has a timer.  This is
\* not leniency for its own sake: the implementations disagree on how to
\* REPRESENT "no timer", not on behaviour.  resonate-pg has one
\* `timeout_at` column that keeps its stale value when a task suspends or
\* halts; the unified model names the timer and zeroes the instant.  In a
\* state with no timer the field is semantically undefined on both sides,
\* so comparing it compares noise.  `timerKind` IS compared in every state,
\* so "the task has no timer" is still checked -- only the meaningless
\* number is skipped.
TaskOK ==
    /\ \A i \in RecordedTaskIds :
        LET r == logline.state.tasks[i] IN
        /\ tasks'[i].exists
        /\ tasks'[i].state     = r.state
        /\ tasks'[i].version   = r.version
        /\ tasks'[i].timerKind = r.timerKind
        /\ tasks'[i].timerKind # "none" => tasks'[i].timerAt = r.timerAt
        \* per-task lease TTL, where the harness records it
        /\ "ttl" \in DOMAIN r => tasks'[i].ttl = r.ttl
    \* and nothing the model invented that the system did not have
    /\ \A i \in Ids : tasks'[i].exists => i \in RecordedTaskIds

CallbacksOK == callbacks' = { <<c[1], c[2]>> : c \in ToSet(logline.state.callbacks) }
ListenersOK == listeners' = { <<c[1], c[2]>> : c \in ToSet(logline.state.listeners) }
ResumesOK   == resumes'   = { <<c[1], c[2]>> : c \in ToSet(logline.state.resumes) }

\* Only `resonate` has a delivery stage and worker-side claims, so only its
\* traces constrain these.  They are what let a recorded execution check the
\* fencing layer -- at-most-once execution -- which no other harness can.
DeliveredOK ==
    delivered' = { [kind |-> "execute", id |-> m.id, version |-> m.version,
                    addr |-> NoAddr] : m \in ToSet(logline.state.delivered) }

ClaimOK ==
    \A w \in Workers :
        /\ claim'[w].task    = logline.state.claim[w].task
        /\ claim'[w].version = logline.state.claim[w].version

\* `resonate` records the NUMBER of buffered resumes per task, not which
\* promise triggered them -- and that is exactly the wire-level view: the
\* specification's TaskRecord carries `resumes : Nat` (Cardinality of the
\* set).  So the count is compared, which is all the trace determines.
ResumeCountsOK ==
    \A i \in DOMAIN logline.state.resumeCounts :
        Cardinality({ r \in resumes' : r[1] = i }) = logline.state.resumeCounts[i]

OutboxOK ==
    outbox' = { [kind |-> m.kind, id |-> m.id, version |-> m.version, addr |-> m.addr]
                  : m \in ToSet(logline.state.outbox) }

StateOK ==
    /\ Observed("promises")  => PromiseOK
    /\ Observed("tasks")     => TaskOK
    /\ Observed("callbacks") => CallbacksOK
    /\ Observed("listeners") => ListenersOK
    /\ Observed("resumes")   => ResumesOK
    /\ Observed("outbox")    => OutboxOK
    /\ Observed("delivered") => DeliveredOK
    /\ Observed("claim")     => ClaimOK
    /\ Observed("resumeCounts") => ResumeCountsOK

(***************************************************************************)
(* Unified.tla's Next conjoins ObsUpdate; the wrappers below call the raw   *)
(* actions, so the history variable is advanced here instead.               *)
(***************************************************************************)
TObsUpdate ==
    obs' = [i \in Ids |->
              IF obs[i] = "none"
                 /\ ProjOf(promises', now', i) \notin {"none", "pending"}
              THEN ProjOf(promises', now', i)
              ELSE obs[i]]

\* The clock jumps to the instant the trace recorded, rather than ticking by
\* one: real timestamps are milliseconds (or nanoseconds).
AdvanceClock ==
    /\ now' = logline.now
    /\ UNCHANGED <<core, ghosts, resVars, resTVars>>

\* Every step: the named action fires, the clock is at the recorded instant,
\* the post-state agrees, and the cursor advances.
TStep(nm, A) ==
    /\ logline.name = nm
    /\ now = logline.now          \* the clock is already where the event says
    /\ A
    /\ TObsUpdate
    /\ StateOK
    /\ l' = l + 1

-----------------------------------------------------------------------------
(* One wrapper per action.  The existential is over recorded ids only, and  *)
(* StateOK is what selects the witness.                                     *)

Anyone == CHOOSE w \in Workers : TRUE

TPromiseCreate ==
    TStep("PromiseCreate",
          \E i \in RecordedIds :
              PromiseCreate(i, logline.state.promises[i].timeoutAt,
                               logline.state.promises[i].kind))

TPromiseSettle ==
    TStep("PromiseSettle",
          \E i \in RecordedIds : PromiseSettle(i, logline.state.promises[i].state))

TRegisterCallback ==
    TStep("RegisterCallback", \E aw, ar \in RecordedIds : RegisterCallback(aw, ar))

TRegisterListener ==
    TStep("RegisterListener",
          \E aw \in RecordedIds, ad \in Addrs : RegisterListener(aw, ad))

TTaskClaim ==
    TStep("TaskClaim",
          \E i \in RecordedIds, w \in Workers, t \in TTLs : TaskClaim(i, w, t))

\* The ttl is whatever the worker presented, so it is drawn from the
\* recorded post-state rather than from a constant.
TTaskAcquire ==
    TStep("TaskAcquire",
          \E w \in Workers, m \in outbox \cup delivered, t \in TTLs :
              TaskAcquire(w, m, t))

TTaskSuspend ==
    TStep("TaskSuspend",
          \E i \in RecordedIds, S \in (SUBSET RecordedIds \ {{}}) : TaskSuspend(i, S))

TTaskFulfill ==
    TStep("TaskFulfill",
          \E i \in RecordedIds : TaskFulfill(i, logline.state.promises[i].state))

TTaskRelease  == TStep("TaskRelease",  \E i \in RecordedIds : TaskRelease(i))
TTaskHalt     == TStep("TaskHalt",     \E i \in RecordedIds : TaskHalt(i))
TTaskContinue == TStep("TaskContinue", \E i \in RecordedIds : TaskContinue(i))
THeartbeat    == TStep("TaskHeartbeat", \E w \in Workers : TaskHeartbeat(w))

TOnPromiseTimeout ==
    TStep("OnPromiseTimeout", \E i \in RecordedIds : OnPromiseTimeout(i))
TOnTaskRetryTimeout ==
    TStep("OnTaskRetryTimeout", \E i \in RecordedIds : OnTaskRetryTimeout(i))
TOnTaskLeaseTimeout ==
    TStep("OnTaskLeaseTimeout", \E i \in RecordedIds : OnTaskLeaseTimeout(i))

\* Pure reads.  No harness records them yet, but a MODEL-GENERATED scenario
\* does (see gen-scenarios.py): a read is how a client observes the
\* contradiction that several of these bugs produce.
TPromiseGet  == TStep("PromiseGet", \E i \in RecordedIds : PromiseGet(i))
TTaskGet     == TStep("TaskGet",    \E i \in RecordedIds : TaskGet(i))
TClientTouch == TStep("ClientTouch",\E i \in RecordedIds : ClientTouch(i))

TDeliver     == TStep("Deliver",  \E m \in outbox : Deliver(m))
TDropMsg     == TStep("DropMsg",  \E m \in outbox : DropMsg(m))
TWorkerCrash == TStep("WorkerCrash", \E w \in Workers : WorkerCrash(w))

(***************************************************************************)
(* BATCH EVENTS.                                                            *)
(*                                                                          *)
(* `resonate`'s harness records `process_timeouts` as ONE event, though it   *)
(* runs three SQL statements and may settle several promises and re-arm      *)
(* several tasks.  The unified model has three separate rules, so one        *)
(* recorded event corresponds to a SEQUENCE of model steps.                  *)
(*                                                                          *)
(* `TBatchStep` fires one internal rule WITHOUT advancing the cursor;        *)
(* `TBatchDone` consumes the event once the state agrees.  Together TLC      *)
(* explores "fire rules until the recorded state is reached, then move on".  *)
(* Each rule disarms its own guard (a re-armed timer is pushed past `now`),  *)
(* so the batch always terminates.                                           *)
(***************************************************************************)
\* Two batch kinds.  `Batch` is the unattended driver (`process_timeouts`),
\* which only settles ARMED promises.  `Touch` is a read that materialises
\* whatever it names, armed or not.
IsBatch == logline.name \in {"Batch", "Touch"}

TBatchStep ==
    /\ IsBatch
    /\ now = logline.now
    /\ \E i \in RecordedIds :
           IF logline.name = "Touch"
           THEN TouchPromise(i)
           ELSE \/ OnPromiseTimeout(i)
                \/ OnTaskRetryTimeout(i)
                \/ OnTaskLeaseTimeout(i)
    /\ TObsUpdate
    /\ UNCHANGED l

TBatchDone ==
    \/ TStep("Batch", UNCHANGED <<now, core, ghosts, resVars, resTVars>>)
    \/ TStep("Touch", UNCHANGED <<now, core, ghosts, resVars, resTVars>>)

\* A scheduler job that fired and found nothing to do.  Convex's deadline
\* jobs run whenever the scheduler runs them and let the HANDLER decide, so
\* the harness records genuine no-ops; the unified model's rules are guarded
\* and cannot fire when not due.  The adapter marks such an event, and it
\* replays as a stutter that still checks the recorded state.
TNoOp == TStep("NoOp", UNCHANGED <<now, core, ghosts, resVars, resTVars>>)
TAdvance == TStep("AdvanceClock", AdvanceClock)

\* The clock is not an event in most harnesses: it is read per action.  A
\* stutter step moves it to the next event's instant without consuming the
\* event, so a trace need not record ticks at all.
TTick ==
    /\ l \in DOMAIN TraceLog
    /\ now < logline.now
    /\ now' = logline.now
    /\ UNCHANGED <<core, ghosts, resVars, resTVars, obs, l>>

-----------------------------------------------------------------------------

TraceInit == Init /\ l = 1

TraceNext ==
    IF l > Len(TraceLog) THEN UNCHANGED tvars ELSE
    \/ TTick
    \/ TPromiseCreate \/ TPromiseSettle \/ TRegisterCallback \/ TRegisterListener
    \/ TTaskClaim \/ TTaskAcquire \/ TTaskSuspend \/ TTaskFulfill
    \/ TTaskRelease \/ TTaskHalt \/ TTaskContinue \/ THeartbeat
    \/ TOnPromiseTimeout \/ TOnTaskRetryTimeout \/ TOnTaskLeaseTimeout
    \/ TDeliver \/ TAdvance \/ TNoOp \/ TDropMsg \/ TWorkerCrash
    \/ TPromiseGet \/ TTaskGet \/ TClientTouch
    \/ TBatchStep \/ TBatchDone

TraceSpec == TraceInit /\ [][TraceNext]_tvars

(***************************************************************************)
(* THE CHECK -- a REACHABILITY question, deliberately.                      *)
(*                                                                          *)
(* "Can the model reproduce this trace?" is: does some behaviour drive the  *)
(* cursor past the last event.  It is NOT "does every behaviour", which is  *)
(* what a `<>Replayed` temporal property would demand and which is wrong    *)
(* here: the recorded state does not always determine WHICH id an event     *)
(* acted on, so the existential may pick a witness that agrees with the     *)
(* recorded state now and dead-ends later.  Such a branch is an artefact of *)
(* the ambiguity, not evidence against the model.                           *)
(*                                                                          *)
(* So the check is run as an invariant that a full replay VIOLATES:         *)
(*                                                                          *)
(*   TLC reports `NotReplayed is violated`  ->  the trace replayed, and the *)
(*     counterexample it prints IS the replay                               *)
(*   TLC reports `No error has been found`  ->  the trace did NOT replay;   *)
(*     no path reaches the end                                              *)
(***************************************************************************)
Replayed    == l > Len(TraceLog)
NotReplayed == ~Replayed

=============================================================================
