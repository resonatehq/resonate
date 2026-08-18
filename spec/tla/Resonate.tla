-------------------------------- MODULE Resonate --------------------------------
(***************************************************************************)
(* The Resonate protocol as a state machine, at the altitude of            *)
(* `spec/02-abstract` -- but with the atomicity taken OUT.                 *)
(*                                                                         *)
(* The Lean machine makes one request one transition: `stepOf` runs a      *)
(* handler and `applyAll` folds its whole effect list onto the pre-state,  *)
(* and no other request can be observed in between. That is the right      *)
(* altitude for saying WHAT each request means, and the wrong one for      *)
(* asking what two concurrent requests do to each other -- because at that *)
(* altitude, they cannot do anything to each other.                        *)
(*                                                                         *)
(* Four state components, and each is a different KIND of thing:           *)
(*                                                                         *)
(*     objects    what is true. Durable. A promise and its optional task   *)
(*                are ONE unit -- they are settled together, fulfilled     *)
(*                together, and never observed disagreeing                 *)
(*     timeouts   what is due. An INDEX over the deadlines the objects     *)
(*                already carry, which is exactly why it can drift         *)
(*     outbox     what has been said. Keyed, and the only thing that       *)
(*                leaves the system                                        *)
(*     steps      what is in flight. Volatile: a crash drops one and       *)
(*                nothing durable refers to it                             *)
(*                                                                         *)
(* and six transitions:                                                    *)
(*                                                                         *)
(*     SubmitExternal   a client request arrives                           *)
(*     SubmitInternal   the server takes a step on its own initiative      *)
(*     Process          READ AND DECIDE. Writes nothing                    *)
(*     Perform          apply ONE effect -- or, with none left, answer and *)
(*                      retire                                             *)
(*     Crash            the step vanishes; what it committed stays         *)
(*     Clock            time advances, on its own and unprompted           *)
(*                                                                         *)
(* WHERE THE INTERLEAVING IS. Between a step's Process and its last        *)
(* Perform, any number of other steps may run to completion. So a step     *)
(* commits its state and then says things about a world that has since     *)
(* moved on: that is stale dispatch -- a drain-issued Execute surviving a  *)
(* settle -- which is one of the four defects the Verus port found, and it *)
(* is a defect this machine can EXHIBIT rather than merely be told about.  *)
(*                                                                         *)
(* WHAT THIS MODULE IS FOR. The objects and the wheel are TWO STORES, and  *)
(* nothing writes them together. A step that settles a promise and clears  *)
(* its timeout does two writes, at two moments, with the world running in  *)
(* between -- and the question this module exists to ask is what an        *)
(* abstract model needs in order to be correct anyway.                     *)
(*                                                                         *)
(* So `Process` writes NOTHING. It reads, it decides, and it leaves an     *)
(* ordered list of effects behind; `Perform` applies them ONE AT A TIME,   *)
(* each its own transition, each interleavable with every other step. That *)
(* is `KStep::Prepare` and `KStep::Perform` in the Verus executor --        *)
(* `set_phase` touches `steps` and nothing else -- carried up to the       *)
(* altitude of the Lean spec, where there is no document, no etag and no   *)
(* store to be a fact about.                                               *)
(*                                                                         *)
(* THE ORDER OF THE EFFECTS IS THEREFORE PROTOCOL. Verus emits             *)
(* `sched + put + ack + emits + respond`: arm BEFORE the write, disarm     *)
(* AFTER it. Both directions of getting that wrong are failures and they   *)
(* are not the same failure -- arm-then-write leaves a spurious entry, and *)
(* a spurious entry is noise; write-then-arm leaves a deadline that        *)
(* nothing will ever fire, and that is silence. A crash between any two    *)
(* effects is what makes the difference visible.                           *)
(*                                                                         *)
(* THERE IS NO FENCE. No etag, no version, no compare-and-swap: two steps  *)
(* may read the same object and both write it, and the second wins. That   *)
(* is not an oversight and it is not a claim that it is safe -- it is the  *)
(* experiment. Put the fence in first and the model can only confirm that  *)
(* a fence is sufficient; leave it out and the model has to say what goes  *)
(* wrong without one, which is the thing worth knowing.                    *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TYPES                                                               *)
(*                                                                         *)
(* Apalache needs every variable's shape declared, and writing them down   *)
(* forced three encodings that plain TLA+ let us leave vague. All three    *)
(* are improvements, not concessions:                                      *)
(*                                                                         *)
(*   - THERE IS NO `None`. An earlier draft had one value standing for     *)
(*     absence in six places at once -- absent settlement, absent lease,   *)
(*     absent worker, absent task -- which is not a value any type system  *)
(*     will admit, and was hiding that those are four different questions. *)
(*     Absent instants are `NoTime`, which is `-1` and cannot be confused  *)
(*     with an instant because instants are non-negative. An absent worker *)
(*     is `NoPid`, a constant ASSUMEd outside `Pid`. And an absent task is *)
(*     a task in state `"none"` -- which is the honest reading: whether a  *)
(*     promise is targeted is part of the unit's state, not a hole in it.  *)
(*                                                                         *)
(*   - THE ALPHABET IS A VARIANT. 20 events with 20 different payloads is  *)
(*     a tagged union, and writing it as a set of records with a `kind`    *)
(*     field only worked because nothing checked that the fields lined up. *)
(*     `$event` says it: one tag, one payload, and a handler that forgets  *)
(*     a case is a type error rather than a silent fallthrough.            *)
(*                                                                         *)
(*   - `taskFence` CARRIES A VARIANT TOO. Its action is a create or a      *)
(*     settle, which is `TaskFenceAction` in the Lean and was an untagged  *)
(*     union of two record types here.                                     *)
(***************************************************************************)

(*
  @typeAlias: id = { origin: ORIGIN, rest: REST };

  @typeAlias: promise = {
      state: Str,
      param: VALUE,
      value: VALUE,
      tags: TAGS,
      timeoutAt: Int,
      createdAt: Int,
      settledAt: Int,
      callbacks: Set($id),
      listeners: Set(ADDR)
  };

  @typeAlias: task = {
      state: Str,
      version: Int,
      ttl: Int,
      pid: PID,
      expiresAt: Int,
      retryAt: Int,
      resumes: Set($id)
  };

  @typeAlias: object = { promise: $promise, task: $task };

  @typeAlias: entry = { at: Int, id: $id, kind: Str };

  @typeAlias: message =
      Execute({ id: $id, version: Int })
    | Unblock({ id: $id, state: Str });

  @typeAlias: outEntry = { address: ADDR, message: $message };

  @typeAlias: msgKey = { kind: Str, id: $id, address: ADDR };

  @typeAlias: createReq = { id: $id, timeoutAt: Int, param: VALUE, tags: TAGS };
  @typeAlias: settleReq = { id: $id, state: Str, value: VALUE };
  @typeAlias: callbackReq = { awaited: $id, awaiter: $id };
  @typeAlias: taskRef = { id: $id, version: Int };

  @typeAlias: fenceAction = Create({ req: $createReq }) | Settle({ req: $settleReq });

  @typeAlias: event =
      PromiseGet({ id: $id })
    | PromiseCreate({ req: $createReq })
    | PromiseSettle({ req: $settleReq })
    | PromiseRegisterCallback({ req: $callbackReq })
    | PromiseRegisterListener({ awaited: $id, address: ADDR })
    | PromiseSearch(UNIT)
    | TaskGet({ id: $id })
    | TaskCreate({ pid: PID, ttl: Int, action: $createReq })
    | TaskAcquire({ id: $id, version: Int, pid: PID, ttl: Int })
    | TaskFence({ id: $id, version: Int, action: $fenceAction })
    | TaskHeartbeat({ pid: PID, tasks: Set($taskRef) })
    | TaskSuspend({ id: $id, version: Int, actions: Set($callbackReq) })
    | TaskFulfill({ id: $id, version: Int, action: $settleReq })
    | TaskRelease({ id: $id, version: Int })
    | TaskHalt({ id: $id })
    | TaskContinue({ id: $id })
    | TaskSearch(UNIT)
    | Timeout({ entry: $entry })
    | ListenerDrain({ id: $id, address: ADDR })
    | CallbackDrain({ id: $id, awaiter: $id });

  @typeAlias: effect =
      PutObject({ id: $id, obj: $object })
    | ArmTimeout({ entry: $entry })
    | DisarmTimeout({ entry: $entry })
    | Send({ entry: $outEntry });

  @typeAlias: inFlight = {
      ev: $event,
      phase: Str,
      pending: Seq($effect),
      res: RESPONSE
  };

  @typeAlias: env = {
      objects: $id -> $object,
      timeouts: Set($entry),
      outbox: Set($outEntry),
      now: Int,
      mat: Bool,
      config: { retryTimeout: Int }
  };

  @typeAlias: outcome = { effects: Seq($effect), res: RESPONSE };
*)
ResonateAliases == TRUE

-----------------------------------------------------------------------------

CONSTANTS
    \* @type: Set(ORIGIN);
    Origin,
    \* @type: Set(REST);
    Rest,
    \* @type: Set(ADDR);
    Address,
    \* @type: Set(PID);
    Pid,
    \* @type: Set(VALUE);
    Value,
    \* @type: Set(TAGS);
    Tags,
    \* @type: Set(Int);
    Ttl,
    \* @type: Set(RID);
    Rid,
    \* @type: PID;
    NoPid,
    \* @type: ADDR;
    NoAddr,
    \* @type: RESPONSE;
    Silent,
    \* @type: Bool;
    Materialise,
    \* @type: Int;
    RetryTimeout,
    \* @type: Int;
    MaxTime,
    \* @type: Int;
    MaxVersion

ASSUME NoPid  \notin Pid
ASSUME NoAddr \notin Address
ASSUME MaxTime    >= 0
ASSUME MaxVersion >= 0

Time    == 0 .. MaxTime
Version == 0 .. MaxVersion

(* Absence of an instant. `-1` and not a distinguished constant, because
   instants are non-negative and the arithmetic never reaches it. *)
NoTime == -1

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE IDENTIFIER                                                          *)
(*                                                                         *)
(* An identifier is a PAIR, and the pair is what is unique -- `rest` alone *)
(* is not. Both halves are opaque here: what an origin denotes is not      *)
(* something any transition in this module reads, and the moment it        *)
(* becomes something the protocol reads it stops being a constant.         *)
(*                                                                         *)
(* One identifier names one object, and an object is a promise and, if the *)
(* promise is targeted, the task that drives it. That is why there is no   *)
(* separate task identifier: `setTask { id := p.id, .. }` everywhere in    *)
(* the Lean, one name for both roles.                                      *)
(***************************************************************************)

Id == [origin : Origin, rest : Rest]

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE OBJECTS                                                             *)
(*                                                                         *)
(* An object is a promise and, if the promise is targeted, the task that   *)
(* drives it -- `PromiseObject` and `TaskObject` from                      *)
(* `02-abstract/state.lean`, fused. The fusion is not a compression: it    *)
(* is the claim that the pair is what gets written, and it makes           *)
(* `setSettled` -- settle the promise, fulfill the task -- ONE write       *)
(* rather than two that a reader could catch half-done.                    *)
(*                                                                         *)
(* There is nothing else. Schedules are not objects and are not modelled   *)
(* here: a schedule creates promises on a cron, which makes it a source of *)
(* events rather than a thing the protocol acts on, and folding it in      *)
(* would have cost a second key space, a second deadline kind and a tagged *)
(* union, to say nothing this module is trying to say. `scheduleGet`,      *)
(* `scheduleCreate`, `scheduleDelete`, `scheduleSearch` and the Lean's     *)
(* `r7` are out of the alphabet with it.                                   *)
(*                                                                         *)
(* One change from the Lean: `callbacks` and `listeners` are SETS, not     *)
(* lists -- they are ledgers, and `promiseEq` already compares them        *)
(* without regard to order.                                                *)
(*                                                                         *)
(* An object carries no clock. The Verus `Workflow.clock` is a per-        *)
(* document high-water mark that exists because that executor writes to a  *)
(* store which may serve a stale read, and `clamp` is what keeps the       *)
(* document's own notion of time from regressing. That is a fact about     *)
(* the store, not about the protocol. Here there is one clock, `now`, and  *)
(* every step reads it directly.                                           *)
(***************************************************************************)

PromiseState == {"pending", "resolved", "rejected",
                 "rejectedCanceled", "rejectedTimedout"}

(* `"none"` is the state of a unit whose promise is not targeted -- there
   is no task. It is a state and not a hole: whether a promise is driven
   by a task is something the unit knows about itself. *)
TaskState == {"none", "pending", "acquired", "suspended", "halted", "fulfilled"}

(* @type: $task; *)
NoTask ==
    [ state |-> "none", version |-> 0, ttl |-> NoTime, pid |-> NoPid,
      expiresAt |-> NoTime, retryAt |-> NoTime, resumes |-> {} ]

Promise == [
    state     : PromiseState,
    param     : Value,
    value     : Value,
    tags      : Tags,
    timeoutAt : Time,                    \* deadline -- armed as "promise"
    createdAt : Time,
    settledAt : Time \cup {NoTime},
    callbacks : SUBSET Id,               \* awaiters to resume when this settles
    listeners : SUBSET Address ]         \* addresses to notify when this settles

Task == [
    state     : TaskState,
    version   : Version,                 \* the fencing token
    ttl       : Ttl \cup {NoTime},       \* the WORKER's number: lease asked for
    pid       : Pid \cup {NoPid},
    expiresAt : Time \cup {NoTime},      \* deadline -- armed as "lease"
    retryAt   : Time \cup {NoTime},      \* deadline -- armed as "retry"
    resumes   : SUBSET Id ]              \* triggers buffered while not suspended

Object == [promise : Promise, task : Task]

Message ==
       { Variant("Execute", [id |-> i, version |-> v]) : i \in Id, v \in Version }
  \cup { Variant("Unblock", [id |-> i, state |-> s]) : i \in Id, s \in PromiseState }

OutEntry == [address : Address, message : Message]

(* `OutboxEntry.key`: what a later message REPLACES rather than joins. Per
   task for execute -- so a second dispatch of the same task supersedes the
   first wherever it was going -- and per (promise, address) for unblock,
   since two listeners on one promise are two separate things to say.
   `NoAddr` is what makes the execute key address-blind while keeping one
   record type for both, which is what a type system asks for and what a
   pair of differently-shaped tuples would not have given.
   @type: $outEntry => $msgKey; *)
MsgKey(e) ==
    IF VariantTag(e.message) = "Execute"
    THEN [kind |-> "execute",
          id      |-> VariantGetUnsafe("Execute", e.message).id,
          address |-> NoAddr]
    ELSE [kind |-> "unblock",
          id      |-> VariantGetUnsafe("Unblock", e.message).id,
          address |-> e.address]

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TIMEOUTS                                                            *)
(*                                                                         *)
(* The wheel. Every entry names a deadline that an object already carries: *)
(* `promise` is `Promise.timeoutAt`, `lease` is `Task.expiresAt`, and      *)
(* `retry` is `Task.retryAt`.                                              *)
(*                                                                         *)
(* So the wheel is REDUNDANT, and that is the point of having it. It is an *)
(* index kept by a different mechanism than the thing it indexes, which    *)
(* means it can drift -- and the two directions of drift are not the same  *)
(* failure. An entry left behind after its deadline was cleared fires a    *)
(* step that reads the object, finds nothing due, and does nothing: noise. *)
(* A deadline armed with no entry is a timeout that NEVER FIRES: silence,  *)
(* and unrecoverable. `WheelComplete` below is the invariant that forbids  *)
(* the second, and the effect ordering in `Process` is what maintains it.  *)
(*                                                                         *)
(* Verus arms only `min_deadline(w)`, one entry per origin, and re-arms on *)
(* every write. Here every deadline is its own entry -- the same behaviour *)
(* with the coalescing not yet done, and a strictly easier obligation.     *)
(***************************************************************************)

DeadlineKind == {"promise", "lease", "retry"}

Entry == [at : Time, id : Id, kind : DeadlineKind]

(* The deadline an object actually carries for a given kind -- what the
   wheel is an index OF. `NoTime` when the object carries no such deadline
   live: a settled promise has no timeout, an unacquired task no lease.
   @type: ($object, Str) => Int; *)
Deadline(obj, kind) ==
    CASE kind = "promise" ->
             IF obj.promise.state = "pending" THEN obj.promise.timeoutAt ELSE NoTime
      [] kind = "lease" -> obj.task.expiresAt
      [] OTHER          -> obj.task.retryAt

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ALPHABET                                                            *)
(*                                                                         *)
(* `Equivalence.Request` and the internal constructors of                  *)
(* `Abstraction.Step`, as one variant. The external/internal line is the   *)
(* line between what a client asks for and what the server does on its own *)
(* initiative -- `Step.isExternal` and `Step.isInternal` -- and it is the  *)
(* line the two Submit transitions are organised around: an external event *)
(* may arrive at any moment, an internal one only when the server has a    *)
(* reason to take it.                                                      *)
(***************************************************************************)

CreateReq   == [id : Id, timeoutAt : Time, param : Value, tags : Tags]
SettleReq   == [id : Id, state : PromiseState, value : Value]
CallbackReq == [awaited : Id, awaiter : Id]
TaskRefT    == [id : Id, version : Version]

FenceAction ==
       { Variant("Create", [req |-> r]) : r \in CreateReq }
  \cup { Variant("Settle", [req |-> r]) : r \in SettleReq }

ExternalEvent ==
       { Variant("PromiseGet",    [id |-> i])  : i \in Id }
  \cup { Variant("PromiseCreate", [req |-> r]) : r \in CreateReq }
  \cup { Variant("PromiseSettle", [req |-> r]) : r \in SettleReq }
  \cup { Variant("PromiseRegisterCallback", [req |-> r]) : r \in CallbackReq }
  \cup { Variant("PromiseRegisterListener", [awaited |-> i, address |-> a])
         : i \in Id, a \in Address }
  \cup { Variant("PromiseSearch", UNIT) }
  \cup { Variant("TaskGet", [id |-> i]) : i \in Id }
  \cup { Variant("TaskCreate", [pid |-> p, ttl |-> t, action |-> r])
         : p \in Pid, t \in Ttl, r \in CreateReq }
  \cup { Variant("TaskAcquire", [id |-> i, version |-> v, pid |-> p, ttl |-> t])
         : i \in Id, v \in Version, p \in Pid, t \in Ttl }
  \cup { Variant("TaskFence", [id |-> i, version |-> v, action |-> a])
         : i \in Id, v \in Version, a \in FenceAction }
  \cup { Variant("TaskHeartbeat", [pid |-> p, tasks |-> ts])
         : p \in Pid, ts \in SUBSET TaskRefT }
  \cup { Variant("TaskSuspend", [id |-> i, version |-> v, actions |-> as])
         : i \in Id, v \in Version, as \in SUBSET CallbackReq }
  \cup { Variant("TaskFulfill", [id |-> i, version |-> v, action |-> r])
         : i \in Id, v \in Version, r \in SettleReq }
  \cup { Variant("TaskRelease", [id |-> i, version |-> v]) : i \in Id, v \in Version }
  \cup { Variant("TaskHalt",     [id |-> i]) : i \in Id }
  \cup { Variant("TaskContinue", [id |-> i]) : i \in Id }
  \cup { Variant("TaskSearch", UNIT) }

(* The steps no client asks for. Two shapes, not five, and the split is by
   WHAT ENABLES THEM rather than by what they do:

     - a due wheel entry. This is the Lean's r1, r5 and r6 -- promise
       timeout, lease timeout, retry dispatch -- collapsed into one event,
       because with the deadline in the entry there is nothing left to
       distinguish them at this altitude. It is the Verus `Input::Tick`,
       which rides only on an armed entry that is due: "everything else
       the machine does on its own is a TIMEOUT TICK".

     - a non-empty ledger on a settled promise. This is r3 and r4, the
       listener and callback drains, and they are NOT timer-driven: what
       enables them is a settlement that has happened and a name still
       written down next to it. *)
InternalEvent ==
       { Variant("Timeout", [entry |-> e]) : e \in Entry }
  \cup { Variant("ListenerDrain", [id |-> i, address |-> a]) : i \in Id, a \in Address }
  \cup { Variant("CallbackDrain", [id |-> i, awaiter |-> w]) : i \in Id, w \in Id }

Event == ExternalEvent \cup InternalEvent

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE VARIABLES                                                           *)
(***************************************************************************)

VARIABLES
    \* @type: $id -> $object;
    objects,    \* partial: DOMAIN is what exists
    \* @type: Set($entry);
    timeouts,   \* the wheel
    \* @type: Set($outEntry);
    outbox,     \* at most one entry per MsgKey
    \* @type: RID -> $inFlight;
    steps,      \* the requests currently in flight
    \* @type: Int;
    now         \* the clock. NOT part of the store: a server does not own
                \* the time, it reads it. Two machines at the same state
                \* and different instants are at the same state

vars == <<objects, timeouts, outbox, steps, now>>

(* What a step has left to do once it has decided: a list of effects to
   apply, in order, and the answer to give when it has applied them.

   The answer is a FIELD and not the last element of the list. The Verus
   executor makes it an effect -- `Effect::Respond { rid, status }` -- so
   that its pending list is never empty and one `Perform` rule retires the
   step. Here retiring IS answering: a step whose list is empty has nothing
   left but the answer, and giving it is the same act as going away. That
   leaves `pending` as exactly what it says -- what has not happened yet.
   An internal step answers `Silent` to nobody.
   @type: $event => $inFlight; *)
Fresh(ev) ==
    [ev |-> ev, phase |-> "process", pending |-> << >>, res |-> Silent]

(***************************************************************************)
(* WHY A STEP HAS AN IDENTITY                                              *)
(*                                                                         *)
(* It carries no information. A step IS its event, its phase and what it   *)
(* has left to say -- the identity is not a field of `$inFlight`, and an   *)
(* earlier draft that made it one was storing the map's key inside the     *)
(* map's value.                                                            *)
(*                                                                         *)
(* The KEY still has to exist, for one reason: two steps that are equal in *)
(* every field are still two steps. Two clients sending the same request   *)
(* at the same moment is not a hypothetical -- it is the retry -- and a    *)
(* bare set of records would silently make them one. `steps` is a function *)
(* so that a duplicate is a duplicate, and so that `Process(r)` has        *)
(* something to name.                                                      *)
(*                                                                         *)
(* Three other shapes admit duplicates, and none is better:                *)
(*                                                                         *)
(*   - a SEQUENCE. It adds an order the protocol does not have: <<a, b>>   *)
(*     and <<b, a>> are different values for the same situation, and       *)
(*     nothing reads arrival order. A function has the same redundancy --  *)
(*     [r1 |-> a, r2 |-> b] and the swap are also distinct -- but there    *)
(*     the redundancy is REMOVABLE: declare `Rid` a symmetry set and TLC   *)
(*     quotients the permutations away. You cannot declare a sequence's    *)
(*     indices interchangeable, because for a sequence they are not.       *)
(*     (Symmetry reduction is unreliable for LIVENESS checking, so expect  *)
(*     it to pay for the invariants and not for the fairness properties.   *)
(*     Apalache does not use symmetry at all -- it is symbolic -- so this  *)
(*     argument is TLC's alone, and under Apalache the shapes tie.)        *)
(*     A sequence also renumbers on removal, so an unrelated `Crash`       *)
(*     renames every step after it.                                        *)
(*                                                                         *)
(*   - a BAG. Exactly right in principle -- duplicates without order --    *)
(*     and clunky at every use site: no EXCEPT, and each update spells     *)
(*     itself as a remove and an add. The honest fallback if symmetry      *)
(*     ever has to go.                                                     *)
(*                                                                         *)
(*   - a SET OF RECORDS EACH CARRYING ITS OWN `rid`. This works, and it    *)
(*     reads better where a step is picked -- `\E st \in steps` hands you  *)
(*     the record instead of a key to index with. But it puts the key back *)
(*     inside the value, and it turns uniqueness from a fact into an       *)
(*     obligation: nothing structurally forbids two records sharing a rid, *)
(*     so `TypeOK` grows a conjunct that a function gets for free. Minting *)
(*     gets worse too -- `\E r \in Rid : \A st \in steps : st.rid /= r`    *)
(*     against `\E r \in Rid \ DOMAIN steps`.                              *)
(*                                                                         *)
(* `Rid` is a CONSTANT and finite. Two things follow, both wanted:         *)
(*                                                                         *)
(*   - identities are MINTED BY CHOICE, `\E r \in Rid \ DOMAIN steps`, not *)
(*     by a counter. A counter would be a variable that never repeats a    *)
(*     value, so no state would ever repeat either, and TLC would walk an  *)
(*     infinite state space for a machine whose real state is finite.      *)
(*     Nothing reads the order steps arrived in, so nothing misses it.     *)
(*                                                                         *)
(*   - |Rid| bounds how many steps are in flight at once. That is the      *)
(*     concurrency knob, and it belongs in the model file where you can    *)
(*     turn it: |Rid| = 1 is the Lean machine, |Rid| = 2 is where the      *)
(*     interesting things start.                                           *)
(*                                                                         *)
(* Identities are REUSED after a step retires, which is sound only while   *)
(* nothing outlives a step and names it. Nothing does: the response        *)
(* vanishes when it is given. In the Verus executor the identity is not a  *)
(* modelling handle but a return address -- `Effect::Respond { rid,        *)
(* status }` has to reach the caller that asked -- and the day this        *)
(* machine grows a reply channel, reuse stops being free and `rid` becomes *)
(* protocol.                                                               *)
(***************************************************************************)

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT A HANDLER IS                                                       *)
(*                                                                         *)
(* `Handle(ev, env)` is a pure function of the event and the state, and it *)
(* reads nothing else. It returns what to commit and what to say:          *)
(*                                                                         *)
(*     effects  what to do, IN ORDER -- a crash applies a prefix           *)
(*     res      the response to give                                       *)
(*                                                                         *)
(* This is `AbstractModel.H`: a list of effects and a value, which is what *)
(* the Lean monad accumulates and what Verus's `prepare()` returns. An     *)
(* earlier draft sorted the effects into `writes`/`arm`/`disarm`/`sends`   *)
(* so that the durable ones could be committed in a single action. That    *)
(* buried the very thing this module is about: the ORDER is the protocol,  *)
(* and a bag of writes has no order to get right.                          *)
(*                                                                         *)
(* `env.mat` is the read discipline -- whether a read that projects a      *)
(* settled promise also PERSISTS that settlement. It is a parameter, not a *)
(* second machine.                                                         *)
(*                                                                         *)
(* IT IS A STUB. The body below is the handler that does nothing, so the   *)
(* module type-checks and runs today and the machinery around it can be    *)
(* exercised before a single case is written. It could not stay a          *)
(* `CONSTANT Handle(_, _)`: Apalache admits constant VALUES, not constant  *)
(* OPERATORS, and an uninterpreted handler is the one thing in this file   *)
(* it would have refused outright.                                         *)
(*                                                                         *)
(* Swapping the body for `Gen(3)` gives the adversarial handler -- every   *)
(* outcome the type admits, including malicious ones -- which is the right *)
(* way to find out which invariants the MACHINE maintains and which ones   *)
(* only the HANDLERS do. Expect it to refute nearly everything; that is    *)
(* the measurement.                                                        *)
(***************************************************************************)

(* @type: $env; *)
Env ==
    [ objects  |-> objects,
      timeouts |-> timeouts,
      outbox   |-> outbox,
      now      |-> now,
      mat      |-> Materialise,
      config   |-> [retryTimeout |-> RetryTimeout] ]

(* @type: ($event, $env) => $outcome; *)
Handle(ev, env) == [ effects |-> << >>, res |-> Silent ]

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHEN AN INTERNAL EVENT HAS A REASON TO FIRE                             *)
(*                                                                         *)
(* Every internal handler in `internal.lean` re-checks its own condition   *)
(* on the state it reads, and does nothing when it does not hold. So this  *)
(* guard is not a semantic commitment -- dropping it would admit strictly  *)
(* more SubmitInternal steps and every extra one would commit nothing. It  *)
(* is here because it is what a real timer wheel and drain loop do, and    *)
(* because an unguarded submit makes the fairness conditions vacuous.      *)
(*                                                                         *)
(* Note the condition is checked HERE, against the state at submit time,   *)
(* and again inside the handler, against the state at Process time. Those  *)
(* are different states, and the gap between them is not a defect but the  *)
(* subject: a lease that expires and is released by its worker in between  *)
(* fires and does nothing -- exactly the `pure ()` branch in               *)
(* `processLeaseTimeout`.                                                  *)
(***************************************************************************)

(* @type: $id => Bool; *)
Live(id) == id \in DOMAIN objects

(* @type: $id => $promise; *)
Prom(id) == objects[id].promise

(* Settled AS READ: a pending promise past its deadline is settled whether
   or not anyone has written that down yet. `PromiseObject.project`.
   @type: $promise => Bool; *)
SettledNow(p) == p.state /= "pending" \/ p.timeoutAt <= now

(* @type: $event => Bool; *)
Fires(ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET e == VariantGetUnsafe("Timeout", ev).entry IN
             /\ e \in timeouts
             /\ e.at <= now
      [] VariantTag(ev) = "ListenerDrain" ->
             LET d == VariantGetUnsafe("ListenerDrain", ev) IN
             /\ Live(d.id)
             /\ SettledNow(Prom(d.id))
             /\ d.address \in Prom(d.id).listeners
      [] VariantTag(ev) = "CallbackDrain" ->
             LET d == VariantGetUnsafe("CallbackDrain", ev) IN
             /\ Live(d.id)
             /\ SettledNow(Prom(d.id))
             /\ d.awaiter \in Prom(d.id).callbacks
      [] OTHER -> FALSE

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE TRANSITIONS                                                         *)
(***************************************************************************)

(* @type: (RID -> $inFlight, RID, $inFlight) => (RID -> $inFlight); *)
PutStep(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]

(* @type: (RID -> $inFlight, RID) => (RID -> $inFlight); *)
DropStep(f, k) == [x \in (DOMAIN f) \ {k} |-> f[x]]

(* A client request arrives. Unguarded: a client may send anything at any
   time, including nonsense -- rejecting it is the handler's job, and the
   response it gives for nonsense is part of the protocol.
   @type: $event => Bool; *)
SubmitExternal(ev) ==
    /\ ev \in ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = PutStep(steps, r, Fresh(ev))
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* The server takes a step on its own initiative: a due entry, or a name
   still written down next to a settled promise. Same shape, guarded, and
   nobody is waiting for the answer.
   @type: $event => Bool; *)
SubmitInternal(ev) ==
    /\ ev \in InternalEvent
    /\ Fires(ev)
    /\ \E r \in Rid \ DOMAIN steps : steps' = PutStep(steps, r, Fresh(ev))
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* READ AND DECIDE, against one state, and WRITE NOTHING. This is
   `KStep::Prepare`, whose whole postcondition is `set_phase` -- the store
   and the wheel are untouched, and every write the step will do is now a
   pending effect that some later transition applies.

   Deciding is atomic even though doing is not: the handler sees one
   environment, so no read of a step can observe a write of its own step.
   That is the Lean monad's `bind` discipline, and it is the only atomicity
   this module grants.

   Nothing here re-reads. A step that decided from a state which has since
   changed will still apply what it decided -- see THERE IS NO FENCE.
   @type: RID => Bool; *)
Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ LET out == Handle(steps[r].ev, Env)
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending = out.effects,
                                  ![r].res     = out.res]
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* @type: ($id -> $object, $id, $object) => ($id -> $object); *)
PutObj(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]

(* ONE effect, atomically, and exactly one store. An object write does not
   touch the wheel; an arm does not touch the objects. That separation is
   the premise of the whole module, and it is why this is a CASE over four
   kinds rather than one commit over three variables.
   @type: $effect => Bool; *)
Apply(e) ==
    CASE VariantTag(e) = "PutObject" ->
             /\ objects' = LET w == VariantGetUnsafe("PutObject", e)
                           IN  PutObj(objects, w.id, w.obj)
             /\ UNCHANGED <<timeouts, outbox>>
      [] VariantTag(e) = "ArmTimeout" ->
             /\ timeouts' = timeouts \cup {VariantGetUnsafe("ArmTimeout", e).entry}
             /\ UNCHANGED <<objects, outbox>>
      [] VariantTag(e) = "DisarmTimeout" ->
             /\ timeouts' = timeouts \ {VariantGetUnsafe("DisarmTimeout", e).entry}
             /\ UNCHANGED <<objects, outbox>>
      [] OTHER ->
             /\ outbox' = LET en == VariantGetUnsafe("Send", e).entry
                          IN  {o \in outbox : MsgKey(o) /= MsgKey(en)} \cup {en}
             /\ UNCHANGED <<objects, timeouts>>

(* Do the next thing -- or, when there is nothing left to do, give the
   answer and go. Those are the same action because they are the same act:
   a step that has done everything has only its response left, and there is
   no state in which it has given the response and not yet retired. That is
   why there is no Complete.

   Between any two of these, every other step may run. A `Crash` here
   leaves a PREFIX applied: the object written and the timer never armed,
   or the timer armed and the object never written. Which of those is
   survivable is what the wheel invariants are for.
   @type: RID => Bool; *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ IF steps[r].pending = << >>
       THEN /\ steps'  = DropStep(steps, r)
            /\ UNCHANGED <<objects, timeouts, outbox>>
       ELSE /\ Apply(Head(steps[r].pending))
            /\ steps' = [steps EXCEPT ![r].pending = Tail(@)]
    /\ UNCHANGED now

(* The step disappears. What it committed stays committed; a PREFIX of
   what it had to say was said, and the rest never will be. No response is
   given. This is the whole reason `effects` is a sequence and not a set,
   and it is what a client retrying a request has to survive.
   @type: RID => Bool; *)
Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = DropStep(steps, r)
    /\ UNCHANGED <<objects, timeouts, outbox, now>>

(* The clock. Free-running and independent of everything else -- what makes
   a deadline pass is time moving, not anyone acting. There is exactly one
   of these, and every step reads it. Bounded so the state space is. *)
Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<objects, timeouts, outbox, steps>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE SPECIFICATION                                                       *)
(***************************************************************************)

Init ==
    /\ objects  = SetAsFun({})
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = SetAsFun({})
    /\ now      = 0

Next ==
    \/ \E ev \in ExternalEvent : SubmitExternal(ev)
    \/ \E ev \in InternalEvent : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

(* A step in flight must make progress, or "the server answers" is not a
   claim this machine makes. An internal event that stays enabled must
   eventually be taken, or no timeout is ever taken and every liveness
   property is vacuous. Nothing is assumed of clients (`SubmitExternal`)
   or of failure (`Crash`): a client need never send, a crash need never
   happen.

   TLC reads this. Apalache checks invariants over `Init`/`Next` and does
   not use it. *)
Fairness ==
    /\ \A r  \in Rid           : WF_vars(Process(r) \/ Perform(r))
    /\ \A ev \in InternalEvent : WF_vars(SubmitInternal(ev))
    /\ WF_vars(Clock)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(***************************************************************************)
(* INVARIANTS                                                              *)
(*                                                                         *)
(* `TypeOK` is much shorter than it was, and on purpose. Membership in     *)
(* `Promise`, `Task`, `$inFlight` and the rest is what the `@type`         *)
(* annotations assert, checked once by Apalache rather than at every       *)
(* state -- and half of those sets are not enumerable anyway (`Seq(...)`   *)
(* is infinite). What is left here is what a type cannot say: the          *)
(* relationships BETWEEN fields.                                           *)
(***************************************************************************)

TypeOK ==
    /\ now \in Time
    /\ DOMAIN objects \subseteq Id
    /\ DOMAIN steps   \subseteq Rid
    (* one entry per key -- the outbox is keyed, not a log *)
    /\ \A a, b \in outbox : MsgKey(a) = MsgKey(b) => a = b
    (* a step in "process" has decided nothing yet *)
    /\ \A r \in DOMAIN steps :
           steps[r].phase = "process" =>
               steps[r].pending = << >> /\ steps[r].res = Silent
    (* a unit with no task carries the canonical one, so that "no task" is
       one value and not a family of junk ones *)
    /\ \A o \in DOMAIN objects :
           objects[o].task.state = "none" => objects[o].task = NoTask

(* The wheel names only deadlines that exist. Violations are NOISE: the
   entry fires, the handler reads the object, nothing is due, nothing
   happens. Stated to be measured, not because it must hold. *)
WheelSound ==
    \A e \in timeouts :
        /\ e.id \in DOMAIN objects
        /\ Deadline(objects[e.id], e.kind) = e.at

(* Every deadline that exists is on the wheel. Violations are SILENCE: a
   timeout that never fires, and nothing downstream recovers. This is the
   one the effect ordering in `Process` exists to maintain, and the one
   worth model-checking first. *)
WheelComplete ==
    \A o \in DOMAIN objects, k \in DeadlineKind :
        Deadline(objects[o], k) /= NoTime =>
            [at |-> Deadline(objects[o], k), id |-> o, kind |-> k] \in timeouts

(* The promise and its task are one unit -- the reason for fusing them.
   A settled promise never coexists with an unfulfilled task, at any
   instant, for any reader. In the Lean this is `setSettled`, two writes
   in one transaction; here there is only one write and nothing to
   interleave between. *)
UnitCoherent ==
    \A o \in DOMAIN objects :
        (/\ objects[o].promise.state /= "pending"
         /\ objects[o].task.state \notin {"none", "fulfilled"})
            => FALSE

-----------------------------------------------------------------------------
(***************************************************************************)
(* WHAT IS BEING ASKED                                                     *)
(*                                                                         *)
(* UPWARD, to `spec/02-abstract`: take `Crash` away and let each step run  *)
(* Process..Perform with no other step interleaved, and the sequence IS    *)
(* `stepOf` -- one event, one atomic transition, effects folded on in      *)
(* order by `applyAll`. So the Lean machine is this machine under a        *)
(* scheduler that never interleaves and never crashes (`NoInterleave`),    *)
(* and the 95 catalogue properties hold of exactly those runs. WHICH OF    *)
(* THEM SURVIVE THE OTHER RUNS is the question.                            *)
(*                                                                         *)
(* Three answers are possible for each property, and they are not equally  *)
(* interesting:                                                            *)
(*                                                                         *)
(*   - it survives because of how the STATE is shaped. `UnitCoherent` is   *)
(*     the example: a promise and its task are one object, so one          *)
(*     `PutObject` moves both and no interleaving can catch them           *)
(*     disagreeing. Fusing them bought that, and it cost nothing.          *)
(*                                                                         *)
(*   - it survives only if the EFFECTS ARE ORDERED right. `WheelComplete`  *)
(*     is the example: the wheel is a second store, so arming and writing  *)
(*     are two moments, and which comes first decides whether a crash      *)
(*     leaves noise or silence. This is where the handlers have to be      *)
(*     careful, and where a handler that is merely correct in the Lean is  *)
(*     not yet correct here.                                               *)
(*                                                                         *)
(*   - it does not survive at all without something this module does not   *)
(*     have. That is the finding worth having, and the candidate is the    *)
(*     absent fence: two steps read one object, both write, one update is  *)
(*     lost. Whether any catalogue property actually falls to that -- and  *)
(*     which -- is the experiment.                                         *)
(*                                                                         *)
(* MULTI-UNIT WRITES are where the third answer is most likely. These      *)
(* handlers touch more than one object, which now means more than one      *)
(* `PutObject`, applied at different moments with the world running        *)
(* between them:                                                           *)
(*                                                                         *)
(*   `promiseRegisterCallback` writes the awaited promise while reading    *)
(*   the awaiter; `taskSuspend` writes every awaited promise in its action *)
(*   list; `CallbackDrain` writes the awaited promise (striking the        *)
(*   callback) and the awaiter's task (resuming it).                       *)
(*                                                                         *)
(* Verus never has to face this: its document is a whole `Workflow` --     *)
(* `promises: Map<Id, Promise>`, plural -- so both ends of a callback live *)
(* in one document and one `PutDocument` moves them together. That is a    *)
(* fact about how it CHUNKS the store, and this module deliberately does   *)
(* not have it, because the question is what the protocol needs rather     *)
(* than what one storage layout provides.                                  *)
(*                                                                         *)
(* `SameOrigin` is the conjecture that would restore it: if the two ends   *)
(* of every callback share an origin, then chunking by origin makes every  *)
(* multi-unit write single-chunk again. Nothing here makes it true.        *)
(***************************************************************************)

SameOrigin ==
    \A o \in DOMAIN objects :
        \A w \in objects[o].promise.callbacks : w.origin = o.origin

Draining(r) == steps[r].phase = "perform"

NoInterleave ==
    \A a, b \in DOMAIN steps : (Draining(a) /\ Draining(b)) => a = b

=============================================================================
