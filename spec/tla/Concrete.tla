-------------------------------- MODULE Concrete --------------------------------
(***************************************************************************)
(* The same protocol under the Verus executor's discipline: ONE DOCUMENT   *)
(* PER ORIGIN, and a compare-and-swap on it.                               *)
(*                                                                         *)
(* `Abstract` deliberately has no fence, and TLC said what that costs: two *)
(* steps read one object, both write, the second erases the first --       *)
(* `preserved_settled_promise_record` and `WheelComplete` fall together.   *)
(* This module is the other half of the experiment.                        *)
(*                                                                         *)
(* IT CHANGES NOTHING ABOUT THE PROTOCOL. The handlers, the effects, the   *)
(* alphabet, the wheel and the invariants all come from `Abstract` through *)
(* the instance below, so a difference in behaviour is a difference in the *)
(* EXECUTOR and cannot be anything else.                                   *)
(*                                                                         *)
(* Two changes, both from `src/concrete_spec/executor.rs`:                 *)
(*                                                                         *)
(*   - the store is keyed by ORIGIN, and one document holds every object   *)
(*     at that origin. This is `Workflow.promises: Map<Id, Promise>` --    *)
(*     plural -- and it is what makes a multi-unit write single-document   *)
(*     whenever the units share an origin.                                 *)
(*                                                                         *)
(*   - every write carries the etag its decision was read under            *)
(*     (`expect: etag_of(held)`). If the document has moved, the write is  *)
(*     REFUSED and the step goes back to deciding --                       *)
(*     `restart(s, rid, ..)`. Nothing stale lands.                         *)
(*                                                                         *)
(* THE NAMES ARE DELIBERATELY THE SAME. `timeouts`, `outbox`, `steps` and  *)
(* `now` mean here exactly what they mean in `Abstract`, and the instance  *)
(* below leaves them alone. Only `objects` needs saying, because only      *)
(* `objects` is represented differently -- which is the whole content of   *)
(* this refinement, and it should be the only thing in the `WITH`.         *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, Variants, Apalache

CONSTANTS Origin, Rest, Address, Pid, Value, Ttl, Rid, Implemented, NoPid, NoAddr,
          NoValue, RetryTimeout, MaxTime, MaxVersion, MaxBatch,
          Fenced

VARIABLES
    docs,       \* [ORIGIN -> ($id -> $object)] -- one document per origin
    timeouts,   \* the wheel. Same variable, same meaning as Abstract's
    outbox,     \* likewise
    steps,      \* likewise, plus `expect` and `org`
    now         \* likewise

vars == <<docs, timeouts, outbox, steps, now>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE ABSTRACTION FUNCTION                                                *)
(*                                                                         *)
(* What a chunked store denotes: the flat one that forgets the chunking.   *)
(* An identifier already carries the origin it belongs to, so finding the  *)
(* document an object lives in is `i.origin` and nothing else -- which is  *)
(* what the pair-shaped identifier was for.                                *)
(*                                                                         *)
(* The etags denote NOTHING. There is no abstract variable they map to,    *)
(* and that is the point of them: a fence is machinery for keeping a       *)
(* promise the abstract machine states without it.                         *)
(***************************************************************************)

Objects == [ i \in UNION { DOMAIN docs[o] : o \in Origin } |-> docs[i.origin][i] ]

A == INSTANCE Abstract WITH objects <- Objects

(***************************************************************************)
(* Three of this machine's six variables map to NOTHING, and that is the   *)
(* whole shape of the result:                                              *)
(*                                                                         *)
(*     expect     a fence. The document a step decided against, compared   *)
(*                on the way in, and machinery for keeping a promise the   *)
(*                abstract machine states without it                       *)
(*     timeouts   an index, so an executor need not scan for what is due.  *)
(*                `Abstract` reads deadlines off the objects               *)
(*     steps      work in flight. `Abstract` has no interval between       *)
(*                deciding and writing, so it has nothing to track         *)
(*                                                                         *)
(* `outbox` and `now` map by name; only `objects` needs saying. So every   *)
(* transition here that touches nothing but the three unmapped variables   *)
(* is a STUTTER upstairs -- submitting, deciding, arming, disarming, and   *)
(* a refused compare-and-swap sending a step back to decide again.         *)
(*                                                                         *)
(* Exactly one transition is not a stutter: the `PutObject` that lands.    *)
(* THAT is where the abstract machine takes its single atomic step, and it *)
(* is why the fence matters -- the etag is what makes the write land       *)
(* against the state it was decided against, which is the only way one     *)
(* instant can stand for the whole decision.                               *)
(*                                                                         *)
(* Nothing above needed a `Restart` in `Abstract`. An earlier draft mapped *)
(* `steps` across, so a refused CAS showed up as an abstract step running  *)
(* backwards and had to be legislated for. Hiding the bookkeeping is what  *)
(* removed the need -- the retry is not a smaller abstract step, it is no  *)
(* abstract step at all.                                                   *)
(***************************************************************************)

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE EXECUTOR                                                            *)
(***************************************************************************)

(* Which document a step touches.
   
   THIS IS WHERE THE ONE-DOCUMENT PREMISE BITES. `Process` hands the
   handler `docs[o]` and nothing else, so a handler that reads or writes
   an object at another origin cannot be served. Several now do:
   `promiseRegisterCallback` reads the awaiter, `taskSuspend` writes
   every awaited promise, `CallbackDrain` writes the awaiter's task, and
   `taskFence` names a target that must be a different ID -- which is
   not the same as a different origin, and nothing constrains the
   origin.
   
   With a single origin every object is in one document and all of them
   are single-document operations -- which is `SameOrigin` holding, by
   construction rather than by proof. The model runs that way. Raising
   `|Origin|` above one is exactly the experiment `SameOrigin` names,
   and this operator is where it would have to be answered: a
   cross-origin handler needs either two document reads or a
   transaction, and this executor offers neither.
   @type: $event => ORIGIN; *)
OriginOf(ev) ==
    LET p(tag) == VariantGetUnsafe(tag, ev) IN
    CASE VariantTag(ev) = "PromiseGet"              -> p("PromiseGet").id.origin
      [] VariantTag(ev) = "PromiseCreate"           -> p("PromiseCreate").req.id.origin
      [] VariantTag(ev) = "PromiseSettle"           -> p("PromiseSettle").req.id.origin
      [] VariantTag(ev) = "PromiseRegisterCallback" ->
             p("PromiseRegisterCallback").req.awaited.origin
      [] VariantTag(ev) = "PromiseRegisterListener" ->
             p("PromiseRegisterListener").awaited.origin
      [] VariantTag(ev) = "TaskGet"       -> p("TaskGet").id.origin
      [] VariantTag(ev) = "TaskCreate"    -> p("TaskCreate").action.id.origin
      [] VariantTag(ev) = "TaskAcquire"   -> p("TaskAcquire").id.origin
      [] VariantTag(ev) = "TaskFence"     -> p("TaskFence").id.origin
      [] VariantTag(ev) = "TaskSuspend"   -> p("TaskSuspend").id.origin
      [] VariantTag(ev) = "TaskFulfill"   -> p("TaskFulfill").id.origin
      [] VariantTag(ev) = "TaskRelease"   -> p("TaskRelease").id.origin
      [] VariantTag(ev) = "TaskHalt"      -> p("TaskHalt").id.origin
      [] VariantTag(ev) = "TaskContinue"  -> p("TaskContinue").id.origin
      [] VariantTag(ev) = "Timeout"       -> p("Timeout").id.origin
      [] VariantTag(ev) = "ListenerDrain" -> p("ListenerDrain").id.origin
      [] VariantTag(ev) = "CallbackDrain" -> p("CallbackDrain").id.origin
      \* heartbeat names a SET of tasks and the searches name none
      [] OTHER -> CHOOSE o \in Origin : TRUE

Fresh(ev) ==
    [ ev |-> ev, phase |-> "process", pending |-> << >>,
      expect |-> SetAsFun({}), at |-> 0, org |-> OriginOf(ev) ]

Put(f, k, v) == [x \in (DOMAIN f) \cup {k} |-> IF x = k THEN v ELSE f[x]]
Drop(f, k)   == [x \in (DOMAIN f) \ {k}    |-> f[x]]

SubmitExternal(ev) ==
    /\ ev \in A!ExternalEvent
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* THE ASYMMETRY, stated plainly, because it is the point of having two
   machines rather than one:

     Abstract may take an internal step whenever the TIMEOUT VALUES ON AN
     OBJECT allow it.

     Concrete may take one only when there is an ARMED ENTRY for it.

   So this machine is strictly more restricted, and the two directions of
   that are not the same:

     - for SAFETY it is free. Fewer behaviours is what refinement wants,
       and an entry that fires with nothing due merely produces a step
       that writes nothing -- a stutter upstairs.

     - for LIVENESS it is the whole risk. `A!Fairness` says a deadline
       that has come due is eventually acted on. Down here that can only
       happen if the wheel holds an entry for it, so an object carrying a
       deadline the wheel has forgotten is a timeout that never fires and
       an abstract obligation this machine cannot meet.

   Which is why `C_WheelComplete` is not a side condition somebody
   thought to write down. It is exactly what `A!Spec` demands of anything
   that keeps an index instead of reading the objects, and if it holds,
   every internal step `Abstract` may take is one `Concrete` may take too.
   @type: $event => Bool; *)
Fires(ev) ==
    CASE VariantTag(ev) = "Timeout" ->
             LET d == VariantGetUnsafe("Timeout", ev) IN
             \E e \in timeouts : /\ e.id   = d.id
                                /\ e.kind = d.kind
                                /\ e.at  <= now
      [] OTHER -> FALSE

SubmitInternal(ev) ==
    /\ ev \in A!InternalEvent
    /\ Fires(ev)
    /\ \E r \in Rid \ DOMAIN steps : steps' = Put(steps, r, Fresh(ev))
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* @type: (($id -> $object), Int) => Set({ id: $id, kind: Str }); *)
DrainableTimeouts(doc, t) ==
    { [id |-> i, kind |-> "promise"]
        : i \in { j \in DOMAIN doc : /\ doc[j].promise.state = "pending"
                                     /\ doc[j].promise.timeoutAt <= t } }

(* NOTHING HAPPENS OUTSIDE A REQUEST OR A TIMER. That is the rule this
   executor lives by, and it is why `Fires` is the wheel and nothing
   else: a drain is not work this machine goes looking for and not an
   event it can schedule.

   It is what happens TO A DOCUMENT when a request or a timer touches
   one. So it is written here as a transformation of the document, with
   no event vocabulary involved -- `drain_doc` inside `prepare_doc`.

   A settled promise still carrying a name owes that name an answer: a
   listener gets `Unblock`, a callback gets its awaiter resumed. Both
   strike the name in the same write that answers it, which is why a
   drain happens once.
   @type: (($id -> $object), Int) => Set({ id: $id, address: ADDR }); *)
DrainableListeners(doc, t) ==
    UNION { { [id |-> i, address |-> a] : a \in doc[i].promise.listeners }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

(* @type: (($id -> $object), Int) => Set({ id: $id, awaiter: $id }); *)
DrainableCallbacks(doc, t) ==
    UNION { { [id |-> i, awaiter |-> w] : w \in doc[i].promise.callbacks }
              : i \in { j \in DOMAIN doc :
                            \/ doc[j].promise.state /= "pending"
                            \/ doc[j].promise.timeoutAt <= t } }

(* SWEEPING A CHOSEN AMOUNT. The drain itself is PROTOCOL, so it is the
   protocol's own operator that performs it -- `A!ProcessListener` and
   `A!ProcessCallback` are functions from a document to effects, not
   events being scheduled, and calling one is not this machine taking an
   internal step.

   The document is threaded so two drains on one promise do not
   overwrite each other's strike. One pass, not a fixpoint: a drain that
   only becomes possible because of another waits for the next access.
   @type: (($id -> $object), Int, Set({ id: $id, address: ADDR }),
           Set({ id: $id, awaiter: $id }))
              => { doc: $id -> $object, fx: Seq($effect) }; *)
Sweep(doc, t, TS, LS, CS) ==
    LET EnvOf(d) == [ objects |-> d, now |-> t,
                      config  |-> [retryTimeout |-> RetryTimeout] ]
        Step(H(_,_), st, req) ==
            LET out == H(req, EnvOf(st.doc))
            IN  [ doc |-> A!PutsInto(st.doc, out.effects),
                  fx  |-> st.fx \o out.effects ]
        afterT == ApaFoldSet(LAMBDA st, d : Step(A!ProcessPromiseTimeout, st, d),
                             [doc |-> doc, fx |-> << >>], TS)
        afterL == ApaFoldSet(LAMBDA st, d : Step(A!ProcessListener, st, d),
                             afterT, LS)
    IN  ApaFoldSet(LAMBDA st, d : Step(A!ProcessCallback, st, d), afterL, CS)

(* Read ONE DOCUMENT, decide against it, and remember the etag it was read
   under. The handler is `Abstract`'s: the protocol does not know it is
   being run by a different executor. *)
(* MAINTAINING THE INDEX IS THE EXECUTOR'S JOB, and this is where it
   happens. `Abstract` states what a handler writes and stops there --
   a deadline is a field on an object, and `internal.lean` never says
   arm or disarm. A machine that keeps a WHEEL has to work out what
   that write did to the deadlines it indexes, and the only way to know
   is to compare the document before against the document after.

   Verus does this in `prepare`, from `w0` and `w2`:

     let m1 = state::min_deadline(w2);
     let sched = if m1 is Some
         && (m1 != state::min_deadline(w0) || completing is Some) { ... }

   Two differences from Verus worth naming rather than hiding. It arms
   ONE timer at the earliest deadline and carries a generation to make
   stale firings recognisable; this keeps one entry per object and kind,
   which is a coarser wheel with no generations, so `WheelSound` failing
   here is partly that choice and not only the arm-before-write order.
   And `completing` gives Verus a reason to re-arm even when the minimum
   has not moved, which has no counterpart here.
   @type: (ORIGIN, Set({ id: $id, obj: $object })) => Seq($effect); *)
Before(o, i) == IF i \in DOMAIN docs[o] THEN docs[o][i] ELSE A!Absent

ArmFor(i, old, new, k) ==
    IF A!Deadline(new, k) /= A!NoTime /\ A!Deadline(new, k) /= A!Deadline(old, k)
    THEN << Variant("ArmTimeout",
                    [entry |-> [at |-> A!Deadline(new, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

DisarmFor(i, old, new, k) ==
    IF A!Deadline(old, k) /= A!NoTime /\ A!Deadline(old, k) /= A!Deadline(new, k)
    THEN << Variant("DisarmTimeout",
                    [entry |-> [at |-> A!Deadline(old, k), id |-> i, kind |-> k]]) >>
    ELSE << >>

(* Arm before the write and disarm after, which is Verus's
   `sched + put + ack`. Arming first means a crash between the arm and
   the write leaves an entry for an object that is not there -- noise,
   which `C_WheelSound` sees and which the handler re-checks away when
   it fires. Writing first would leave an object carrying a deadline
   nothing will fire -- silence, which `C_WheelComplete` sees and which
   nothing recovers from. Flip the two folds and the checker should hand
   back a trace ending in a `Crash` with a deadline nobody holds.

   This is a claim about EXECUTORS THAT KEEP AN INDEX. It used to sit in
   `Abstract`, which made it look like protocol; it is not, and no
   handler states it. *)
Arms(o, W) ==
    ApaFoldSet(LAMBDA acc, w :
                 acc \o ArmFor(w.id, Before(o, w.id), w.obj, "promise")
                     \o ArmFor(w.id, Before(o, w.id), w.obj, "lease")
                     \o ArmFor(w.id, Before(o, w.id), w.obj, "retry"),
               << >>, W)

Disarms(o, W) ==
    ApaFoldSet(LAMBDA acc, w :
                 acc \o DisarmFor(w.id, Before(o, w.id), w.obj, "promise")
                     \o DisarmFor(w.id, Before(o, w.id), w.obj, "lease")
                     \o DisarmFor(w.id, Before(o, w.id), w.obj, "retry"),
               << >>, W)

Process(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "process"
    /\ \E TS \in SUBSET DrainableTimeouts(docs[steps[r].org], now),
          LS \in SUBSET DrainableListeners(docs[steps[r].org], now),
          CS \in SUBSET DrainableCallbacks(docs[steps[r].org], now) :
       LET o   == steps[r].org
           swept == Sweep(docs[o], now, TS, LS, CS)
           env == [ objects  |-> swept.doc,
                    timeouts |-> timeouts,
                    outbox   |-> outbox,
                    now      |-> now,
                    config   |-> [retryTimeout |-> RetryTimeout] ]
           out == A!Handle(steps[r].ev, env)
           fx  == swept.fx \o out.effects
           W   == A!Puts(fx)
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",
                                  ![r].pending = Arms(o, W) \o fx
                                                 \o Disarms(o, W),
                                  ![r].expect  = docs[o],
                                  ![r].at      = now]
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

(* One effect. A write whose document has moved since the read is REFUSED,
   and the step goes back to deciding -- which under the abstraction is
   nothing at all.

   THE FENCE COVERS THE CLOCK AS WELL AS THE DOCUMENT, and that is not
   belt-and-braces -- the refinement demanded it. With only the etag
   compared, TLC returned: decide at `now = 0`, `Clock` ticks, write lands
   at `now = 1`. Upstairs the write IS the whole step, so the abstract
   machine runs the handler at the instant the write lands and gets a
   different answer -- a `createdAt`, a `settledAt`, a born-settled promise
   where the concrete decided a pending one. A decision is only valid at
   the instant it was made, and a store etag says nothing about the clock.

   Verus takes the other road: `Phase::Perform` freezes `at` at prepare
   time and every effect uses it, so the skew is absorbed rather than
   refused. That works against the Lean, where `now` is an argument to
   each step rather than a variable running underneath it. Here it is a
   variable, so the choice is to re-decide, and `steps[r].at` is what the
   comparison is against.

   `Fenced` is a CONSTANT so the fence can be switched OFF and the
   refinement made to prove its own necessity: with `Fenced = FALSE` a
   stale decision lands, and `Spec => A!Spec` should fail and hand back
   the trace that shows why.

   ONE STEP, ONE DOCUMENT WRITE -- WHICH IS WHAT THE EXECUTOR DOES.

   A step lands every `PutObject` it decided on in a single write, and
   every message those writes justify with them. Only the wheel effects
   stay separate, which is the premise the whole model is here to test.

   This operator used to apply one effect per `Perform`, walking the
   list. Two refinement counterexamples came out of that, and both were
   reported as findings about the protocol before they were understood.
   Neither was. They were this module having drifted away from the
   executor it claims to model.

   `Process` hands `pending` the effect list `Abstract!Handle` returned,
   which is the right instinct -- the protocol should not know which
   executor is running it. But `Abstract!Commit` emits a `PutObject` PER
   OBJECT, and upstairs that granularity carries no meaning at all,
   because `Apply` folds the whole list at one instant. Walking the same
   list one entry at a time turns a per-object DESCRIPTION into a
   sequence of per-object STORE OPERATIONS. `Write2` became two document
   writes; `CommitAll` became N.

   The executor emits exactly one, and its body is the whole workflow:

     let put = seq![Effect::PutDocument { key, expect: etag_of(held),
                                          body: w2 }];
     ... sched + put + ack(..) + emits(..) + respond

   The messages are already in that body -- the outbox is a field of the
   workflow, and `lemma_sends_outbox` fixes `step(w, cs, now).outbox` as
   `fold_send(w.outbox, sends_step(w, cs, now))`. `Effect::Send` carries
   an entry from that record onto the WIRE, `s.sent`, which this model
   does not have. So `outbox` here is the record, and the record belongs
   in the write.

   THE LESSON IS ABOUT EFFECT LISTS, not about outboxes. A list written
   for an atomic machine does not carry execution granularity, and
   reading it as though it did is silent: `C_TypeOK` passes, so does
   `C_UnitCoherent`, so do the catalogue properties, because every one
   of them is a statement about states and this was an error about WHEN
   states change. The refinement is what caught it, and catching drift
   between the executor and its model is the thing a refinement against
   a hand-written abstraction function is genuinely FOR.

   `Puts` and `Says` are taken over the whole remaining `pending`, and
   the write is built exactly as `Apply` builds `objects'` -- same fold,
   same `CHOOSE` for two writes to one id -- so the two agree by
   construction rather than by luck. Folding at the write rather than in
   `Process` is a structural difference from the executor, which folds
   when it prepares; the body is fixed at `Process` either way, so the
   two are the same machine.

   The `Send` branch below survives for handlers that emit without
   writing: there the message alone IS the whole decision, and `Apply`
   with an empty `Puts` is exactly a step that moves `outbox` and leaves
   `objects` where it was. The arm and disarm branches survive because
   the wheel is the one thing still allowed to lag, which is the whole
   experiment. *)
Perform(r) ==
    /\ r \in DOMAIN steps
    /\ steps[r].phase = "perform"
    /\ IF steps[r].pending = << >>
       THEN /\ steps' = Drop(steps, r)
            /\ UNCHANGED <<docs, timeouts, outbox>>
       ELSE LET e == Head(steps[r].pending)
                o == steps[r].org
            IN CASE VariantTag(e) = "PutObject" ->
                    IF Fenced => (docs[o] = steps[r].expect /\ now = steps[r].at)
                    THEN /\ docs'  = [docs EXCEPT ![o] =
                                         LET W == A!Puts(steps[r].pending) IN
                                         [ i \in (DOMAIN docs[o])
                                                  \cup {w.id : w \in W} |->
                                             IF \E w \in W : w.id = i
                                             THEN (CHOOSE w \in W : w.id = i).obj
                                             ELSE docs[o][i] ]]
                         /\ outbox' = LET S == A!Says(steps[r].pending) IN
                                        { x \in outbox :
                                            ~\E m \in S : A!MsgKey(x) = A!MsgKey(m) } \cup S
                         /\ steps' = [steps EXCEPT ![r].pending =
                                        SelectSeq(Tail(@),
                                                  LAMBDA f :
                                                    VariantTag(f) \notin
                                                      {"PutObject", "Send"})]
                         /\ UNCHANGED timeouts
                    ELSE /\ steps' = [steps EXCEPT ![r].phase   = "process",
                                                   ![r].pending = << >>]
                         /\ UNCHANGED <<docs, timeouts, outbox>>
                 [] VariantTag(e) = "ArmTimeout" ->
                    /\ timeouts' = timeouts \cup {VariantGetUnsafe("ArmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, outbox>>
                 [] VariantTag(e) = "DisarmTimeout" ->
                    /\ timeouts' = timeouts \ {VariantGetUnsafe("DisarmTimeout", e).entry}
                    /\ steps'    = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, outbox>>
                 [] OTHER ->
                    /\ outbox' = LET en == VariantGetUnsafe("Send", e).entry
                                 IN  {x \in outbox : A!MsgKey(x) /= A!MsgKey(en)} \cup {en}
                    /\ steps'  = [steps EXCEPT ![r].pending = Tail(@)]
                    /\ UNCHANGED <<docs, timeouts>>
    /\ UNCHANGED now

Crash(r) ==
    /\ r \in DOMAIN steps
    /\ steps' = Drop(steps, r)
    /\ UNCHANGED <<docs, timeouts, outbox, now>>

Clock ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<docs, timeouts, outbox, steps>>

Init ==
    /\ docs     = [o \in Origin |-> SetAsFun({})]
    /\ timeouts = {}
    /\ outbox   = {}
    /\ steps    = SetAsFun({})
    /\ now      = 0

(***************************************************************************)
(* CRASHES ARE ALWAYS IN THE MACHINE                                       *)
(*                                                                         *)
(* There was a `Crashing` constant here, and switching it off was how       *)
(* liveness got claimed. That is a worse thing to say than it looks: it     *)
(* claims something about an executor that never fails, which is not an     *)
(* executor anyone has. `EventuallyStable` says the thing actually meant    *)
(* -- steps do not fail FOREVER -- so the crash can stay, both claims can   *)
(* be made about the same machine, and the difference between them is one   *)
(* formula rather than one configuration.                                   *)
(***************************************************************************)

Next ==
    \/ \E ev \in A!ExternalEvent : SubmitExternal(ev)
    \/ \E ev \in A!InternalEvent : SubmitInternal(ev)
    \/ \E r \in DOMAIN steps : Process(r) \/ Perform(r) \/ Crash(r)
    \/ Clock

(* The fairness `Abstract` asks of itself, asked of this executor. It has
   to be here: `A!Spec` carries `A!Fairness`, and a machine that may
   stall forever cannot implement a machine that may not.

   `SubmitInternal` is STRONGLY fair, and that is not a modelling
   convenience -- weak fairness was not enough and TLC said why. Upstairs
   an internal step is available whenever an object's deadline says so,
   full stop. Down here it is available only when a step slot is free,
   and `|Rid|` is finite. So the counterexample was a cycle in which
   clients kept arriving, the slots kept filling, and a due timeout with
   a properly armed entry was never once submitted -- legal under weak
   fairness, because the action was not CONTINUOUSLY enabled, only
   enabled again and again.

   Strong fairness is the honest reading of what the abstract machine
   demands: an executor must not indefinitely prefer client work to work
   that has come due. That is a scheduling obligation on the real server
   -- a fair choice between the API path and the timer path -- and it is
   the kind of requirement that is invisible until liveness is checked
   against a bounded resource. *)
(***************************************************************************)
(* CRASHES THAT STOP, without a counter                                    *)
(*                                                                         *)
(* `Crashing = FALSE` says an executor never fails, which is not what we   *)
(* mean and not something anyone would claim. What we mean is that steps   *)
(* do not fail FOREVER, and that is sayable directly:                      *)
(*                                                                         *)
(*     <>[][ \A r \in Rid : ~Crash(r) ]_vars                               *)
(*                                                                         *)
(* `[][A]_vars` holds of a step when it satisfies `A` or leaves `vars`     *)
(* alone. A crash does neither -- it removes a step -- so `[][~Crash]_vars`*)
(* forbids crash steps, and `<>[]` says: from some point on, forever.      *)
(* No bound, no counter, no constant. Just "not forever".                  *)
(*                                                                         *)
(* Crashes stay in the machine, safety is still checked against them, and  *)
(* only the liveness claim is conditioned on their ceasing. That is the    *)
(* shape every liveness result under failure has -- an eventual-stability  *)
(* assumption -- written as a formula instead of an English caveat.        *)
(*                                                                         *)
(* WHERE IT GOES IS NOT FREE. The obvious spelling is a hypothesis on the  *)
(* property, `EventuallyStable => A!Spec`, and TLC refuses it:             *)
(*                                                                         *)
(*     Temporal formulas containing actions must be of forms               *)
(*     <>[]A or []<>A.                                                     *)
(*                                                                         *)
(* An action-containing formula is admitted only in those two shapes, and  *)
(* an implication with one as its antecedent is not among them. As a       *)
(* CONJUNCT OF THE SPECIFICATION it is accepted, which is what `SpecStable`*)
(* is. `RefinesSpecStable` is kept because it says what is meant; it is    *)
(* just not the form a checker will take.                                  *)
(*                                                                         *)
(* Note also what this does NOT say. `<>[]` means crashes stop ENTIRELY    *)
(* after some point, which is stronger than "not always". The weaker       *)
(* reading -- infinitely many crashes, but never always the attempt that   *)
(* matters -- is what fairness would express, and it is exactly what the   *)
(* slot-recycling counterexample showed cannot be written over `Rid`:      *)
(* fairness names a slot, and a throwaway no-op step in the same slot      *)
(* discharges it. Eventual stability is the honest thing that is actually  *)
(* expressible here.                                                       *)
(***************************************************************************)

EventuallyStable == <>[][ \A r \in Rid : ~Crash(r) ]_vars


Fairness ==
    /\ \A r  \in Rid             : WF_vars(Process(r) \/ Perform(r))
    /\ \A ev \in A!InternalEvent : SF_vars(SubmitInternal(ev))
    /\ WF_vars(Clock)
    /\ EventuallyStable

(* The stronger promise, as a SECOND SPECIFICATION rather than a constant
   guard. `StrongSteps => \A r : SF_vars(..)` looked like the tidy way to
   make it switchable, and it is not: TLC recognises fairness only in a
   restricted syntactic form -- WF/SF terms and quantifications over them
   -- and an implication is not in that grammar. It produced a
   "counterexample" that plainly violated the very condition it was
   supposed to assume, which is how the mistake was caught. A fairness
   condition that is not syntactically a fairness condition is not an
   assumption; it is decoration. *)
FairnessSF ==
    /\ Fairness
    /\ \A r \in Rid : SF_vars(Process(r))
    /\ \A r \in Rid : SF_vars(Perform(r))

Spec   == Init /\ [][Next]_vars /\ Fairness
SpecSF == Init /\ [][Next]_vars /\ FairnessSF

-----------------------------------------------------------------------------
(***************************************************************************)
(* THE REFINEMENT                                                          *)
(*                                                                         *)
(* The statement we MEAN is                                                *)
(*                                                                         *)
(*     THEOREM Spec => A!Spec                                              *)
(*                                                                         *)
(* -- every behaviour of the chunked, fenced machine is, once you read a   *)
(* document as the objects it holds, a behaviour of the abstract machine.  *)
(*                                                                         *)
(* No existential is needed any more. `Abstract` has no `steps` and no     *)
(* `timeouts` to fill in, so there is no hidden variable to quantify away  *)
(* and no witness to choose -- the mapping is total and there is only one  *)
(* of it. That is worth more than the convenience: it means the theorem no *)
(* longer depends on a choice I made, and an executor with entirely        *)
(* different bookkeeping is measured by the same statement rather than     *)
(* having to supply its own.                                               *)
(*                                                                         *)
(* `A!Spec` and not `A!Safety`: the obligation includes the abstract       *)
(* machine's FAIRNESS. Refining only safety would let an implementation    *)
(* satisfy the specification by doing nothing -- accepting a request,      *)
(* deciding, and stalling forever is safe, and is not an implementation of *)
(* anything.                                                               *)
(*                                                                         *)
(* Note the direction. It does NOT say the two machines admit the same     *)
(* behaviours; the fence exists precisely to admit FEWER.                  *)
(*                                                                         *)
(* And it is a real obligation rather than a formality: the first time it  *)
(* was checked it FAILED, on a refused compare-and-swap sending a step     *)
(* from "perform" back to "process". `Abstract` had no action that went    *)
(* backwards, so every behaviour containing a retry was outside the        *)
(* specification. `Abstract!Restart` is what that failure bought.          *)
(*                                                                         *)
(* That failure is gone, and the way it went is the lesson. It was never a *)
(* fact about the protocol -- it came from mapping `steps` across, so that *)
(* an executor's retry had to be legislated for upstairs. Take the         *)
(* bookkeeping out of the abstract machine and the retry stops being a     *)
(* step it has to permit. `Abstract!Restart` is gone with it.              *)
(***************************************************************************)

(* Both halves, named separately, so that checking one never means editing
   the module. An earlier version had a single `Refinement` that got
   sed-swapped between the two for one-off runs -- and a commit landed
   while it was swapped. A definition you have to edit to check is a
   definition that will eventually be committed mid-edit. *)

RefinesSafety == A!Safety
RefinesSpec   == A!Spec

THEOREM Spec => A!Spec

(* The invariants the abstract machine fails, asked of this one. That the
   fence restores them is the other half of the result, and it is not
   implied by the refinement -- refinement bounds what this machine MAY
   do, and says nothing about what it GUARANTEES. *)
C_TypeOK          == A!TypeOK
C_UnitCoherent    == A!UnitCoherent

(* The wheel invariants live HERE now, because the wheel does. They are
   not among the 95 and could not be: they say an INDEX agrees with the
   deadlines it indexes, and the protocol has no index.

   `WheelSound` fails, and should: arming before writing admits an entry
   for an object that is not there yet, which is noise the handler
   re-checks away. `WheelComplete` is the one that matters -- a deadline
   nothing holds is a timeout that never fires, and nothing recovers. *)
C_WheelSound ==
    \A e \in timeouts :
        /\ e.id \in DOMAIN Objects
        /\ A!Deadline(Objects[e.id], e.kind) = e.at

C_WheelComplete ==
    \A o \in DOMAIN Objects, k \in A!DeadlineKind :
        A!Deadline(Objects[o], k) /= A!NoTime =>
            [at |-> A!Deadline(Objects[o], k), id |-> o, kind |-> k] \in timeouts

(* A WITNESS, NOT AN INVARIANT.

   `CallbackDrain` writes two objects, so it emits two `PutObject`s and
   this executor performs them one at a time. The state in between --
   one object written, the other still in hand -- is the multi-unit
   write the header warns about, and the expectation was that it would
   break the refinement the same way a write with its message still in
   hand does.

   There is a reason it might not, and it is worth stating before the
   answer: refinement asks for SOME abstract step between the two mapped
   states, not the same one. A lone object write is what half the
   alphabet does -- a settle, a create, a claim -- so the half-finished
   pair may be a state `Abstract` reaches all the time, just under a
   different request than the one this machine is running. A message has
   no such cover: nothing in `Abstract` ever says without writing in the
   same breath. If that asymmetry is the whole story then writes may be
   split and messages may not.

   THESE THREE OPERATORS EXIST BECAUSE THE FIRST ANSWER WAS VACUOUS. A
   run with `MaxTime = 0` reported no violation and had never run the
   drain at all: a promise created at time 0 with a timeout of 0 is born
   settled, so no callback is ever registered and the multi-unit write
   never happens. `NoDrainRan` is what caught it. With the clock back,
   all three fire -- the drain runs, a step carries two puts, and the
   half-written state is reached -- so a refinement check against that
   configuration is asking the question rather than missing it.

   With one `Rid` nothing else writes, so an etag past what the step
   read means the step itself wrote; a `PutObject` still pending means
   it has not finished. Check `~SplitWrite` and TLC hands back the
   state, which is the witness. *)
SplitWrite ==
    \E r \in DOMAIN steps :
        /\ steps[r].phase = "perform"
        /\ docs[steps[r].org] /= steps[r].expect
        /\ \E j \in DOMAIN steps[r].pending :
               VariantTag(steps[r].pending[j]) = "PutObject"

NoSplitWrite == ~SplitWrite

DrainRan   == \E r \in DOMAIN steps : VariantTag(steps[r].ev) = "CallbackDrain"
NoDrainRan == ~DrainRan

TwoPuts == \E r \in DOMAIN steps :
    Cardinality({ j \in DOMAIN steps[r].pending :
                    VariantTag(steps[r].pending[j]) = "PutObject" }) > 1
NoTwoPuts == ~TwoPuts

CT_preserved_settled_promise_record    == [][A!preserved_settled_promise_record]_vars
CT_consistent_promise_settlement_stamp == [][A!consistent_promise_settlement_stamp]_vars

=============================================================================
