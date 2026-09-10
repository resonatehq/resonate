------------------------------- MODULE Invariants -------------------------------
(* THE GUARANTEES, from spec/02-abstract/properties.lean, one for one under
   the Lean catalogue's names. Stated over the abstract data model; `Abstract`
   instances this with its own variables, `Concrete` through its abstraction
   function, so one file says what both machines owe.

   The mapping from Lean's stores to ours: promises and tasks coalesce into
   `objects`, and a task exists iff its state is not "none". Option fields
   read absence as NoTime/NoPid/NoValue. Lists of callbacks, listeners and
   resumes are sets here, which settles seven catalogue entries by
   construction rather than by checking:

     well_formed_promise_callbacks_unique     well_formed_promise_listeners_unique
     well_formed_task_resumes_unique          well_formed_store_object_ids_unique
     preserved_promise_no_duplicate_ids       well_formed_store_schedule_ids_unique
                                              (schedules not being modeled at all)

   Not carried, and why: the four well_formed_schedule_* properties and
   preserved_schedule_birth_fields_immutable (no schedules in this model);
   the two *_edge_internal_admissible properties (they quantify over which KIND of step fired, and a next-state
   relation checked with [][...]_vars cannot see that);
   well_formed_config_retry_positive (an ASSUME in Requests, checked at
   parse). consistent_new_unblock_carries_stored_record is carried in the
   state our Unblock actually has -- id and state -- not the five-field
   record Lean's wire message copies. *)
EXTENDS Requests

VARIABLES
    objects,
    outbox,
    now

-----------------------------------------------------------------------------
(* Stage 1 -- state *)

well_formed_promise_created_at_lte_timeout_at ==
    \A i \in DOMAIN objects :
        objects[i].promise.createdAt <= objects[i].promise.timeoutAt

well_formed_promise_pending_created_before_deadline ==
    \A i \in DOMAIN objects :
        objects[i].promise.state = "pending" =>
            objects[i].promise.createdAt < objects[i].promise.timeoutAt

well_formed_promise_settled_at_lte_timeout_at ==
    \A i \in DOMAIN objects :
        objects[i].promise.settledAt /= NoTime =>
            objects[i].promise.settledAt <= objects[i].promise.timeoutAt

well_formed_promise_created_at_lte_settled_at ==
    \A i \in DOMAIN objects :
        objects[i].promise.settledAt /= NoTime =>
            objects[i].promise.createdAt <= objects[i].promise.settledAt

well_formed_promise_settled_at_iff_not_pending ==
    \A i \in DOMAIN objects :
        (objects[i].promise.state /= "pending")
            <=> (objects[i].promise.settledAt /= NoTime)

well_formed_promise_pending_has_no_value ==
    \A i \in DOMAIN objects :
        objects[i].promise.state = "pending" =>
            objects[i].promise.value = NoValue

well_formed_promise_deadline_verdict_matches_timer_tag ==
    \A i \in DOMAIN objects :
        LET p == objects[i].promise IN
        p.settledAt = p.timeoutAt =>
            p.state = (IF p.tags.timer THEN "resolved" ELSE "rejectedTimedout")

well_formed_promise_deadline_settlement_has_no_value ==
    \A i \in DOMAIN objects :
        LET p == objects[i].promise IN
        p.settledAt = p.timeoutAt => p.value = NoValue

well_formed_promise_timer_not_targeted ==
    \A i \in DOMAIN objects :
        ~(objects[i].promise.tags.timer /\ objects[i].promise.tags.targeted)

well_formed_promise_timedout_is_server_owned ==
    \A i \in DOMAIN objects :
        LET p == objects[i].promise IN
        p.state = "rejectedTimedout" => p.settledAt = p.timeoutAt

well_formed_promise_obligations_require_external ==
    \A i \in DOMAIN objects :
        LET p == objects[i].promise IN
        \/ p.callbacks = {} /\ p.listeners = {}
        \/ IsExternal(p)

well_formed_promise_awaiter_is_not_self ==
    \A i \in DOMAIN objects :
        i \notin objects[i].promise.callbacks

well_formed_promise_created_at_lte_now ==
    \A i \in DOMAIN objects :
        objects[i].promise.createdAt <= now

well_formed_promise_settled_at_lte_now ==
    \A i \in DOMAIN objects :
        objects[i].promise.settledAt /= NoTime =>
            objects[i].promise.settledAt <= now

well_formed_task_acquired_iff_has_pid ==
    \A i \in DOMAIN objects :
        (objects[i].task.state = "acquired") <=> (objects[i].task.pid /= NoPid)

well_formed_task_acquired_iff_has_ttl ==
    \A i \in DOMAIN objects :
        (objects[i].task.state = "acquired") <=> (objects[i].task.ttl /= NoTime)

well_formed_task_acquired_iff_has_lease_timeout_at ==
    \A i \in DOMAIN objects :
        (objects[i].task.state = "acquired") <=> (objects[i].task.leaseTimeoutAt /= NoTime)

well_formed_task_pending_iff_has_retry_timeout_at ==
    \A i \in DOMAIN objects :
        (objects[i].task.state = "pending") <=> (objects[i].task.retryTimeoutAt /= NoTime)

well_formed_task_fulfilled_is_cleared ==
    \A i \in DOMAIN objects :
        LET t == objects[i].task IN
        t.state = "fulfilled" =>
            /\ t.pid = NoPid /\ t.ttl = NoTime
            /\ t.leaseTimeoutAt = NoTime /\ t.retryTimeoutAt = NoTime
            /\ t.resumes = {}

well_formed_task_suspended_is_cleared ==
    \A i \in DOMAIN objects :
        LET t == objects[i].task IN
        t.state = "suspended" =>
            /\ t.pid = NoPid /\ t.ttl = NoTime
            /\ t.leaseTimeoutAt = NoTime /\ t.retryTimeoutAt = NoTime

well_formed_task_halted_is_cleared ==
    \A i \in DOMAIN objects :
        LET t == objects[i].task IN
        t.state = "halted" =>
            /\ t.pid = NoPid /\ t.ttl = NoTime
            /\ t.leaseTimeoutAt = NoTime /\ t.retryTimeoutAt = NoTime

well_formed_task_suspended_has_no_resumes ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "suspended" => objects[i].task.resumes = {}

well_formed_task_acquired_version_positive ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "acquired" => 1 <= objects[i].task.version

well_formed_store_outbox_keys_unique ==
    \A e, f \in outbox :
        MsgKey(e) = MsgKey(f) => e = f

consistent_task_iff_targeted_promise ==
    \A i \in DOMAIN objects :
        (objects[i].task.state /= "none") <=> objects[i].promise.tags.targeted

consistent_settled_promise_has_fulfilled_task ==
    \A i \in DOMAIN objects :
        objects[i].promise.state /= "pending" =>
            objects[i].task.state \in {"none", "fulfilled"}

consistent_callback_awaiter_is_targeted ==
    \A i \in DOMAIN objects :
        \A w \in objects[i].promise.callbacks :
            /\ w \in DOMAIN objects
            /\ objects[w].promise.tags.targeted

consistent_outbox_execute_names_existing_task ==
    \A e \in outbox :
        e.message.tag = "Execute" =>
            /\ e.message.id \in DOMAIN objects
            /\ objects[e.message.id].task.state /= "none"

consistent_outbox_never_ahead ==
    \A e \in outbox :
        e.message.tag = "Execute" /\ e.message.id \in DOMAIN objects =>
            e.message.version <= objects[e.message.id].task.version

consistent_outbox_execute_address_is_target_tag ==
    \A e \in outbox :
        e.message.tag = "Execute" /\ e.message.id \in DOMAIN objects =>
            e.address = objects[e.message.id].promise.tags.target

consistent_outbox_unblock_names_settled_promise ==
    \A e \in outbox :
        e.message.tag = "Unblock" =>
            /\ e.message.state /= "pending"
            /\ e.message.id \in DOMAIN objects
            /\ objects[e.message.id].promise.state /= "pending"

consistent_suspended_task_holds_rung ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "suspended" =>
            \/ objects[i].promise.state /= "pending"
            \/ objects[i].promise.timeoutAt <= now
            \/ \E j \in DOMAIN objects : i \in objects[j].promise.callbacks

consistent_settled_task_promise_settled ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "fulfilled" =>
            objects[i].promise.state /= "pending"

well_formed_task_ttl_positive ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "acquired" => 0 < objects[i].task.ttl

well_formed_promise_target_is_nonempty ==
    \A i \in DOMAIN objects :
        objects[i].promise.tags.targeted =>
            objects[i].promise.tags.target /= NoAddr

well_formed_promise_delay_before_deadline ==
    \A i \in DOMAIN objects :
        objects[i].promise.tags.delay < objects[i].promise.timeoutAt

-----------------------------------------------------------------------------
(* Stage 3 -- transition *)

preserved_promise_birth_fields_immutable ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET p == objects[i].promise
                q == objects'[i].promise
            IN  /\ q.param     = p.param
                /\ q.tags      = p.tags
                /\ q.timeoutAt = p.timeoutAt
                /\ q.createdAt = p.createdAt

preserved_settled_promise_record ==
    \A i \in DOMAIN objects :
        objects[i].promise.state /= "pending" =>
            /\ i \in DOMAIN objects'
            /\ LET p == objects[i].promise
                   q == objects'[i].promise
               IN  /\ q.state     = p.state
                   /\ q.settledAt = p.settledAt
                   /\ q.value     = p.value

monotone_promise_set_grows ==
    DOMAIN objects \subseteq DOMAIN objects'

monotone_task_set_grows ==
    \A i \in DOMAIN objects :
        objects[i].task.state /= "none" =>
            /\ i \in DOMAIN objects'
            /\ objects'[i].task.state /= "none"

monotone_task_version_increases_only_on_acquisition ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET t == objects[i].task
                u == objects'[i].task
            IN  IF t.state = "pending" /\ u.state = "acquired"
                THEN u.version = t.version + 1
                ELSE u.version = t.version

preserved_fulfilled_task ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "fulfilled" =>
            /\ i \in DOMAIN objects'
            /\ LET u == objects'[i].task IN
               /\ u.state = "fulfilled"
               /\ u.version = objects[i].task.version
               /\ u.resumes = {}
               /\ u.pid = NoPid /\ u.ttl = NoTime
               /\ u.leaseTimeoutAt = NoTime /\ u.retryTimeoutAt = NoTime

preserved_no_dead_dispatch ==
    \A i \in DOMAIN objects' :
        objects'[i].task.state = "pending" =>
            \/ i \in DOMAIN objects /\ objects[i].task.state = "pending"
            \/ /\ objects'[i].promise.state = "pending"
               /\ now < objects'[i].promise.timeoutAt

preserved_execute_only_for_live_task ==
    \A e \in outbox' :
        e.message.tag = "Execute" =>
            \/ e \in outbox
            \/ /\ e.message.id \in DOMAIN objects'
               /\ objects'[e.message.id].promise.state = "pending"
               /\ now < objects'[e.message.id].promise.timeoutAt

preserved_promise_state_frozen_once_settled ==
    \A i \in DOMAIN objects :
        objects[i].promise.state /= "pending" =>
            /\ i \in DOMAIN objects'
            /\ objects'[i].promise.state = objects[i].promise.state

preserved_promise_settlement_is_one_way ==
    \A i \in DOMAIN objects' :
        objects'[i].promise.state = "pending" =>
            \/ i \notin DOMAIN objects
            \/ objects[i].promise.state = "pending"

consistent_promise_settled_at_moves_with_state ==
    \A i \in DOMAIN objects :
        /\ i \in DOMAIN objects'
        /\ LET p == objects[i].promise
               q == objects'[i].promise
           IN  (q.settledAt /= p.settledAt) <=> (q.state /= p.state)

preserved_promise_value_until_settlement ==
    \A i \in DOMAIN objects :
        /\ i \in DOMAIN objects'
        /\ \/ objects'[i].promise.state /= "pending"
           \/ objects'[i].promise.value = objects[i].promise.value

monotone_promise_callbacks_grow_while_pending ==
    \A i \in DOMAIN objects' :
        objects'[i].promise.state = "pending" =>
            IF i \in DOMAIN objects
            THEN objects[i].promise.callbacks \subseteq objects'[i].promise.callbacks
            ELSE objects'[i].promise.callbacks = {}

monotone_promise_callbacks_shrink_once_settled ==
    \A i \in DOMAIN objects' :
        objects'[i].promise.state /= "pending" =>
            IF i \in DOMAIN objects
            THEN objects'[i].promise.callbacks \subseteq objects[i].promise.callbacks
            ELSE objects'[i].promise.callbacks = {}

monotone_promise_listeners_grow_while_pending ==
    \A i \in DOMAIN objects' :
        objects'[i].promise.state = "pending" =>
            IF i \in DOMAIN objects
            THEN objects[i].promise.listeners \subseteq objects'[i].promise.listeners
            ELSE objects'[i].promise.listeners = {}

monotone_promise_listeners_shrink_once_settled ==
    \A i \in DOMAIN objects' :
        objects'[i].promise.state /= "pending" =>
            IF i \in DOMAIN objects
            THEN objects'[i].promise.listeners \subseteq objects[i].promise.listeners
            ELSE objects'[i].promise.listeners = {}

consistent_promise_state_edge_admissible ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            <<objects[i].promise.state, objects'[i].promise.state>> \in
                { <<"pending", "pending">>,
                  <<"pending", "resolved">>,
                  <<"pending", "rejected">>,
                  <<"pending", "rejectedCanceled">>,
                  <<"pending", "rejectedTimedout">>,
                  <<"resolved", "resolved">>,
                  <<"rejected", "rejected">>,
                  <<"rejectedCanceled", "rejectedCanceled">>,
                  <<"rejectedTimedout", "rejectedTimedout">> }

consistent_task_state_edge_admissible ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' /\ objects[i].task.state /= "none" =>
            <<objects[i].task.state, objects'[i].task.state>> \in
                { <<"pending", "pending">>,
                  <<"pending", "acquired">>,
                  <<"pending", "halted">>,
                  <<"pending", "fulfilled">>,
                  <<"acquired", "pending">>,
                  <<"acquired", "acquired">>,
                  <<"acquired", "suspended">>,
                  <<"acquired", "halted">>,
                  <<"acquired", "fulfilled">>,
                  <<"suspended", "pending">>,
                  <<"suspended", "suspended">>,
                  <<"suspended", "halted">>,
                  <<"suspended", "fulfilled">>,
                  <<"halted", "pending">>,
                  <<"halted", "halted">>,
                  <<"halted", "fulfilled">>,
                  <<"fulfilled", "fulfilled">> }

preserved_task_acquisition_only_from_pending ==
    \A i \in DOMAIN objects' :
        objects'[i].task.state = "acquired" =>
            \/ i \notin DOMAIN objects
            \/ objects[i].task.state \in {"none", "pending", "acquired"}

preserved_task_suspension_only_from_acquired ==
    \A i \in DOMAIN objects' :
        objects'[i].task.state = "suspended" =>
            /\ i \in DOMAIN objects
            /\ objects[i].task.state \in {"acquired", "suspended"}

preserved_task_halted_only_reenters_via_pending ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "halted" =>
            /\ i \in DOMAIN objects'
            /\ objects'[i].task.state \in {"halted", "pending", "fulfilled"}

consistent_settlement_fulfils_task ==
    \A i \in DOMAIN objects :
        objects[i].promise.state = "pending" /\ i \in DOMAIN objects' =>
            \/ objects'[i].promise.state = "pending"
            \/ objects'[i].task.state \in {"none", "fulfilled"}

consistent_task_fulfilment_needs_settlement ==
    \A i \in DOMAIN objects' :
        objects'[i].task.state = "fulfilled" =>
            \/ i \notin DOMAIN objects
            \/ objects[i].task.state \in {"none", "fulfilled"}
            \/ /\ objects[i].promise.state = "pending"
               /\ objects'[i].promise.state /= "pending"

consistent_obligation_discharge_requires_settled ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            \/ /\ objects[i].promise.callbacks \subseteq objects'[i].promise.callbacks
               /\ objects[i].promise.listeners \subseteq objects'[i].promise.listeners
            \/ objects'[i].promise.state /= "pending"

consistent_callback_consumption_resumes_awaiter ==
    \A i \in DOMAIN objects :
        \A w \in objects[i].promise.callbacks :
            \/ i \in DOMAIN objects' /\ w \in objects'[i].promise.callbacks
            \/ w \notin DOMAIN objects'
            \/ objects'[w].task.state \in {"none", "fulfilled"}
            \/ i \in objects'[w].task.resumes

consistent_listener_consumption_enqueues_unblock ==
    \A i \in DOMAIN objects :
        \A a \in objects[i].promise.listeners :
            \/ i \in DOMAIN objects' /\ a \in objects'[i].promise.listeners
            \/ \E e \in outbox' :
                   /\ e.address = a
                   /\ e.message.tag = "Unblock"
                   /\ e.message.id = i
                   /\ e.message.state /= "pending"

consistent_wake_follows_callback_consumption ==
    \A i \in DOMAIN objects :
        ( /\ i \in DOMAIN objects'
          /\ objects[i].task.state = "suspended"
          /\ objects'[i].task.state = "pending" ) =>
        \E j \in DOMAIN objects :
            /\ i \in objects[j].promise.callbacks
            /\ j \in DOMAIN objects'
            /\ i \notin objects'[j].promise.callbacks
            /\ j \in objects'[i].task.resumes

consistent_suspension_registers_callback ==
    \A i \in DOMAIN objects' :
        objects'[i].task.state = "suspended" =>
            \/ /\ i \in DOMAIN objects
               /\ objects[i].task.state = "suspended"
            \/ \E j \in DOMAIN objects' :
                   /\ i \in objects'[j].promise.callbacks
                   /\ \/ j \notin DOMAIN objects
                      \/ i \notin objects[j].promise.callbacks
                   /\ objects'[j].promise.state = "pending"

consistent_task_birth_couples_promise_birth ==
    \A i \in DOMAIN objects' :
        LET u == objects'[i].task
            q == objects'[i].promise
        IN
        /\ u.state /= "none" /\ (i \notin DOMAIN objects \/ objects[i].task.state = "none") =>
               /\ i \notin DOMAIN objects
               /\ q.tags.targeted
               /\ IF u.state = "fulfilled" THEN q.state /= "pending"
                                           ELSE q.state = "pending"
               /\ \/ u.state = "pending"   /\ u.version = 0
                  \/ u.state = "acquired"  /\ 1 <= u.version
                  \/ u.state = "fulfilled" /\ u.version = 0
        /\ i \notin DOMAIN objects /\ q.tags.targeted => u.state /= "none"

monotone_outbox_keys_never_disappear ==
    \A e \in outbox :
        \E f \in outbox' : MsgKey(f) = MsgKey(e)

consistent_new_execute_matches_task_and_target ==
    \A e \in outbox' :
        e.message.tag = "Execute" =>
            \/ e \in outbox
            \/ /\ e.message.id \in DOMAIN objects'
               /\ objects'[e.message.id].task.version = e.message.version
               /\ e.address = objects'[e.message.id].promise.tags.target

consistent_new_unblock_carries_stored_record ==
    \A e \in outbox' :
        e.message.tag = "Unblock" =>
            \/ \E f \in outbox : MsgKey(f) = MsgKey(e)
            \/ /\ e.message.state /= "pending"
               /\ e.message.id \in DOMAIN objects'
               /\ objects'[e.message.id].promise.state = e.message.state

consistent_new_unblock_discharges_its_listener ==
    \A e \in outbox' :
        e.message.tag = "Unblock" =>
            \/ \E f \in outbox : MsgKey(f) = MsgKey(e)
            \/ /\ e.message.id \in DOMAIN objects
               /\ e.address \in objects[e.message.id].promise.listeners
               /\ e.message.id \in DOMAIN objects'
               /\ e.address \notin objects'[e.message.id].promise.listeners

consistent_task_birth_state ==
    \A i \in DOMAIN objects' :
        LET u == objects'[i].task IN
        u.state /= "none" /\ i \notin DOMAIN objects =>
            \/ /\ u.state = "pending" /\ u.retryTimeoutAt /= NoTime
               /\ u.pid = NoPid /\ u.ttl = NoTime
               /\ u.leaseTimeoutAt = NoTime /\ u.resumes = {}
            \/ /\ u.state = "fulfilled" /\ u.retryTimeoutAt = NoTime
               /\ u.pid = NoPid /\ u.ttl = NoTime
               /\ u.leaseTimeoutAt = NoTime /\ u.resumes = {}
            \/ /\ u.state = "acquired" /\ 1 <= u.version /\ u.retryTimeoutAt = NoTime
               /\ u.pid /= NoPid /\ u.ttl /= NoTime
               /\ u.leaseTimeoutAt /= NoTime /\ u.resumes = {}

consistent_task_lease_released_atomically ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET t == objects[i].task
                u == objects'[i].task
            IN  ( t.state = "acquired" /\ u.state /= "acquired" =>
                      /\ u.pid = NoPid /\ u.ttl = NoTime /\ u.leaseTimeoutAt = NoTime
                      /\ u.version = t.version )

preserved_task_lease_holder_stable ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET t == objects[i].task
                u == objects'[i].task
            IN  ( /\ t.state = "acquired" /\ u.state = "acquired"
                  /\ u.version = t.version
                  => u.pid = t.pid /\ u.ttl = t.ttl )

consistent_task_lease_fields_move_together ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET t == objects[i].task
                u == objects'[i].task
            IN  \/ u.pid = t.pid /\ u.ttl = t.ttl /\ u.leaseTimeoutAt = t.leaseTimeoutAt
                \/ /\ t.state /= "acquired" /\ u.state = "acquired"
                   /\ u.pid /= NoPid /\ u.ttl /= NoTime /\ u.leaseTimeoutAt /= NoTime
                \/ /\ t.state = "acquired" /\ u.state /= "acquired"
                   /\ u.pid = NoPid /\ u.ttl = NoTime /\ u.leaseTimeoutAt = NoTime
                \/ /\ t.state = "acquired" /\ u.state = "acquired"
                   /\ u.pid = t.pid /\ u.ttl = t.ttl

monotone_task_resumes_grow_or_clear ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            \/ objects'[i].task.resumes = {}
            \/ objects[i].task.resumes \subseteq objects'[i].task.resumes

consistent_task_resumes_cleared_only_on_dispatch_or_park ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            ( /\ objects[i].task.resumes /= {}
              /\ objects'[i].task.resumes = {}
              => objects'[i].task.state \in {"acquired", "suspended", "fulfilled"} )

consistent_task_acquisition_is_atomic ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            LET t == objects[i].task
                u == objects'[i].task
            IN  ( t.state /= "acquired" /\ u.state = "acquired" =>
                      /\ t.state = "pending"
                      /\ t.version < u.version
                      /\ u.pid /= NoPid /\ u.ttl /= NoTime
                      /\ u.leaseTimeoutAt = now + u.ttl
                      /\ u.retryTimeoutAt = NoTime /\ u.resumes = {}) 

consistent_task_lease_deadline_is_now_plus_ttl ==
    \A i \in DOMAIN objects' :
        objects'[i].task.leaseTimeoutAt /= NoTime =>
            \/ objects'[i].task.leaseTimeoutAt = now + objects'[i].task.ttl
            \/ /\ i \in DOMAIN objects
               /\ objects[i].task.leaseTimeoutAt = objects'[i].task.leaseTimeoutAt
               /\ objects[i].task.ttl = objects'[i].task.ttl
               /\ objects[i].task.state = objects'[i].task.state

consistent_task_pending_entry_arms_retry ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            ( /\ objects[i].task.state /= "pending"
              /\ objects'[i].task.state = "pending"
              => objects'[i].task.retryTimeoutAt = now )

consistent_task_retry_rearm_only_when_due ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            ( /\ objects[i].task.state = "pending"
              /\ objects'[i].task.state = "pending"
              /\ objects'[i].task.retryTimeoutAt /= objects[i].task.retryTimeoutAt
              => objects[i].task.retryTimeoutAt <= now )

consistent_task_wake_records_resume ==
    \A i \in DOMAIN objects :
        i \in DOMAIN objects' =>
            ( /\ objects[i].task.state = "suspended"
              /\ objects'[i].task.state = "pending"
              => /\ objects'[i].task.resumes /= {}
                 /\ objects'[i].task.retryTimeoutAt = now
                 /\ objects'[i].task.version = objects[i].task.version )

consistent_promise_settlement_stamp ==
    \A i \in DOMAIN objects :
        objects[i].promise.state = "pending" =>
            /\ i \in DOMAIN objects'
            /\ LET q == objects'[i].promise IN
               \/ q.state = "pending"
               \/ /\ q.settledAt = now
                  /\ now < q.timeoutAt
                  /\ q.state /= "rejectedTimedout"
               \/ /\ q.settledAt = q.timeoutAt
                  /\ q.timeoutAt <= now
                  /\ q.state = (IF q.tags.timer THEN "resolved"
                                                ELSE "rejectedTimedout")
                  /\ q.value = objects[i].promise.value

preserved_timedout_is_server_owned ==
    \A i \in DOMAIN objects :
        /\ objects[i].promise.state = "pending"
        /\ i \in DOMAIN objects'
        /\ objects'[i].promise.state = "rejectedTimedout"
        => /\ objects[i].promise.timeoutAt <= now
           /\ objects'[i].promise.settledAt = objects[i].promise.timeoutAt

consistent_new_promise_born_clean ==
    \A i \in DOMAIN objects' :
        i \notin DOMAIN objects =>
            LET q == objects'[i].promise IN
            /\ q.callbacks = {} /\ q.listeners = {}
            /\ q.value = NoValue
            /\ q.createdAt <= now
            /\ \/ /\ q.state = "pending"
                  /\ q.settledAt = NoTime
                  /\ q.createdAt < q.timeoutAt
               \/ /\ q.settledAt = q.timeoutAt
                  /\ q.createdAt = q.timeoutAt
                  /\ q.timeoutAt <= now
                  /\ q.state = (IF q.tags.timer THEN "resolved"
                                                ELSE "rejectedTimedout")

monotone_task_retry_rearm_advances ==
    \A i \in DOMAIN objects :
        objects[i].task.state = "pending" /\ i \in DOMAIN objects' =>
            \/ objects'[i].task.state /= "pending"
            \/ objects'[i].task.retryTimeoutAt = objects[i].task.retryTimeoutAt
            \/ now < objects'[i].task.retryTimeoutAt

===============================================================================
