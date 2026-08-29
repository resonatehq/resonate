import «01-protocol».«types»

/-!  # Protocol — validation and shared semantics

Everything both machines agree on beyond the wire types: the tag
algebra, settlement validity, request-level helpers, the message
channel, and the opaque cron semantics. This layer imports nothing but
`types.lean`; both machines import it; neither machine's internals leak
into it.  -/

namespace ServerModel

def Tags.get? (t : Tags) (k : String) : Option String :=
  (t.find? (·.fst == k)).map (·.snd)

def Tags.has (t : Tags) (k : String) : Bool :=
  (t.get? k).isSome

def Tags.isTimer (t : Tags) : Bool :=
  t.get? "resonate:timer" == some "true"

/-! ### The one axis a promise is classified on

    Three tags — `resonate:external`, `resonate:target`, `resonate:timer`
    — and ONE decision taken from them. Reading a decision off another
    decision's tag is where implementations go wrong, so the decision is
    named and the tags are not consulted anywhere else.

    `otype` answers WHO MAY BE BLOCKED ON THIS, what runs it, and
    whether its deadline is armed — one judgment, three consequences:

      `internal`  nobody outside its own call graph; nothing runs it;
                  NO ARMED TIMEOUT — its deadline is a projection every
                  read applies, never a write the machine owes
      `external`  anyone may await it; nothing runs it — a person, a
                  webhook or the clock settles it; ARMED TIMEOUT
      `runnable`  anyone may await it, AND a worker is handed the
                  execution; ARMED TIMEOUT

    The arming follows from the first column rather than sitting beside
    it: a deadline is owed as a write exactly where someone can be
    waiting to observe it, which is why `internal` costs no timer.

    It was two axes for a while — `otype` crossed with an `okind` of
    task or idle — and the pair carried a proof that `internal + task`
    is unrepresentable, because `resonate:target` was a disjunct of
    both. Three values say that instead of proving it: the chain is
    the type, and the dead cell has nowhere to be written. Awaitability
    is `otype.awaitable`, which is every value but `internal`.

    It stays DERIVED — read off the tags, never stored, never settable
    apart — which is what keeps the classification a function of the
    request rather than an invariant somebody has to enforce at every
    write.

    What is deliberately NOT an axis here is the deadline verdict. A
    timer resolves at its deadline; everything else is rejected there.
    `isTimer` is read for that verdict alone, at sites that select a
    VALUE rather than gate a branch: `otype` gates, `isTimer` values. A
    third consumer that BRANCHES on `isTimer` would be the signal that
    it is an axis after all — until then it is one expression in one
    role, and wrapping it would be symmetry for its own sake. -/
inductive OType
  | internal
  | external
  | runnable
  deriving Repr, DecidableEq

/-- Awaitable: every value but `internal`. The one place the question is
    answered, so no site re-derives it by listing constructors. -/
def OType.awaitable : OType → Bool
  | .internal => false
  | _         => true

/-- Awaitable. The disjunction is the definition, and this is the only
    place it may be written: no site re-derives it inline.

    `resonate:scope = global` is the form the wire actually carries —
    `work/ts/README.md` reads it back off a live server, where a remote
    child gets `scope: global` alongside its target and a local child
    gets `scope: local` and no target. It is a disjunct in its own
    right, NOT a synonym for the target tag, and the promise that
    proves it is the human-in-the-loop one: created global so that
    anyone may await it, with no target, because nothing executes it —
    a person does. Key this on the target tag and every HITL promise
    reads internal, its awaiters are refused at the door, and its
    deadline is never armed.

    `resonate:external` is kept as the open-ended escape hatch: a way
    for a client to say "awaitable" for a kind nobody has enumerated.
    It appears nowhere in observed server traffic. -/
def Tags.otype (t : Tags) : OType :=
  if t.has "resonate:target" then
    .runnable
  else if t.get? "resonate:scope" == some "global"
      || t.get? "resonate:external" == some "true"
      || t.isTimer then
    .external
  else
    .internal

/-- A worker is handed the execution exactly when a target names one. -/
theorem otype_runnable_iff_targeted (t : Tags) :
    t.otype = .runnable ↔ t.has "resonate:target" = true := by
  cases ht : t.has "resonate:target" with
  | true  => simp [Tags.otype, ht]
  | false => simp [Tags.otype, ht]; split <;> simp

/-- Anything a target names is awaitable, which used to need a proof
    about two axes and is now a case split on one. -/
theorem targeted_implies_awaitable (t : Tags) :
    t.has "resonate:target" = true → t.otype.awaitable = true := by
  intro h; simp [Tags.otype, h, OType.awaitable]

/-- A TIMER IS NEVER TARGETED. The two tags name incompatible things:
    `resonate:target` says a worker owns this promise's execution, and
    the machine gives it a task to carry that execution; `resonate:timer`
    says nothing executes it at all — it resolves when its deadline
    arrives, and that is its whole life. A promise carrying both would
    be handed a task no worker should ever run.

    Malformed, therefore, and refused at every door a promise can be
    born through — `promise.create`, `task.create`, and `schedule.create`
    (whose `promiseTags` become its occurrences' tags, so an unchecked
    schedule would smuggle the combination past the other two).
    `task.fence` needs no guard of its own: its create action is
    `promise.create`, and the inner refusal is what it reports.

    With the combination refused, `task.create` faces no timers: its
    `resonate:target` requirement and this internal step are exclusive, so a task
    is never born onto a timer promise and the birth verdict there is
    `rejectedTimedout`, with no `isTimer` case to answer for. Fact P's
    timer verdict remains where it is reachable — a timer, untargeted,
    born or timing out past its deadline, which has no task at all. -/
def Tags.timerTargeted (t : Tags) : Bool :=
  t.isTimer && t.has "resonate:target"

/-- The terminal states a client may settle into. `pending` is not a
    settlement, and `rejectedTimedout` is server-owned: only the timeout
    path writes it, so a client can never forge one. -/
def PromiseState.settable : PromiseState → Bool
  | .resolved | .rejected | .rejectedCanceled => true
  | _ => false

/-- The promise id a fenced action operates on. -/
def TaskFenceAction.targetId : TaskFenceAction → Ident
  | .create r => r.id
  | .settle r => r.id

/-!  String scans, structurally recursive over `toList`. Core's
    `String.startsWith`/`contains`/`toNat!` are defined by well-founded
    recursion over byte positions, which kernel reduction cannot unfold
    — using them would exile every proof that touches an address or a
    delay tag to `native_decide`. These run under plain `decide`,
    keeping the whole spec kernel-decidable, with one trusted checker. -/

def prefixOf : List Char → List Char → Bool
  | [], _ => true
  | _, [] => false
  | c :: cs, d :: ds => c == d && prefixOf cs ds

/-- The remainder after the first `://`, if there is one. Structural on the
    list, so it reduces under `decide` like everything else here. -/
def afterScheme : List Char → Option (List Char)
  | ':' :: '/' :: '/' :: rest => some rest
  | _ :: cs => afterScheme cs
  | [] => none

/-- A deliverable listener address: `scheme://rest` for ANY scheme, with a
    non-empty scheme and a non-empty remainder.

    This used to name a closed set — `http(s)://…` and `poll://…@group` — and
    that was the specification owning a list it has no reason to own. A
    transport is named by its scheme, and adding a transport is not a protocol
    change: an implementation routing `echod://bundle` or `bash://docker/image`
    is not thereby non-conformant, and a checker that refuses those refutes
    conformant implementations. What the predicate is *for* is rejecting an
    address that could never be routed at all — no scheme, or nothing after it.
    Whether a well-formed address is actually deliverable is the router's
    business, not the model's.

    The relaxation is monotone: every address admitted before is admitted now,
    so any state invariant quantifying over `addressValid` (`properties.lean`
    listeners/unblock) is weakened, never strengthened. -/
def addressValid (a : String) : Bool :=
  match a.toList with
  | [] => false
  | cs =>
    match afterScheme cs with
    | none => false
    -- `afterScheme` dropped `k` characters then the three of `://`, so
    -- `cs.length = k + 3 + rest.length`. The scheme is non-empty exactly when
    -- that is a strict inequality, which `==` on `Nat` decides without
    -- reaching for a `Prop`.
    | some rest => !rest.isEmpty && !(rest.length + 3 == cs.length)

/-- Total decimal parse (digits fold left; a malformed tag yields a
    garbage number rather than a panic — tags are client-supplied, and
    the machine is total). -/
def parseNat (s : String) : Nat :=
  go s.toList 0
where
  go : List Char → Nat → Nat
    | [], acc => acc
    | c :: cs, acc => go cs (acc * 10 + (c.toNat - '0'.toNat))

inductive Message
  | execute (taskId : Ident) (version : Nat)
  | unblock (promise : PromiseRecord)
  deriving Repr

structure OutboxEntry where
  address : String
  message : Message
  deriving Repr

/-- What makes two outbox entries the same entry.

    Structured rather than a rendered string: the key used to be
    `taskId` for an execute and `"{id}:notify:{address}"` for an
    unblock, and with `origin:suffix` ids that separator now appears
    inside one of its own fields. Nothing collided -- an id carries one
    colon, so no task id can spell `{promise}:notify:{address}` -- but
    the uniqueness `well_formed_store_outbox_keys_unique` states was
    resting on that arithmetic rather than on the data. As a sum there
    is no separator to overload. -/
inductive OutboxKey
  | execute (taskId : Ident)
  | notify  (promise : Ident) (address : String)
  deriving Repr, DecidableEq

instance : BEq OutboxKey := instBEqOfDecidableEq

def OutboxEntry.key : OutboxEntry → OutboxKey
  | { message := .execute taskId _,  .. } => .execute taskId
  | { address, message := .unblock p }    => .notify p.id address

/-- Next cron fire time strictly after the given instant. -/
opaque nextCron : (cron : String) → (after : Nat) → Nat

/-- All cron occurrence instants in `[since, now]`, in order — the
    `nextCron` chain restricted to the window. Opaque like `nextCron`
    itself: WHICH instants a cron expression denotes is calendar math,
    outside the protocol. The window is finite, so this function
    exists totally — no machine walks the chain recursively; and no
    machine trusts it either: every consumer re-checks due-ness on
    each element, so an occurrence means NOT BEFORE, enforced by the
    machine, not by the calendar. -/
opaque occurrences : (cron : String) → (since now : Nat) → List Nat

/-- Expand a schedule's promise-id template against one occurrence. -/
opaque expand : (template id : Ident) → (timestamp : Nat) → Ident

/-! ### What these three must satisfy, and do not yet

`nextCron`, `occurrences` and `expand` are `opaque` with no value, so
every schedule property in `02-abstract/properties.lean` is asserted
rather than checked: no script can reach a schedule state, and the four
`well_formed_schedule_*` entries are carried unexercised.

Two things follow that are easy to get wrong:

  * An AXIOM would make them provable and would still leave them
    untested. `decide` reduces terms and an axiom has no reduction
    rule, exactly like `opaque`.
  * `#eval` is worse than useless here. The opaque constants return
    `Inhabited.default`, so `nextCron c t = 0`, `occurrences … = []`,
    `expand … = ⟨"", ""⟩`. The schedule step becomes a silent no-op and
    `nextCron` returns an instant BEFORE its argument, violating the
    machine's own `well_formed_schedule_created_at_lte_next_run_at`.
    Anyone who reaches for `#eval` or `native_decide` to "test
    schedules" gets a green run that proves the opposite.

The contract the protocol needs from the calendar:

    nextCron_strictly_after       ∀ c t, t < nextCron c t
    occurrences_in_window         ∀ c s n t, t ∈ occurrences c s n → s ≤ t ∧ t ≤ n
    occurrences_sorted            occurrences c s n is strictly increasing
    occurrences_is_the_chain      it enumerates exactly the nextCron chain in the window
    expand_injective_in_timestamp t ≠ t' → expand tpl id t ≠ expand tpl id t'
    expand_injective_in_id        two schedules sharing a template do not collide
    expand_ids_are_not_forgeable  an occurrence id is not reachable as a client promise id

`occurrences_empty_when_unreached` is NOT needed separately: an
inverted window has no element satisfying `s ≤ t ≤ n`, so it follows
from `occurrences_in_window`.

The last one is a protocol assumption rather than a lemma, and it is
load-bearing: without it a client can pre-create a promise at an
occurrence's id and have the schedule silently adopt it instead of
firing. It is the same defect class as the outbox key collision — the
specification has no id-namespacing discipline.

Testing them needs a definition, not an axiom: a total, kernel-reducible
toy cron behind a parameter, `opaque` in production and the toy in
tests. That is the only way to check the contract above is satisfiable
rather than vacuous. -/

end ServerModel
