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

/-! ### The two axes a promise is classified on

    Three tags — `resonate:external`, `resonate:target`, `resonate:timer`
    — but only two decisions are ever taken from them, and reading one
    decision off the other's tag is where implementations go wrong.

    `otype` answers WHO MAY BE BLOCKED ON THIS. An external promise is
    one a client can await: it may carry callbacks and listeners, and
    the machine therefore owes it an observation of its own deadline.
    `okind` answers WHAT CAUSES IT TO RUN — a `.task` promise names a
    worker that must be handed the execution.

    The axes are NOT independent, and the dead cell is stated rather
    than left for a reader to find: `okind_task_implies_external` below.
    `resonate:target` is a disjunct of `otype`, so `internal + task` is
    unrepresentable. Both stay DERIVED — read off the tags, never
    stored, never settable apart — which is what keeps that implication
    true by construction rather than by an invariant somebody has to
    enforce at every write.

    What is deliberately NOT an axis here is the deadline verdict. A
    timer resolves at its deadline; everything else is rejected there.
    `isTimer` is read for that verdict alone, at sites that select a
    VALUE rather than gate a branch: `otype` gates, `isTimer` values. A
    third consumer that BRANCHES on `isTimer` would be the signal that
    it is an axis after all — until then it is one expression in one
    role, and wrapping it would be symmetry for its own sake. -/
inductive OType
  | external
  | internal
  deriving Repr, DecidableEq

inductive OKind
  | task
  | idle
  deriving Repr, DecidableEq

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
  if t.get? "resonate:scope" == some "global"
      || t.get? "resonate:external" == some "true"
      || t.has "resonate:target" || t.isTimer then
    .external
  else
    .internal

/-- Executed by a worker — which is exactly what carries a task. -/
def Tags.okind (t : Tags) : OKind :=
  if t.has "resonate:target" then .task else .idle

/-- THE DEAD CELL, stated. Because `target` is a disjunct of `otype`, a
    promise that carries a task is external by construction, and the
    2x2 is really a chain: internal, external-idle, external-task. A
    `match` on the pair that writes an arm for `internal + task` is
    writing for a state no tag list can produce. -/
theorem okind_task_implies_external (t : Tags) :
    t.okind = .task → t.otype = .external := by
  intro h
  cases ht : t.has "resonate:target" with
  | true  => simp [Tags.otype, ht]
  | false => simp [Tags.okind, ht] at h

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
def TaskFenceAction.targetId : TaskFenceAction → String
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

/-- A deliverable listener address: `http(s)://…`, or `poll://…` carrying an
    `@group` (e.g. `poll://any@default` — a bare `poll://default` names no
    group and could never be routed). -/
def addressValid (a : String) : Bool :=
  prefixOf "http://".toList a.toList || prefixOf "https://".toList a.toList ||
  (prefixOf "poll://".toList a.toList && a.toList.contains '@')

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
  | execute (taskId : String) (version : Nat)
  | unblock (promise : PromiseRecord)
  deriving Repr

structure OutboxEntry where
  address : String
  message : Message
  deriving Repr

def OutboxEntry.key : OutboxEntry → String
  | { message := .execute taskId _,  .. } => taskId
  | { address, message := .unblock p }    => s!"{p.id}:notify:{address}"

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
opaque expand : (template id : String) → (timestamp : Nat) → String

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
    `expand … = ""`. The schedule step becomes a silent no-op and
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
