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
    `resonate:target` requirement and this rule are exclusive, so a task
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

end ServerModel
