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
