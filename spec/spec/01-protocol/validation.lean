import «01-protocol».«types»

namespace ServerModel

def Tags.get? (t : Tags) (k : String) : Option String :=
  (t.find? (·.fst == k)).map (·.snd)

def Tags.has (t : Tags) (k : String) : Bool :=
  (t.get? k).isSome

def Tags.isTimer (t : Tags) : Bool :=
  t.get? "resonate:timer" == some "true"

inductive OType
  | internal
  | external
  | runnable
  deriving Repr, DecidableEq

def OType.awaitable : OType → Bool
  | .internal => false
  | _         => true

def Tags.otype (t : Tags) : OType :=
  if t.has "resonate:target" then
    .runnable
  else if t.get? "resonate:scope" == some "global"
      || t.get? "resonate:external" == some "true"
      || t.isTimer then
    .external
  else
    .internal

theorem otype_runnable_iff_targeted (t : Tags) :
    t.otype = .runnable ↔ t.has "resonate:target" = true := by
  cases ht : t.has "resonate:target" with
  | true  => simp [Tags.otype, ht]
  | false => simp [Tags.otype, ht]; split <;> simp

theorem targeted_implies_awaitable (t : Tags) :
    t.has "resonate:target" = true → t.otype.awaitable = true := by
  intro h; simp [Tags.otype, h, OType.awaitable]

def Tags.timerTargeted (t : Tags) : Bool :=
  t.isTimer && t.has "resonate:target"

def PromiseState.settable : PromiseState → Bool
  | .resolved | .rejected | .rejectedCanceled => true
  | _ => false

def TaskFenceAction.targetId : TaskFenceAction → Ident
  | .create r => r.id
  | .settle r => r.id

def prefixOf : List Char → List Char → Bool
  | [], _ => true
  | _, [] => false
  | c :: cs, d :: ds => c == d && prefixOf cs ds

def afterScheme : List Char → Option (List Char)
  | ':' :: '/' :: '/' :: rest => some rest
  | _ :: cs => afterScheme cs
  | [] => none

def addressValid (a : String) : Bool :=
  match a.toList with
  | [] => false
  | cs =>
    match afterScheme cs with
    | none => false
    | some rest => !rest.isEmpty && !(rest.length + 3 == cs.length)

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

inductive OutboxKey
  | execute (taskId : Ident)
  | notify  (promise : Ident) (address : String)
  deriving Repr, DecidableEq

instance : BEq OutboxKey := instBEqOfDecidableEq

def OutboxEntry.key : OutboxEntry → OutboxKey
  | { message := .execute taskId _,  .. } => .execute taskId
  | { address, message := .unblock p }    => .notify p.id address

opaque nextCron : (cron : String) → (after : Nat) → Nat

opaque occurrences : (cron : String) → (since now : Nat) → List Nat

opaque expand : (template id : Ident) → (timestamp : Nat) → Ident

end ServerModel
