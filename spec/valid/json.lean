import Lean.Data.Json
import «valid».validator

/-!  # Decoding a recorded run

Reads the NDJSON a capture writes — one object per external call:

    {"kind":"promise.create","now":1000,"req":{…},"res":{…}}

where `req` is the `data` the client sent and `res` is the whole
`{kind, head, data}` envelope the server returned. That is resonate's
wire format, not a shape invented here, so a capture is just tee'd
traffic.

Two decoding notes that matter for comparison rather than parsing:

* **Tag order.** `Tags` is an association LIST in the specification, so
  order is observable in `BEq`. JSON objects have no order, and
  `Lean.Json` stores them in a key-sorted tree — so both the request tags
  the specification will echo and the response tags the server produced
  come out sorted, and they line up. Nothing has to be normalised.
* **Failure payloads.** resonate answers a non-2xx with `data` as a bare
  string message rather than an object, so every record decoder is
  attempted only when the payload is actually an object. -/

namespace TraceCheck.Json

open Lean ServerModel Equivalence TraceCheck

abbrev D := Except String

private def obj? (j : Json) (k : String) : D Json := j.getObjVal? k

private def str (j : Json) (k : String) : D String := do (← obj? j k).getStr?
private def nat (j : Json) (k : String) : D Nat := do (← obj? j k).getNat?

private def strOpt (j : Json) (k : String) : Option String :=
  match obj? j k with
  | .ok v => match v.getStr? with | .ok s => some s | _ => none
  | _ => none

private def natOpt (j : Json) (k : String) : Option Nat :=
  match obj? j k with
  | .ok v => match v.getNat? with | .ok n => some n | _ => none
  | _ => none

/-- A JSON object as an association list, in key order. -/
def tagsOf (j : Json) : Tags :=
  match j.getObj? with
  | .ok o => o.toList.map (fun (k, v) => (k, match v.getStr? with | .ok s => s | _ => ""))
  | _ => []

def tagsAt (j : Json) (k : String) : Tags :=
  match obj? j k with | .ok v => tagsOf v | _ => []

/-- `{"headers":{…},"data":"…"}`, both optional — resonate emits a bare
    `{}` when neither is set. -/
def valueOf (j : Json) : Value :=
  { headers := tagsAt j "headers", data := strOpt j "data" }

def valueAt (j : Json) (k : String) : Value :=
  match obj? j k with | .ok v => valueOf v | _ => {}

def promiseState : String → PromiseState
  | "resolved"          => .resolved
  | "rejected"          => .rejected
  | "rejected_canceled" => .rejectedCanceled
  | "rejected_timedout" => .rejectedTimedout
  | _                   => .pending

def taskState : String → TaskState
  | "acquired"  => .acquired
  | "suspended" => .suspended
  | "halted"    => .halted
  | "fulfilled" => .fulfilled
  | _           => .pending

def promiseOf (j : Json) : D PromiseRecord := do
  return { id        := ← str j "id",
           state     := promiseState (← str j "state"),
           param     := valueAt j "param",
           value     := valueAt j "value",
           tags      := tagsAt j "tags",
           timeoutAt := ← nat j "timeoutAt",
           createdAt := ← nat j "createdAt",
           settledAt := natOpt j "settledAt" }

def taskOf (j : Json) : D TaskRecord := do
  return { id      := ← str j "id",
           state   := taskState (← str j "state"),
           version := ← nat j "version",
           resumes := (natOpt j "resumes").getD 0,
           ttl     := natOpt j "ttl",
           pid     := strOpt j "pid" }

/-- `some` only when the field is present AND an object: a non-2xx
    payload is a message string, and must not decode to a record. -/
private def sub? {α} (dec : Json → D α) (j : Json) (k : String) : Option α :=
  match obj? j k with
  | .ok v => match v.getObj? with
             | .ok _ => (dec v).toOption
             | _     => none
  | _ => none

/-! ## Requests -/

def decodeRequest (kind : String) (d : Json) : D Request := do
  match kind with
  | "promise.get" => do
      let id ← str d "id"
      return Request.promiseGet { id := id }
  | "promise.create" => do
      let id ← str d "id"; let t ← nat d "timeoutAt"
      return Request.promiseCreate
        { id := id, timeoutAt := t, param := valueAt d "param", tags := tagsAt d "tags" }
  | "promise.settle" => do
      let id ← str d "id"; let st ← str d "state"
      return Request.promiseSettle
        { id := id, state := promiseState st, value := valueAt d "value" }
  | "promise.register_callback" => do
      let aw ← str d "awaited"; let ar ← str d "awaiter"
      return Request.promiseRegisterCallback { awaited := aw, awaiter := ar }
  | "promise.register_listener" => do
      let aw ← str d "awaited"; let ad ← str d "address"
      return Request.promiseRegisterListener { awaited := aw, address := ad }
  | "task.get" => do
      let id ← str d "id"
      return Request.taskGet { id := id }
  | "task.acquire" => do
      let id ← str d "id"; let v ← nat d "version"
      let pid ← str d "pid"; let ttl ← nat d "ttl"
      return Request.taskAcquire { id := id, version := v, pid := pid, ttl := ttl }
  | "task.release" => do
      let id ← str d "id"; let v ← nat d "version"
      return Request.taskRelease { id := id, version := v }
  | "task.halt" => do
      let id ← str d "id"
      return Request.taskHalt { id := id }
  | "task.continue" => do
      let id ← str d "id"
      return Request.taskContinue { id := id }
  | "task.create" => do
      let a ← obj? d "action"
      let pid ← str d "pid"; let ttl ← nat d "ttl"
      let aid ← str a "id"; let ato ← nat a "timeoutAt"
      let act : PromiseCreateReq :=
        { id := aid, timeoutAt := ato, param := valueAt a "param", tags := tagsAt a "tags" }
      return Request.taskCreate { pid := pid, ttl := ttl, action := act }
  | "task.suspend" => do
      -- actions are themselves `{kind, head, data}` envelopes
      let acts ← (← obj? d "actions").getArr?
      let mut out : List PromiseRegisterCallbackReq := []
      for a in acts do
        let ad ← obj? a "data"
        let aw ← str ad "awaited"; let ar ← str ad "awaiter"
        out := out ++ [{ awaited := aw, awaiter := ar }]
      let id ← str d "id"; let v ← nat d "version"
      return Request.taskSuspend { id := id, version := v, actions := out }
  | "task.fulfill" => do
      let ad ← obj? (← obj? d "action") "data"
      let id ← str d "id"; let v ← nat d "version"
      let pid ← str ad "id"; let st ← str ad "state"
      let act : PromiseSettleReq :=
        { id := pid, state := promiseState st, value := valueAt ad "value" }
      return Request.taskFulfill { id := id, version := v, action := act }
  | "task.heartbeat" => do
      let ts ← (← obj? d "tasks").getArr?
      let mut out : List TaskRef := []
      for t in ts do
        let tid ← str t "id"; let tv ← nat t "version"
        out := out ++ [{ id := tid, version := tv }]
      let pid ← str d "pid"
      return Request.taskHeartbeat { pid := pid, tasks := out }
  | k => throw s!"unsupported request kind: {k}"

/-! ## Responses -/

def decodeResponse (kind : String) (status : Nat) (d : Json) : D Response := do
  let p := sub? promiseOf d "promise"
  let t := sub? taskOf d "task"
  match kind with
  | "promise.get"    => return Response.promiseGet { status := status, promise := p }
  | "promise.create" => return Response.promiseCreate { status := status, promise := p }
  | "promise.settle" => return Response.promiseSettle { status := status, promise := p }
  | "promise.register_callback" =>
      return Response.promiseRegisterCallback { status := status, promise := p }
  | "promise.register_listener" =>
      return Response.promiseRegisterListener { status := status, promise := p }
  | "task.get"       => return Response.taskGet { status := status, task := t }
  | "task.create"    => return Response.taskCreate { status := status, task := t, promise := p }
  | "task.acquire"   => return Response.taskAcquire { status := status, task := t, promise := p }
  | "task.suspend"   => return Response.taskSuspend { status := status }
  | "task.fulfill"   => return Response.taskFulfill { status := status, promise := p }
  | "task.release"   => return Response.taskRelease { status := status }
  | "task.halt"      => return Response.taskHalt { status := status }
  | "task.continue"  => return Response.taskContinue { status := status }
  | "task.heartbeat" => return Response.taskHeartbeat { status := status }
  | k => throw s!"unsupported response kind: {k}"

/-! ## Events -/

def decodeEvent (j : Json) : D Observation := do
  let kind ← str j "kind"
  let now  ← nat j "now"
  let req  ← decodeRequest kind (← obj? j "req")
  let res  ← obj? j "res"
  let status ← nat (← obj? res "head") "status"
  let data := (obj? res "data").toOption.getD (Json.mkObj [])
  return { req, res := ← decodeResponse kind status data, now }

/-- Decode NDJSON text into observations. One event per line; blank
    lines are skipped so a trailing newline is not an error. -/
def parseTrace (text : String) : IO (List Observation) := do
  let mut out : List Observation := []
  let mut i := 0
  for line in text.splitOn "\n" do
    if line.trimAscii.isEmpty then continue
    i := i + 1
    match Json.parse line with
    | .error e => throw (IO.userError s!"line {i}: bad JSON: {e}")
    | .ok j =>
      match decodeEvent j with
      | .error e => throw (IO.userError s!"line {i}: {e}")
      | .ok o    => out := out ++ [o]
  return out

/-- The checker reads stdin and nothing else. Pointing it at a file is
    the shell's job — `validate.sh` redirects — which keeps the `Option`
    plumbing and the "which source?" dispatch out of Lean entirely. -/
def loadTraceStdin : IO (List Observation) := do
  parseTrace (← (← IO.getStdin).readToEnd)

end TraceCheck.Json
