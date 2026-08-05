import «05-sqlite».schema

/-!  # Every handler, as SQL

`Req → (now : Nat) → m Res` for each protocol action, where `m` is any
monad with a `MonadQuery` instance. The handlers reach state only
through statement text.

Transcribed from `spec/03-concrete/m/` — the materialized machine, one
file per handler there, one definition per handler here, in the same
order. Where the specification writes an effect (`setPromise`,
`delTaskTimeout`, `setMessage`) this writes the statement that realises
it; the small helpers in the first section are exactly the specification's
effect vocabulary, each one or two statements.

Two effects need a word:

* **Keyed collapse.** `setMessage`, `defer`, `setTaskTimeout` and
  `addCallback` all mean "replace any entry with this key, then add".
  That is `DELETE … WHERE key` followed by `INSERT`, which is what the
  specification's `entry :: xs.filter (key ≠ ·)` says. `ON CONFLICT` would
  also do, but only on a single-column key, and two of these are keyed on
  a pair.
* **Cron.** `nextCron` and `occurrences` are opaque in the specification —
  calendar maths, outside the protocol. The handler computes them in Lean
  and passes the result as a bound parameter, which is what a real driver
  does too; no SQL dialect is being asked to know about cron. -/

namespace Sqlite

open Sql ServerModel

variable {m : Type → Type} [Monad m] [MonadQuery m]

/-! ## Effects -/

def promiseCols : String :=
  "id, state, param_headers, param_data, value_headers, value_data, tags, target, timer, external, timeout_at, created_at, settled_at"

def getPromiseRow (id : String) : m (Option Row) := do
  let r ← query ("SELECT " ++ promiseCols ++ " FROM promises WHERE id = ?1") [.text id]
  return r.rows.head?

def getTaskRow (id : String) : m (Option Row) := do
  let r ← query "SELECT id, state, version, ttl, pid FROM tasks WHERE id = ?1" [.text id]
  return r.rows.head?

def resumeCount (id : String) : m Nat := do
  let r ← query "SELECT awaited_id FROM resumes WHERE task_id = ?1" [.text id]
  return r.rows.length

/-- `setMessage address (.execute taskId version)`. Key is the task id,
    matching `OutboxEntry.key`. -/
def setExecute (address taskId : String) (version : Nat) : m Unit := do
  let _ ← query "DELETE FROM outbox WHERE key = ?1" [.text taskId]
  let _ ← query
    "INSERT INTO outbox (key, address, kind, task_id, version, promise_id) VALUES (?1, ?2, 'execute', ?3, ?4, NULL)"
    [.text taskId, .text address, .text taskId, .int (Int.ofNat version)]

/-- `setMessage address (.unblock p)`. Key is `id:notify:address`. -/
def setUnblock (address promiseId : String) : m Unit := do
  let key := promiseId ++ ":notify:" ++ address
  let _ ← query "DELETE FROM outbox WHERE key = ?1" [.text key]
  let _ ← query
    "INSERT INTO outbox (key, address, kind, task_id, version, promise_id) VALUES (?1, ?2, 'unblock', NULL, NULL, ?3)"
    [.text key, .text address, .text promiseId]

def defer (awaited awaiter : String) : m Unit := do
  let _ ← query "DELETE FROM deferred WHERE awaited_id = ?1 AND awaiter_id = ?2"
                [.text awaited, .text awaiter]
  let _ ← query "INSERT INTO deferred (awaited_id, awaiter_id) VALUES (?1, ?2)"
                [.text awaited, .text awaiter]

def undefer (awaited awaiter : String) : m Unit := do
  let _ ← query "DELETE FROM deferred WHERE awaited_id = ?1 AND awaiter_id = ?2"
                [.text awaited, .text awaiter]

def setTaskTimeout (id : String) (kind timeout : Nat) : m Unit := do
  let _ ← query "DELETE FROM task_timeouts WHERE id = ?1 AND kind = ?2"
                [.text id, .int (Int.ofNat kind)]
  let _ ← query "INSERT INTO task_timeouts (id, kind, timeout_at) VALUES (?1, ?2, ?3)"
                [.text id, .int (Int.ofNat kind), .int (Int.ofNat timeout)]

def delTaskTimeout (id : String) : m Unit := do
  let _ ← query "DELETE FROM task_timeouts WHERE id = ?1" [.text id]

def addCallback (awaited awaiter : String) : m Unit := do
  let _ ← query "DELETE FROM callbacks WHERE awaited_id = ?1 AND awaiter_id = ?2"
                [.text awaited, .text awaiter]
  let _ ← query "INSERT INTO callbacks (awaited_id, awaiter_id) VALUES (?1, ?2)"
                [.text awaited, .text awaiter]

def addListener (promiseId address : String) : m Unit := do
  let _ ← query "DELETE FROM listeners WHERE promise_id = ?1 AND address = ?2"
                [.text promiseId, .text address]
  let _ ← query "INSERT INTO listeners (promise_id, address) VALUES (?1, ?2)"
                [.text promiseId, .text address]

/-- Insert a promise. `target`, `timer` and `external` are driver-
    maintained denormalisations of `tags`; the caller computes them from
    the same `Tags` value it serialises into the `tags` column. -/
def insertPromise (id : String) (state : PromiseState) (param value : Value)
    (tags : Tags) (timeoutAt createdAt : Nat) (settledAt : Option Nat) : m Unit := do
  let _ ← query
    "INSERT INTO promises (id, state, param_headers, param_data, value_headers, value_data, tags, target, timer, external, timeout_at, created_at, settled_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
    [ .text id, .text (promiseStateText state)
    , .text (tagsJson param.headers), optText param.data
    , .text (tagsJson value.headers), optText value.data
    , .text (tagsJson tags)
    , optText (tags.get? "resonate:target")
    , .int (if tags.isTimer then 1 else 0)
    , .int (if tags.get? "resonate:external" == some "true"
                || tags.has "resonate:target" || tags.isTimer then 1 else 0)
    , .int (Int.ofNat timeoutAt), .int (Int.ofNat createdAt), optInt settledAt ]

def insertTask (id : String) (state : TaskState) (version : Nat)
    (ttl : Option Nat) (pid : Option String) : m Unit := do
  let _ ← query "DELETE FROM tasks WHERE id = ?1" [.text id]
  let _ ← query "INSERT INTO tasks (id, state, version, ttl, pid) VALUES (?1, ?2, ?3, ?4, ?5)"
    [.text id, .text (taskStateText state), .int (Int.ofNat version), optInt ttl, optText pid]

def setPromiseTimeout (id : String) (t : Nat) : m Unit := do
  let _ ← query "DELETE FROM promise_timeouts WHERE id = ?1" [.text id]
  let _ ← query "INSERT INTO promise_timeouts (id, timeout_at) VALUES (?1, ?2)"
                [.text id, .int (Int.ofNat t)]

def delPromiseTimeout (id : String) : m Unit := do
  let _ ← query "DELETE FROM promise_timeouts WHERE id = ?1" [.text id]

def setScheduleTimeout (id : String) (t : Nat) : m Unit := do
  let _ ← query "DELETE FROM schedule_timeouts WHERE id = ?1" [.text id]
  let _ ← query "INSERT INTO schedule_timeouts (id, timeout_at) VALUES (?1, ?2)"
                [.text id, .int (Int.ofNat t)]

/-! ## Touch — the `-m` core

`touchPromise` IS the promise-timeout τ, fired at the moment of
observation. Transcribed from `03-concrete/m/00-touch.lean`. -/

def touchPromise (id : String) (now : Nat) : m (Option Row) := do
  match ← getPromiseRow id with
  | none => return none
  | some r =>
    if txt r "state" != "pending" || int r "timeout_at" > Int.ofNat now then
      return some r
    else
      let ls ← query "SELECT address FROM listeners WHERE promise_id = ?1" [.text id]
      let cbs ← query "SELECT awaiter_id FROM callbacks WHERE awaited_id = ?1" [.text id]
      let newState := if bool r "timer" then "resolved" else "rejected_timedout"
      let _ ← query "UPDATE promises SET state = ?2, settled_at = ?3 WHERE id = ?1"
                    [.text id, .text newState, .int (int r "timeout_at")]
      let _ ← query "DELETE FROM callbacks WHERE awaited_id = ?1" [.text id]
      let _ ← query "DELETE FROM listeners WHERE promise_id = ?1" [.text id]
      delPromiseTimeout id
      match ← getTaskRow id with
      | some _ =>
          let _ ← query "UPDATE tasks SET state = 'fulfilled', pid = NULL, ttl = NULL WHERE id = ?1"
                        [.text id]
          let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text id]
          delTaskTimeout id
      | none => pure ()
      for row in ls.rows do
        setUnblock (txt row "address") id
      for row in cbs.rows do
        defer id (txt row "awaiter_id")
      return (← getPromiseRow id)

/-- Read a task with its promise, materialising first; the task is
    re-read because the touch may have fulfilled it. -/
def touchTask (id : String) (now : Nat) : m (Option (Row × Option Row)) := do
  match ← getTaskRow id with
  | none => return none
  | some t =>
    match ← touchPromise id now with
    | none => return some (t, none)
    | some p =>
      match ← getTaskRow id with
      | none    => return some (t, some p)
      | some t' => return some (t', some p)

/-- Live in the specification's sense: pending AND not past its deadline.
    After a touch these coincide with stored state, which is what makes
    the guards below `-p`'s verbatim. -/
def liveRow (r : Row) (now : Nat) : Bool :=
  txt r "state" == "pending" && int r "timeout_at" > Int.ofNat now

def taskRecordOf (t : Row) : m TaskRecord := do
  return rowToTask t (← resumeCount (txt t "id"))

/-! ## P-01 · promise.get -/

def promiseGet (req : PromiseGetReq) (now : Nat) : m PromiseGetRes := do
  match ← touchPromise req.id now with
  | none   => return { status := 404 }
  | some r => return { status := 200, promise := some (rowToPromise r) }

/-! ## P-02 · promise.create -/

def promiseCreate (req : PromiseCreateReq) (now : Nat) : m PromiseCreateRes := do
  match ← touchPromise req.id now with
  | some r => return { status := 200, promise := some (rowToPromise r) }
  | none =>
    if req.timeoutAt > now then
      insertPromise req.id .pending req.param {} req.tags req.timeoutAt now none
      let external := req.tags.get? "resonate:external" == some "true"
                        || req.tags.has "resonate:target" || req.tags.isTimer
      if external then
        setPromiseTimeout req.id req.timeoutAt
      match req.tags.get? "resonate:target" with
      | none => pure ()
      | some target =>
          insertTask req.id .pending 0 none none
          match req.tags.get? "resonate:delay" with
          | none =>
              setTaskTimeout req.id 0 (now + retryTimeout)
              setExecute target req.id 0
          | some delayStr =>
              let delay := parseNat delayStr
              if delay > now then
                setTaskTimeout req.id 0 delay
              else
                setTaskTimeout req.id 0 (now + retryTimeout)
                setExecute target req.id 0
      return { status := 200, promise := (← getPromiseRow req.id).map rowToPromise }
    else
      let st := if req.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout
      insertPromise req.id st req.param {} req.tags req.timeoutAt req.timeoutAt (some req.timeoutAt)
      if req.tags.has "resonate:target" then
        insertTask req.id .fulfilled 0 none none
      return { status := 200, promise := (← getPromiseRow req.id).map rowToPromise }

/-! ## P-03 · promise.settle -/

def promiseSettle (req : PromiseSettleReq) (now : Nat) : m PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← touchPromise req.id now with
  | none => return { status := 404 }
  | some r =>
    if !liveRow r now then
      return { status := 200, promise := some (rowToPromise r) }
    else
      let ls ← query "SELECT address FROM listeners WHERE promise_id = ?1" [.text req.id]
      let cbs ← query "SELECT awaiter_id FROM callbacks WHERE awaited_id = ?1" [.text req.id]
      let _ ← query
        "UPDATE promises SET state = ?2, value_headers = ?3, value_data = ?4, settled_at = ?5 WHERE id = ?1"
        [ .text req.id, .text (promiseStateText req.state)
        , .text (tagsJson req.value.headers), optText req.value.data
        , .int (Int.ofNat now) ]
      let _ ← query "DELETE FROM callbacks WHERE awaited_id = ?1" [.text req.id]
      let _ ← query "DELETE FROM listeners WHERE promise_id = ?1" [.text req.id]
      delPromiseTimeout req.id
      match ← getTaskRow req.id with
      | some _ =>
          let _ ← query "UPDATE tasks SET state = 'fulfilled', pid = NULL, ttl = NULL WHERE id = ?1"
                        [.text req.id]
          let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text req.id]
          delTaskTimeout req.id
      | none => pure ()
      for row in ls.rows do
        setUnblock (txt row "address") req.id
      for row in cbs.rows do
        defer req.id (txt row "awaiter_id")
      return { status := 200, promise := (← getPromiseRow req.id).map rowToPromise }

/-! ## P-04 · promise.register_callback -/

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) :
    m PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none => return { status := 404 }
  | some pa =>
    match ← touchPromise req.awaiter now with
    | none => return { status := 422 }
    | some pr =>
      if (txt? pr "target").isNone then
        return { status := 422 }
      if !bool pa "external" then
        return { status := 422 }
      -- `pa` predates the awaiter's touch, which may have settled it
      let pa := (← getPromiseRow req.awaited).getD pa
      if liveRow pa now then
        if liveRow pr now then
          addCallback req.awaited req.awaiter
        return { status := 200, promise := some (rowToPromise pa) }
      else
        return { status := 200, promise := some (rowToPromise pa) }

/-! ## P-05 · promise.register_listener -/

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) :
    m PromiseRegisterListenerRes := do
  if !addressValid req.address then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none => return { status := 404 }
  | some pa =>
    if !bool pa "external" then
      return { status := 422 }
    if liveRow pa now then
      addListener req.awaited req.address
    return { status := 200, promise := some (rowToPromise pa) }

/-! ## P-06 · promise.search — 501, as in the specification -/

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : m PromiseSearchRes := do
  return { status := 501 }

/-! ## S-01..S-04 · schedules

`scheduleGet` and `scheduleDelete` do not touch: the specification's
schedule handlers take no `now` and read stored state directly. -/

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : m ScheduleGetRes := do
  let r ← query "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?1" [.text req.id]
  match r.rows.head? with
  | none   => return { status := 404 }
  | some s => return { status := 200, schedule := some (rowToSchedule s) }

def scheduleCreate (req : ScheduleCreateReq) (now : Nat) : m ScheduleCreateRes := do
  let existing ← query "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?1" [.text req.id]
  match existing.rows.head? with
  | some s => return { status := 200, schedule := some (rowToSchedule s) }
  | none =>
      -- cron is calendar maths, computed here and bound as a parameter
      let next := nextCron req.cron now
      let _ ← query
        "INSERT INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)"
        [ .text req.id, .text req.cron, .text req.promiseId
        , .int (Int.ofNat req.promiseTimeout)
        , .text (tagsJson req.promiseParam.headers), optText req.promiseParam.data
        , .text (tagsJson req.promiseTags)
        , .int (Int.ofNat now), .int (Int.ofNat next) ]
      setScheduleTimeout req.id next
      let r ← query "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?1" [.text req.id]
      return { status := 200, schedule := r.rows.head?.map rowToSchedule }

def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : m ScheduleDeleteRes := do
  let r ← query "SELECT id FROM schedules WHERE id = ?1" [.text req.id]
  match r.rows.head? with
  | none => return { status := 404 }
  | some _ =>
      let _ ← query "DELETE FROM schedules WHERE id = ?1" [.text req.id]
      let _ ← query "DELETE FROM schedule_timeouts WHERE id = ?1" [.text req.id]
      return { status := 200 }

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : m ScheduleSearchRes := do
  return { status := 501 }

/-! ## T-01 · task.get -/

def taskGet (req : TaskGetReq) (now : Nat) : m TaskGetRes := do
  match ← touchTask req.id now with
  | none            => return { status := 404 }
  | some (_, none)  => return { status := 404 }
  | some (t, some p) =>
      if liveRow p now then
        return { status := 200, task := some (← taskRecordOf t) }
      else
        -- the one task projection: `.fulfilled` is inert
        return { status := 200,
                 task := some { id := txt t "id", state := .fulfilled,
                                version := nat t "version", resumes := 0,
                                ttl := none, pid := none } }

/-! ## T-02 · task.create -/

def taskCreate (req : TaskCreateReq) (now : Nat) : m TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") then
    return { status := 400 }
  match ← touchPromise a.id now with
  | none =>
      if a.timeoutAt > now then
        insertPromise a.id .pending a.param {} a.tags a.timeoutAt now none
        setPromiseTimeout a.id a.timeoutAt
        insertTask a.id .acquired 1 (some req.ttl) (some req.pid)
        setTaskTimeout a.id 1 (now + req.ttl)
        let p ← getPromiseRow a.id
        let t ← getTaskRow a.id
        return { status := 200, task := ← t.mapM taskRecordOf, promise := p.map rowToPromise }
      else
        let st := if a.tags.isTimer then PromiseState.resolved else PromiseState.rejectedTimedout
        insertPromise a.id st a.param {} a.tags a.timeoutAt a.timeoutAt (some a.timeoutAt)
        insertTask a.id .fulfilled 0 none none
        let p ← getPromiseRow a.id
        let t ← getTaskRow a.id
        return { status := 200, task := ← t.mapM taskRecordOf, promise := p.map rowToPromise }
  | some p =>
      if (txt? p "target").isNone then
        return { status := 422 }
      if liveRow p now then
        match ← getTaskRow a.id with
        | none => return { status := 409 }
        | some t =>
          if txt t "state" == "fulfilled" then
            return { status := 200, task := some (← taskRecordOf t), promise := some (rowToPromise p) }
          else if txt t "state" == "pending" then
            let _ ← query
              "UPDATE tasks SET state = 'acquired', version = version + 1, ttl = ?2, pid = ?3 WHERE id = ?1"
              [.text a.id, .int (Int.ofNat req.ttl), .text req.pid]
            let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text a.id]
            delTaskTimeout a.id
            setTaskTimeout a.id 1 (now + req.ttl)
            let t' ← getTaskRow a.id
            return { status := 200, task := ← t'.mapM taskRecordOf, promise := some (rowToPromise p) }
          else
            return { status := 409 }
      else
        match ← getTaskRow a.id with
        | none => return { status := 409 }
        | some t =>
            return { status := 200,
                     task := some { id := txt t "id", state := .fulfilled,
                                    version := nat t "version", resumes := 0,
                                    ttl := none, pid := none },
                     promise := some (rowToPromise p) }

/-! ## T-03 · task.acquire -/

def taskAcquire (req : TaskAcquireReq) (now : Nat) : m TaskAcquireRes := do
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 409 }
  | some (t, some p) =>
      if txt t "state" != "pending" then return { status := 409 }
      if !liveRow p now then return { status := 409 }
      if nat t "version" != req.version then return { status := 409 }
      let _ ← query
        "UPDATE tasks SET state = 'acquired', version = version + 1, ttl = ?2, pid = ?3 WHERE id = ?1"
        [.text req.id, .int (Int.ofNat req.ttl), .text req.pid]
      let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text req.id]
      delTaskTimeout req.id
      setTaskTimeout req.id 1 (now + req.ttl)
      let t' ← getTaskRow req.id
      return { status := 200, task := ← t'.mapM taskRecordOf, promise := some (rowToPromise p) }

/-! ## T-04 · task.fence -/

def taskFence (req : TaskFenceReq) (now : Nat) : m TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 409 }
  | some (t, some p) =>
      if txt t "state" != "acquired" then return { status := 409 }
      if !liveRow p now then return { status := 409 }
      if nat t "version" != req.version then return { status := 409 }
      match req.action with
      | .create r => return { status := 200, action := some (.create (← promiseCreate r now)) }
      | .settle r => return { status := 200, action := some (.settle (← promiseSettle r now)) }

/-! ## T-05 · task.heartbeat -/

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : m TaskHeartbeatRes := do
  for ref in req.tasks do
    match ← touchTask ref.id now with
    | some (t, some p) =>
        if txt t "state" == "acquired" && nat t "version" == ref.version
             && txt? t "pid" == some req.pid && liveRow p now then
          delTaskTimeout ref.id
          setTaskTimeout ref.id 1 (now + ((int? t "ttl").map Int.toNat).getD 0)
    | _ => pure ()
  return { status := 200 }

/-! ## T-06 · task.suspend -/

def taskSuspend (req : TaskSuspendReq) (now : Nat) : m TaskSuspendRes := do
  if req.actions.isEmpty then return { status := 400 }
  if req.actions.any (·.awaited == req.id) then return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then return { status := 400 }
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 409 }
  | some (t, some tp) =>
      if txt t "state" != "acquired" then return { status := 409 }
      if !liveRow tp now then return { status := 409 }
      if nat t "version" != req.version then return { status := 409 }
      let mut settled := false
      let mut bad := false
      for action in req.actions do
        match ← touchPromise action.awaited now with
        | none => bad := true
        | some pa =>
            if !bool pa "external" then bad := true
            else if !liveRow pa now then settled := true
      if bad then return { status := 422 }
      if settled then
        let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text req.id]
        return { status := 300 }
      else
        for action in req.actions do
          match ← touchPromise action.awaited now with
          | some _ => addCallback action.awaited req.id
          | none   => pure ()
        let _ ← query
          "UPDATE tasks SET state = 'suspended', pid = NULL, ttl = NULL WHERE id = ?1" [.text req.id]
        let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text req.id]
        delTaskTimeout req.id
        return { status := 200 }

/-! ## T-07 · task.fulfill -/

def taskFulfill (req : TaskFulfillReq) (now : Nat) : m TaskFulfillRes := do
  if !req.action.state.settable then return { status := 400 }
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 409 }
  | some (t, some p) =>
      if txt t "state" != "acquired" then return { status := 409 }
      if !liveRow p now then return { status := 409 }
      if nat t "version" != req.version then return { status := 409 }
      let ls ← query "SELECT address FROM listeners WHERE promise_id = ?1" [.text req.id]
      let cbs ← query "SELECT awaiter_id FROM callbacks WHERE awaited_id = ?1" [.text req.id]
      let _ ← query
        "UPDATE promises SET state = ?2, value_headers = ?3, value_data = ?4, settled_at = ?5 WHERE id = ?1"
        [ .text req.id, .text (promiseStateText req.action.state)
        , .text (tagsJson req.action.value.headers), optText req.action.value.data
        , .int (Int.ofNat now) ]
      let _ ← query "DELETE FROM callbacks WHERE awaited_id = ?1" [.text req.id]
      let _ ← query "DELETE FROM listeners WHERE promise_id = ?1" [.text req.id]
      delPromiseTimeout req.id
      let _ ← query "UPDATE tasks SET state = 'fulfilled', pid = NULL, ttl = NULL WHERE id = ?1"
                    [.text req.id]
      let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text req.id]
      delTaskTimeout req.id
      for row in ls.rows do
        setUnblock (txt row "address") req.id
      for row in cbs.rows do
        defer req.id (txt row "awaiter_id")
      return { status := 200, promise := (← getPromiseRow req.id).map rowToPromise }

/-! ## T-08 · task.release -/

def taskRelease (req : TaskReleaseReq) (now : Nat) : m TaskReleaseRes := do
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 409 }
  | some (t, some p) =>
      if txt t "state" != "acquired" then return { status := 409 }
      if !liveRow p now then return { status := 409 }
      if nat t "version" != req.version then return { status := 409 }
      let _ ← query "UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL WHERE id = ?1"
                    [.text req.id]
      delTaskTimeout req.id
      setTaskTimeout req.id 0 (now + retryTimeout)
      setExecute ((txt? p "target").getD "") req.id (nat t "version")
      return { status := 200 }

/-! ## T-09 · task.halt -/

def taskHalt (req : TaskHaltReq) (now : Nat) : m TaskHaltRes := do
  match ← touchTask req.id now with
  | none           => return { status := 404 }
  | some (_, none) => return { status := 404 }
  | some (t, some p) =>
      if !liveRow p now then return { status := 409 }
      if txt t "state" == "fulfilled" then return { status := 409 }
      if txt t "state" == "halted" then return { status := 200 }
      let _ ← query "UPDATE tasks SET state = 'halted', pid = NULL, ttl = NULL WHERE id = ?1"
                    [.text req.id]
      delTaskTimeout req.id
      return { status := 200 }

/-! ## T-10 · task.continue -/

def taskContinue (req : TaskContinueReq) (now : Nat) : m TaskContinueRes := do
  match ← touchTask req.id now with
  | none => return { status := 404 }
  | some (t, pOpt) =>
      if txt t "state" != "halted" then return { status := 409 }
      match pOpt with
      | none => return { status := 404 }
      | some p =>
        if !liveRow p now then return { status := 409 }
        let _ ← query "UPDATE tasks SET state = 'pending' WHERE id = ?1" [.text req.id]
        setTaskTimeout req.id 0 (now + retryTimeout)
        setExecute ((txt? p "target").getD "") req.id (nat t "version")
        return { status := 200 }

/-! ## T-11 · task.search — 501 -/

def taskSearch (_req : TaskSearchReq) (_now : Nat) : m TaskSearchRes := do
  return { status := 501 }

/-! ## τ · internal transitions

From `03-concrete/m/02-timeouts.lean` and `03-resume.lean`. -/

def onPromiseTimeout (id : String) (now : Nat) : m Unit := do
  let _ ← touchPromise id now

def onTaskRetryTimeout (id : String) (now : Nat) : m Unit := do
  let tt ← query "SELECT timeout_at FROM task_timeouts WHERE id = ?1 AND kind = 0" [.text id]
  match tt.rows.head? with
  | none => pure ()
  | some tr =>
    if int tr "timeout_at" > Int.ofNat now then pure ()
    else
      match ← getTaskRow id with
      | none => pure ()
      | some t =>
        if txt t "state" != "pending" then pure ()
        else
          match ← getPromiseRow id with
          | none => pure ()
          | some p =>
            -- TIMEOUT ALWAYS WINS: no new work for a logically dead task
            if liveRow p now then
              delTaskTimeout id
              setTaskTimeout id 0 (now + retryTimeout)
              setExecute ((txt? p "target").getD "") id (nat t "version")

def onTaskLeaseTimeout (id : String) (now : Nat) : m Unit := do
  let tt ← query "SELECT timeout_at FROM task_timeouts WHERE id = ?1 AND kind = 1" [.text id]
  match tt.rows.head? with
  | none => pure ()
  | some tr =>
    if int tr "timeout_at" > Int.ofNat now then pure ()
    else
      match ← getTaskRow id with
      | none => pure ()
      | some t =>
        if txt t "state" != "acquired" then pure ()
        else
          match ← getPromiseRow id with
          | none => pure ()
          | some p =>
            if liveRow p now then
              let _ ← query "UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL WHERE id = ?1"
                            [.text id]
              delTaskTimeout id
              setTaskTimeout id 0 (now + retryTimeout)
              setExecute ((txt? p "target").getD "") id (nat t "version")

def fireOccurrence (s : Schedule) (t : Nat) : m Unit := do
  let _ ← promiseCreate
    { id := expand s.promiseId s.id t, timeoutAt := t + s.promiseTimeout,
      param := s.promiseParam, tags := s.promiseTags } t

def fireAll (s : Schedule) : List Nat → m Unit
  | []      => pure ()
  | t :: ts => do fireOccurrence s t; fireAll s ts

def onScheduleTimeout (id : String) (now : Nat) : m Unit := do
  let r ← query "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?1" [.text id]
  match r.rows.head? with
  | none => pure ()
  | some row =>
    let s0 := rowToSchedule row
    if s0.nextRunAt > now then pure ()
    else
      let ts := (occurrences s0.cron s0.nextRunAt now).filter (· ≤ now)
      fireAll s0 ts
      match ts.getLast? with
      | some last =>
          let _ ← query "UPDATE schedules SET last_run_at = ?2, next_run_at = ?3 WHERE id = ?1"
                        [.text id, .int (Int.ofNat last),
                         .int (Int.ofNat (nextCron s0.cron last))]
          let _ ← query "DELETE FROM schedule_timeouts WHERE id = ?1" [.text id]
          setScheduleTimeout id (nextCron s0.cron last)
      | none =>
          let _ ← query "DELETE FROM schedule_timeouts WHERE id = ?1" [.text id]
          setScheduleTimeout id s0.nextRunAt

/-- `onResume` — `03-concrete/m/03-resume.lean`. -/
def onResume (awaited awaiter : String) (now : Nat) : m ResumeRes := do
  match ← touchTask awaiter now with
  | none           => return { outcome := .absent }
  | some (_, none) => return { outcome := .absent }
  | some (t, some p) =>
      if now >= nat p "timeout_at" then
        return { outcome := .expired }
      match taskStateOf (txt t "state") with
      | .suspended =>
          let _ ← query "UPDATE tasks SET state = 'pending' WHERE id = ?1" [.text awaiter]
          let _ ← query "DELETE FROM resumes WHERE task_id = ?1" [.text awaiter]
          let _ ← query "INSERT INTO resumes (task_id, awaited_id) VALUES (?1, ?2)"
                        [.text awaiter, .text awaited]
          setTaskTimeout awaiter 0 (now + retryTimeout)
          let target := (txt? p "target").getD ""
          if target != "" then
            setExecute target awaiter (nat t "version")
          return { outcome := .resumed }
      | .pending | .acquired | .halted =>
          let ex ← query "SELECT awaited_id FROM resumes WHERE task_id = ?1 AND awaited_id = ?2"
                         [.text awaiter, .text awaited]
          if ex.rows.isEmpty then
            let _ ← query "INSERT INTO resumes (task_id, awaited_id) VALUES (?1, ?2)"
                          [.text awaiter, .text awaited]
            return { outcome := .buffered }
          else
            return { outcome := .duplicate }
      | .fulfilled =>
          return { outcome := .fulfilled }

/-- Drain every deferred obligation — the eager schedule. -/
def drain (now : Nat) : m Unit := do
  let ds ← query "SELECT awaited_id, awaiter_id FROM deferred" []
  for d in ds.rows do
    undefer (txt d "awaited_id") (txt d "awaiter_id")
    let _ ← onResume (txt d "awaited_id") (txt d "awaiter_id") now

end Sqlite
