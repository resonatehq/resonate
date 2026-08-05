import «05-sqlite».driver

/-!  # promise.get, with the SQL in the handler

The effect is `query : String → List SqlValue → m QueryResult`. Every
statement below is the text `rusqlite` receives, copied from
`resonatehq/resonate`. Nothing stands between the handler and the
engine — no typed accessor, no per-statement Lean function whose
correspondence to the SQL is asserted in a comment.

The difference from `P-01-promise.get.lean` is not stylistic. There, a
`MonadSql` method named `settlementEnqueued` did what I believed
`settlement_enqueued` did, and the sweep confirmed my belief agreed with
the specification. Here `settlementEnqueued` issues

    UPDATE tasks SET state = 'fulfilled' WHERE id = ?1 AND state != 'fulfilled'

which — unlike the typed version I first wrote — does NOT clear `pid` or
`ttl`, because in this schema `tasks` has no such columns. That was
invisible before and is checkable now.
-/

namespace SqliteDriver

open Sql ServerModel

/-- The cadence the driver writes into `task_timeouts`. One source of
    truth with `alpha`'s `retryCadence`, so the implementation and the
    specification's config cannot disagree by accident. -/
def retryTimeout : Int := (retryCadence : Int)

/-- `settlement_enqueued` — `persistence_sqlite.rs:210`. -/
def settlementEnqueued [Monad m] [MonadQuery m] (promiseId : String) : m Bool := do
  let r ← query
    "UPDATE tasks SET state = 'fulfilled' WHERE id = ?1 AND state != 'fulfilled'"
    [.text promiseId]
  if r.changes > 0 then
    let _ ← query "DELETE FROM task_timeouts WHERE id = ?1" [.text promiseId]
    let _ ← query "DELETE FROM callbacks WHERE awaiter_id = ?1" [.text promiseId]
    return true
  else
    return false

/-- `resumption_enqueued` — `persistence_sqlite.rs:229`. -/
def resumptionEnqueued [Monad m] [MonadQuery m]
    (awaitedId : String) (time : Int) (excludeFulfilled : List String) : m Unit := do
  let _ ← query "UPDATE callbacks SET ready = true WHERE awaited_id = ?1" [.text awaitedId]
  let sel ← query
    "SELECT DISTINCT c.awaiter_id FROM callbacks c JOIN tasks t ON t.id = c.awaiter_id WHERE c.awaited_id = ?1 AND c.ready = true AND t.state = 'suspended'"
    [.text awaitedId]
  let awaiterIds := sel.rows.filterMap (txt? · "awaiter_id")
  for awaiterId in awaiterIds do
    unless excludeFulfilled.contains awaiterId do
      let upd ← query
        "UPDATE tasks SET state = 'pending' WHERE id = ?1 AND state = 'suspended'"
        [.text awaiterId]
      if upd.changes > 0 then
        let v ← query "SELECT version FROM tasks WHERE id = ?1" [.text awaiterId]
        let version := (v.rows.head?.map (int · "version")).getD 0
        let _ ← query
          "INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?1, ?2, 0, ?3) ON CONFLICT (id) DO UPDATE SET timeout_at = ?1, timeout_type = 0, process_id = NULL, ttl = ?3"
          [.int (time + retryTimeout), .text awaiterId, .int retryTimeout]
        let t ← query "SELECT target FROM promises WHERE id = ?1" [.text awaiterId]
        match t.rows.head?.bind (txt? · "target") with
        | some target =>
            let _ ← query
              "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address"
              [.text awaiterId, .int version, .text target]
            pure ()
        | none => pure ()

/-- `listener_unblocked` — `persistence_sqlite.rs:304`. -/
def listenerUnblocked [Monad m] [MonadQuery m] (promiseId : String) : m Unit := do
  let _ ← query
    "INSERT INTO outgoing_unblock (promise_id, address) SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = ?1 ON CONFLICT DO NOTHING"
    [.text promiseId]
  let _ ← query "DELETE FROM listeners WHERE promise_id = ?1" [.text promiseId]

/-- `try_timeout` — `persistence_sqlite.rs:368`, including its three-pass
    shape: settle every expired id, then resume for every id, then unblock
    for every id. -/
def tryTimeout [Monad m] [MonadQuery m] (ids : List String) (time : Int) : m Unit := do
  if ids.isEmpty then return ()
  let sel ← query
    "SELECT id, timer, timeout_at FROM promises WHERE id IN (SELECT value FROM json_each(?1)) AND state = 'pending' AND timeout_at <= ?2"
    [.text (jsonArray ids), .int time]
  let expired := sel.rows
  if expired.isEmpty then return ()
  let mut fulfilled : List String := []
  for row in expired do
    let id := txt row "id"
    let newState := if bool row "timer" then "resolved" else "rejected_timedout"
    let _ ← query
      "UPDATE promises SET state = ?2, settled_at = ?3 WHERE id = ?1 AND state = 'pending'"
      [.text id, .text newState, .int (int row "timeout_at")]
    let _ ← query "DELETE FROM promise_timeouts WHERE id = ?1" [.text id]
    if ← settlementEnqueued id then
      fulfilled := fulfilled ++ [id]
  for row in expired do
    resumptionEnqueued (txt row "id") time fulfilled
  for row in expired do
    listenerUnblocked (txt row "id")

/-- **The handler** — `op_promise_get`, `server.rs:489`. -/
def promiseGet [Monad m] [MonadQuery m]
    (req : PromiseGetReq) (now : Nat) : m PromiseGetRes := do
  tryTimeout [req.id] (now : Int)
  let r ← query
    "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?1"
    [.text req.id]
  match r.rows.head? with
  | some row => return { status := 200, promise := some (rowToPromise row) }
  | none     => return { status := 404 }

end SqliteDriver
