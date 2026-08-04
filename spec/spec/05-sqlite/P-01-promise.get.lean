import «05-sqlite».«store»

/-!  # P-01 · promise.get, over SQL

Transcribed from `resonatehq/resonate`:

    // src/server.rs:489  op_promise_get
    db.try_timeout(&[&r.id], now)?;
    match db.promise_get(&r.id)? {
        Some(promise) => 200 { promise },
        None          => 404,
    }

Note what that is. `try_timeout` fires the anticipated timeout at the
moment of observation and `promise_get` then serves STORED state — the
`-m` discipline exactly, arrived at independently. It is not `-p`: the
storage layer's `promise_get` does not take `now` at all
(`persistence_sqlite.rs:432`), so it cannot project. The read has to
materialise first or serve a stale row.

So the specification this handler is measured against is
`Materialized.promiseGet`, and `tryTimeout` below is measured against
`Materialized.touchPromise`. They line up statement for effect:

| `try_timeout` (`persistence_sqlite.rs:368`) | `touchPromise` (`m/00-touch.lean`) |
|---|---|
| `SELECT id, timer, timeout_at … state='pending' AND timeout_at <= ?2` | the `p.state != .pending ∨ p.timeoutAt > now` guard |
| `UPDATE promises SET state = ?2, settled_at = ?3` | `setPromise (p.project p.timeoutAt)` |
| `DELETE FROM promise_timeouts WHERE id = ?1` | `delPromiseTimeout p.id` |
| `settlement_enqueued` | `setTask { t with state := .fulfilled … }`, `delTaskTimeout` |
| `resumption_enqueued` | `for awaiterId in callbacks do defer …` |
| `listener_unblocked` | `for address in listeners do setMessage address (.unblock …)` |

One ordering difference, and it is worth naming rather than smoothing
over: the specification emits listener messages BEFORE deferring
resumes; `try_timeout` runs `resumption_enqueued` for every expired id
before `listener_unblocked` for every expired id. Both writes are
keyed collapse-on-set into disjoint components, so the order is not
observable — but that is a claim, and `sqliteRefinesM_state` in
`equiv.lean` is where it gets checked instead of assumed.
-/

namespace SqliteModel

open ServerModel

/-- `try_timeout(&[id], now)` — `persistence_sqlite.rs:368`, at the
    single-id instance the `promise.get` path uses. The general form
    takes a list and loops; nothing about the body changes. -/
def tryTimeout [Monad m] [MonadSql m] (id : String) (now : Int) : m Unit := do
  match ← selectExpired id now with
  | none => pure ()
  | some (id, timer, timeoutAt) =>
      -- the settle loop
      updateSettle id (if timer then "resolved" else "rejected_timedout") timeoutAt
      deletePromiseTimeout id
      let _ ← settlementEnqueued id
      -- then the two cascades, in the driver's order
      resumptionEnqueued id now
      listenerUnblocked id

/-- **The handler.** Same type as `ServerModel.promiseGet` and
    `Materialized.promiseGet` up to the monad: `Req → (now) → m Res`.
    `now` stays `Nat` — the wire clock is the specification's — and
    widens to `Int` only at the statement boundary, where SQLite's
    signed `INTEGER` begins. -/
def promiseGet [Monad m] [MonadSql m]
    (req : PromiseGetReq) (now : Nat) : m PromiseGetRes := do
  tryTimeout req.id (now : Int)
  match ← selectPromise req.id with
  | none =>
      return { status := 404 }
  | some row =>
      return { status := 200, promise := some row.toRecord }

/-- The pure instantiation — a closed term, hence `decide`-able. -/
abbrev promiseGetPure (req : PromiseGetReq) (now : Nat) : SqlM PromiseGetRes :=
  promiseGet req now

end SqliteModel
