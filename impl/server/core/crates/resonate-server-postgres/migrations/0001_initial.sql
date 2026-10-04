-- =============================================================================
-- Postgres schema, v0
-- =============================================================================
-- Constraints included: they are statements in this file, not a catalogue
-- applied beside it, so a database carrying these tables carries these
-- invariants.
--
-- One promise is one row. Beside it sits only `schedules` — a separate id space
-- and a genuinely different entity.
--
-- Replaces the eight tables of the multi-table backend: promises,
-- promise_timeouts, tasks, task_timeouts, callbacks, listeners,
-- outgoing_execute, outgoing_unblock — plus schedule_timeouts, whose
-- (timeout_at, id) was a verbatim copy of (next_run_at, id).
--
-- There is no outbox either. A message is returned by the transition that
-- emitted it and delivered by the caller, so there is nothing to store and
-- nothing to drain.
--
-- =============================================================================
--
-- Edited in place, not followed by a 0002.
--
-- Until release there is no database anyone needs carried forward, so a schema
-- change goes into THIS file and the migration set stays at one. Nothing
-- accumulates, and the schema reads as the shape it is rather than as a shape
-- plus a history of amendments.
--
-- The cost is that a database created before an edit holds version 1 under the
-- old checksum. The migrator refuses it rather than guessing: drop the
-- database and let the server create it again. After release this reverses —
-- 0001 freezes and changes become 0002 onward.
--

CREATE SCHEMA IF NOT EXISTS resonate;
SET search_path TO resonate, public;

-- --- promises ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS promises (
  -- promise ------------------------------------------------------------------
  id            TEXT PRIMARY KEY,
  -- Domain: promises_state_check, in the invariants trigger below.
  state         TEXT   NOT NULL DEFAULT 'pending',
  param_headers JSONB  NOT NULL DEFAULT '{}',
  param_data    TEXT,
  value_headers JSONB  NOT NULL DEFAULT '{}',
  value_data    TEXT,
  tags          JSONB  NOT NULL DEFAULT '{}',
  timeout_at    BIGINT NOT NULL,
  created_at    BIGINT NOT NULL,
  settled_at    BIGINT,

  -- projections of id / tags -------------------------------------------------
  origin_id     TEXT    GENERATED ALWAYS AS (split_part(id, ':', 1)) STORED,
  parent_id     TEXT    GENERATED ALWAYS AS (tags->>'resonate:parent') STORED,
  branch_id     TEXT    GENERATED ALWAYS AS (tags->>'resonate:branch') STORED,
  target        TEXT    GENERATED ALWAYS AS (tags->>'resonate:target') STORED,
  is_timer      BOOLEAN NOT NULL GENERATED ALWAYS AS (
                  COALESCE(tags->>'resonate:timer', '') = 'true') STORED,
  -- `resonate_core::types::otype`, in SQL: who may be blocked on this. Four
  -- alternatives, not a hierarchy — `scope = global` is the one the wire
  -- actually carries, and the one that makes a caller's promise awaitable.
  external      BOOLEAN NOT NULL GENERATED ALWAYS AS (
                  COALESCE(tags->>'resonate:scope', '')       = 'global'
                  OR COALESCE(tags->>'resonate:external', '') = 'true'
                  OR tags->>'resonate:target' IS NOT NULL
                  OR COALESCE(tags->>'resonate:timer', '')    = 'true') STORED,

  -- Was the target of the outbox foreign key, which needed a TOTAL key to
  -- reference where "the promises that are tasks" is a partial set. The outbox
  -- is gone; the catalogue still keys task constraints off it.
  task_key      TEXT    GENERATED ALWAYS AS (
                  CASE WHEN tags ? 'resonate:target' THEN id END) STORED,

  -- task — NULL task_state ⟺ the promise carries no resonate:target -----------
  -- Domain: promises_task_state_check, in the invariants trigger below.
  task_state    TEXT,
  task_version  INT NOT NULL DEFAULT 0,

  -- task deadlines. The multi-table task_timeouts.timeout_type discriminator
  -- collapses into two columns: the retry deadline is live exactly while
  -- task_state='pending', the lease deadline exactly while 'acquired'.
  retry_timeout_at      BIGINT,
  lease_timeout_at    BIGINT,
  ttl           BIGINT,
  pid           TEXT,

  -- callbacks, both directions ------------------------------------------------
  callbacks      TEXT[] NOT NULL DEFAULT '{}',  -- ids blocked on me   (ready = false)
  resumes       TEXT[] NOT NULL DEFAULT '{}',  -- ids ready for me    (ready = true)

  -- listeners -----------------------------------------------------------------
  listeners     TEXT[] NOT NULL DEFAULT '{}',

  -- ── unconsumed ─────────────────────────────────────────────────────────
  -- Nothing reads these yet.
  --
  -- Declared last on purpose. Postgres records a tuple's attribute count in
  -- its header and stops storing at the last non-NULL one, so a run of
  -- trailing NULLs occupies no space; the null bitmap that covers them also
  -- stays 4 bytes, since 30 columns fit the same word 26 did. Measured: a
  -- pending promise with all four unset is 131 bytes, exactly what it was
  -- before these columns existed. Put them anywhere earlier and every row
  -- pays for the slot -- which matters here, because a task state transition
  -- rewrites the whole promise row.
  pmessage      TEXT,
  tmessage      TEXT,
  func          TEXT,
  args          TEXT
);

-- Vacuum this table early. Its rows are rewritten on every task transition,
-- and the deadline indexes below churn fastest of all: an entry goes stale the
-- moment its row is settled, acquired or redispatched. At the default (vacuum
-- once a fifth of the table is dead) a table of a few million promises carries
-- hundreds of thousands of dead entries between vacuums, and the timer's
-- refresh, which walks the front of those indexes, slowed from 1.6 ms to
-- 130 ms reading them. These are the owner's to set, so they live here.
ALTER TABLE promises SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay = 0
);

-- The deadline queues. Each is a partial index whose predicate is exactly the
-- queue's membership and whose key is `(deadline, id)` — the order the timer
-- reads them in — so the timer's refresh (`upcoming`) is a top-N walk of the
-- front of each index, and firing a named deadline is a primary-key probe.
-- Nothing ever scans the table to find what is due.
--
-- The promise queue is `state = 'pending' AND external`: an internal promise
-- arms nothing and times out lazily, on first touch (try_timeout), so it is
-- not in the index at all.
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at
  ON promises (timeout_at, id) WHERE state = 'pending' AND external;
CREATE INDEX IF NOT EXISTS idx_task_retry_timeout_at
  ON promises (retry_timeout_at, id) WHERE task_state = 'pending';
CREATE INDEX IF NOT EXISTS idx_task_lease_timeout_at
  ON promises (lease_timeout_at, id) WHERE task_state = 'acquired';

-- The console's lineage read (`ui.execution.get`, `ui.executions.search`).
CREATE INDEX IF NOT EXISTS idx_promises_origin_id
  ON promises (origin_id);
-- The console's executions list: roots only, in its default order. A page is
-- a walk of the front (or back) of this, and the total an index-only count;
-- without it both were a parallel scan of every promise on every refresh.
CREATE INDEX IF NOT EXISTS idx_promises_roots
  ON promises (created_at, id) WHERE id = origin_id;
-- Preload: a task's branch siblings.
CREATE INDEX IF NOT EXISTS idx_promises_branch_id
  ON promises (branch_id) WHERE branch_id IS NOT NULL;
-- task.search.
CREATE INDEX IF NOT EXISTS idx_promises_task
  ON promises (task_state, id) WHERE task_state IS NOT NULL;

-- No index on `target`: nothing selects by it, and a promise row is rewritten
-- on every task transition, so every index it carries is paid for on every
-- one of those writes.

-- Fan-out in the other direction: "which rows list me as an awaiter", the
-- One-row stand-in for `DELETE FROM callbacks WHERE awaiter_id = $1`.
--
-- Partial, over the rows that have an awaiter at all — a pending promise
-- something is suspended on, a sliver of the table. Unconditional, every
-- insert and every task transition wrote an empty-array entry into it. And
-- no fast update: with it, those entries queued in a pending list that every
-- lookup reads end to end, and a settle that fans out paid for the whole
-- list (measured: 167 pages, 43k tuples, on every task.fulfill). A query
-- reaches this index only if it says `callbacks <> '{}'` itself.
CREATE INDEX IF NOT EXISTS idx_promises_callbacks
  ON promises USING GIN (callbacks) WITH (fastupdate = off)
  WHERE callbacks <> '{}';

-- --- schedules --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS schedules (
  id                    TEXT PRIMARY KEY,
  cron                  TEXT NOT NULL,
  promise_id            TEXT NOT NULL,
  promise_timeout       BIGINT NOT NULL,
  promise_param_headers JSONB NOT NULL DEFAULT '{}',
  promise_param_data    TEXT,
  promise_tags          JSONB NOT NULL DEFAULT '{}',
  created_at            BIGINT NOT NULL,
  next_run_at           BIGINT NOT NULL,
  last_run_at           BIGINT
);

CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at
  ON schedules (next_run_at ASC, id ASC);

-- --- promise → wire JSON -----------------------------------------------------
-- An unblock message carries the settled promise, built here so the engine can
-- return it from the statement that settled it. Field names and omissions match
-- `PromiseRecord`'s serde: camelCase timestamps, `settledAt`/`headers`/`data`
-- omitted when absent.
CREATE OR REPLACE FUNCTION resonate._promise_json(
  id TEXT, state TEXT,
  param_headers JSONB, param_data TEXT,
  value_headers JSONB, value_data TEXT,
  tags JSONB, timeout_at BIGINT, created_at BIGINT, settled_at BIGINT
) RETURNS JSONB LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$
  SELECT jsonb_strip_nulls(jsonb_build_object(
    'id',    id,
    'state', state,
    'param', jsonb_strip_nulls(jsonb_build_object(
               'headers', NULLIF(param_headers, '{}'::jsonb), 'data', param_data)),
    'value', jsonb_strip_nulls(jsonb_build_object(
               'headers', NULLIF(value_headers, '{}'::jsonb), 'data', value_data)),
    'tags',      tags,
    'timeoutAt', timeout_at,
    'createdAt', created_at,
    'settledAt', settled_at
  ))
$$;


-- =============================================================================
-- Constraints
-- =============================================================================
-- Part of the schema, not a catalogue applied beside it. `init` installs this
-- file whole, so a database carrying the tables carries the constraints too and
-- there is no configuration under which the server runs without them.
--
-- Names are the specification's property names verbatim wherever an entry has
-- one, so a violation reports the same string the Lean catalogue and the trace
-- checker use. Every statement is DROP IF EXISTS then ADD, so re-running `init`
-- against an existing database is idempotent and installs anything missing.
-- =============================================================================

SET search_path TO resonate, public;

-- `task_key` is the id exactly when the row is a task; UNIQUE tolerates the
-- NULLs of the rows that are not. It backed the outbox foreign key, which
-- needed a TOTAL key to reference where "the promises that are tasks" is a
-- partial set; the outbox is gone and the uniqueness entry remains.
-- task_key is declared as a generated column above; kept here as a comment so
-- the constraint section stays a faithful copy of the generated catalogue.
-- ALTER TABLE promises ADD COLUMN IF NOT EXISTS task_key TEXT
--   GENERATED ALWAYS AS (CASE WHEN tags ? 'resonate:target' THEN id END) STORED;


-- --- promises: keys --------------------------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_pkey;
ALTER TABLE promises ADD CONSTRAINT promises_pkey
  PRIMARY KEY (id);

-- No UNIQUE (task_key): `task_key` is either `id` or NULL, so the primary key
-- already makes it unique, and the index that enforced it again was one more
-- write on every insert and every task transition.
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_key_unique;


-- --- promises: the invariants, as one trigger --------------------------------
--
-- Every invariant on a promise row is checked by `promises_well_formed`, an
-- AFTER ROW trigger, rather than by a CHECK constraint each. Same expressions,
-- verbatim, over NEW; same names; same failure — SQLSTATE 23514, "new row for
-- relation "promises" violates check constraint "<name>"", and the statement
-- aborts with nothing written, exactly as a CHECK would.
--
-- Why not CHECK: Postgres re-reads and re-plans every CHECK expression of a
-- table at the start of every statement that writes it, per writing node. With
-- 31 of them that was 0.23 ms per UPDATE (measured: 0.35 ms against 0.12 ms on
-- the same table without them), paid two or three times by a settle that fans
-- out — the single largest cost of a transition. A PL/pgSQL function plans its
-- expressions once per session; the same checks cost 0.05 ms or less.
--
-- What CHECK had that this does not: a trigger can be disabled
-- (`ALTER TABLE ... DISABLE TRIGGER`, `session_replication_role = replica`).
-- Nothing in this server does either.

ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_state_check;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_state_check;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_task_iff_targeted_promise;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_promise_has_fulfilled_task;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_task_promise_settled;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_timeout_at;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_created_before_deadline;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_lte_timeout_at;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_settled_at;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_iff_not_pending;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_has_no_value;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_verdict_matches_timer_tag;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_settlement_has_no_value;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timedout_is_server_owned;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timer_not_targeted;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_obligations_require_external;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_awaiter_is_not_self;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_pid;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_ttl;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_lease_timeout_at;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_pending_iff_has_retry_timeout_at;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_fulfilled_is_cleared;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_is_cleared;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_halted_is_cleared;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_has_no_resumes;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_version_positive;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_callbacks_unique;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_listeners_unique;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_resumes_unique;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_listener_addresses_deliverable;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_id_at_most_one_separator;

CREATE OR REPLACE FUNCTION resonate._violated(name TEXT) RETURNS void
  LANGUAGE plpgsql AS
$$
BEGIN
  RAISE EXCEPTION USING
    ERRCODE = 'check_violation',
    MESSAGE = format('new row for relation "promises" violates check constraint "%s"', name),
    CONSTRAINT = name, SCHEMA = 'resonate', TABLE = 'promises';
END
$$;

CREATE OR REPLACE FUNCTION resonate._promises_well_formed() RETURNS trigger
  LANGUAGE plpgsql AS
$$
BEGIN
  -- The array checks are written out rather than calling helper functions: a
  -- SQL function called from here was planned again on every call, where an
  -- expression in this body is planned once per session. Uniqueness of an
  -- array is its cardinality against its distinct count; a deliverable address
  -- is any URI with a scheme, as `core::is_valid_address` accepts.
  --
  -- --- promises: domains — the two state enums ------------------------------
  IF NOT ((NEW.state = ANY (ARRAY['pending'::text, 'resolved'::text, 'rejected'::text, 'rejected_canceled'::text, 'rejected_timedout'::text]))) THEN
    PERFORM resonate._violated('promises_state_check');
  END IF;

  IF NOT ((NEW.task_state = ANY (ARRAY['pending'::text, 'acquired'::text, 'suspended'::text, 'halted'::text, 'fulfilled'::text]))) THEN
    PERFORM resonate._violated('promises_task_state_check');
  END IF;


  -- --- promises: promise ⊕ task — the entries a two-table layout cannot state ---
  IF NOT (((NEW.task_state IS NOT NULL) = (NEW.target IS NOT NULL))) THEN
    PERFORM resonate._violated('consistent_task_iff_targeted_promise');
  END IF;

  IF NOT (((NEW.state = 'pending'::text) OR (NEW.task_state IS NULL) OR (NEW.task_state = 'fulfilled'::text))) THEN
    PERFORM resonate._violated('consistent_settled_promise_has_fulfilled_task');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'fulfilled'::text) OR (NEW.state <> 'pending'::text))) THEN
    PERFORM resonate._violated('consistent_settled_task_promise_settled');
  END IF;


  -- --- promises: promise well-formedness -------------------------------------
  IF NOT ((NEW.created_at <= NEW.timeout_at)) THEN
    PERFORM resonate._violated('well_formed_promise_created_at_lte_timeout_at');
  END IF;

  IF NOT (((NEW.state <> 'pending'::text) OR (NEW.created_at < NEW.timeout_at))) THEN
    PERFORM resonate._violated('well_formed_promise_pending_created_before_deadline');
  END IF;

  IF NOT (((NEW.settled_at IS NULL) OR (NEW.settled_at <= NEW.timeout_at))) THEN
    PERFORM resonate._violated('well_formed_promise_settled_at_lte_timeout_at');
  END IF;

  IF NOT (((NEW.settled_at IS NULL) OR (NEW.created_at <= NEW.settled_at))) THEN
    PERFORM resonate._violated('well_formed_promise_created_at_lte_settled_at');
  END IF;

  IF NOT (((NEW.state <> 'pending'::text) = (NEW.settled_at IS NOT NULL))) THEN
    PERFORM resonate._violated('well_formed_promise_settled_at_iff_not_pending');
  END IF;

  IF NOT (((NEW.state <> 'pending'::text) OR ((NEW.value_data IS NULL) AND (NEW.value_headers = '{}'::jsonb)))) THEN
    PERFORM resonate._violated('well_formed_promise_pending_has_no_value');
  END IF;

  IF NOT (((NEW.settled_at IS DISTINCT FROM NEW.timeout_at) OR (NEW.state = CASE WHEN NEW.is_timer THEN 'resolved'::text ELSE 'rejected_timedout'::text END))) THEN
    PERFORM resonate._violated('well_formed_promise_deadline_verdict_matches_timer_tag');
  END IF;

  IF NOT (((NEW.settled_at IS DISTINCT FROM NEW.timeout_at) OR ((NEW.value_data IS NULL) AND (NEW.value_headers = '{}'::jsonb)))) THEN
    PERFORM resonate._violated('well_formed_promise_deadline_settlement_has_no_value');
  END IF;

  IF NOT (((NEW.state <> 'rejected_timedout'::text) OR (NEW.settled_at = NEW.timeout_at))) THEN
    PERFORM resonate._violated('well_formed_promise_timedout_is_server_owned');
  END IF;

  IF NOT ((NOT (NEW.is_timer AND (NEW.target IS NOT NULL)))) THEN
    PERFORM resonate._violated('well_formed_promise_timer_not_targeted');
  END IF;

  IF NOT ((NEW.external OR ((NEW.callbacks = '{}'::text[]) AND (NEW.listeners = '{}'::text[])))) THEN
    PERFORM resonate._violated('well_formed_promise_obligations_require_external');
  END IF;

  IF NOT ((NOT (NEW.id = ANY (NEW.callbacks)))) THEN
    PERFORM resonate._violated('well_formed_promise_awaiter_is_not_self');
  END IF;


  -- --- promises: task well-formedness ----------------------------------------
  IF NOT (((NEW.task_state IS NULL) OR ((NEW.task_state = 'acquired'::text) = (NEW.pid IS NOT NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_acquired_iff_has_pid');
  END IF;

  IF NOT (((NEW.task_state IS NULL) OR ((NEW.task_state = 'acquired'::text) = (NEW.ttl IS NOT NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_acquired_iff_has_ttl');
  END IF;

  IF NOT (((NEW.task_state IS NULL) OR ((NEW.task_state = 'acquired'::text) = (NEW.lease_timeout_at IS NOT NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_acquired_iff_has_lease_timeout_at');
  END IF;

  IF NOT (((NEW.task_state IS NULL) OR ((NEW.task_state = 'pending'::text) = (NEW.retry_timeout_at IS NOT NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_pending_iff_has_retry_timeout_at');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'fulfilled'::text) OR ((NEW.pid IS NULL) AND (NEW.ttl IS NULL) AND (NEW.lease_timeout_at IS NULL) AND (NEW.retry_timeout_at IS NULL) AND (NEW.resumes = '{}'::text[])))) THEN
    PERFORM resonate._violated('well_formed_task_fulfilled_is_cleared');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'suspended'::text) OR ((NEW.pid IS NULL) AND (NEW.ttl IS NULL) AND (NEW.lease_timeout_at IS NULL) AND (NEW.retry_timeout_at IS NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_suspended_is_cleared');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'halted'::text) OR ((NEW.pid IS NULL) AND (NEW.ttl IS NULL) AND (NEW.lease_timeout_at IS NULL) AND (NEW.retry_timeout_at IS NULL)))) THEN
    PERFORM resonate._violated('well_formed_task_halted_is_cleared');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'suspended'::text) OR (NEW.resumes = '{}'::text[]))) THEN
    PERFORM resonate._violated('well_formed_task_suspended_has_no_resumes');
  END IF;

  IF NOT (((NEW.task_state IS DISTINCT FROM 'acquired'::text) OR (NEW.task_version >= 1))) THEN
    PERFORM resonate._violated('well_formed_task_acquired_version_positive');
  END IF;


  -- --- promises: obligations and uniqueness ----------------------------------
  IF NOT (((cardinality(NEW.callbacks) < 2) OR cardinality(NEW.callbacks) = (SELECT count(DISTINCT e) FROM unnest(NEW.callbacks) e))) THEN
    PERFORM resonate._violated('well_formed_promise_callbacks_unique');
  END IF;

  IF NOT (((cardinality(NEW.listeners) < 2) OR cardinality(NEW.listeners) = (SELECT count(DISTINCT e) FROM unnest(NEW.listeners) e))) THEN
    PERFORM resonate._violated('well_formed_promise_listeners_unique');
  END IF;

  IF NOT (((cardinality(NEW.resumes) < 2) OR cardinality(NEW.resumes) = (SELECT count(DISTINCT e) FROM unnest(NEW.resumes) e))) THEN
    PERFORM resonate._violated('well_formed_task_resumes_unique');
  END IF;

  IF NOT (((NEW.listeners = '{}'::text[]) OR (SELECT bool_and(e IS NOT NULL AND e ~ '^[A-Za-z][A-Za-z0-9+.-]*:') FROM unnest(NEW.listeners) e))) THEN
    PERFORM resonate._violated('consistent_listener_addresses_deliverable');
  END IF;


  -- --- promises: id format — this deployment's convention, not a catalogue entry ---
  IF NOT ((NEW.id ~ '^[^:]*(:[^:]*)?$'::text)) THEN
    PERFORM resonate._violated('well_formed_promise_id_at_most_one_separator');
  END IF;

  RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS promises_well_formed ON promises;
CREATE TRIGGER promises_well_formed
  AFTER INSERT OR UPDATE ON promises
  FOR EACH ROW EXECUTE FUNCTION resonate._promises_well_formed();


-- --- schedules: schedules --------------------------------------------------
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_pkey;
ALTER TABLE schedules ADD CONSTRAINT schedules_pkey
  PRIMARY KEY (id);

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_created_at_lte_next_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_next_run_at
  CHECK ((created_at <= next_run_at));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_created_at_lte_last_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_last_run_at
  CHECK (((last_run_at IS NULL) OR (created_at <= last_run_at)));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_last_run_at_lt_next_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_last_run_at_lt_next_run_at
  CHECK (((last_run_at IS NULL) OR (last_run_at < next_run_at)));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_promise_tags_not_timer_targeted;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_promise_tags_not_timer_targeted
  CHECK ((NOT ((COALESCE((promise_tags ->> 'resonate:timer'::text),
  ''::text) = 'true'::text) AND (promise_tags ? 'resonate:target'::text))));
