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

-- --- helpers the constraint catalogue depends on -----------------------------

-- Array elements are pairwise distinct.
CREATE OR REPLACE FUNCTION resonate._arr_uniq(a TEXT[]) RETURNS BOOLEAN
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT cardinality(a) = (SELECT count(DISTINCT e) FROM unnest(a) e) $$;

-- Deliverable address. Mirrors core::is_valid_address, which accepts any URI
-- with a scheme (`url::Url::parse` succeeds).
CREATE OR REPLACE FUNCTION resonate._addr_valid(a TEXT) RETURNS BOOLEAN
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT a IS NOT NULL AND a ~ '^[A-Za-z][A-Za-z0-9+.-]*:' $$;

CREATE OR REPLACE FUNCTION resonate._addrs_valid(a TEXT[]) RETURNS BOOLEAN
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT bool_and(resonate._addr_valid(e)) FROM unnest(a) e $$;

-- --- promises ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS promises (
  -- promise ------------------------------------------------------------------
  id            TEXT PRIMARY KEY,
  state         TEXT   NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'resolved', 'rejected',
                                 'rejected_canceled', 'rejected_timedout')),
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
  -- `resonate_core::types::is_awaitable`, in SQL: every otype but internal —
  -- external by any of `scope = global` (the form the wire actually carries),
  -- the escape hatch, or a timer; runnable by a target. Doors ask this, and
  -- the eager sweep is exactly this: awaitable and armed are one rule, so an
  -- internal promise costs no timer.
  awaitable     BOOLEAN NOT NULL GENERATED ALWAYS AS (
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
  task_state    TEXT CHECK (task_state IN ('pending', 'acquired', 'suspended',
                                           'halted', 'fulfilled')),
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

-- The promise timeout sweep queue is NOT a column: promise_timeouts membership
-- is exactly (state = 'pending' AND awaitable). An internal promise is never
-- swept eagerly — its deadline is a projection every read applies
-- (try_timeout), never a write the machine owes.
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at
  ON promises (timeout_at) WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_promises_origin_id
  ON promises (origin_id);
CREATE INDEX IF NOT EXISTS idx_promises_branch_id
  ON promises (branch_id) WHERE branch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_target
  ON promises (target) WHERE target IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_retry_timeout_at
  ON promises (retry_timeout_at) WHERE task_state = 'pending';
CREATE INDEX IF NOT EXISTS idx_task_lease_timeout_at
  ON promises (lease_timeout_at) WHERE task_state = 'acquired';
CREATE INDEX IF NOT EXISTS idx_promises_task
  ON promises (task_state, id) WHERE task_state IS NOT NULL;

-- Fan-out in the other direction: "which rows list me as an awaiter", the
-- One-row stand-in for `DELETE FROM callbacks WHERE awaiter_id = $1`.
CREATE INDEX IF NOT EXISTS idx_promises_callbacks
  ON promises USING GIN (callbacks);

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

ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_key_unique;
ALTER TABLE promises ADD CONSTRAINT promises_task_key_unique
  UNIQUE (task_key);


-- --- promises: domains — the two state enums, inline in CREATE TABLE -------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_state_check;
ALTER TABLE promises ADD CONSTRAINT promises_state_check
  CHECK ((state = ANY (ARRAY['pending'::text, 'resolved'::text,
  'rejected'::text, 'rejected_canceled'::text, 'rejected_timedout'::text])));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_state_check;
ALTER TABLE promises ADD CONSTRAINT promises_task_state_check
  CHECK ((task_state = ANY (ARRAY['pending'::text, 'acquired'::text,
  'suspended'::text, 'halted'::text, 'fulfilled'::text])));


-- --- promises: promise ⊕ task — the entries a two-table layout cannot state ---
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_task_iff_targeted_promise;
ALTER TABLE promises ADD CONSTRAINT consistent_task_iff_targeted_promise
  CHECK (((task_state IS NOT NULL) = (target IS NOT NULL)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_promise_has_fulfilled_task;
ALTER TABLE promises ADD CONSTRAINT consistent_settled_promise_has_fulfilled_task
  CHECK (((state = 'pending'::text) OR (task_state IS NULL) OR (task_state =
  'fulfilled'::text)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_task_promise_settled;
ALTER TABLE promises ADD CONSTRAINT consistent_settled_task_promise_settled
  CHECK (((task_state IS DISTINCT FROM 'fulfilled'::text) OR (state <>
  'pending'::text)));


-- --- promises: promise well-formedness -------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_timeout_at
  CHECK ((created_at <= timeout_at));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_created_before_deadline;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_created_before_deadline
  CHECK (((state <> 'pending'::text) OR (created_at < timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_lte_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_lte_timeout_at
  CHECK (((settled_at IS NULL) OR (settled_at <= timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_settled_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_settled_at
  CHECK (((settled_at IS NULL) OR (created_at <= settled_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_iff_not_pending;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_iff_not_pending
  CHECK (((state <> 'pending'::text) = (settled_at IS NOT NULL)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_has_no_value;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_has_no_value
  CHECK (((state <> 'pending'::text) OR ((value_data IS NULL) AND
  (value_headers = '{}'::jsonb))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_verdict_matches_timer_tag;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_verdict_matches_timer_tag
  CHECK (((settled_at IS DISTINCT FROM timeout_at) OR (state =
  CASE
      WHEN is_timer THEN 'resolved'::text
      ELSE 'rejected_timedout'::text
  END)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_settlement_has_no_value;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_settlement_has_no_value
  CHECK (((settled_at IS DISTINCT FROM timeout_at) OR ((value_data IS NULL)
  AND (value_headers = '{}'::jsonb))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timedout_is_server_owned;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timedout_is_server_owned
  CHECK (((state <> 'rejected_timedout'::text) OR (settled_at =
  timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timer_not_targeted;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timer_not_targeted
  CHECK ((NOT (is_timer AND (target IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_obligations_require_external;
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_obligations_require_awaitable;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_obligations_require_awaitable
  CHECK ((awaitable OR ((callbacks = '{}'::text[]) AND (listeners =
  '{}'::text[]))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_awaiter_is_not_self;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_awaiter_is_not_self
  CHECK ((NOT (id = ANY (callbacks))));


-- --- promises: task well-formedness ----------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_pid;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_pid
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) = (pid IS
  NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_ttl;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_ttl
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) = (ttl IS
  NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_lease_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_lease_timeout_at
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) =
  (lease_timeout_at IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_pending_iff_has_retry_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_pending_iff_has_retry_timeout_at
  CHECK (((task_state IS NULL) OR ((task_state = 'pending'::text) =
  (retry_timeout_at IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_fulfilled_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_fulfilled_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'fulfilled'::text) OR ((pid IS NULL)
  AND (ttl IS NULL) AND (lease_timeout_at IS NULL) AND (retry_timeout_at IS NULL) AND
  (resumes = '{}'::text[]))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'suspended'::text) OR ((pid IS NULL)
  AND (ttl IS NULL) AND (lease_timeout_at IS NULL) AND (retry_timeout_at IS NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_halted_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_halted_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'halted'::text) OR ((pid IS NULL) AND
  (ttl IS NULL) AND (lease_timeout_at IS NULL) AND (retry_timeout_at IS NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_has_no_resumes;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_has_no_resumes
  CHECK (((task_state IS DISTINCT FROM 'suspended'::text) OR (resumes =
  '{}'::text[])));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_version_positive;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_version_positive
  CHECK (((task_state IS DISTINCT FROM 'acquired'::text) OR (task_version >=
  1)));


-- --- promises: obligations and uniqueness ----------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_callbacks_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_callbacks_unique
  CHECK (((cardinality(callbacks) < 2) OR resonate._arr_uniq(callbacks)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_listeners_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_listeners_unique
  CHECK (((cardinality(listeners) < 2) OR resonate._arr_uniq(listeners)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_resumes_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_resumes_unique
  CHECK (((cardinality(resumes) < 2) OR resonate._arr_uniq(resumes)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_listener_addresses_deliverable;
ALTER TABLE promises ADD CONSTRAINT consistent_listener_addresses_deliverable
  CHECK (((listeners = '{}'::text[]) OR resonate._addrs_valid(listeners)));


-- --- promises: id format — this deployment's convention, not a catalogue entry ---
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_id_at_most_one_separator;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_id_at_most_one_separator
  CHECK ((id ~ '^[^:]*(:[^:]*)?$'::text));


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
