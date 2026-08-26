-- =============================================================================
-- Single-table Postgres schema
-- =============================================================================
-- One promise is one row. Beside it sit only `outbox` (a message is not a
-- promise attribute — a settled promise can owe several) and `schedules`
-- (a separate id space and a genuinely different entity).
--
-- Replaces the eight tables of the multi-table backend: promises,
-- promise_timeouts, tasks, task_timeouts, callbacks, listeners,
-- outgoing_execute, outgoing_unblock — plus schedule_timeouts, whose
-- (timeout_at, id) was a verbatim copy of (next_run_at, id).
--
-- Column names, the schema name and the shape of `outbox` are chosen so that
-- `single-table-constraints.sql` applies to this schema unchanged.
-- =============================================================================

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
  external      BOOLEAN NOT NULL GENERATED ALWAYS AS (
                  tags->>'resonate:target' IS NOT NULL
                  OR COALESCE(tags->>'resonate:timer', '')    = 'true'
                  OR COALESCE(tags->>'resonate:external', '') = 'true') STORED,

  -- The outbox foreign key needs a TOTAL key to reference, and "the promises
  -- that are tasks" is a partial set. task_key is the id exactly when the row
  -- is a task; UNIQUE tolerates the NULLs of the rows that are not.
  task_key      TEXT    GENERATED ALWAYS AS (
                  CASE WHEN tags ? 'resonate:target' THEN id END) STORED,

  -- task — NULL task_state ⟺ the promise carries no resonate:target -----------
  task_state    TEXT CHECK (task_state IN ('pending', 'acquired', 'suspended',
                                           'halted', 'fulfilled')),
  task_version  INT NOT NULL DEFAULT 0,

  -- task deadlines. The multi-table task_timeouts.timeout_type discriminator
  -- collapses into two columns: the retry deadline is live exactly while
  -- task_state='pending', the lease deadline exactly while 'acquired'.
  retry_at      BIGINT,
  expires_at    BIGINT,
  ttl           BIGINT,
  pid           TEXT,

  -- callbacks, both directions ------------------------------------------------
  callbacks      TEXT[] NOT NULL DEFAULT '{}',  -- ids blocked on me   (ready = false)
  resumes       TEXT[] NOT NULL DEFAULT '{}',  -- ids ready for me    (ready = true)

  -- listeners -----------------------------------------------------------------
  listeners     TEXT[] NOT NULL DEFAULT '{}'
);

-- The promise timeout sweep queue is NOT a column: promise_timeouts membership
-- is exactly (state = 'pending' AND target IS NOT NULL), given
-- `consistent_task_iff_targeted_promise`. A targetless promise has no task and
-- is never swept eagerly; it times out lazily on first touch (try_timeout).
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at
  ON promises (timeout_at) WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_promises_origin_id
  ON promises (origin_id);
CREATE INDEX IF NOT EXISTS idx_promises_branch_id
  ON promises (branch_id) WHERE branch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_target
  ON promises (target) WHERE target IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_timeout_retry_at
  ON promises (retry_at) WHERE task_state = 'pending';
CREATE INDEX IF NOT EXISTS idx_task_timeout_lease_at
  ON promises (expires_at) WHERE task_state = 'acquired';
CREATE INDEX IF NOT EXISTS idx_promises_task
  ON promises (task_state, id) WHERE task_state IS NOT NULL;

-- Fan-out in the other direction: "which rows list me as an awaiter", the
-- single-table stand-in for `DELETE FROM callbacks WHERE awaiter_id = $1`.
CREATE INDEX IF NOT EXISTS idx_promises_callbacks
  ON promises USING GIN (callbacks);

-- --- outbox -----------------------------------------------------------------
-- `key` carries the deduplication the two outgoing_* tables got from their
-- primary keys: one pending execute per task (outgoing_execute PK (id)), one
-- pending unblock per (promise, address) (outgoing_unblock PK (promise_id, address)).

CREATE TABLE IF NOT EXISTS outbox (
  key      TEXT PRIMARY KEY,
  kind     TEXT NOT NULL CHECK (kind IN ('execute', 'unblock')),
  address  TEXT NOT NULL,
  task_id  TEXT,
  version  INT,
  promise  JSONB
);

CREATE INDEX IF NOT EXISTS idx_outbox_kind ON outbox (kind, key);

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
-- The outbox stores the settled promise with the unblock message rather than
-- joining back at delivery time, so `consistent_outbox_unblock_names_settled_promise`
-- is checkable. Field names and omissions match `PromiseRecord`'s serde:
-- camelCase timestamps, `settledAt`/`headers`/`data` omitted when absent.
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
