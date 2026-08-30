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

CREATE TABLE IF NOT EXISTS promises (
  id VARCHAR(255) NOT NULL,
  state VARCHAR(50) NOT NULL DEFAULT 'pending',
  param_headers LONGTEXT,
  param_data LONGTEXT,
  value_headers LONGTEXT,
  value_data LONGTEXT,
  tags LONGTEXT NOT NULL,
  target VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:target"') STORED,
  -- Lineage, named as Postgres names it. `origin_id` is a projection of the
  -- ID, not of the tags: the origin is everything before the first ':', which
  -- is what `resonate_core::types::origin` computes and what Postgres's
  -- split_part(id, ':', 1) computes. Reading the tag instead gave a different
  -- answer whenever the tag was absent.
  origin_id VARCHAR(255) GENERATED ALWAYS AS (SUBSTRING_INDEX(id, ':', 1)) STORED,
  parent_id VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:parent"') STORED,
  branch_id VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:branch"') STORED,
  is_timer BOOLEAN GENERATED ALWAYS AS (COALESCE(tags->>'$."resonate:timer"', '') = 'true') STORED NOT NULL,
  -- `resonate_core::types::is_awaitable`, in SQL: every otype but internal.
  -- Doors ask this, and the eager sweep is exactly this — awaitable and
  -- armed are one rule, so an internal promise costs no timer.
  awaitable BOOLEAN GENERATED ALWAYS AS (
    COALESCE(tags->>'$."resonate:scope"', '') = 'global'
    OR COALESCE(tags->>'$."resonate:external"', '') = 'true'
    OR tags->>'$."resonate:target"' IS NOT NULL
    OR COALESCE(tags->>'$."resonate:timer"', '') = 'true') STORED NOT NULL,
  timeout_at BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  settled_at BIGINT,

  -- was the `tasks` table. NULL task_state means this promise has no task,
  -- which is what `LEFT JOIN tasks` used to express.
  task_state VARCHAR(50) NULL,
  task_version INT NOT NULL DEFAULT 0,

  -- was `task_timeouts`, whose timeout_type discriminated two queues.
  -- Two nullable columns say the same thing without the row.
  retry_timeout_at BIGINT NULL,
  lease_timeout_at BIGINT NULL,
  ttl BIGINT NULL,
  pid VARCHAR(255) NULL,

  PRIMARY KEY (id),
  INDEX idx_promises_timeout_at (timeout_at),
  INDEX idx_promises_target (target),
  INDEX idx_promises_branch_id (branch_id),
  -- `promise_timeouts` is gone: a pending, targeted promise past its
  -- timeout_at is exactly the queue, and idx_promises_timeout_at is the index
  -- the table carried.
  INDEX idx_promises_retry_timeout_at (retry_timeout_at ASC, id ASC),
  INDEX idx_promises_lease_timeout_at (lease_timeout_at ASC, id ASC),
  INDEX idx_promises_pid (pid),
  CONSTRAINT promises_state_check CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
  CONSTRAINT promises_task_state_check CHECK (task_state IS NULL OR task_state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled'))
);

CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id VARCHAR(255) NOT NULL,
  awaiter_id VARCHAR(255) NOT NULL,
  ready BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (awaited_id, awaiter_id),
  INDEX idx_callbacks_awaiter_id (awaiter_id),
  FOREIGN KEY (awaited_id) REFERENCES promises (id) ON DELETE CASCADE,
  FOREIGN KEY (awaiter_id) REFERENCES promises (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS listeners (
  promise_id VARCHAR(255) NOT NULL,
  address VARCHAR(255) NOT NULL,
  PRIMARY KEY (promise_id, address),
  FOREIGN KEY (promise_id) REFERENCES promises (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schedules (
  id VARCHAR(255) NOT NULL,
  cron TEXT NOT NULL,
  promise_id VARCHAR(255) NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers LONGTEXT,
  promise_param_data LONGTEXT,
  promise_tags LONGTEXT NOT NULL,
  created_at BIGINT NOT NULL,
  next_run_at BIGINT NOT NULL,
  last_run_at BIGINT,
  PRIMARY KEY (id),
  -- `schedule_timeouts` is gone: next_run_at already is the queue.
  INDEX idx_schedules_next_run_at (next_run_at ASC, id ASC)
);
