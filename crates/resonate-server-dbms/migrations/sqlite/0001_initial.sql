CREATE TABLE IF NOT EXISTS promises (
  id TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
  param_headers TEXT,
  param_data TEXT,
  value_headers TEXT,
  value_data TEXT,
  tags TEXT NOT NULL DEFAULT '{}',
  target TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:target')) STORED,
  -- Lineage, named as Postgres names it. `origin_id` is a projection of
  -- the ID, not of the tags: the origin is everything before the first
  -- ':', which is what `resonate_core::types::origin` computes and what
  -- Postgres's split_part(id, ':', 1) computes. Reading the tag instead
  -- gave a different answer whenever the tag was absent.
  origin_id TEXT GENERATED ALWAYS AS (
    CASE WHEN instr(id, ':') > 0 THEN substr(id, 1, instr(id, ':') - 1) ELSE id END) STORED,
  parent_id TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:parent')) STORED,
  branch_id TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:branch')) STORED,
  timer BOOLEAN NOT NULL GENERATED ALWAYS AS (COALESCE(json_extract(tags, '$.resonate:timer'), '') = 'true') STORED,
  timeout_at BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  settled_at BIGINT,

  -- was the `tasks` table. NULL task_state means this promise has no
  -- task, which is what `LEFT JOIN tasks` used to express.
  task_state TEXT
    CHECK (task_state IS NULL OR task_state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled')),
  task_version INT NOT NULL DEFAULT 0,

  -- was `task_timeouts`, whose timeout_type discriminated two queues.
  -- Two nullable columns say the same thing without the row.
  retry_timeout_at   BIGINT,   -- type 0: redispatch a pending task
  lease_timeout_at BIGINT,   -- type 1: an acquired task's lease
  ttl        BIGINT,
  pid        TEXT
);
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at ON promises (timeout_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_promises_target ON promises (target) WHERE target IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_branch_id ON promises (branch_id) WHERE branch_id IS NOT NULL;

-- `promise_timeouts` is gone: a pending promise past its timeout_at is
-- exactly the queue, and the partial index above is the same index the
-- table carried.
CREATE INDEX IF NOT EXISTS idx_promises_retry_timeout_at ON promises (retry_timeout_at ASC, id ASC) WHERE retry_timeout_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_lease_timeout_at ON promises (lease_timeout_at ASC, id ASC) WHERE lease_timeout_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_pid ON promises (pid) WHERE pid IS NOT NULL;

CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  awaiter_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  ready BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (awaited_id, awaiter_id)
);
CREATE INDEX IF NOT EXISTS idx_callbacks_awaiter_id ON callbacks (awaiter_id);
CREATE INDEX IF NOT EXISTS idx_callbacks_ready ON callbacks (awaiter_id) WHERE ready = true;

CREATE TABLE IF NOT EXISTS listeners (
  promise_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  address TEXT NOT NULL,
  PRIMARY KEY (promise_id, address)
);

-- No outbox. A message is returned by the transition that emitted it.

CREATE TABLE IF NOT EXISTS schedules (
  id TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  promise_id TEXT NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers TEXT,
  promise_param_data TEXT,
  promise_tags TEXT NOT NULL DEFAULT '{}',
  created_at BIGINT NOT NULL,
  next_run_at BIGINT NOT NULL,
  last_run_at BIGINT
);
-- `schedule_timeouts` is gone: next_run_at already is the queue.
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at ON schedules (next_run_at ASC, id ASC);
