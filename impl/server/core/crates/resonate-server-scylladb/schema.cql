-- The resonate-on-scylladb schema, carried over: one promises table
-- partitioned by origin (the call-graph root, everything before the id's
-- first ':'), so every transition a request makes — settle plus its awaiter
-- fanout, suspend plus its callback registrations — is a single-partition
-- LWT or conditional batch.
--
-- The timeout queues are separate, bucketed and sharded tables, which makes
-- every promises-row LWT paired with a queue write cross-partition and
-- non-atomic. The substitute for atomicity is the pre-insert protocol:
-- write the queue entry first (PK-idempotent), run the LWT, and on failure
-- delete the entry only when the surviving row does not legitimately own
-- it. Failures leave an orphan entry, never a missing one, and the tick
-- handlers classify and delete orphans.
--
-- Eager, like the Go implementation: EVERY pending promise gets a
-- promise_timeouts entry, internal included. The relational engines are
-- deliberately lazy for internal promises — the differential is the
-- experiment that decides whether the difference is observable.

CREATE KEYSPACE IF NOT EXISTS resonate;

USE resonate;

CREATE TABLE IF NOT EXISTS promises (
    id                 TEXT,
    origin             TEXT,
    branch             TEXT,
    parent             TEXT,
    target             TEXT,
    state              TEXT,
    param_headers      MAP<TEXT, TEXT>,
    param_data         TEXT,
    value_headers      MAP<TEXT, TEXT>,
    value_data         TEXT,
    tags               MAP<TEXT, TEXT>,
    timeout_at         BIGINT,
    created_at         BIGINT,
    settled_at         BIGINT,
    callbacks          SET<TEXT>,
    listeners          SET<TEXT>,
    task_state         TEXT,
    task_version       INT,
    task_ttl           BIGINT,
    task_pid           TEXT,
    task_resumes       SET<TEXT>,
    task_timeout_retry BIGINT,
    task_timeout_lease BIGINT,
    PRIMARY KEY (origin, id)
);

CREATE TABLE IF NOT EXISTS schedules (
    id                    TEXT,
    origin                TEXT,
    cron                  TEXT,
    promise_id            TEXT,
    promise_timeout       BIGINT,
    promise_param_headers MAP<TEXT, TEXT>,
    promise_param_data    TEXT,
    promise_tags          MAP<TEXT, TEXT>,
    next_run_at           BIGINT,
    last_run_at           BIGINT,
    created_at            BIGINT,
    create_token          UUID,
    PRIMARY KEY (origin, id)
);

CREATE TABLE IF NOT EXISTS promise_timeouts (
    bucket     BIGINT,
    shard      SMALLINT,
    timeout_at BIGINT,
    origin     TEXT,
    promise_id TEXT,
    PRIMARY KEY ((bucket, shard), timeout_at, origin, promise_id)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS task_timeouts (
    bucket             BIGINT,
    shard              SMALLINT,
    timeout_at         BIGINT,
    timeout_type       TINYINT,
    origin             TEXT,
    task_id            TEXT,
    promise_timeout_at BIGINT,
    PRIMARY KEY ((bucket, shard), timeout_at, timeout_type, origin, task_id)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS schedule_timeouts (
    bucket       BIGINT,
    shard        SMALLINT,
    timeout_at   BIGINT,
    origin       TEXT,
    schedule_id  TEXT,
    create_token UUID,
    PRIMARY KEY ((bucket, shard), timeout_at, origin, schedule_id, create_token)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS workers (
    worker_id UUID,
    PRIMARY KEY (worker_id)
);
