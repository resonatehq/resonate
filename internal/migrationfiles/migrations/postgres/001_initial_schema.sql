CREATE TABLE IF NOT EXISTS migrations (
	id INTEGER,
	PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS promises (
	id                           TEXT,
	sort_id                      SERIAL,
	state                        INTEGER DEFAULT 1,
	param_headers                JSONB,
	param_data                   BYTEA,
	value_headers                JSONB,
	value_data                   BYTEA,
	timeout                      BIGINT,
	idempotency_key_for_create   TEXT,
	idempotency_key_for_complete TEXT,
	tags                         JSONB,
	created_on                   BIGINT,
	completed_on                 BIGINT,
	PRIMARY KEY(id)
);

CREATE INDEX IF NOT EXISTS idx_promises_sort_id ON promises(sort_id);

CREATE TABLE IF NOT EXISTS callbacks (
	id              TEXT UNIQUE,
	promise_id      TEXT,
	root_promise_id TEXT,
	recv            BYTEA,
	mesg            BYTEA,
	timeout         BIGINT,
	created_on      BIGINT
);

CREATE TABLE IF NOT EXISTS schedules (
	id                    TEXT,
	sort_id               SERIAL,
	description           TEXT,
	cron                  TEXT,
	tags                  JSONB,
	promise_id            TEXT,
	promise_timeout       BIGINT,
	promise_param_headers JSONB,
	promise_param_data    BYTEA,
	promise_tags          JSONB,
	last_run_time         BIGINT,
	next_run_time         BIGINT,
	idempotency_key       TEXT,
	created_on            BIGINT,
	PRIMARY KEY(id)
);

CREATE INDEX IF NOT EXISTS idx_schedules_sort_id ON schedules(sort_id);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_time ON schedules(next_run_time);

CREATE TABLE IF NOT EXISTS locks (
	resource_id  TEXT,
	execution_id TEXT,
	process_id   TEXT,
	ttl          BIGINT,
	expires_at   BIGINT,
	PRIMARY KEY(resource_id)
);

CREATE INDEX IF NOT EXISTS idx_locks_id ON locks(resource_id, execution_id);
CREATE INDEX IF NOT EXISTS idx_locks_process_id ON locks(process_id);
CREATE INDEX IF NOT EXISTS idx_locks_expires_at ON locks(expires_at);

CREATE TABLE IF NOT EXISTS tasks (
	id              TEXT,
	sort_id         SERIAL,
	process_id      TEXT,
	state           INTEGER DEFAULT 1,
	root_promise_id TEXT,
	recv            BYTEA,
	mesg            BYTEA,
	timeout         BIGINT,
	counter         INTEGER DEFAULT 1,
	attempt         INTEGER DEFAULT 0,
	ttl             BIGINT DEFAULT 0,
	expires_at      BIGINT DEFAULT 0,
	created_on      BIGINT,
	completed_on    BIGINT,
	PRIMARY KEY(id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_process_id ON tasks(process_id);
CREATE INDEX IF NOT EXISTS idx_tasks_expires_at ON tasks(expires_at);
