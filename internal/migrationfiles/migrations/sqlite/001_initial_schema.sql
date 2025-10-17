CREATE TABLE IF NOT EXISTS migrations (
	id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS promises (
	id                           TEXT UNIQUE,
	sort_id                      INTEGER PRIMARY KEY AUTOINCREMENT,
	state                        INTEGER DEFAULT 1,
	param_headers                BLOB,
	param_data                   BLOB,
	value_headers                BLOB,
	value_data                   BLOB,
	timeout                      INTEGER,
	idempotency_key_for_create   TEXT,
	idempotency_key_for_complete TEXT,
	tags                         BLOB,
	created_on                   INTEGER,
	completed_on                 INTEGER
);

CREATE INDEX IF NOT EXISTS idx_promises_id ON promises(id);

CREATE TABLE IF NOT EXISTS callbacks (
	id              TEXT UNIQUE,
	promise_id      TEXT,
	root_promise_id TEXT,
	recv            BLOB,
	mesg            BLOB,
	timeout         INTEGER,
	created_on      INTEGER
);

CREATE TABLE IF NOT EXISTS schedules (
	id                    TEXT UNIQUE,
	sort_id               INTEGER PRIMARY KEY AUTOINCREMENT,
	description           TEXT,
	cron                  TEXT,
	tags                  BLOB,
	promise_id            TEXT,
	promise_timeout       INTEGER,
	promise_param_headers BLOB,
	promise_param_data    BLOB,
	promise_tags          BLOB,
	last_run_time         INTEGER,
	next_run_time         INTEGER,
	idempotency_key       TEXT,
	created_on            INTEGER
);

CREATE INDEX IF NOT EXISTS idx_schedules_id ON schedules(id);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_time ON schedules(next_run_time);

CREATE TABLE IF NOT EXISTS locks (
	resource_id  TEXT UNIQUE,
	execution_id TEXT,
	process_id   TEXT,
	ttl          INTEGER,
	expires_at   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_locks_id ON locks(resource_id, execution_id);
CREATE INDEX IF NOT EXISTS idx_locks_process_id ON locks(process_id);
CREATE INDEX IF NOT EXISTS idx_locks_expires_at ON locks(expires_at);

CREATE TABLE IF NOT EXISTS tasks (
	id              TEXT UNIQUE,
	sort_id         INTEGER PRIMARY KEY AUTOINCREMENT,
	process_id      TEXT,
	state           INTEGER DEFAULT 1,
	root_promise_id TEXT,
	recv            BLOB,
	mesg            BLOB,
	timeout         INTEGER,
	counter         INTEGER DEFAULT 1,
	attempt         INTEGER DEFAULT 0,
	ttl             INTEGER DEFAULT 0,
	expires_at      INTEGER DEFAULT 0,
	created_on      INTEGER,
	completed_on    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_process_id ON tasks(process_id);
CREATE INDEX IF NOT EXISTS idx_tasks_expires_at ON tasks(expires_at);
