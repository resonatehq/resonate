-- CreateTable 
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

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_id ON promises(id);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_invocation ON promises(json_extract(tags, '$.resonate:invocation'));

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_timeout ON promises(json_extract(tags, '$.resonate:timeout'));

-- CreateTable 
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

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_schedules_id ON schedules(id);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_time ON schedules(next_run_time);

-- CreateTable 
CREATE TABLE IF NOT EXISTS tasks (
    id                TEXT UNIQUE,
    counter           INTEGER,
    promise_id        TEXT,
    claim_timeout     INTEGER, 
    complete_timeout  INTEGER, 
    promise_timeout   INTEGER,
    created_on        INTEGER,
    completed_on      INTEGER, 		
    is_completed      BOOLEAN
); 

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_tasks_id ON tasks(id); 

-- CreateTable 
CREATE TABLE IF NOT EXISTS locks (
    resource_id            TEXT UNIQUE,
    process_id             TEXT,
    execution_id           TEXT, 
    expiry_in_milliseconds INTEGER,
    timeout                INTEGER
); 

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_locks_acquire_id ON locks(resource_id, execution_id); 

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_locks_heartbeat_id ON locks(process_id); 

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_locks_timeout ON locks(timeout);

-- CreateTable 
CREATE TABLE IF NOT EXISTS timeouts (
    id   TEXT,
    time INTEGER,
    PRIMARY KEY(id)
);

-- CreateTable 
CREATE TABLE IF NOT EXISTS subscriptions (
    id           TEXT,
    promise_id   TEXT,
    sort_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    url          TEXT,
    retry_policy BLOB,
    created_on   INTEGER,
    UNIQUE(id, promise_id)
);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_subscriptions_id ON subscriptions(id);

-- CreateTable 
CREATE TABLE IF NOT EXISTS notifications (
    id           TEXT,
    promise_id   TEXT,
    url          TEXT,
    retry_policy BLOB,
    time         INTEGER,
    attempt      INTEGER,
    PRIMARY KEY(id, promise_id)
);

-- CreateTable 
CREATE TABLE IF NOT EXISTS migrations (
    id    INTEGER PRIMARY KEY
);

--- CreateSchemaVersion 
INSERT INTO migrations (id) VALUES (0) ON CONFLICT(id) DO NOTHING;