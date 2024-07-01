-- CreateTable 
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

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_sort_id ON promises(sort_id);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_invocation ON promises((tags->>'resonate:invocation'));

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_promises_timeout ON promises((tags->>'resonate:timeout'));

-- CreateTable
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

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_schedules_sort_id ON schedules(sort_id);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_time ON schedules(next_run_time);

-- CreateTable
CREATE TABLE IF NOT EXISTS tasks (
    id                TEXT,
    counter           INTEGER,
    promise_id        TEXT,
    claim_timeout     BIGINT, 
    complete_timeout  BIGINT, 
    promise_timeout   BIGINT,
    created_on        BIGINT,
    completed_on      BIGINT, 		
    is_completed      BOOLEAN, 
    PRIMARY KEY(id)
); 

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_tasks_id ON tasks(id); 

-- CreateTable
CREATE TABLE IF NOT EXISTS locks (
    resource_id            TEXT,
    process_id             TEXT,
    execution_id           TEXT,  
    expiry_in_milliseconds BIGINT,
    timeout                BIGINT, 
    PRIMARY KEY(resource_id)
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
    time BIGINT,
    PRIMARY KEY(id)
);

-- CreateTable
CREATE TABLE IF NOT EXISTS subscriptions (
    id           TEXT,
    sort_id      SERIAL,
    promise_id   TEXT,
    url          TEXT,
    retry_policy BYTEA,
    created_on   BIGINT,
    PRIMARY KEY(id, promise_id)
);

-- CreateIndex
CREATE INDEX IF NOT EXISTS idx_subscriptions_sort_id ON subscriptions(sort_id);

-- CreateTable
CREATE TABLE IF NOT EXISTS notifications (
    id           TEXT,
    promise_id   TEXT,
    url          TEXT,
    retry_policy BYTEA,
    time         BIGINT,
    attempt      INTEGER,
    PRIMARY KEY(id, promise_id)
);
