-- Fixture for diff/encbench.rs — stands in for the CTE outputs a message
-- fragment reads from: suspended_awaiters (execute) and settled-promise ×
-- listeners (unblock). `grp` is the total message count, split roughly
-- 30% unblock / 70% execute, as a settlement cascade produces.
--
--   createdb bench && psql -d bench -f config/postgres/encbench-fixture.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;
DROP SCHEMA IF EXISTS enc CASCADE;
CREATE SCHEMA enc;

CREATE TYPE enc.out_message AS (
  kind TEXT, address TEXT, task_id TEXT, version INT,
  p_id TEXT, p_state TEXT, param_headers JSONB, param_data TEXT,
  value_headers JSONB, value_data TEXT, tags JSONB,
  timeout_at BIGINT, created_at BIGINT, settled_at BIGINT
);

CREATE TABLE enc.msgsrc (
  grp INT NOT NULL, kind TEXT NOT NULL,
  address TEXT NOT NULL, task_id TEXT, version INT,
  p_id TEXT, p_state TEXT, param_headers JSONB, param_data TEXT,
  value_headers JSONB, value_data TEXT, tags JSONB,
  timeout_at BIGINT, created_at BIGINT, settled_at BIGINT
);
CREATE INDEX ON enc.msgsrc (grp, kind);

INSERT INTO enc.msgsrc
SELECT g.k,
       CASE WHEN i <= greatest(g.k/3, CASE WHEN g.k > 0 THEN 1 ELSE 0 END)
            THEN 'unblock' ELSE 'execute' END,
       'poll://any@group/worker-' || i,
       'task:0194f2b8-' || lpad(i::text, 4, '0'), i,
       'promise:0194f2b8-' || lpad(i::text, 4, '0'), 'resolved',
       '{}'::jsonb, substr(encode(gen_random_bytes(200), 'base64'), 1, 200),
       '{}'::jsonb, substr(encode(gen_random_bytes(200), 'base64'), 1, 200),
       '{"resonate:target":"poll://any@group","resonate:branch":"b1"}'::jsonb,
       1000000300000, 1000000000000, 1000000200000
FROM (VALUES (0), (1), (3), (10), (100)) AS g(k)
CROSS JOIN generate_series(1, 100) i
WHERE i <= g.k;

ANALYZE enc.msgsrc;
