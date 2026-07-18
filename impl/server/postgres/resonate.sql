-- =============================================================================
-- A Resonate Server in a single Postgres file
-- =============================================================================
-- Apply this file to a Postgres 16+ database:
-- psql -d yourdb -f resonate.sql
-- =============================================================================

-- =============================================================================
-- SECTION 1 · SCHEMA
-- =============================================================================
-- Conventions: time is ms since epoch, passed as explicit `now` to every action
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS resonate;

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION resonate.get_schema_version() RETURNS text
  LANGUAGE sql IMMUTABLE AS $$ SELECT '0.1.0'::text $$;

-- --- promises ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS promises (
  id            TEXT PRIMARY KEY,
  state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending','resolved','rejected',
                                   'rejected_canceled','rejected_timedout')),
  param_headers JSONB NOT NULL DEFAULT '{}',
  param_data    TEXT,
  value_headers JSONB NOT NULL DEFAULT '{}',
  value_data    TEXT,
  tags          JSONB NOT NULL DEFAULT '{}',
  target        TEXT GENERATED ALWAYS AS (tags->>'resonate:target') STORED,
  origin_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:origin') STORED,
  parent_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:parent') STORED,
  branch_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:branch') STORED,
  is_timer      BOOLEAN NOT NULL
                  GENERATED ALWAYS AS (COALESCE(tags->>'resonate:timer','') = 'true') STORED,
  kind          TEXT GENERATED ALWAYS AS (
                  CASE WHEN tags->>'resonate:target' IS NOT NULL
                         OR COALESCE(tags->>'resonate:timer','') = 'true'
                       THEN 'external' ELSE 'internal' END) STORED,
  timeout_at    BIGINT NOT NULL,
  created_at    BIGINT NOT NULL,
  settled_at    BIGINT
);
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at ON promises (timeout_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_promises_origin_id ON promises (origin_id) WHERE origin_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_branch_id ON promises (branch_id) WHERE branch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_settled_at ON promises (settled_at) WHERE state <> 'pending';

-- --- tasks ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
  id         TEXT PRIMARY KEY REFERENCES promises(id) ON DELETE CASCADE,
  state      TEXT NOT NULL DEFAULT 'pending'
               CHECK (state IN ('pending','acquired','suspended','halted','fulfilled')),
  version    INT  NOT NULL DEFAULT 0,
  ttl        BIGINT,
  pid        TEXT,
  timeout_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_tasks_timeout_at ON tasks (timeout_at) WHERE state IN ('pending','acquired');

CREATE TABLE IF NOT EXISTS task_resumes (
  task_id    TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  awaited_id TEXT NOT NULL,
  PRIMARY KEY (task_id, awaited_id)
);

-- --- registrations ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  awaiter_id TEXT NOT NULL,
  PRIMARY KEY (awaited_id, awaiter_id)
);
CREATE INDEX IF NOT EXISTS idx_callbacks_awaiter_id ON callbacks (awaiter_id);

CREATE TABLE IF NOT EXISTS listeners (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  address    TEXT NOT NULL,
  PRIMARY KEY (awaited_id, address)
);

-- --- schedules --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schedules (
  id             TEXT PRIMARY KEY,
  cron           TEXT NOT NULL,
  promise_id     TEXT NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers JSONB NOT NULL DEFAULT '{}',
  promise_param_data    TEXT,
  promise_tags   JSONB NOT NULL DEFAULT '{}',
  created_at     BIGINT NOT NULL,
  next_run_at    BIGINT NOT NULL,
  last_run_at    BIGINT
);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at ON schedules (next_run_at);

-- --- outbox (messages) ------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox (
  key       TEXT PRIMARY KEY,
  kind      TEXT NOT NULL CHECK (kind IN ('execute','unblock')),
  address   TEXT NOT NULL,
  task_id   TEXT,
  version   INT,
  promise   JSONB,
  seq       BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch ON outbox (kind, address, seq);

-- =============================================================================
-- NOTIFY plumbing
-- =============================================================================
-- Per-address NOTIFY channel; one definition shared by server + client. md5
CREATE OR REPLACE FUNCTION outbox_channel(p_address text) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$ SELECT 'resonate_q_' || md5(p_address) $$;

CREATE OR REPLACE FUNCTION _http_available() RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = 'net' AND p.proname = 'http_post');
$$;
CREATE OR REPLACE FUNCTION _http_sync_schema() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE p.proname = 'http_post' AND n.nspname <> 'net'
   ORDER BY (n.nspname = 'public') DESC, n.nspname LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION _outbox_http_body(o outbox) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN o.kind = 'execute'
    THEN jsonb_build_object('kind', 'execute', 'head', '{}'::jsonb,
           'data', jsonb_build_object('task',
                     jsonb_build_object('id', o.task_id, 'version', o.version)))
    ELSE jsonb_build_object('kind', 'unblock', 'head', '{}'::jsonb,
           'data', jsonb_build_object('promise', o.promise)) END;
$$;

CREATE OR REPLACE FUNCTION _notify_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE sync_sch text;
BEGIN
  IF NEW.address LIKE 'http://%' OR NEW.address LIKE 'https://%' THEN
    IF _http_available() THEN
      BEGIN
        PERFORM net.http_post(url := NEW.address, body := _outbox_http_body(NEW));
        DELETE FROM outbox WHERE key = NEW.key;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'resonate: http push (pg_net) to % failed: %', NEW.address, SQLERRM;
      END;
    ELSE
      sync_sch := _http_sync_schema();
      IF sync_sch IS NOT NULL THEN
        BEGIN
          EXECUTE format('SELECT %I.http_post($1, $2, $3)', sync_sch)
            USING NEW.address, _outbox_http_body(NEW)::text, 'application/json';
          DELETE FROM outbox WHERE key = NEW.key;
        EXCEPTION WHEN OTHERS THEN
          RAISE WARNING 'resonate: http push (pgsql-http) to % failed: %', NEW.address, SQLERRM;
        END;
      END IF;
    END IF;
  ELSE
    PERFORM pg_notify(outbox_channel(NEW.address), NEW.kind);
  END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER trg_outbox_notify
  AFTER INSERT OR UPDATE ON outbox
  FOR EACH ROW EXECUTE FUNCTION _notify_outbox();

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 2 · HELPERS · projection, values, resume, settlement cascade, cron
-- ═══════════════════════════════════════════════════════════════════════════

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _retry_timeout() RETURNS bigint
  LANGUAGE sql IMMUTABLE AS $$ SELECT 5000::bigint $$;

-- --- JSON serialization ------------------------------------------------------
CREATE OR REPLACE FUNCTION _promise_json(p promises, now bigint) RETURNS jsonb
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE st text := p.state; sa bigint := p.settled_at;
BEGIN
  IF p.state = 'pending' AND p.timeout_at <= now THEN
    st := CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END;
    sa := p.timeout_at;
  END IF;
  RETURN jsonb_build_object(
    'id',        p.id,
    'state',     st,
    'param',     jsonb_build_object('headers', p.param_headers, 'data', p.param_data),
    'value',     jsonb_build_object('headers', p.value_headers, 'data', p.value_data),
    'tags',      p.tags,
    'timeoutAt', p.timeout_at,
    'createdAt', p.created_at,
    'settledAt', sa);
END $$;

CREATE OR REPLACE FUNCTION _promise_json_raw(p promises) RETURNS jsonb
  LANGUAGE sql IMMUTABLE AS $$ SELECT _promise_json(p, -1) $$;

CREATE OR REPLACE FUNCTION _promise_timed(p promises) RETURNS boolean
  LANGUAGE sql STABLE AS $$
  SELECT p.state = 'pending'
     AND (p.is_timer OR EXISTS (SELECT 1 FROM tasks WHERE tasks.id = p.id));
$$;

CREATE OR REPLACE FUNCTION _task_json(t tasks) RETURNS jsonb
  LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object(
    'id',      t.id,
    'state',   t.state,
    'version', t.version,
    'resumes', (SELECT count(*) FROM task_resumes r WHERE r.task_id = t.id),
    'ttl',     t.ttl,
    'pid',     t.pid);
$$;

CREATE OR REPLACE FUNCTION _schedule_json(s schedules) RETURNS jsonb
  LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_build_object(
    'id',             s.id,
    'cron',           s.cron,
    'promiseId',      s.promise_id,
    'promiseTimeout', s.promise_timeout,
    'promiseParam',   jsonb_build_object('headers', s.promise_param_headers,
                                         'data',    s.promise_param_data),
    'promiseTags',    s.promise_tags,
    'nextRunAt',      s.next_run_at,
    'lastRunAt',      s.last_run_at,
    'createdAt',      s.created_at);
$$;

-- --- outbox emitters (spec setMessage; dedup by OutboxEntry.key) -------------
CREATE OR REPLACE FUNCTION _emit_execute(addr text, tid text, ver int) RETURNS void
  LANGUAGE sql AS $$
  INSERT INTO outbox (key, kind, address, task_id, version, promise)
  VALUES (tid, 'execute', addr, tid, ver, NULL)
  ON CONFLICT (key) DO UPDATE
    SET kind = 'execute', address = EXCLUDED.address,
        task_id = EXCLUDED.task_id, version = EXCLUDED.version, promise = NULL;
$$;

CREATE OR REPLACE FUNCTION _enqueue_resume(p_awaited text, p_awaiter text, now bigint)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE t tasks; tgt text;
BEGIN
  SELECT * INTO t FROM tasks WHERE id = p_awaiter FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;

  IF t.state = 'suspended' THEN
    UPDATE tasks SET state = 'pending',
                     timeout_at = now + _retry_timeout() WHERE id = t.id;
    DELETE FROM task_resumes WHERE task_id = t.id;
    INSERT INTO task_resumes (task_id, awaited_id) VALUES (t.id, p_awaited)
      ON CONFLICT DO NOTHING;
    SELECT target INTO tgt FROM promises WHERE id = p_awaiter;
    IF tgt IS NOT NULL AND tgt <> '' THEN
      PERFORM _emit_execute(tgt, t.id, t.version);
    END IF;
  ELSIF t.state IN ('pending', 'acquired', 'halted') THEN
    INSERT INTO task_resumes (task_id, awaited_id) VALUES (t.id, p_awaited)
      ON CONFLICT DO NOTHING;
  END IF;
END $$;

-- --- settlement cascade ------------------------------------------------------
CREATE OR REPLACE FUNCTION _cascade_settle(p promises, now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE awaiter text;
BEGIN
  UPDATE tasks SET state = 'fulfilled', pid = NULL, ttl = NULL WHERE id = p.id;
  DELETE FROM task_resumes WHERE task_id = p.id;
  DELETE FROM outbox WHERE task_id = p.id;

  INSERT INTO outbox (key, kind, address, task_id, version, promise)
  SELECT p.id || ':notify:' || l.address, 'unblock', l.address, NULL, NULL,
         _promise_json(p, now)
  FROM listeners l WHERE l.awaited_id = p.id
  ORDER BY l.awaited_id, l.address
  ON CONFLICT (key) DO UPDATE
    SET kind = 'unblock', address = EXCLUDED.address,
        task_id = NULL, version = NULL, promise = EXCLUDED.promise;

  DELETE FROM callbacks
    WHERE awaiter_id = p.id
      AND awaited_id IN (SELECT id FROM promises WHERE state = 'pending');

  PERFORM _lock(awaiter_id) FROM callbacks
    WHERE awaited_id = p.id ORDER BY awaited_id, awaiter_id;
  FOR awaiter IN SELECT awaiter_id FROM callbacks WHERE awaited_id = p.id ORDER BY awaited_id, awaiter_id LOOP
    PERFORM _enqueue_resume(p.id, awaiter, now);
  END LOOP;

  DELETE FROM callbacks WHERE awaited_id = p.id;
  DELETE FROM listeners WHERE awaited_id = p.id;
END $$;

-- --- cron --------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _cron_field(spec text, lo int, hi int) RETURNS int[]
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE part text; rng text; step text; a int; b int; s int; acc int[] := '{}'; i int;
BEGIN
  FOREACH part IN ARRAY string_to_array(spec, ',') LOOP
    step := '1'; rng := part;
    IF position('/' in part) > 0 THEN
      rng  := split_part(part, '/', 1);
      step := split_part(part, '/', 2);
    END IF;
    s := step::int;
    IF s < 1 THEN RAISE EXCEPTION 'invalid cron step: %', spec; END IF;
    IF rng = '*' THEN
      a := lo; b := hi;
    ELSIF position('-' in rng) > 0 THEN
      a := split_part(rng, '-', 1)::int; b := split_part(rng, '-', 2)::int;
    ELSE
      a := rng::int; b := CASE WHEN position('/' in part) > 0 THEN hi ELSE rng::int END;
    END IF;
    i := a;
    WHILE i <= b LOOP acc := acc || i; i := i + s; END LOOP;
  END LOOP;
  RETURN acc;
END $$;

CREATE OR REPLACE FUNCTION _next_cron(cron text, after bigint) RETURNS bigint
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  f text[] := regexp_split_to_array(trim(cron), '\s+');
  mins int[]; hrs int[]; doms int[]; mons int[]; dows int[];
  dom_star bool; dow_star bool;
  ts timestamptz; cutoff timestamptz; ok bool; dow int;
BEGIN
  IF array_length(f, 1) <> 5 THEN
    RAISE EXCEPTION 'unsupported cron (need 5 fields): %', cron;
  END IF;
  mins := _cron_field(f[1], 0, 59);
  hrs  := _cron_field(f[2], 0, 23);
  doms := _cron_field(f[3], 1, 31);
  mons := _cron_field(f[4], 1, 12);
  dows := _cron_field(f[5], 0, 6);
  dom_star := (f[3] = '*'); dow_star := (f[5] = '*');

  ts := date_trunc('minute', to_timestamp(after / 1000.0) AT TIME ZONE 'UTC') + interval '1 minute';
  cutoff := ts + interval '9 years';
  WHILE ts < cutoff LOOP
    IF NOT (extract(month from ts)::int = ANY(mons)) THEN
      ts := date_trunc('month', ts) + interval '1 month'; CONTINUE;
    END IF;
    dow := extract(dow from ts)::int;
    IF dom_star AND dow_star THEN ok := true;
    ELSIF dom_star THEN ok := dow = ANY(dows);
    ELSIF dow_star THEN ok := extract(day from ts)::int = ANY(doms);
    ELSE ok := extract(day from ts)::int = ANY(doms) OR dow = ANY(dows);
    END IF;
    IF NOT ok THEN
      ts := date_trunc('day', ts) + interval '1 day'; CONTINUE;
    END IF;
    IF NOT (extract(hour from ts)::int = ANY(hrs)) THEN
      ts := date_trunc('hour', ts) + interval '1 hour'; CONTINUE;
    END IF;
    IF extract(minute from ts)::int = ANY(mins) THEN
      RETURN (extract(epoch from ts) * 1000)::bigint;
    END IF;
    ts := ts + interval '1 minute';
  END LOOP;
  RAISE EXCEPTION 'no cron match within horizon: %', cron;
END $$;

CREATE OR REPLACE FUNCTION _expand(template text, sid text, ts bigint) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
  SELECT replace(replace(template, '{{.timestamp}}', ts::text), '{{.id}}', sid);
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 3 · PROMISE ACTIONS · P-01 .. P-06
-- ═══════════════════════════════════════════════════════════════════════════

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _lock(id text) RETURNS void
  LANGUAGE sql AS $$ SELECT pg_advisory_xact_lock(hashtextextended(id, 0)); $$;

CREATE OR REPLACE FUNCTION promise_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE p promises;
BEGIN
  SELECT * INTO p FROM promises WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

CREATE OR REPLACE FUNCTION promise_create(
    p_id text, p_timeout_at bigint, p_param jsonb, p_tags jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p promises; tgt text; delay bigint; st text;
  ph jsonb := COALESCE(p_param->'headers', '{}'::jsonb);
  pd text  := p_param->>'data';
  tags jsonb := COALESCE(p_tags, '{}'::jsonb);
BEGIN
  IF p_id IS NULL OR p_timeout_at IS NULL THEN RETURN jsonb_build_object('status', 400); END IF;
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF FOUND THEN
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  END IF;

  IF p_timeout_at > p_now THEN
    INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at)
    VALUES (p_id, 'pending', ph, pd, tags, p_timeout_at, p_now)
    RETURNING * INTO p;

    tgt := tags->>'resonate:target';
    IF tgt IS NOT NULL THEN
      IF tags ? 'resonate:delay' AND (tags->>'resonate:delay')::bigint > p_now THEN
        delay := (tags->>'resonate:delay')::bigint;
      ELSE
        delay := p_now + _retry_timeout();
        PERFORM _emit_execute(tgt, p.id, 0);
      END IF;
      INSERT INTO tasks (id, state, version, timeout_at) VALUES (p.id, 'pending', 0, delay);
    END IF;
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  ELSE
    st := CASE WHEN COALESCE(tags->>'resonate:timer','') = 'true'
               THEN 'resolved' ELSE 'rejected_timedout' END;
    INSERT INTO promises (id, state, param_headers, param_data, tags,
                          timeout_at, created_at, settled_at)
    VALUES (p_id, st, ph, pd, tags, p_timeout_at, p_timeout_at, p_timeout_at)
    RETURNING * INTO p;
    IF tags ? 'resonate:target' THEN
      INSERT INTO tasks (id, state, version) VALUES (p.id, 'fulfilled', 0);
    END IF;
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  END IF;
EXCEPTION WHEN unique_violation THEN
  SELECT * INTO p FROM promises WHERE id = p_id;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

CREATE OR REPLACE FUNCTION promise_settle(
    p_id text, p_state text, p_value jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p promises;
  vh jsonb := COALESCE(p_value->'headers', '{}'::jsonb);
  vd text  := p_value->>'data';
BEGIN
  -- Only these states are client-settable: 'pending' is not a settlement and
  -- 'rejected_timedout' is server-owned (the timeout path writes it), so a
  -- client can never forge one. Rejected before any state is consulted (400
  -- precedes the 404 on a missing promise and the sticky terminal echo).
  IF p_state IS NULL OR p_state NOT IN ('resolved','rejected','rejected_canceled') THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF p.state = 'pending' AND p.timeout_at > p_now THEN
    UPDATE promises
       SET state = p_state, value_headers = vh, value_data = vd, settled_at = p_now
     WHERE id = p.id
     RETURNING * INTO p;
    PERFORM _cascade_settle(p, p_now);
  END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

CREATE OR REPLACE FUNCTION promise_register_callback(
    p_awaited text, p_awaiter text, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE pa promises; pw promises;
BEGIN
  IF p_awaited = p_awaiter THEN RETURN jsonb_build_object('status', 400); END IF;
  PERFORM _lock(LEAST(p_awaited, p_awaiter));
  PERFORM _lock(GREATEST(p_awaited, p_awaiter));

  SELECT * INTO pa FROM promises WHERE id = p_awaited FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO pw FROM promises WHERE id = p_awaiter;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 422); END IF;
  IF NOT (pw.tags ? 'resonate:target') THEN RETURN jsonb_build_object('status', 422); END IF;

  IF pa.state = 'pending' THEN
    IF pa.timeout_at > p_now THEN
      IF pw.state = 'pending' AND pw.timeout_at > p_now THEN
        INSERT INTO callbacks (awaited_id, awaiter_id) VALUES (p_awaited, p_awaiter)
          ON CONFLICT DO NOTHING;
      END IF;
    END IF;
  END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(pa, p_now));
END $$;

CREATE OR REPLACE FUNCTION promise_register_listener(
    p_awaited text, p_address text, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE pa promises;
BEGIN
  IF p_address IS NULL
     OR NOT (p_address LIKE 'http://%' OR p_address LIKE 'https://%'
             OR (p_address LIKE 'poll://%' AND position('@' IN p_address) > 0)) THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  PERFORM _lock(p_awaited);
  SELECT * INTO pa FROM promises WHERE id = p_awaited FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF pa.state = 'pending' AND pa.timeout_at > p_now THEN
    INSERT INTO listeners (awaited_id, address) VALUES (p_awaited, p_address)
      ON CONFLICT DO NOTHING;
  END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(pa, p_now));
END $$;

CREATE OR REPLACE FUNCTION promise_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 4 · TASK ACTIONS · T-01 .. T-11
-- ═══════════════════════════════════════════════════════════════════════════

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION task_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  SELECT * INTO t FROM tasks WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF p.state = 'pending' AND p.timeout_at > p_now THEN
    RETURN jsonb_build_object('status', 200, 'task', _task_json(t));
  END IF;
  RETURN jsonb_build_object('status', 200, 'task', jsonb_build_object(
    'id', t.id, 'state', 'fulfilled', 'version', t.version,
    'resumes', 0, 'ttl', NULL, 'pid', NULL));
END $$;

DROP FUNCTION IF EXISTS task_create_typed(text, bigint, text, bigint, jsonb, text, jsonb, bigint);

CREATE OR REPLACE FUNCTION task_create(
    p_pid text, p_ttl bigint, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p promises; t tasks; st text;
  a_id      text   := p_action->>'id';
  a_toat    bigint := (p_action->>'timeoutAt')::bigint;
  a_headers jsonb  := COALESCE(p_action#>'{param,headers}', '{}'::jsonb);
  a_data    text   := p_action#>>'{param,data}';
  a_tags    jsonb  := COALESCE(p_action->'tags', '{}'::jsonb);
BEGIN
  IF a_id IS NULL OR a_toat IS NULL OR p_pid IS NULL OR p_ttl IS NULL THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  IF NOT (a_tags ? 'resonate:target') THEN RETURN jsonb_build_object('status', 400); END IF;
  PERFORM _lock(a_id);
  SELECT * INTO p FROM promises WHERE id = a_id FOR UPDATE;
  IF NOT FOUND THEN
    IF a_toat > p_now THEN
      INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at)
      VALUES (a_id, 'pending', a_headers, a_data, a_tags, a_toat, p_now) RETURNING * INTO p;
      INSERT INTO tasks (id, state, version, ttl, pid, timeout_at)
      VALUES (p.id, 'acquired', 1, p_ttl, p_pid, p_now + p_ttl) RETURNING * INTO t;
    ELSE
      st := CASE WHEN COALESCE(a_tags->>'resonate:timer','') = 'true'
                 THEN 'resolved' ELSE 'rejected_timedout' END;
      INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
      VALUES (a_id, st, a_headers, a_data, a_tags, a_toat, a_toat, a_toat) RETURNING * INTO p;
      INSERT INTO tasks (id, state, version) VALUES (p.id, 'fulfilled', 0) RETURNING * INTO t;
    END IF;
  ELSE
    -- The id names a plain promise (no resonate:target): not a malformed
    -- request but an unprocessable target — 422, matching the spec (T-02).
    IF NOT (p.tags ? 'resonate:target') THEN RETURN jsonb_build_object('status', 422); END IF;
    SELECT * INTO t FROM tasks WHERE id = p.id FOR UPDATE;
    IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;
    IF t.state = 'fulfilled' THEN
      NULL;
    ELSIF t.state = 'pending' THEN
      DELETE FROM task_resumes WHERE task_id = t.id;
      UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                       timeout_at = p_now + p_ttl WHERE id = t.id RETURNING * INTO t;
    ELSE
      RETURN jsonb_build_object('status', 409);
    END IF;
  END IF;
  RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                            'promise', _promise_json_raw(p));
END $$;

CREATE OR REPLACE FUNCTION task_acquire(
    p_id text, p_version int, p_pid text, p_ttl bigint, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  IF p_pid IS NULL OR p_ttl IS NULL THEN RETURN jsonb_build_object('status', 400); END IF;
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'pending' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  DELETE FROM task_resumes WHERE task_id = t.id;
  UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                   timeout_at = p_now + p_ttl
    WHERE id = t.id RETURNING * INTO t;
  RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                            'promise', _promise_json(p, p_now));
END $$;

CREATE OR REPLACE FUNCTION task_fence(
    p_id text, p_version int, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises; r jsonb; kind text := p_action->>'kind'; req jsonb := p_action->'req';
BEGIN
  -- A fence aimed at its own promise makes no sense: settling yourself is
  -- task.fulfill's job, and allowing it would fulfill the fencing task as a
  -- side effect of its own action. Rejected before any state is consulted.
  IF req->>'id' = p_id THEN RETURN jsonb_build_object('status', 400); END IF;
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  IF kind = 'create' THEN
    r := promise_create(req->>'id', (req->>'timeoutAt')::bigint,
                        req->'param', COALESCE(req->'tags','{}'::jsonb), p_now);
    RETURN jsonb_build_object('status', 200, 'action', jsonb_build_object('create', r));
  ELSIF kind = 'settle' THEN
    r := promise_settle(req->>'id', req->>'state', req->'value', p_now);
    RETURN jsonb_build_object('status', 200, 'action', jsonb_build_object('settle', r));
  ELSE
    RETURN jsonb_build_object('status', 422);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION task_heartbeat(p_pid text, p_tasks jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
BEGIN
  PERFORM _lock(lid) FROM (
    SELECT DISTINCT ref->>'id' AS lid
    FROM jsonb_array_elements(COALESCE(p_tasks, '[]'::jsonb)) ref
  ) s ORDER BY lid;

  UPDATE tasks t SET timeout_at = p_now + COALESCE(t.ttl, 0)
  FROM (SELECT ref->>'id' AS id, (ref->>'version')::int AS version
        FROM jsonb_array_elements(COALESCE(p_tasks, '[]'::jsonb)) ref) r
  JOIN promises p ON p.id = r.id
  WHERE t.id = r.id AND t.state = 'acquired' AND t.version = r.version AND t.pid = p_pid
    AND p.state = 'pending' AND p.timeout_at > p_now;

  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_suspend(
    p_id text, p_version int, p_actions jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; tp promises; missing int; settled bool;
BEGIN
  -- A task awaiting its own promise is a self-deadlock by construction: the
  -- callback it registers could only be fired by its own completion. A
  -- malformed request, rejected with highest precedence — before existence,
  -- state, or version are consulted.
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) act
             WHERE act->>'awaited' = p_id) THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  PERFORM _lock(lid) FROM (
    SELECT p_id AS lid
    UNION
    SELECT act->>'awaited'
      FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) AS act
  ) s ORDER BY lid;
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO tp FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF tp.state <> 'pending' OR tp.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  SELECT count(*) FILTER (WHERE pa.id IS NULL),
         COALESCE(bool_or(pa.state <> 'pending' OR pa.timeout_at <= p_now), false)
    INTO missing, settled
  FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) act
  LEFT JOIN promises pa ON pa.id = act->>'awaited';

  IF missing > 0 THEN RETURN jsonb_build_object('status', 422); END IF;
  IF settled THEN
    DELETE FROM task_resumes WHERE task_id = t.id;
    RETURN jsonb_build_object('status', 300);
  END IF;

  INSERT INTO callbacks (awaited_id, awaiter_id)
  SELECT act->>'awaited', t.id
  FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) act
  ON CONFLICT DO NOTHING;
  DELETE FROM task_resumes WHERE task_id = t.id;
  UPDATE tasks SET state = 'suspended', pid = NULL, ttl = NULL WHERE id = t.id;
  RETURN jsonb_build_object('status', 200);
END $$;

DROP FUNCTION IF EXISTS task_fulfill_typed(text, int, text, jsonb, text, bigint);

CREATE OR REPLACE FUNCTION task_fulfill(
    p_id text, p_version int, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  t tasks; p promises;
  a_state   text  := p_action->>'state';
  a_headers jsonb := COALESCE(p_action#>'{value,headers}', '{}'::jsonb);
  a_data    text  := p_action#>>'{value,data}';
BEGIN
  -- Same validation as promise_settle, same precedence: the carried settle
  -- must name a client-settable state before anything else is considered.
  IF a_state IS NULL OR a_state NOT IN ('resolved','rejected','rejected_canceled') THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;
  UPDATE promises SET state = a_state, value_headers = a_headers, value_data = a_data,
                      settled_at = p_now WHERE id = p.id RETURNING * INTO p;
  PERFORM _cascade_settle(p, p_now);
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json_raw(p));
END $$;

CREATE OR REPLACE FUNCTION task_release(p_id text, p_version int, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL,
                   timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_halt(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  IF t.state = 'fulfilled' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.state = 'halted' THEN RETURN jsonb_build_object('status', 200); END IF;

  UPDATE tasks SET state = 'halted', pid = NULL, ttl = NULL WHERE id = t.id;
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_continue(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  IF t.state <> 'halted' THEN RETURN jsonb_build_object('status', 409); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  UPDATE tasks SET state = 'pending', timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 5 · SCHEDULE ACTIONS · S-01 .. S-04
-- ═══════════════════════════════════════════════════════════════════════════

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION schedule_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE s schedules;
BEGIN
  SELECT * INTO s FROM schedules WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
END $$;

CREATE OR REPLACE FUNCTION schedule_create(
    p_id text, p_cron text, p_promise_id text, p_promise_timeout bigint,
    p_promise_param jsonb, p_promise_tags jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s schedules; nxt bigint;
BEGIN
  IF p_id IS NULL OR p_cron IS NULL OR p_promise_id IS NULL OR p_promise_timeout IS NULL THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF FOUND THEN
    RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
  END IF;

  BEGIN
    nxt := _next_cron(p_cron, p_now);
  EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object('status', 400);
  END;
  INSERT INTO schedules (id, cron, promise_id, promise_timeout,
                         promise_param_headers, promise_param_data, promise_tags,
                         created_at, next_run_at, last_run_at)
  VALUES (p_id, p_cron, p_promise_id, p_promise_timeout,
          COALESCE(p_promise_param->'headers','{}'::jsonb), p_promise_param->>'data',
          COALESCE(p_promise_tags,'{}'::jsonb), p_now, nxt, NULL)
  RETURNING * INTO s;
  RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
END $$;

CREATE OR REPLACE FUNCTION schedule_delete(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE s schedules;
BEGIN
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  DELETE FROM schedules WHERE id = p_id;
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION schedule_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 6 · INTERNAL TRANSITIONS · timeouts + process_timeouts driver
-- ═══════════════════════════════════════════════════════════════════════════
-- Postgres has no timers -- timeouts are rows. Each on_* is the exact transition

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _on_promise_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE p promises; st text;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF p.state <> 'pending' THEN RETURN; END IF;

  st := CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END;
  UPDATE promises SET state = st, settled_at = p.timeout_at WHERE id = p.id RETURNING * INTO p;
  PERFORM _cascade_settle(p, p_now);
END $$;

CREATE OR REPLACE FUNCTION _on_task_retry_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF t.state <> 'pending' THEN RETURN; END IF;

  UPDATE tasks SET timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN; END IF;
  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION _on_task_lease_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF t.state <> 'acquired' THEN RETURN; END IF;

  UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL,
                   timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN; END IF;
  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION _on_schedule_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE s schedules; cron_time bigint; pid text;
BEGIN
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;

  WHILE s.next_run_at <= p_now LOOP
    cron_time := s.next_run_at;
    pid := _expand(s.promise_id, s.id, cron_time);
    PERFORM promise_create(
      pid, cron_time + s.promise_timeout,
      jsonb_build_object('headers', s.promise_param_headers, 'data', s.promise_param_data),
      s.promise_tags, cron_time);
    s.last_run_at := cron_time;
    s.next_run_at := _next_cron(s.cron, cron_time);
  END LOOP;

  UPDATE schedules SET last_run_at = s.last_run_at, next_run_at = s.next_run_at WHERE id = s.id;
END $$;

CREATE OR REPLACE FUNCTION process_promise_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id FROM promises
           WHERE _promise_timed(promises) AND timeout_at <= p_now
           ORDER BY timeout_at, id LOOP
    PERFORM _on_promise_timeout(r.id, p_now); cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

CREATE OR REPLACE FUNCTION process_task_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id, state FROM tasks
           WHERE state IN ('pending','acquired') AND timeout_at <= p_now
           ORDER BY timeout_at, id LOOP
    IF r.state = 'pending' THEN PERFORM _on_task_retry_timeout(r.id, p_now);
    ELSE                        PERFORM _on_task_lease_timeout(r.id, p_now); END IF;
    cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

CREATE OR REPLACE FUNCTION process_schedule_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id FROM schedules WHERE next_run_at <= p_now ORDER BY next_run_at, id LOOP
    PERFORM _on_schedule_timeout(r.id, p_now); cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

CREATE OR REPLACE FUNCTION process_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE cnt int; _attempt int;
BEGIN
  FOR _attempt IN 1..50 LOOP
    BEGIN
      cnt := 0;
      cnt := cnt + process_promise_timeouts(p_now);
      cnt := cnt + process_task_timeouts(p_now);
      cnt := cnt + process_schedule_timeouts(p_now);
      RETURN cnt;
    EXCEPTION WHEN deadlock_detected THEN
      IF _attempt >= 50 THEN RAISE; END IF;
    END;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION process_timeouts() RETURNS int
  LANGUAGE sql AS $$ SELECT process_timeouts((extract(epoch from clock_timestamp()) * 1000)::bigint); $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 7 · WIRE DISPATCHER · resonate_rpc(jsonb) — resonate-sdk-ts protocol
-- ═══════════════════════════════════════════════════════════════════════════
-- The one entrypoint the SDK network layer calls. Request:

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _status_text(status int) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE status
    WHEN 400 THEN 'bad request'    WHEN 404 THEN 'not found'
    WHEN 409 THEN 'conflict'       WHEN 422 THEN 'unprocessable entity'
    WHEN 501 THEN 'not implemented' ELSE 'error' END;
$$;

CREATE OR REPLACE FUNCTION _rpc_data(kind text, status int, r jsonb) RETURNS jsonb
  LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF status NOT IN (200, 300) THEN
    RETURN to_jsonb(_status_text(status));
  END IF;
  RETURN CASE kind
    WHEN 'promise.get'               THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.create'            THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.settle'            THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.register_callback' THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.register_listener' THEN jsonb_build_object('promise', r->'promise')
    WHEN 'task.get'                  THEN jsonb_build_object('task', r->'task')
    WHEN 'task.create'               THEN jsonb_build_object('task', r->'task', 'promise', r->'promise', 'preload', '[]'::jsonb)
    WHEN 'task.acquire'              THEN jsonb_build_object('task', r->'task', 'promise', r->'promise', 'preload', '[]'::jsonb)
    WHEN 'task.fulfill'              THEN jsonb_build_object('promise', r->'promise')
    WHEN 'task.suspend'              THEN jsonb_build_object('preload', '[]'::jsonb)
    WHEN 'schedule.get'              THEN jsonb_build_object('schedule', r->'schedule')
    WHEN 'schedule.create'           THEN jsonb_build_object('schedule', r->'schedule')
    ELSE '{}'::jsonb
  END;
END $$;

CREATE OR REPLACE FUNCTION resonate_rpc(req jsonb) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE
  kind text := req->>'kind';
  head jsonb := COALESCE(req->'head', '{}'::jsonb);
  corr text := head->>'corrId';
  ver  text := COALESCE(head->>'version', '1');
  d    jsonb := COALESCE(req->'data', '{}'::jsonb);
  now  bigint := COALESCE((head->>'resonate:debug_time')::bigint,
                          (extract(epoch from clock_timestamp()) * 1000)::bigint);
  r jsonb; status int; data jsonb;
  ik text; inner_raw jsonb; inner_kind text; inner_status int; inner_res jsonb;
  _attempt int;
BEGIN
  FOR _attempt IN 1..50 LOOP
   BEGIN
  IF kind LIKE 'debug.%' THEN
   BEGIN
     EXECUTE 'SELECT resonate._debug($1, $2, $3)' INTO r USING kind, d, now;
   EXCEPTION WHEN undefined_function THEN
     r := jsonb_build_object('status', 501);
   END;
  ELSE
  CASE kind
    WHEN 'promise.get' THEN
      r := promise_get(d->>'id', now);
    WHEN 'promise.create' THEN
      r := promise_create(d->>'id', (d->>'timeoutAt')::bigint, d->'param', COALESCE(d->'tags','{}'::jsonb), now);
    WHEN 'promise.settle' THEN
      r := promise_settle(d->>'id', d->>'state', d->'value', now);
    WHEN 'promise.register_callback' THEN
      r := promise_register_callback(d->>'awaited', d->>'awaiter', now);
    WHEN 'promise.register_listener' THEN
      r := promise_register_listener(d->>'awaited', d->>'address', now);
    WHEN 'promise.search' THEN
      r := promise_search(d, now);

    WHEN 'task.get' THEN
      r := task_get(d->>'id', now);
    WHEN 'task.create' THEN
      r := task_create(d->>'pid', (d->>'ttl')::bigint, (d->'action')->'data', now);
    WHEN 'task.acquire' THEN
      r := task_acquire(d->>'id', (d->>'version')::int, d->>'pid', (d->>'ttl')::bigint, now);
    WHEN 'task.release' THEN
      r := task_release(d->>'id', (d->>'version')::int, now);
    WHEN 'task.heartbeat' THEN
      r := task_heartbeat(d->>'pid', COALESCE(d->'tasks','[]'::jsonb), now);
    WHEN 'task.suspend' THEN
      r := task_suspend(d->>'id', (d->>'version')::int,
             COALESCE((SELECT jsonb_agg(e->'data') FROM jsonb_array_elements(d->'actions') e), '[]'::jsonb),
             now);
    WHEN 'task.fulfill' THEN
      r := task_fulfill(d->>'id', (d->>'version')::int, (d->'action')->'data', now);
    WHEN 'task.halt' THEN
      r := task_halt(d->>'id', now);
    WHEN 'task.continue' THEN
      r := task_continue(d->>'id', now);
    WHEN 'task.fence' THEN
      inner_kind := CASE WHEN (d->'action')->>'kind' = 'promise.create' THEN 'create' ELSE 'settle' END;
      r := task_fence(d->>'id', (d->>'version')::int,
                      jsonb_build_object('kind', inner_kind, 'req', (d->'action')->'data'), now);
    WHEN 'task.search' THEN
      r := task_search(d, now);

    WHEN 'schedule.get' THEN
      r := schedule_get(d->>'id', now);
    WHEN 'schedule.create' THEN
      r := schedule_create(d->>'id', d->>'cron', d->>'promiseId', (d->>'promiseTimeout')::bigint,
                           d->'promiseParam', COALESCE(d->'promiseTags','{}'::jsonb), now);
    WHEN 'schedule.delete' THEN
      r := schedule_delete(d->>'id', now);
    WHEN 'schedule.search' THEN
      r := schedule_search(d, now);

    ELSE
      r := jsonb_build_object('status', 501);
  END CASE;
  END IF;

  status := (r->>'status')::int;

  IF kind LIKE 'debug.%' THEN
    data := COALESCE(r->'data', '{}'::jsonb);
  ELSIF kind = 'task.fence' AND status = 200 THEN
    inner_raw    := COALESCE(r->'action'->'create', r->'action'->'settle');
    inner_kind   := CASE WHEN r->'action'->'create' IS NOT NULL THEN 'promise.create' ELSE 'promise.settle' END;
    inner_status := (inner_raw->>'status')::int;
    inner_res := jsonb_build_object(
      'kind', inner_kind,
      'head', jsonb_build_object('corrId', corr, 'status', inner_status, 'version', ver),
      'data', _rpc_data(inner_kind, inner_status, inner_raw));
    data := jsonb_build_object('action', inner_res, 'preload', '[]'::jsonb);
  ELSE
    data := _rpc_data(kind, status, r);
  END IF;

  RETURN jsonb_build_object(
    'kind', kind,
    'head', jsonb_build_object('corrId', corr, 'status', status, 'version', ver),
    'data', data);
   EXCEPTION
     WHEN deadlock_detected THEN
       IF _attempt >= 50 THEN RAISE; END IF;
     WHEN OTHERS THEN
       status := CASE WHEN SQLSTATE LIKE '22%' OR SQLSTATE = '23502' THEN 400 ELSE 500 END;
       RETURN jsonb_build_object('kind', kind,
         'head', jsonb_build_object('corrId', corr, 'status', status, 'version', ver),
         'data', to_jsonb(_status_text(status)));
   END;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION resonate.invoke(
    id      text,
    func    text,
    args    jsonb,
    target  text,
    version int    DEFAULT 1,
    timeout bigint DEFAULT NULL)
  RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  now   bigint := (extract(epoch from clock_timestamp()) * 1000)::bigint;
  toat  bigint := COALESCE(timeout, now + 86400000);
  inv   text   := jsonb_build_object('func', func, 'args', args, 'version', version)::text;
  param jsonb  := jsonb_build_object('headers', '{}'::jsonb,
                    'data', replace(encode(convert_to(inv, 'utf8'), 'base64'), E'\n', ''));
  tags  jsonb  := jsonb_strip_nulls(jsonb_build_object(
                    'resonate:target', NULLIF(target, ''),
                    'resonate:scope',  'global',
                    'resonate:origin', id,
                    'resonate:branch', id,
                    'resonate:parent', id));
BEGIN
  RETURN resonate.promise_create(id, toat, param, tags, now) -> 'promise';
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 8 · pg_cron TIMER DRIVER
-- ═══════════════════════════════════════════════════════════════════════════
-- pg_cron is the sole timer driver: it polls process_timeouts() every 5s so

DO $cron$
DECLARE
  v_db text := current_database();
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
    RAISE WARNING 'resonate: pg_cron not available -- timers will NOT fire until it is installed and scheduled (section 8)';
    RETURN;
  END IF;

  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
      CREATE EXTENSION pg_cron;
    END IF;

    BEGIN
      PERFORM cron.schedule_in_database('resonate_process_timeouts', '5 seconds',
                'SELECT resonate.process_timeouts()', v_db);
    EXCEPTION WHEN OTHERS THEN
      BEGIN
        PERFORM cron.schedule('resonate_process_timeouts', '5 seconds',
                  'SELECT resonate.process_timeouts()');
      EXCEPTION WHEN OTHERS THEN
        PERFORM cron.schedule('resonate_process_timeouts', '* * * * *',
                  'SELECT resonate.process_timeouts()');
      END;
    END;

    RAISE NOTICE 'resonate: pg_cron enabled; process_timeouts scheduled (database %)', v_db;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'resonate: pg_cron present but could not be scheduled (%) -- timers will NOT fire until scheduled', SQLERRM;
  END;
END
$cron$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 9 · DISPATCH · addressed outbox drain (execute + unblock)
-- ═══════════════════════════════════════════════════════════════════════════
-- Each dequeue is a destructive, addressed, ORDER BY seq, FOR UPDATE SKIP LOCKED

CREATE OR REPLACE FUNCTION dequeue_execute(p_target text, p_limit int DEFAULT 100)
  RETURNS TABLE(task_id text, version int)
  LANGUAGE sql AS $$
  WITH d AS (
    DELETE FROM outbox
    WHERE ctid IN (
      SELECT ctid FROM outbox
      WHERE kind = 'execute' AND address = p_target
        AND address NOT LIKE 'http://%' AND address NOT LIKE 'https://%'
      ORDER BY seq
      FOR UPDATE SKIP LOCKED
      LIMIT p_limit)
    RETURNING task_id, version, seq)
  SELECT task_id, version FROM d ORDER BY seq;
$$;

CREATE OR REPLACE FUNCTION dequeue_unblock(p_address text, p_limit int DEFAULT 100)
  RETURNS TABLE(promise jsonb)
  LANGUAGE sql AS $$
  WITH d AS (
    DELETE FROM outbox
    WHERE ctid IN (
      SELECT ctid FROM outbox
      WHERE kind = 'unblock' AND address = p_address
        AND address NOT LIKE 'http://%' AND address NOT LIKE 'https://%'
      ORDER BY seq
      FOR UPDATE SKIP LOCKED
      LIMIT p_limit)
    RETURNING promise, seq)
  SELECT promise FROM d ORDER BY seq;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 10 · RETENTION · gc
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION gc(p_settled_before bigint, p_limit int DEFAULT 10000)
  RETURNS bigint LANGUAGE sql AS $$
  WITH doomed AS (
    SELECT p.ctid FROM promises p
    WHERE p.state <> 'pending' AND p.settled_at <= p_settled_before
      AND NOT EXISTS (SELECT 1 FROM promises r
                      WHERE r.id = p.origin_id AND r.state = 'pending')
    ORDER BY p.settled_at
    LIMIT p_limit),
  del AS (
    DELETE FROM promises WHERE ctid IN (SELECT ctid FROM doomed) RETURNING id),
  unb AS (
    DELETE FROM outbox
    WHERE kind = 'unblock' AND promise->>'id' IN (SELECT id FROM del)
    RETURNING 1),
  tr AS (
    DELETE FROM task_resumes WHERE awaited_id IN (SELECT id FROM del)
    RETURNING 1)
  SELECT count(*) FROM del;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 11 · AUTHZ · least-privilege resonate_worker role
-- ═══════════════════════════════════════════════════════════════════════════
-- Lock EXECUTE (default PUBLIC) to one worker role. The wire entrypoints are

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'resonate_worker') THEN
    CREATE ROLE resonate_worker;
  END IF;
END $$;

REVOKE ALL     ON ALL TABLES    IN SCHEMA resonate FROM PUBLIC;
REVOKE ALL     ON ALL SEQUENCES IN SCHEMA resonate FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA resonate FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA resonate REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

GRANT USAGE ON SCHEMA resonate TO resonate_worker;

ALTER FUNCTION resonate_rpc(jsonb)        SECURITY DEFINER;
ALTER FUNCTION dequeue_execute(text, int) SECURITY DEFINER;
ALTER FUNCTION dequeue_unblock(text, int) SECURITY DEFINER;

DO $pin$
DECLARE f regprocedure;
BEGIN
  FOR f IN
    SELECT p.oid::regprocedure
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'resonate'
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = resonate, pg_temp', f);
  END LOOP;
END $pin$;

GRANT EXECUTE ON FUNCTION
  resonate_rpc(jsonb),
  get_schema_version(),
  invoke(text, text, jsonb, text, int, bigint),
  dequeue_execute(text, int),
  dequeue_unblock(text, int),
  outbox_channel(text)
  TO resonate_worker;
