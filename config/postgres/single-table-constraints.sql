-- =============================================================================
-- Every database-enforced constraint, in one file
-- =============================================================================
-- Generated from a live database, so this IS the enforced set rather than a
-- description of it: 44 constraints over three tables.
--
-- It consolidates what is otherwise spread across three places — the two state
-- enums are inline in `resonate-single.sql`'s CREATE TABLE, the catalogue
-- constraints are in `constraints.sql`, and the id-format one is in
-- `constraints-id.sql`. Those remain the files to EDIT; this is the flat view,
-- and it is regenerated rather than hand-maintained.
--
-- Names are the specification's property names verbatim wherever an entry has
-- one, so a violation reports the same string the Lean catalogue and the trace
-- checker use. Six carry other names: the two enums, the two primary keys that
-- back uniqueness entries, the outbox payload shape, and the id format.
--
-- Every statement is DROP IF EXISTS then ADD, so the file is idempotent and
-- applies to any database already carrying `resonate-single.sql`:
--
--   psql -d yourdb -f resonate-single.sql -f constraints-all.sql
--
-- It depends on the IMMUTABLE helpers `_arr_uniq`, `_addr_valid` and
-- `_addrs_valid`, which `resonate-single.sql` defines.
--
-- NOT here, deliberately:
--   * the 50 `.trans` catalogue entries — claims about a PAIR of states, which
--     no CHECK can see;
--   * 5 cross-ROW entries and 2 clock-relative ones — `constraints.sql` says
--     which, and why each is out of reach;
--   * the 3 specification `gaps` — behaviour-changing, see
--     `constraints-gaps.sql`;
--   * the door checks, which return 400 rather than raising: Tags.timerTargeted
--     at promise.create / task.create / schedule.create, a `resonate:origin`
--     tag disagreeing with the id at promise.create / task.create, and any
--     `resonate:origin` on schedule.create;
--   * the id/origin agreement, which is unrepresentable rather than checked —
--     `origin_id` is `split_part(id, ':', 1)`.
-- =============================================================================

SET search_path TO resonate, public;

-- The outbox foreign key needs a TOTAL key to reference, and "the promises that
-- are tasks" is a partial set. `task_key` is the id exactly when the row is a
-- task; UNIQUE tolerates the NULLs of the rows that are not.
-- task_key is declared as a generated column in single-table.sql; kept here as
-- a comment so this file stays a faithful copy of the generated catalogue.
-- ALTER TABLE promises ADD COLUMN IF NOT EXISTS task_key TEXT
--   GENERATED ALWAYS AS (CASE WHEN tags ? 'resonate:target' THEN id END) STORED;

-- The outbox foreign key references `promises_task_key_unique`, so on a re-run
-- the unique constraint cannot be dropped while it still stands. It comes off
-- first and is re-added at the end of the file, which is what makes the whole
-- thing re-runnable.
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS consistent_outbox_execute_names_existing_task;


-- --- promises: keys --------------------------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_pkey;
ALTER TABLE promises ADD CONSTRAINT promises_pkey
  PRIMARY KEY (id);

ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_key_unique;
ALTER TABLE promises ADD CONSTRAINT promises_task_key_unique
  UNIQUE (task_key);


-- --- promises: domains — the two state enums, inline in CREATE TABLE -------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_state_check;
ALTER TABLE promises ADD CONSTRAINT promises_state_check
  CHECK ((state = ANY (ARRAY['pending'::text, 'resolved'::text,
  'rejected'::text, 'rejected_canceled'::text, 'rejected_timedout'::text])));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS promises_task_state_check;
ALTER TABLE promises ADD CONSTRAINT promises_task_state_check
  CHECK ((task_state = ANY (ARRAY['pending'::text, 'acquired'::text,
  'suspended'::text, 'halted'::text, 'fulfilled'::text])));


-- --- promises: promise ⊕ task — the entries a two-table layout cannot state ---
ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_task_iff_targeted_promise;
ALTER TABLE promises ADD CONSTRAINT consistent_task_iff_targeted_promise
  CHECK (((task_state IS NOT NULL) = (target IS NOT NULL)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_promise_has_fulfilled_task;
ALTER TABLE promises ADD CONSTRAINT consistent_settled_promise_has_fulfilled_task
  CHECK (((state = 'pending'::text) OR (task_state IS NULL) OR (task_state =
  'fulfilled'::text)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_settled_task_promise_settled;
ALTER TABLE promises ADD CONSTRAINT consistent_settled_task_promise_settled
  CHECK (((task_state IS DISTINCT FROM 'fulfilled'::text) OR (state <>
  'pending'::text)));


-- --- promises: promise well-formedness -------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_timeout_at
  CHECK ((created_at <= timeout_at));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_created_before_deadline;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_created_before_deadline
  CHECK (((state <> 'pending'::text) OR (created_at < timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_lte_timeout_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_lte_timeout_at
  CHECK (((settled_at IS NULL) OR (settled_at <= timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_created_at_lte_settled_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_settled_at
  CHECK (((settled_at IS NULL) OR (created_at <= settled_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_settled_at_iff_not_pending;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_iff_not_pending
  CHECK (((state <> 'pending'::text) = (settled_at IS NOT NULL)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_pending_has_no_value;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_has_no_value
  CHECK (((state <> 'pending'::text) OR ((value_data IS NULL) AND
  (value_headers = '{}'::jsonb))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_verdict_matches_timer_tag;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_verdict_matches_timer_tag
  CHECK (((settled_at IS DISTINCT FROM timeout_at) OR (state =
  CASE
      WHEN is_timer THEN 'resolved'::text
      ELSE 'rejected_timedout'::text
  END)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_deadline_settlement_has_no_value;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_settlement_has_no_value
  CHECK (((settled_at IS DISTINCT FROM timeout_at) OR ((value_data IS NULL)
  AND (value_headers = '{}'::jsonb))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timedout_is_server_owned;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timedout_is_server_owned
  CHECK (((state <> 'rejected_timedout'::text) OR (settled_at =
  timeout_at)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_timer_not_targeted;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timer_not_targeted
  CHECK ((NOT (is_timer AND (target IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_obligations_require_external;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_obligations_require_external
  CHECK ((external OR ((awaiters = '{}'::text[]) AND (listeners =
  '{}'::text[]))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_awaiter_is_not_self;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_awaiter_is_not_self
  CHECK ((NOT (id = ANY (awaiters))));


-- --- promises: task well-formedness ----------------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_pid;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_pid
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) = (pid IS
  NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_ttl;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_ttl
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) = (ttl IS
  NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_iff_has_expires_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_expires_at
  CHECK (((task_state IS NULL) OR ((task_state = 'acquired'::text) =
  (expires_at IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_pending_iff_has_retry_at;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_pending_iff_has_retry_at
  CHECK (((task_state IS NULL) OR ((task_state = 'pending'::text) =
  (retry_at IS NOT NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_fulfilled_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_fulfilled_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'fulfilled'::text) OR ((pid IS NULL)
  AND (ttl IS NULL) AND (expires_at IS NULL) AND (retry_at IS NULL) AND
  (resumes = '{}'::text[]))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'suspended'::text) OR ((pid IS NULL)
  AND (ttl IS NULL) AND (expires_at IS NULL) AND (retry_at IS NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_halted_is_cleared;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_halted_is_cleared
  CHECK (((task_state IS DISTINCT FROM 'halted'::text) OR ((pid IS NULL) AND
  (ttl IS NULL) AND (expires_at IS NULL) AND (retry_at IS NULL))));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_suspended_has_no_resumes;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_has_no_resumes
  CHECK (((task_state IS DISTINCT FROM 'suspended'::text) OR (resumes =
  '{}'::text[])));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_acquired_version_positive;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_version_positive
  CHECK (((task_state IS DISTINCT FROM 'acquired'::text) OR (task_version >=
  1)));


-- --- promises: obligations and uniqueness ----------------------------------
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_callbacks_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_callbacks_unique
  CHECK (((cardinality(awaiters) < 2) OR resonate._arr_uniq(awaiters)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_listeners_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_listeners_unique
  CHECK (((cardinality(listeners) < 2) OR resonate._arr_uniq(listeners)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_task_resumes_unique;
ALTER TABLE promises ADD CONSTRAINT well_formed_task_resumes_unique
  CHECK (((cardinality(resumes) < 2) OR resonate._arr_uniq(resumes)));

ALTER TABLE promises DROP CONSTRAINT IF EXISTS consistent_listener_addresses_deliverable;
ALTER TABLE promises ADD CONSTRAINT consistent_listener_addresses_deliverable
  CHECK (((listeners = '{}'::text[]) OR resonate._addrs_valid(listeners)));


-- --- promises: id format — this deployment's convention, not a catalogue entry ---
ALTER TABLE promises DROP CONSTRAINT IF EXISTS well_formed_promise_id_at_most_one_separator;
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_id_at_most_one_separator
  CHECK ((id ~ '^[^:]*(:[^:]*)?$'::text));


-- --- schedules: schedules --------------------------------------------------
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_pkey;
ALTER TABLE schedules ADD CONSTRAINT schedules_pkey
  PRIMARY KEY (id);

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_created_at_lte_next_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_next_run_at
  CHECK ((created_at <= next_run_at));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_created_at_lte_last_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_last_run_at
  CHECK (((last_run_at IS NULL) OR (created_at <= last_run_at)));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_last_run_at_lt_next_run_at;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_last_run_at_lt_next_run_at
  CHECK (((last_run_at IS NULL) OR (last_run_at < next_run_at)));

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS well_formed_schedule_promise_tags_not_timer_targeted;
ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_promise_tags_not_timer_targeted
  CHECK ((NOT ((COALESCE((promise_tags ->> 'resonate:timer'::text),
  ''::text) = 'true'::text) AND (promise_tags ? 'resonate:target'::text))));


-- --- outbox: outbox --------------------------------------------------------
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_pkey;
ALTER TABLE outbox ADD CONSTRAINT outbox_pkey
  PRIMARY KEY (key);

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_kind_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_kind_check
  CHECK ((kind = ANY (ARRAY['execute'::text, 'unblock'::text])));

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS well_formed_outbox_payload_matches_kind;
ALTER TABLE outbox ADD CONSTRAINT well_formed_outbox_payload_matches_kind
  CHECK (
  CASE kind
      WHEN 'execute'::text THEN ((task_id IS NOT NULL) AND (version IS NOT NULL) AND (promise IS NULL))
      WHEN 'unblock'::text THEN ((task_id IS NULL) AND (version IS NULL) AND (promise IS NOT NULL))
      ELSE NULL::boolean
  END);

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS consistent_outbox_unblock_names_settled_promise;
ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_unblock_names_settled_promise
  CHECK (((kind <> 'unblock'::text) OR ((promise ->> 'state'::text) <>
  'pending'::text)));

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS consistent_outbox_unblock_address_deliverable;
ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_unblock_address_deliverable
  CHECK (((kind <> 'unblock'::text) OR resonate._addr_valid(address)));

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS consistent_outbox_execute_names_existing_task;
ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_execute_names_existing_task
  FOREIGN KEY (task_id) REFERENCES resonate.promises(task_key) ON DELETE
  CASCADE;
