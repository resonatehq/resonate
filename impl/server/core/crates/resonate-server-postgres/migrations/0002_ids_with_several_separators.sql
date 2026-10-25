-- Promise ids may hold more than one ':'.
--
-- The protocol admits any id without a NUL byte, and the origin is the text
-- before the first ':' (`origin_id`), so `wf:ns:run:1` is an id with origin
-- `wf`. 0001 carried a check, well_formed_promise_id_at_most_one_separator,
-- that allowed at most one ':'. It was a deployment convention, not an entry
-- of the constraint catalogue, and it made Postgres the only store to refuse
-- such ids, with a 500. This drops it from a database 0001 created.
ALTER TABLE resonate.promises
  DROP CONSTRAINT IF EXISTS well_formed_promise_id_at_most_one_separator;
