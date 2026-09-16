// The Neo4j schema: constraints and indexes, nothing else. Neo4j has no
// tables, so there is nothing to create before the first node is written;
// what is declared here is what makes the writes safe and the reads indexed.
//
// One node per promise, labelled :Promise. A task is not a second node: a
// promise runs as a task exactly when it carries a resonate:target, and the
// task's columns sit on the same node as the promise's — the Postgres
// collapse, carried over. Beside it sits :Schedule, a separate id space.
//
// Two relationship types carry what the relational engines keep in arrays:
//
//   (awaiter:Promise)-[:AWAITS {ready: false}]->(awaited:Promise)
//     the awaited promise's `callbacks` entry for this awaiter
//   (awaiter:Promise)-[:AWAITS {ready: true}]->(awaited:Promise)
//     the awaiter's task's `resumes` entry for this awaited promise
//
// One edge, one flag, because a callback and a resume are the same fact at two
// points in time: settling the awaited promise flips the flag. Fulfilling the
// awaiter's task deletes its outgoing edges, which is the GIN-indexed reverse
// lookup the Postgres schema needs an index for.
//
//   (child:Promise)-[:CHILD_OF]->(parent:Promise)
//     the resonate:parent tag, as an edge. Nothing in the protocol reads it;
//     it is there so the call tree is a path a graph tool can draw.
//
// Every statement is IF NOT EXISTS, so applying this to a database that
// already carries it is a no-op. Neo4j runs schema statements outside data
// transactions, one per query.

CREATE CONSTRAINT resonate_promise_id IF NOT EXISTS
  FOR (p:Promise) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT resonate_schedule_id IF NOT EXISTS
  FOR (s:Schedule) REQUIRE s.id IS UNIQUE;

// Root-ness and the console's tree: every promise of one execution shares an
// origin, everything before the id's first ':'.
CREATE INDEX resonate_promise_origin IF NOT EXISTS
  FOR (p:Promise) ON (p.origin);

// The branch siblings a task response preloads.
CREATE INDEX resonate_promise_branch IF NOT EXISTS
  FOR (p:Promise) ON (p.branch_id);

// The four deadline queues are four predicates over the same node, as they
// are four partial indexes in Postgres. Composite, because Neo4j has no
// partial indexes: the state column narrows the scan the way the WHERE did.
CREATE INDEX resonate_promise_timeout IF NOT EXISTS
  FOR (p:Promise) ON (p.state, p.timeout_at);

CREATE INDEX resonate_promise_retry IF NOT EXISTS
  FOR (p:Promise) ON (p.task_state, p.retry_timeout_at);

CREATE INDEX resonate_promise_lease IF NOT EXISTS
  FOR (p:Promise) ON (p.task_state, p.lease_timeout_at);

// The console's default sort.
CREATE INDEX resonate_promise_created IF NOT EXISTS
  FOR (p:Promise) ON (p.created_at);

CREATE INDEX resonate_schedule_next_run IF NOT EXISTS
  FOR (s:Schedule) ON (s.next_run_at);
