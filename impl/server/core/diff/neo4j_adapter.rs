//! The Neo4j engine, behind the SQL family's engine trait.
//!
//! `resonate-server-neo4j` carries its own copy of `Engine`, `Input`, `Output`,
//! `Timeout` and `Scheduled` — a deliberate third copy beside `resonate-sql`'s
//! and ScyllaDB's, so the copies can be diffed. The differential and the
//! console tests speak `resonate_sql::engine::Engine`, so this is the twenty
//! lines that let the copies meet: every variant maps to its namesake and
//! nothing else changes.
//!
//! Included by `#[path]` from `diff/differential.rs` and `tests/ui.rs`.

use async_trait::async_trait;
use resonate_server_neo4j::engine as neo;
use resonate_server_neo4j::Neo4jEngine;
use resonate_sql::engine::{Engine, Input, Outgoing, Output, Scheduled, Timeout};
use resonate_sql::{StorageError, StorageResult};

pub struct Neo4jBackend(pub Neo4jEngine);

fn to_neo(t: Timeout) -> neo::Timeout {
    match t {
        Timeout::PromiseTimeout { promise_id } => neo::Timeout::PromiseTimeout { promise_id },
        Timeout::TaskRetryTimeout { task_id } => neo::Timeout::TaskRetryTimeout { task_id },
        Timeout::TaskLeaseTimeout { task_id, pid } => {
            neo::Timeout::TaskLeaseTimeout { task_id, pid }
        }
        Timeout::ScheduleDue { schedule_id } => neo::Timeout::ScheduleDue { schedule_id },
    }
}

fn from_neo(t: neo::Timeout) -> Timeout {
    match t {
        neo::Timeout::PromiseTimeout { promise_id } => Timeout::PromiseTimeout { promise_id },
        neo::Timeout::TaskRetryTimeout { task_id } => Timeout::TaskRetryTimeout { task_id },
        neo::Timeout::TaskLeaseTimeout { task_id, pid } => {
            Timeout::TaskLeaseTimeout { task_id, pid }
        }
        neo::Timeout::ScheduleDue { schedule_id } => Timeout::ScheduleDue { schedule_id },
    }
}

fn scheduled(s: neo::Scheduled) -> Scheduled {
    Scheduled {
        at: s.at,
        timeout: from_neo(s.timeout),
    }
}

fn outgoing(m: neo::Outgoing) -> Outgoing {
    match m {
        neo::Outgoing::Execute {
            address,
            task_id,
            version,
        } => Outgoing::Execute {
            address,
            task_id,
            version,
        },
        neo::Outgoing::Unblock { address, promise } => Outgoing::Unblock { address, promise },
    }
}

fn output(o: neo::Output) -> Output {
    Output {
        response: o.response,
        messages: o.messages.into_iter().map(outgoing).collect(),
        timeouts: o.timeouts.into_iter().map(scheduled).collect(),
    }
}

fn storage(e: resonate_server_neo4j::StorageError) -> StorageError {
    match e {
        resonate_server_neo4j::StorageError::Serialization => StorageError::Serialization,
        resonate_server_neo4j::StorageError::InvalidInput(m) => StorageError::InvalidInput(m),
        resonate_server_neo4j::StorageError::Backend(m) => StorageError::Backend(m),
    }
}

#[async_trait]
impl Engine for Neo4jBackend {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        let out = match input {
            Input::External(req) => {
                neo::Engine::process(&self.0, neo::Input::External(req), now).await
            }
            Input::Internal(t) => {
                neo::Engine::process(&self.0, neo::Input::Internal(to_neo(t)), now).await
            }
        };
        output(out)
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        neo::Engine::tick(&self.0, now)
            .await
            .map(|(n, m, s)| {
                (
                    n,
                    m.into_iter().map(outgoing).collect(),
                    s.into_iter().map(scheduled).collect(),
                )
            })
            .map_err(storage)
    }

    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        neo::Engine::upcoming(&self.0, limit)
            .await
            .map(|s| s.into_iter().map(scheduled).collect())
            .map_err(storage)
    }

    /// Like every engine now: what a transition emitted comes back on its
    /// `Output`, and the snapshot's `messages` is empty by construction.
    fn returns_messages(&self) -> bool {
        true
    }
}

/// Connect to the Neo4j named by `TEST_NEO4J_URI`, as the differential and
/// the console tests do for Postgres and MySQL. `TEST_NEO4J_USER` and
/// `TEST_NEO4J_PASSWORD` default to the CI service's `neo4j` / `resonate`.
pub async fn connect_from_env(retry_timeout: i64, preload_limit: u32) -> Option<Neo4jBackend> {
    let uri = std::env::var("TEST_NEO4J_URI").ok()?;
    let cfg = resonate_server_neo4j::Config {
        uri,
        user: std::env::var("TEST_NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string()),
        password: std::env::var("TEST_NEO4J_PASSWORD").unwrap_or_else(|_| "resonate".to_string()),
        retry_timeout,
        preload_limit,
        ..resonate_server_neo4j::Config::default()
    };
    let engine = Neo4jEngine::connect(&cfg, true)
        .await
        .expect("neo4j connect");
    engine.init(true).await.expect("neo4j schema init");
    Some(Neo4jBackend(engine))
}
