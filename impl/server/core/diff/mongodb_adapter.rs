//! The MongoDB engine, behind the SQL family's engine trait.
//!
//! `resonate-server-mongodb` carries its own copy of `Engine`, `Input`, `Output`,
//! `Timeout` and `Scheduled` — a deliberate copy beside `resonate-sql`'s,
//! ScyllaDB's and Neo4j's, so the copies can be diffed. The differential and the
//! console tests speak `resonate_sql::engine::Engine`, so this is the twenty
//! lines that let the copies meet: every variant maps to its namesake and
//! nothing else changes.
//!
//! Included by `#[path]` from `diff/differential.rs` and `tests/ui.rs`.

use async_trait::async_trait;
use resonate_server_mongodb::engine as mongo;
use resonate_server_mongodb::MongoDbEngine;
use resonate_sql::engine::{Engine, Input, Outgoing, Output, Scheduled, Timeout};
use resonate_sql::{StorageError, StorageResult};

pub struct MongoDbBackend(pub MongoDbEngine);

fn to_mongo(t: Timeout) -> mongo::Timeout {
    match t {
        Timeout::PromiseTimeout { promise_id } => mongo::Timeout::PromiseTimeout { promise_id },
        Timeout::TaskRetryTimeout { task_id } => mongo::Timeout::TaskRetryTimeout { task_id },
        Timeout::TaskLeaseTimeout { task_id, pid } => {
            mongo::Timeout::TaskLeaseTimeout { task_id, pid }
        }
        Timeout::ScheduleDue { schedule_id } => mongo::Timeout::ScheduleDue { schedule_id },
    }
}

fn from_mongo(t: mongo::Timeout) -> Timeout {
    match t {
        mongo::Timeout::PromiseTimeout { promise_id } => Timeout::PromiseTimeout { promise_id },
        mongo::Timeout::TaskRetryTimeout { task_id } => Timeout::TaskRetryTimeout { task_id },
        mongo::Timeout::TaskLeaseTimeout { task_id, pid } => {
            Timeout::TaskLeaseTimeout { task_id, pid }
        }
        mongo::Timeout::ScheduleDue { schedule_id } => Timeout::ScheduleDue { schedule_id },
    }
}

fn scheduled(s: mongo::Scheduled) -> Scheduled {
    Scheduled {
        at: s.at,
        timeout: from_mongo(s.timeout),
    }
}

fn outgoing(m: mongo::Outgoing) -> Outgoing {
    match m {
        mongo::Outgoing::Execute {
            address,
            task_id,
            version,
        } => Outgoing::Execute {
            address,
            task_id,
            version,
        },
        mongo::Outgoing::Unblock { address, promise } => Outgoing::Unblock { address, promise },
    }
}

fn output(o: mongo::Output) -> Output {
    Output {
        response: o.response,
        messages: o.messages.into_iter().map(outgoing).collect(),
        timeouts: o.timeouts.into_iter().map(scheduled).collect(),
    }
}

fn storage(e: resonate_server_mongodb::StorageError) -> StorageError {
    match e {
        resonate_server_mongodb::StorageError::Serialization => StorageError::Serialization,
        resonate_server_mongodb::StorageError::InvalidInput(m) => StorageError::InvalidInput(m),
        resonate_server_mongodb::StorageError::Backend(m) => StorageError::Backend(m),
    }
}

#[async_trait]
impl Engine for MongoDbBackend {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        let out = match input {
            Input::External(req) => {
                mongo::Engine::process(&self.0, mongo::Input::External(req), now).await
            }
            Input::Internal(t) => {
                mongo::Engine::process(&self.0, mongo::Input::Internal(to_mongo(t)), now).await
            }
        };
        output(out)
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        mongo::Engine::tick(&self.0, now)
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
        mongo::Engine::upcoming(&self.0, limit)
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

/// Connect to the MongoDB named by `TEST_MONGODB_URI`, as the differential and
/// the console tests do for Postgres, MySQL and Neo4j. The database is the one
/// the URI's path names — `cargo xtask` names a fresh one per job — and
/// `resonate_test` when it names none. The deployment must be a replica set
/// (one member is enough): the engine runs every transition in a transaction.
pub async fn connect_from_env(retry_timeout: i64, preload_limit: u32) -> Option<MongoDbBackend> {
    let uri = std::env::var("TEST_MONGODB_URI").ok()?;
    let named = uri
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map(|(_, path)| path.split('?').next().unwrap_or(""))
        .is_some_and(|db| !db.is_empty());
    let cfg = resonate_server_mongodb::Config {
        uri,
        database: (!named).then(|| "resonate_test".to_string()),
        retry_timeout,
        preload_limit,
        ..resonate_server_mongodb::Config::default()
    };
    let engine = MongoDbEngine::connect(&cfg, true)
        .await
        .expect("mongodb connect");
    engine.init(true).await.expect("mongodb index init");
    Some(MongoDbBackend(engine))
}
