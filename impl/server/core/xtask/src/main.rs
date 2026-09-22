//! `cargo xtask <job>` — the one way to run what CI runs.
//!
//! Every job here is a CI job, and CI runs nothing but these commands, so a
//! green `cargo xtask all` on a laptop is the same claim as a green pipeline.
//!
//! ```text
//! cargo xtask check                       fmt, check, clippy, test — the Check job
//! cargo xtask differential --backend X    the engine and port differentials over X
//! cargo xtask differential --backend X --seed 1 --seed 2   … once per trajectory
//! cargo xtask porcupine --backend X       a live server under load, linearizability-checked
//! cargo xtask all                         every job, for every backend reachable
//! ```
//!
//! # Every database is vanilla
//!
//! A job that needs a database never reuses one. Given an admin connection
//! (`--postgres-admin-url`, `--mysql-admin-url`, or the matching
//! `XTASK_*_ADMIN_URL` variables — `XTASK_`, not `RESONATE_`, because the
//! server reads every `RESONATE_*` variable as configuration and refuses
//! keys it does not know) it creates a database with a fresh,
//! unique name, points the job at it, and drops it afterwards — on success and
//! on failure alike, unless `--keep-db` says to leave it for a look. Two runs
//! in one job get two databases. The SQLite legs open a fresh file or
//! `:memory:`, and blob a fresh in-process store, so they are vanilla by
//! construction. Nothing here depends on a database called `resonate`
//! existing, and nothing here ever touches one that does.
//!
//! Neo4j is the exception that cannot be fresh: Community Edition has one
//! database and no `CREATE DATABASE`. So the rule is kept by checking rather
//! than by construction — given `--neo4j-uri` (or `XTASK_NEO4J_URI`) a job
//! refuses to start on a database that already holds a promise or a schedule,
//! and clears what it wrote when it is done, with the same `--keep-db`
//! exception. It never deletes anything it did not find empty. Point it at a
//! disposable instance, as CI's service container is.
//!
//! # What runs where
//!
//! `check` is exactly the Check job's four steps. `differential` is one leg of
//! the Differential matrix: the engine differential (`diff/differential.rs`)
//! and the port differential (`diff/port.rs`) over SQLite, the oracle, blob
//! and the named backend, each on its own database; the blob leg is blob's
//! own differential and the port differential. `porcupine` is one leg of the
//! Linearizability matrix: build the server and the recorder, start the server
//! in debug mode on a free port, record a concurrent history, and check it with
//! the specification's `conccheck`. That checker is Go, from the
//! specification's `valid/porc` directory — `--spec-dir`, `XTASK_SPEC_DIR`,
//! `spec/valid/porc` (where CI checks it out) or `../../../spec/valid/porc`
//! (the monorepo), in that order.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand, ValueEnum};
use sqlx::Connection;

#[derive(Parser)]
#[command(name = "cargo xtask", about = "The one way to run what CI runs.")]
struct Cli {
    #[command(subcommand)]
    job: Job,
}

#[derive(Subcommand)]
enum Job {
    /// fmt, check, clippy and the workspace tests — the Check job.
    Check,
    /// The engine differential and the port differential over one backend,
    /// each on its own fresh database.
    Differential {
        #[arg(long)]
        backend: Backend,
        /// Trajectories to walk, as generator seeds: `--seed 1 --seed 2` or
        /// `--seed 1,2`. Each seed is a full run of both differentials, on
        /// databases of its own. Omitted, the run is the single trajectory CI
        /// walks.
        #[arg(long = "seed", value_delimiter = ',')]
        seeds: Vec<u64>,
        #[command(flatten)]
        db: DbArgs,
    },
    /// A live server under concurrent load, checked by the specification's
    /// linearizability checker, on its own fresh database.
    Porcupine {
        #[arg(long)]
        backend: Backend,
        #[command(flatten)]
        db: DbArgs,
        #[command(flatten)]
        porc: PorcArgs,
    },
    /// Every job above, for every backend the admin URLs reach.
    All {
        #[command(flatten)]
        db: DbArgs,
        #[command(flatten)]
        porc: PorcArgs,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Backend {
    Sqlite,
    Postgres,
    Mysql,
    Blob,
    Neo4j,
}

#[derive(Args, Clone)]
struct DbArgs {
    /// Admin connection to a PostgreSQL server; its database name is ignored.
    /// Fresh databases are created and dropped here.
    #[arg(long, env = "XTASK_POSTGRES_ADMIN_URL")]
    postgres_admin_url: Option<String>,
    /// Admin connection to a MySQL server; its database name is ignored.
    #[arg(long, env = "XTASK_MYSQL_ADMIN_URL")]
    mysql_admin_url: Option<String>,
    /// Bolt URI of a disposable Neo4j, which must hold no promise or schedule
    /// when a job starts. Cleared after the job.
    #[arg(long, env = "XTASK_NEO4J_URI")]
    neo4j_uri: Option<String>,
    #[arg(long, env = "XTASK_NEO4J_USER", default_value = "neo4j")]
    neo4j_user: String,
    #[arg(long, env = "XTASK_NEO4J_PASSWORD", default_value = "resonate")]
    neo4j_password: String,
    /// Leave the fresh databases in place after the run, for inspection.
    #[arg(long)]
    keep_db: bool,
}

#[derive(Args, Clone)]
struct PorcArgs {
    /// The specification's `valid/porc` directory, where `conccheck` lives.
    #[arg(long, env = "XTASK_SPEC_DIR")]
    spec_dir: Option<PathBuf>,
    /// Concurrent clients the recorder runs.
    #[arg(long, default_value_t = 8)]
    clients: u32,
    /// Operations per client.
    #[arg(long, default_value_t = 600)]
    ops: u32,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match cli.job {
        Job::Check => check(),
        Job::Differential { backend, seeds, db } => differential(backend, &seeds, &db).await,
        Job::Porcupine { backend, db, porc } => porcupine(backend, &db, &porc).await,
        Job::All { db, porc } => all(&db, &porc).await,
    };
    if let Err(e) = result {
        eprintln!("\nxtask: {e}");
        std::process::exit(1);
    }
}

type Result<T> = std::result::Result<T, String>;

/// The workspace root: this crate's parent.
fn core() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives one level under the workspace")
        .to_path_buf()
}

fn cargo() -> Command {
    let mut c = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    c.current_dir(core());
    c
}

fn run(what: &str, cmd: &mut Command) -> Result<()> {
    eprintln!("\n==> {what}");
    let status = cmd
        .status()
        .map_err(|e| format!("{what}: cannot start: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{what}: {status}"))
    }
}

// ---------------------------------------------------------------------------
// check
// ---------------------------------------------------------------------------

fn check() -> Result<()> {
    run(
        "cargo fmt -- --check",
        cargo().args(["fmt", "--", "--check"]),
    )?;
    run(
        "cargo check --workspace --all-targets --all-features",
        cargo().args(["check", "--workspace", "--all-targets", "--all-features"]),
    )?;
    run(
        "cargo clippy --workspace --all-targets --all-features -- -D warnings",
        cargo().args([
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ]),
    )?;
    run(
        "cargo test --workspace --all-features",
        cargo().args(["test", "--workspace", "--all-features"]),
    )
}

// ---------------------------------------------------------------------------
// fresh databases
// ---------------------------------------------------------------------------

/// A database that did not exist before this job and will not after it.
struct FreshDb {
    kind: Backend,
    db: DbArgs,
    admin_url: String,
    name: String,
    /// The URL the job is given: the admin connection, pointed at `name`.
    url: String,
    keep: bool,
}

impl FreshDb {
    async fn create(kind: Backend, db: &DbArgs, admin_url: &str, purpose: &str) -> Result<Self> {
        let keep = db.keep_db;
        if kind == Backend::Neo4j {
            let graph = neo4j(db, admin_url).await?;
            let held = neo4j_resonate_nodes(&graph).await?;
            if held > 0 {
                return Err(format!(
                    "neo4j at {admin_url} already holds {held} promises and schedules; \
                     a job runs only on an empty one, so point --neo4j-uri at a \
                     disposable instance"
                ));
            }
            eprintln!("==> neo4j at {admin_url} is empty");
            return Ok(Self {
                kind,
                db: db.clone(),
                admin_url: admin_url.to_string(),
                name: "neo4j".to_string(),
                url: admin_url.to_string(),
                keep,
            });
        }
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let name = format!("resonate_{purpose}_{}_{}", std::process::id(), nonce);
        let mut u = url::Url::parse(admin_url).map_err(|e| format!("admin url: {e}"))?;
        u.set_path(&format!("/{name}"));
        let url = u.to_string();
        match kind {
            Backend::Postgres => {
                let mut conn = sqlx::PgConnection::connect(admin_url)
                    .await
                    .map_err(|e| format!("postgres admin: {e}"))?;
                sqlx::query(&format!("CREATE DATABASE \"{name}\""))
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("create database {name}: {e}"))?;
            }
            Backend::Mysql => {
                let mut conn = sqlx::MySqlConnection::connect(admin_url)
                    .await
                    .map_err(|e| format!("mysql admin: {e}"))?;
                sqlx::query(&format!("CREATE DATABASE `{name}`"))
                    .execute(&mut conn)
                    .await
                    .map_err(|e| format!("create database {name}: {e}"))?;
            }
            Backend::Sqlite | Backend::Blob | Backend::Neo4j => {
                unreachable!("no server to create a database on")
            }
        }
        eprintln!("==> fresh database {name}");
        Ok(Self {
            kind,
            db: db.clone(),
            admin_url: admin_url.to_string(),
            name,
            url,
            keep,
        })
    }

    /// Drop it, whatever the job did — unless asked to keep it.
    async fn finish(self) {
        if self.keep {
            eprintln!("==> keeping database {} ({})", self.name, self.url);
            return;
        }
        let dropped = match self.kind {
            Backend::Postgres => match sqlx::PgConnection::connect(&self.admin_url).await {
                Ok(mut conn) => {
                    sqlx::query(&format!("DROP DATABASE \"{}\" WITH (FORCE)", self.name))
                        .execute(&mut conn)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                Err(e) => Err(e.to_string()),
            },
            Backend::Mysql => match sqlx::MySqlConnection::connect(&self.admin_url).await {
                Ok(mut conn) => sqlx::query(&format!("DROP DATABASE `{}`", self.name))
                    .execute(&mut conn)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            },
            // Only what the job wrote: the database was empty when it began.
            Backend::Neo4j => match neo4j(&self.db, &self.admin_url).await {
                Ok(graph) => graph
                    .run(neo4rs::query(NEO4J_CLEAR))
                    .await
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e),
            },
            Backend::Sqlite | Backend::Blob => Ok(()),
        };
        match dropped {
            Ok(()) => eprintln!("==> dropped database {}", self.name),
            // Not fatal: the job's verdict stands, and the name says what to
            // clean up by hand.
            Err(e) => eprintln!("==> could not drop database {}: {e}", self.name),
        }
    }
}

/// Every node the server writes; the same match as its `debug.reset`.
const NEO4J_CLEAR: &str = "MATCH (n) WHERE n:Promise OR n:Schedule DETACH DELETE n";

async fn neo4j(db: &DbArgs, uri: &str) -> Result<neo4rs::Graph> {
    let graph = neo4rs::Graph::new(uri, db.neo4j_user.as_str(), db.neo4j_password.as_str())
        .await
        .map_err(|e| format!("neo4j {uri}: {e}"))?;
    Ok(graph)
}

async fn neo4j_resonate_nodes(graph: &neo4rs::Graph) -> Result<i64> {
    let mut rows = graph
        .execute(neo4rs::query(
            "MATCH (n) WHERE n:Promise OR n:Schedule RETURN count(n) AS n",
        ))
        .await
        .map_err(|e| format!("neo4j: {e}"))?;
    let row = rows
        .next()
        .await
        .map_err(|e| format!("neo4j: {e}"))?
        .ok_or("neo4j: count returned no row")?;
    row.get::<i64>("n").map_err(|e| format!("neo4j: {e}"))
}

/// The admin URL for a backend, or the reason there is none.
fn admin_url(db: &DbArgs, backend: Backend) -> Result<Option<String>> {
    Ok(match backend {
        Backend::Postgres => Some(
            db.postgres_admin_url
                .clone()
                .ok_or("postgres needs --postgres-admin-url or XTASK_POSTGRES_ADMIN_URL")?,
        ),
        Backend::Mysql => Some(
            db.mysql_admin_url
                .clone()
                .ok_or("mysql needs --mysql-admin-url or XTASK_MYSQL_ADMIN_URL")?,
        ),
        Backend::Neo4j => Some(
            db.neo4j_uri
                .clone()
                .ok_or("neo4j needs --neo4j-uri or XTASK_NEO4J_URI")?,
        ),
        Backend::Sqlite | Backend::Blob => None,
    })
}

/// Run `job` against a fresh database for `backend`, then drop it.
///
/// Backends without a server get `None` and are vanilla by construction.
async fn with_fresh_db<F>(backend: Backend, db: &DbArgs, purpose: &str, job: F) -> Result<()>
where
    F: FnOnce(Option<&str>) -> Result<()>,
{
    match admin_url(db, backend)? {
        Some(admin) => {
            let fresh = FreshDb::create(backend, db, &admin, purpose).await?;
            let result = job(Some(&fresh.url));
            fresh.finish().await;
            result
        }
        None => job(None),
    }
}

fn url_var(backend: Backend) -> Option<&'static str> {
    match backend {
        Backend::Postgres => Some("TEST_POSTGRES_URL"),
        Backend::Mysql => Some("TEST_MYSQL_URL"),
        Backend::Neo4j => Some("TEST_NEO4J_URI"),
        Backend::Sqlite | Backend::Blob => None,
    }
}

/// What a backend needs besides its URL.
fn with_credentials<'c>(cmd: &'c mut Command, backend: Backend, db: &DbArgs) -> &'c mut Command {
    if backend == Backend::Neo4j {
        cmd.env("TEST_NEO4J_USER", &db.neo4j_user)
            .env("TEST_NEO4J_PASSWORD", &db.neo4j_password);
    }
    cmd
}

// ---------------------------------------------------------------------------
// differential
// ---------------------------------------------------------------------------

/// One run of both differentials per seed, each on databases of its own.
///
/// A seed is a trajectory: the generator is deterministic, so one seed walks
/// one path through the state space and stops at its own coverage plateau.
/// One is what CI walks; several is how a change is convinced, and every one
/// of them gets vanilla databases.
async fn differential(backend: Backend, seeds: &[u64], db: &DbArgs) -> Result<()> {
    let trajectories: Vec<Option<u64>> = if seeds.is_empty() {
        vec![None]
    } else {
        seeds.iter().copied().map(Some).collect()
    };
    for seed in trajectories {
        if let Some(seed) = seed {
            eprintln!("\n=== seed {seed} ===");
        }
        one_differential(backend, seed, db).await?;
    }
    Ok(())
}

/// `TEST_SEED` for a named trajectory, nothing for the default one.
fn with_seed(cmd: &mut Command, seed: Option<u64>) -> &mut Command {
    if let Some(seed) = seed {
        cmd.env("TEST_SEED", seed.to_string());
    }
    cmd
}

async fn one_differential(backend: Backend, seed: Option<u64>, db: &DbArgs) -> Result<()> {
    if backend == Backend::Blob {
        // The engine port is the SQL family's; blob has its own differential,
        // against its in-crate model, and is in every port differential —
        // the one below is the oracle, SQLite and blob.
        run(
            "cargo test --release -p resonate-server-blob --test differential",
            with_seed(
                cargo().args([
                    "test",
                    "--release",
                    "-p",
                    "resonate-server-blob",
                    "--test",
                    "differential",
                    "--",
                    "--nocapture",
                ]),
                seed,
            ),
        )?;
        return run(
            "port differential",
            with_seed(
                cargo().args(["test", "--release", "--test", "port", "--", "--nocapture"]),
                seed,
            ),
        );
    }
    // The engine differential, on its own database.
    with_fresh_db(backend, db, "engine", |url| {
        let mut cmd = cargo();
        cmd.args([
            "test",
            "--release",
            "--test",
            "differential",
            "--all-features",
            "--",
            "--nocapture",
        ]);
        if let (Some(var), Some(url)) = (url_var(backend), url) {
            cmd.env(var, url);
        }
        with_credentials(&mut cmd, backend, db);
        run("engine differential", with_seed(&mut cmd, seed))
    })
    .await?;
    if backend == Backend::Neo4j {
        // Neo4j is not behind the ports yet, so its port differential is the
        // default one: the oracle, SQLite and blob, with nothing to clear.
        return run(
            "port differential",
            with_seed(
                cargo().args(["test", "--release", "--test", "port", "--", "--nocapture"]),
                seed,
            ),
        );
    }
    // The port differential, on another.
    with_fresh_db(backend, db, "port", |url| {
        let mut cmd = cargo();
        cmd.args(["test", "--release", "--test", "port", "--", "--nocapture"]);
        if let (Some(var), Some(url)) = (url_var(backend), url) {
            cmd.env(var, url);
        }
        run("port differential", with_seed(&mut cmd, seed))
    })
    .await
}

// ---------------------------------------------------------------------------
// porcupine
// ---------------------------------------------------------------------------

/// A server process that dies with this handle.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> Result<u16> {
    let l = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("free port: {e}"))?;
    Ok(l.local_addr().map_err(|e| e.to_string())?.port())
}

/// `GET /ready` by hand: one request, no client library.
fn ready(port: u16) -> bool {
    let Ok(mut s) = TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().expect("addr"),
        Duration::from_millis(500),
    ) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    if s.write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf);
    buf.starts_with(b"HTTP/1.1 200")
}

fn spec_dir(porc: &PorcArgs) -> Result<PathBuf> {
    let candidates: Vec<PathBuf> = porc
        .spec_dir
        .clone()
        .into_iter()
        .chain([
            core().join("spec/valid/porc"),
            core().join("../../../spec/valid/porc"),
        ])
        .collect();
    candidates
        .into_iter()
        .find(|p| p.join("cmd/conccheck").is_dir())
        .ok_or_else(|| {
            "the specification's valid/porc directory was not found: pass --spec-dir or \
             set XTASK_SPEC_DIR"
                .to_string()
        })
}

fn scratch(purpose: &str) -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!(
        "resonate-xtask-{purpose}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("scratch dir: {e}"))?;
    Ok(dir)
}

async fn porcupine(backend: Backend, db: &DbArgs, porc: &PorcArgs) -> Result<()> {
    let spec = spec_dir(porc)?;
    if Command::new("go").arg("version").output().is_err() {
        return Err("the linearizability check needs `go` on PATH".into());
    }
    run(
        "cargo build --release --bin resonate --example conctrace",
        cargo().args([
            "build",
            "--release",
            "--bin",
            "resonate",
            "--example",
            "conctrace",
        ]),
    )?;
    let porc = porc.clone();
    with_fresh_db(backend, db, "porcupine", move |url| {
        let dir = scratch("porcupine")?;
        let port = free_port()?;
        let mut server = Command::new(core().join("target/release/resonate"));
        server
            .arg("serve")
            .current_dir(&dir)
            .env("RESONATE_DEBUG", "true")
            .env(
                "RESONATE_GATEWAYS__GATEWAY_HTTP__BIND",
                format!("127.0.0.1:{port}"),
            )
            // Metrics on a free port too: its default, 9090, is whatever
            // else on this machine is a resonate server.
            .env(
                "RESONATE_GATEWAYS__GATEWAY_METRICS__BIND",
                format!("127.0.0.1:{}", free_port()?),
            )
            .stdout(Stdio::from(
                std::fs::File::create(dir.join("server.log")).map_err(|e| e.to_string())?,
            ))
            .stderr(Stdio::from(
                std::fs::File::create(dir.join("server.err")).map_err(|e| e.to_string())?,
            ));
        match backend {
            Backend::Postgres => {
                server
                    .env("RESONATE_SERVERS__ACTIVE", "server_postgres")
                    .env(
                        "RESONATE_SERVERS__SERVER_POSTGRES__URL",
                        url.expect("fresh"),
                    );
            }
            Backend::Mysql => {
                server
                    .env("RESONATE_SERVERS__ACTIVE", "server_mysql")
                    .env("RESONATE_SERVERS__SERVER_MYSQL__URL", url.expect("fresh"));
            }
            Backend::Neo4j => {
                server
                    .env("RESONATE_SERVERS__ACTIVE", "server_neo4j")
                    .env("RESONATE_SERVERS__SERVER_NEO4J__URI", url.expect("fresh"))
                    .env("RESONATE_SERVERS__SERVER_NEO4J__USER", &db.neo4j_user)
                    .env(
                        "RESONATE_SERVERS__SERVER_NEO4J__PASSWORD",
                        &db.neo4j_password,
                    );
            }
            Backend::Blob => {
                // No bucket: the in-process object store, fresh with the
                // process, with real conditional-write semantics.
                server.env("RESONATE_SERVERS__ACTIVE", "server_blob");
            }
            Backend::Sqlite => {
                server.env("RESONATE_SERVERS__ACTIVE", "server_sqlite").env(
                    "RESONATE_SERVERS__SERVER_SQLITE__PATH",
                    dir.join("resonate.db"),
                );
            }
        }
        eprintln!("\n==> server on 127.0.0.1:{port} ({dir:?})");
        let _server = Server(server.spawn().map_err(|e| format!("server: {e}"))?);
        let started = Instant::now();
        while !ready(port) {
            if started.elapsed() > Duration::from_secs(40) {
                return Err(format!(
                    "server did not become ready; see {}",
                    dir.join("server.err").display()
                ));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        // The recorder exits non-zero on a history with no successes or no
        // overlap, so a server that refuses everything, or a run that failed
        // to be concurrent, fails here rather than linearizing vacuously.
        run(
            "conctrace",
            Command::new(core().join("target/release/examples/conctrace"))
                .current_dir(&dir)
                .args([
                    "--url",
                    &format!("http://127.0.0.1:{port}/"),
                    "--out",
                    "trace",
                    "--clients",
                    &porc.clients.to_string(),
                    "--ops",
                    &porc.ops.to_string(),
                ]),
        )?;
        let history = std::fs::File::open(dir.join("trace.history"))
            .map_err(|e| format!("trace.history: {e}"))?;
        run(
            "go run ./cmd/conccheck -partition=false",
            Command::new("go")
                .current_dir(&spec)
                .args(["run", "./cmd/conccheck", "-partition=false"])
                .stdin(Stdio::from(history)),
        )
    })
    .await
}

// ---------------------------------------------------------------------------
// all
// ---------------------------------------------------------------------------

async fn all(db: &DbArgs, porc: &PorcArgs) -> Result<()> {
    check()?;
    let mut servers = vec![Backend::Sqlite];
    if db.postgres_admin_url.is_some() {
        servers.push(Backend::Postgres);
    } else {
        eprintln!("==> no postgres admin url: postgres skipped");
    }
    if db.mysql_admin_url.is_some() {
        servers.push(Backend::Mysql);
    } else {
        eprintln!("==> no mysql admin url: mysql skipped");
    }
    if db.neo4j_uri.is_some() {
        servers.push(Backend::Neo4j);
    } else {
        eprintln!("==> no neo4j uri: neo4j skipped");
    }
    for b in servers.iter().copied().chain([Backend::Blob]) {
        differential(b, &[], db).await?;
    }
    for b in servers.iter().copied().chain([Backend::Blob]) {
        porcupine(b, db, porc).await?;
    }
    Ok(())
}
