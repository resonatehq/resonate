//! Schema migration, the same shape for every backend.
//!
//! Each engine owns a directory under `migrations/`, because the three schemas
//! are genuinely different SQL — Postgres keeps callbacks as arrays on the
//! promise row, the other two keep them in their own table, and the dialects
//! diverge besides. What is NOT different is the mechanism: one embedded
//! [`sqlx::migrate::Migrator`] per engine, applied in version order, recorded
//! in `_sqlx_migrations`, and checksummed so an edited migration that was
//! already applied is an error rather than a silent divergence.
//!
//! Postgres and MySQL run theirs through sqlx directly. SQLite runs on
//! rusqlite, which sqlx cannot drive — and pointing a second sqlx pool at the
//! same path would be a different database entirely for `:memory:`, which is
//! what the differential opens. So [`run_rusqlite`] applies the very same
//! `Migrator` through rusqlite, writing the same bookkeeping table. The
//! executor differs because it must; nothing else does.
//!
//! Every path returns an error rather than continuing: a server whose schema
//! did not migrate must not start.

use rusqlite::{params, Connection};

/// Apply a [`sqlx::migrate::Migrator`] through a rusqlite connection.
///
/// Mirrors what sqlx's own migrator does: create the bookkeeping table, read
/// which versions are applied, verify the checksum of each one that is, and
/// apply the rest in order inside a transaction.
pub fn run_rusqlite(
    conn: &mut Connection,
    migrator: &sqlx::migrate::Migrator,
) -> Result<(), MigrateError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
           version        BIGINT PRIMARY KEY,
           description    TEXT    NOT NULL,
           installed_on   TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
           success        BOOLEAN NOT NULL,
           checksum       BLOB    NOT NULL,
           execution_time BIGINT  NOT NULL
         );",
    )
    .map_err(|e| MigrateError(format!("creating _sqlx_migrations: {e}")))?;

    for migration in migrator.iter() {
        let version = migration.version;
        let applied: Option<Vec<u8>> = conn
            .query_row(
                "SELECT checksum FROM _sqlx_migrations WHERE version = ?1",
                params![version],
                |r| r.get(0),
            )
            .ok();

        if let Some(checksum) = applied {
            // Already applied. A different checksum means the file changed
            // under a database that already has the old version — which no
            // amount of re-running can reconcile, so it is fatal.
            if checksum != *migration.checksum {
                return Err(MigrateError(format!(
                    "migration {version} ({}) was applied with a different checksum — \
                     the file has changed since it ran",
                    migration.description
                )));
            }
            continue;
        }

        let started = std::time::Instant::now();
        let tx = conn
            .transaction()
            .map_err(|e| MigrateError(format!("migration {version}: {e}")))?;
        tx.execute_batch(&migration.sql)
            .map_err(|e| MigrateError(format!("migration {version} failed: {e}")))?;
        tx.execute(
            "INSERT INTO _sqlx_migrations
               (version, description, success, checksum, execution_time)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                version,
                migration.description.as_ref(),
                true,
                migration.checksum.as_ref(),
                started.elapsed().as_nanos() as i64,
            ],
        )
        .map_err(|e| MigrateError(format!("recording migration {version}: {e}")))?;
        tx.commit()
            .map_err(|e| MigrateError(format!("committing migration {version}: {e}")))?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct MigrateError(pub String);

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MigrateError {}

impl From<MigrateError> for rusqlite::Error {
    fn from(e: MigrateError) -> Self {
        // SqliteFailure carries the message through verbatim. The named
        // variants all prefix it with something untrue — a migration that did
        // not apply is not an "invalid parameter name", and this line is what
        // the operator reads when the server refuses to start.
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some(e.0),
        )
    }
}
