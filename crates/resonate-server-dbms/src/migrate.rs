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

#[cfg(feature = "sqlite")]
use rusqlite::{params, Connection};

/// Apply a [`sqlx::migrate::Migrator`] through a rusqlite connection.
///
/// Mirrors what sqlx's own migrator does: create the bookkeeping table, read
/// which versions are applied, verify the checksum of each one that is, and
/// apply the rest in order inside a transaction.
#[cfg(feature = "sqlite")]
pub fn run_rusqlite(
    conn: &mut Connection,
    migrator: &sqlx::migrate::Migrator,
    migrate: bool,
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

    let applied: usize =
        conn.query_row("SELECT COUNT(*) FROM _sqlx_migrations", [], |r| {
            r.get::<_, i64>(0)
        })
        .map_err(|e| MigrateError(format!("reading _sqlx_migrations: {e}")))? as usize;
    may_apply(applied, migrator.iter().count(), migrate)?;

    for migration in migrator.iter() {
        let version = migration.version;
        // Only "no such row" means not-yet-applied. Swallowing every error
        // here — a locked or corrupt table included — would read as "not
        // applied" and re-run the migration, defeating the checksum guard
        // immediately below.
        let previous: Option<Vec<u8>> = match conn.query_row(
            "SELECT checksum FROM _sqlx_migrations WHERE version = ?1",
            params![version],
            |r| r.get(0),
        ) {
            Ok(c) => Some(c),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => {
                return Err(MigrateError(format!(
                    "reading migration {version} from _sqlx_migrations: {e}"
                )))
            }
        };

        if let Some(checksum) = previous {
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

/// Whether this start may write DDL, given what the database already carries.
///
/// An empty database is always created: a server pointed at nothing has
/// nothing to lose. A database that is already current has nothing to do.
/// A database BEHIND the binary is the interesting case, and it is a
/// deployment decision rather than a startup default — applying migrations
/// rewrites data nobody asked to have rewritten, and doing it silently on a
/// restart is how a rollback becomes unavailable. So without the flag the
/// server refuses to start and says what is pending.
pub fn may_apply(applied: usize, total: usize, migrate: bool) -> Result<(), MigrateError> {
    if applied == 0 || applied >= total || migrate {
        return Ok(());
    }
    Err(MigrateError(format!(
        "database is at migration {applied} of {total}; \
         set the backend's `migrate` option to apply the {} pending",
        total - applied
    )))
}

#[derive(Debug)]
pub struct MigrateError(pub String);

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MigrateError {}

#[cfg(feature = "sqlite")]
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

#[cfg(test)]
mod tests {
    use super::may_apply;

    #[test]
    fn an_empty_database_is_always_created() {
        // Nothing to lose and nothing to roll back to.
        assert!(may_apply(0, 1, false).is_ok());
        assert!(may_apply(0, 5, false).is_ok());
    }

    #[test]
    fn a_current_database_needs_no_permission() {
        assert!(may_apply(3, 3, false).is_ok());
    }

    #[test]
    fn a_database_behind_the_binary_needs_the_flag() {
        let err = may_apply(1, 3, false).expect_err("must refuse");
        assert!(err.0.contains("migration 1 of 3"), "{}", err.0);
        assert!(err.0.contains("2 pending"), "{}", err.0);
        assert!(may_apply(1, 3, true).is_ok());
    }

    #[test]
    fn a_database_ahead_of_the_binary_is_not_our_business() {
        // An older binary against a newer schema has nothing to apply; whether
        // it can *run* is the checksum guard's question, not this one.
        assert!(may_apply(5, 3, false).is_ok());
    }
}
