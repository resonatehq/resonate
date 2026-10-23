//! A lock wait timeout is a retryable failure, not an internal error.
//!
//! sqlx's `DatabaseError::code()` is the SQLSTATE. MySQL reports a deadlock
//! (1213) as `40001` but a lock wait timeout (1205) as the catch-all `HY000`,
//! so the error has to be classified by its server error number. This provokes
//! a real 1205 and checks that it maps to `StorageError::Serialization` (503,
//! and one retry inside the engine) rather than `Backend` (500).
//!
//! Needs a live server, like the differential:
//!
//!   TEST_MYSQL_URL=mysql://resonate:resonate@localhost:3306/resonate \
//!     cargo test -p resonate-server-mysql --test retryable
//!
//! Without `TEST_MYSQL_URL` it is skipped.

use resonate_sql::{is_retryable, StorageError};
use sqlx::mysql::MySqlDatabaseError;
use sqlx::{Connection, MySqlConnection};

#[tokio::test]
async fn lock_wait_timeout_is_retryable() {
    let Ok(url) = std::env::var("TEST_MYSQL_URL") else {
        eprintln!("TEST_MYSQL_URL not set — skipped");
        return;
    };
    let table = format!("retryable_probe_{}", std::process::id());

    let mut holder = MySqlConnection::connect(&url).await.expect("connect");
    let mut waiter = MySqlConnection::connect(&url).await.expect("connect");
    sqlx::query(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .execute(&mut holder)
        .await
        .expect("create table");
    sqlx::query(&format!("INSERT INTO {table} VALUES (1)"))
        .execute(&mut holder)
        .await
        .expect("insert");
    sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&mut waiter)
        .await
        .expect("set lock wait timeout");

    let lock = format!("SELECT id FROM {table} WHERE id = 1 FOR UPDATE");
    let mut held = holder.begin().await.expect("begin");
    sqlx::query(&lock)
        .fetch_one(&mut *held)
        .await
        .expect("take the row lock");

    let mut waiting = waiter.begin().await.expect("begin");
    let err = sqlx::query(&lock)
        .fetch_one(&mut *waiting)
        .await
        .expect_err("the row is locked, so this must time out");
    // Roll both back explicitly: dropping a `Transaction` only queues its
    // rollback, and an open transaction would hold the metadata lock that
    // `DROP TABLE` below waits for.
    waiting.rollback().await.expect("rollback");
    held.rollback().await.expect("rollback");

    let db_err = err.as_database_error().expect("a database error");
    let number = db_err
        .try_downcast_ref::<MySqlDatabaseError>()
        .expect("a MySQL error")
        .number();
    let sqlstate = db_err.code().map(|c| c.into_owned());

    sqlx::query(&format!("DROP TABLE {table}"))
        .execute(&mut holder)
        .await
        .expect("drop table");

    assert_eq!(number, 1205, "expected a lock wait timeout");
    // The root cause: the SQLSTATE is not the error number.
    assert_ne!(sqlstate.as_deref(), Some("1205"));
    assert!(
        is_retryable(&err),
        "1205 (SQLSTATE {sqlstate:?}) must be retryable"
    );
    assert!(matches!(
        StorageError::from(err),
        StorageError::Serialization
    ));
}
