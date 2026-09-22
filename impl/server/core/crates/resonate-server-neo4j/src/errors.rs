//! What this server reports when its storage will not answer.
//!
//! Its own, so nothing here depends on the SQL family. The driver errors are
//! mapped by hand rather than through `From`, because there is one driver.

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    /// A backend error, formatted, without exposing the driver's own type.
    Backend(String),
    /// A conditional write lost its round and retries are exhausted. Nothing
    /// was committed; the caller should answer 503 rather than 500.
    Serialization,
    /// A field violates a storage-level constraint. The caller should answer
    /// 400.
    InvalidInput(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Backend(m) => write!(f, "{m}"),
            StorageError::Serialization => write!(f, "write conflict, retries exhausted"),
            StorageError::InvalidInput(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for StorageError {}
