use thiserror::Error;

/// Top-level error type for the Resonate SDK.
#[derive(Error, Debug)]
pub enum Error {
    #[error("function not found: {0}")]
    FunctionNotFound(String),

    #[error("function '{0}' is already registered")]
    AlreadyRegistered(String),

    #[error("server error (code={code}): {message}")]
    ServerError { code: u16, message: String },

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error("decoding error: {0}")]
    DecodingError(String),

    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("http error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("base64 decode error: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("execution suspended")]
    Suspended,

    #[error("promise already settled")]
    AlreadySettled,

    #[error("task join error: {0}")]
    JoinError(String),

    #[error("application error: {message}")]
    Application { message: String },

    #[error("timeout")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;
