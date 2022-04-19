use std::sync::PoisonError;

use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0} is not a valid duration value")]
    DurationError(String),

    #[error("Anchor group {0} not found")]
    AnchorGroupNotFound(String),

    #[error("Feature {0} not found")]
    FeatureNotFound(String),

    #[error("Anchor {0} has no type")]
    MissingFeatureType(String),

    #[error("Anchor {0} has no transformation")]
    MissingTransformation(String),

    #[error("{2} key alias are provided while Anchor {0} has {1} keys")]
    MismatchKeyAlias(String, usize, usize),

    #[error("Key alias {1} not found in derived feature {0}, existing keys are: {2}")]
    KeyAliasNotFound(String, String, String),

    #[error("Source {0} has no HDFS url")]
    MissingHdfsUrl(String),

    #[error("Source {0} has no JDBC url")]
    MissingJdbcUrl(String),

    #[error("Source {0} has neither dbtable nor query set")]
    SourceNoQuery(String),

    #[error("{0}")]
    SyncError(String),
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(e: PoisonError<Guard>) -> Self {
        Error::SyncError(e.to_string())
    }
}