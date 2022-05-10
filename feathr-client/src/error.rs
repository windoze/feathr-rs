use std::sync::PoisonError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0} is not a valid duration value")]
    DurationError(String),

    #[error("Anchor group {0} not found")]
    AnchorGroupNotFound(String),

    #[error("Feature {0} not found")]
    FeatureNotFound(String),

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

    #[error("For anchors of non-INPUT_CONTEXT source, key of feature {0} should be explicitly specified and not left blank")]
    DummyKeyUsedWithInputContext(String),

    #[error("Anchor feature {0} has different key alias than other features in the anchor group {1}")]
    InvalidKeyAlias(String, String),

    #[error("key alias {1} in derived feature {0} must come from its input features key alias list {2}")]
    InvalidDerivedKeyAlias(String, String, String),

    #[error("{0}")]
    SyncError(String),

    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    #[error(transparent)]
    LivyClientError(#[from] livy_client::LivyClientError),

    #[error(transparent)]
    ClientCredentialError(#[from] azure_identity::client_credentials_flow::ClientCredentialError),

    #[error(transparent)]
    AzureStorageError(#[from] azure_storage::Error),

    #[error("Invalid Url {0}")]
    InvalidUrl(String),

    #[error("Timeout")]
    Timeout,

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    InvalidConfig(String),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    YamlError(#[from] serde_yaml::Error),
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(e: PoisonError<Guard>) -> Self {
        Error::SyncError(e.to_string())
    }
}