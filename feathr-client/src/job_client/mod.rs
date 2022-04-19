mod azure_synapse;
mod databricks;

use std::time::Duration;

use async_trait::async_trait;

use crate::Error;

pub use azure_synapse::AzureSynapseClient;
pub use databricks::DatabricksClient;

#[async_trait]
pub trait JobClient {
    type JobId;
    type JobStatus;

    async fn upload_file(&self, path: &str, content: String) -> Result<String, Error>;
    async fn download_file(&self, path: &str, local_cache_dir: &str) -> Result<(), Error>;
    async fn submit_job(&self) -> Result<Self::JobId, Error>;
    async fn wait_job(&self, job_id: Self::JobId, timeout: Option<Duration>) -> Result<(), Error>;
    async fn get_job_status(&self) -> Result<Self::JobStatus, Error>;
    async fn get_job_output_uri(&self, job_id: Self::JobId) -> Result<String, Error>;
}