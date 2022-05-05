mod azure_synapse;
mod databricks;

use std::{time::{Duration, SystemTime}, collections::HashMap};

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Url;

pub use azure_synapse::AzureSynapseClient;
pub use databricks::DatabricksClient;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, thiserror::Error)]
#[error("Invalid Url {0}")]
pub struct InvalidUrl(pub String);

#[derive(Clone, Debug, thiserror::Error)]
#[error("Timeout")]
pub struct Timeout;

pub const OUTPUT_PATH_TAG: &str = "output_path";

#[derive(Clone, Debug, Default)]
pub struct SubmitJobRequest {
    pub name: String,
    pub main_jar_path: Option<String>,
    pub main_class_name: Option<String>,
    pub arguments: Vec<String>,
    pub python_files: Vec<String>,
    pub reference_files: Vec<String>,
    pub job_tags: HashMap<String, String>,
    pub configuration: HashMap<String, String>,
}

#[async_trait]
pub trait JobClient {
    /**
     * The error type used by the client
     */
    type Error: std::fmt::Debug + From<InvalidUrl> + From<Timeout> + From<std::io::Error>;
    /**
     * The job id type
     */
    type JobId: Clone + Send + Sync;
    /**
     * The job status
     */
    type JobStatus: Clone + Send + Sync;

    /**
     * Create file on the remote side and returns Spark compatible URL of the file
     */
    async fn write_remote_file(&self, path: &str, content: &[u8]) -> Result<String, Self::Error>;
    /**
     * Read file content from a Spark compatible URL
     */
    async fn read_remote_file(&self, path: &str) -> Result<Bytes, Self::Error>;
    /**
     * Submit Spark job, upload files if necessary
     */
    async fn submit_job(
        &self,
        request: SubmitJobRequest
    ) -> Result<Self::JobId, Self::Error>;
    /**
     * Check if the status indicates the job has ended
     */
    fn is_ended_status(&self, status: Self::JobStatus) -> bool;
    /**
     * Get job status
     */
    async fn get_job_status(&self, job_id: Self::JobId) -> Result<Self::JobStatus, Self::Error>;
    /**
     * Get job driver log
     */
    async fn get_job_log(&self, job_id: Self::JobId) -> Result<String, Self::Error>;
    /**
     * Get job output URL in Spark compatible format
     */
    async fn get_job_output_url(&self, job_id: Self::JobId) -> Result<Option<String>, Self::Error>;
    /**
     * Upload file if it's local, or move the file to the workspace if it's at somewhere else
     */
    async fn upload_or_get_url(&self, path: &str) -> Result<String, Self::Error>;

    /**
     * Same as `upload_or_get_url`, but for multiple files
     */
    async fn multi_upload_or_get_url(&self, paths: &[String]) -> Result<Vec<String>, Self::Error> {
        let mut ret = vec![];
        for path in paths.into_iter() {
            ret.push(self.upload_or_get_url(path).await?);
        }
        Ok(ret)
    }

    /**
     * Construct remote URL for the filename
     */
    fn get_remote_url(&self, filename: &str) -> String;
    /**
     * Wait until the job is ended successfully or not
     */
    async fn wait_for_job(
        &self,
        job_id: Self::JobId,
        timeout: Option<Duration>,
    ) -> Result<Self::JobStatus, Self::Error> {
        let wait_until = timeout.map(|d| SystemTime::now() + d);
        loop {
            let status = self.get_job_status(job_id.clone()).await?;
            if self.is_ended_status(status.clone()) {
                return Ok(status.clone());
            } else {
                if let Some(t) = wait_until {
                    if SystemTime::now() > t {
                        break;
                    }
                }
            }
            // Check every 3 seconds
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Err(Timeout.into())
    }
    /**
     * Download a file from remote side to local cache dir
     */
    async fn download_file(&self, url: &str, local_cache_dir: &str) -> Result<(), Self::Error> {
        let mut bytes = self.read_remote_file(url).await?;
        let dir = std::path::Path::new(local_cache_dir);
        let file_path = dir.join(self.get_file_name(url)?);
        let mut file = tokio::fs::File::create(file_path).await?;
        file.write_all_buf(&mut bytes).await?;
        Ok(())
    }
    /**
     * Get the file name part of the path or url
     */
    fn get_file_name(&self, path_or_url: &str) -> Result<String, Self::Error> {
        Ok(
            if !path_or_url.contains("://") || path_or_url.starts_with("dbfs:") {
                // It's a local path
                let path = std::path::Path::new(path_or_url);
                path.file_name()
                    .to_owned()
                    .ok_or_else(|| InvalidUrl(path_or_url.to_string()))?
                    .to_string_lossy()
                    .to_string()
            } else {
                let url =
                    Url::parse(path_or_url).map_err(|_| InvalidUrl(path_or_url.to_string()))?;
                let path: Vec<String> = url.path().split("/").map(|s| s.to_string()).collect();
                path.into_iter()
                    .last()
                    .ok_or_else(|| InvalidUrl(path_or_url.to_string()))?
                    .to_string()
            },
        )
    }
}
