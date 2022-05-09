mod azure_synapse;
mod databricks;
mod env_utils;

use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Url;
use tokio::io::AsyncWriteExt;

pub use azure_synapse::AzureSynapseClient;
pub use databricks::DatabricksClient;
pub use env_utils::*;

use self::env_utils::VarSource;

pub(crate) const OUTPUT_PATH_TAG: &str = "output_path";
pub(crate) const FEATHR_JOB_JAR_PATH: &str =
    "wasbs://public@azurefeathrstorage.blob.core.windows.net/feathr-assembly-LATEST.jar";
pub(crate) const JOIN_JOB_MAIN_CLASS_NAME: &str = "com.linkedin.feathr.offline.job.FeatureJoinJob";
pub(crate) const GEN_JOB_MAIN_CLASS_NAME: &str = "com.linkedin.feathr.offline.job.FeatureGenJob";

#[derive(Clone, Debug, Default)]
pub struct SubmitJobRequest {
    pub name: String,
    pub input: String,
    pub output: String,
    pub main_jar_path: String,
    pub main_class_name: String,
    pub feature_config: String,
    pub join_job_config: String,
    pub gen_job_config: String,
    pub python_files: Vec<String>,
    pub reference_files: Vec<String>,
    pub job_tags: HashMap<String, String>,
    pub configuration: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JobId(pub u64);

#[async_trait]
pub trait JobClient
where
    Self: Sized,
{
    /**
     * The job status
     */
    type JobStatus: Clone + Send + Sync;

    /**
     * Create instance from a variable source
     */
    fn from_var_source<T>(var_source: &T) -> Result<Self, crate::Error>
    where
        T: VarSource;
    /**
     * Create file on the remote side and returns Spark compatible URL of the file
     */
    async fn write_remote_file(&self, path: &str, content: &[u8]) -> Result<String, crate::Error>;
    /**
     * Read file content from a Spark compatible URL
     */
    async fn read_remote_file(&self, path: &str) -> Result<Bytes, crate::Error>;
    /**
     * Submit Spark job, upload files if necessary
     */
    async fn submit_job<T>(
        &self,
        var_source: &T,
        request: SubmitJobRequest,
    ) -> Result<JobId, crate::Error>
    where
        T: VarSource + Send + Sync;
    /**
     * Check if the status indicates the job has ended
     */
    fn is_ended_status(&self, status: Self::JobStatus) -> bool;
    /**
     * Get job status
     */
    async fn get_job_status(&self, job_id: JobId) -> Result<Self::JobStatus, crate::Error>;
    /**
     * Get job driver log
     */
    async fn get_job_log(&self, job_id: JobId) -> Result<String, crate::Error>;
    /**
     * Get job output URL in Spark compatible format
     */
    async fn get_job_output_url(&self, job_id: JobId) -> Result<Option<String>, crate::Error>;
    /**
     * Upload file if it's local, or move the file to the workspace if it's at somewhere else
     */
    async fn upload_or_get_url(&self, path: &str) -> Result<String, crate::Error>;

    /**
     * Same as `upload_or_get_url`, but for multiple files
     */
    async fn multi_upload_or_get_url(&self, paths: &[String]) -> Result<Vec<String>, crate::Error> {
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
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<Self::JobStatus, crate::Error> {
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
            // Check every few seconds
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Err(crate::Error::Timeout)
    }
    /**
     * Download a file from remote side to local cache dir
     */
    async fn download_file(&self, url: &str, local_cache_dir: &str) -> Result<(), crate::Error> {
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
    fn get_file_name(&self, path_or_url: &str) -> Result<String, crate::Error> {
        Ok(
            if !path_or_url.contains("://") || path_or_url.starts_with("dbfs:") {
                // It's a local path
                let path = std::path::Path::new(path_or_url);
                path.file_name()
                    .to_owned()
                    .ok_or_else(|| crate::Error::InvalidUrl(path_or_url.to_string()))?
                    .to_string_lossy()
                    .to_string()
            } else {
                let url = Url::parse(path_or_url)
                    .map_err(|_| crate::Error::InvalidUrl(path_or_url.to_string()))?;
                let path: Vec<String> = url.path().split("/").map(|s| s.to_string()).collect();
                path.into_iter()
                    .last()
                    .ok_or_else(|| crate::Error::InvalidUrl(path_or_url.to_string()))?
                    .to_string()
            },
        )
    }

    /**
     * Generate arguments for the Spark job
     */
    async fn get_arguments<T>(
        &self,
        var_source: &T,
        request: &SubmitJobRequest,
    ) -> Result<Vec<String>, crate::Error>
    where
        T: VarSource + Send + Sync,
    {
        let mut ret: Vec<String> = vec![
            "--num-parts".to_string(),
            self.get_output_num_parts(var_source)?,
            // "--s3-config".to_string(),
            // self.get_s3_config(var_source)?,
            "--adls-config".to_string(),
            self.get_adls_config(var_source)?,
            "--blob-config".to_string(),
            self.get_blob_config(var_source)?,
            // "--sql-config".to_string(),
            // self.get_sql_config(var_source)?,
            // "--snowflake-config".to_string(),
            // self.get_snowflake_config(var_source)?,
        ];

        let feature_config_url = self.get_remote_url("feature.conf");
        let feature_config_url = self
            .write_remote_file(&feature_config_url, &request.feature_config.as_bytes())
            .await?;
        ret.extend(vec!["--feature-config".to_string(), feature_config_url].into_iter());

        if request.gen_job_config.is_empty() {
            let job_config_url = self.get_remote_url("feature_join_job.conf");
            let job_config_url = self
                .write_remote_file(&job_config_url, &request.join_job_config.as_bytes())
                .await?;
            ret.extend(
                vec![
                    "--input".to_string(),
                    request.input.clone(),
                    "--output".to_string(),
                    request.output.clone(),
                    "--join-config".to_string(),
                    job_config_url,
                ]
                .into_iter(),
            );
        } else {
            let job_config_url = self.get_remote_url("feature_gen_job.conf");
            let job_config_url = self
                .write_remote_file(&job_config_url, &request.gen_job_config.as_bytes())
                .await?;
            ret.extend(vec!["--generation-config".to_string(), job_config_url].into_iter());
        }
        Ok(ret)
    }

    fn get_output_num_parts<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(var_source.get_environment_variable(&["spark_config", "spark_result_output_parts"])?)
    }

    fn get_s3_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(var_source.get_environment_variable(&["offline_store", "s3", "s3_endpoint"])?)
    }

    fn get_adls_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(format!(
            r#"
            ADLS_ACCOUNT: {}
            ADLS_KEY: "{}"
            "#,
            var_source.get_environment_variable(&["ADLS_ACCOUNT"])?,
            var_source.get_environment_variable(&["ADLS_KEY"])?
        ))
    }

    fn get_blob_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(format!(
            r#"
            BLOB_ACCOUNT: {}
            BLOB_KEY: "{}"
            "#,
            var_source.get_environment_variable(&["BLOB_ACCOUNT"])?,
            var_source.get_environment_variable(&["BLOB_KEY"])?
        ))
    }

    fn get_sql_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(format!(
            r#"
            JDBC_TABLE: {}
            JDBC_USER: {}
            JDBC_PASSWORD: {}
            JDBC_DRIVER: {}
            JDBC_AUTH_FLAG: {}
            JDBC_TOKEN: {}
            "#,
            var_source.get_environment_variable(&["JDBC_TABLE"])?,
            var_source.get_environment_variable(&["JDBC_USER"])?,
            var_source.get_environment_variable(&["JDBC_PASSWORD"])?,
            var_source.get_environment_variable(&["JDBC_DRIVER"])?,
            var_source.get_environment_variable(&["JDBC_AUTH_FLAG"])?,
            var_source.get_environment_variable(&["JDBC_TOKEN"])?,
        ))
    }

    fn get_snowflake_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(format!(
            r#"
            JDBC_SF_URL: {}
            JDBC_SF_USER: {}
            JDBC_SF_ROLE: {}
            JDBC_SF_PASSWORD: {}
            "#,
            var_source.get_environment_variable(&["JDBC_SF_URL"])?,
            var_source.get_environment_variable(&["JDBC_SF_USER"])?,
            var_source.get_environment_variable(&["JDBC_SF_ROLE"])?,
            var_source.get_environment_variable(&["JDBC_SF_PASSWORD"])?,
        ))
    }

    fn get_kafka_config<T>(&self, var_source: &T) -> Result<String, crate::Error>
    where
        T: VarSource + Send,
    {
        Ok(format!(
            r#"
            KAFKA_SASL_JAAS_CONFIG: {}
            "#,
            var_source.get_environment_variable(&["KAFKA_SASL_JAAS_CONFIG"])?,
        ))
    }
}

pub struct SubmitJobRequestBuilder {
    job_name: String,
    input_path: String,
    main_jar_path: Option<String>,
    main_class_name: Option<String>,
    output_path: Option<String>,
    is_join_job: bool,
    python_files: Vec<String>,
    reference_files: Vec<String>,
    configuration: HashMap<String, String>,
    feature_config: String,
    feature_join_config: String,
    feature_gen_config: String,
}

impl SubmitJobRequestBuilder {
    pub(crate) fn new_join(
        job_name: String,
        input_path: String,
        feature_config: String,
        job_config: String, // feature_join_config or feature_gen_config
    ) -> Self {
        Self {
            job_name,
            input_path,
            main_jar_path: None,
            main_class_name: None,
            output_path: None,
            is_join_job: true,
            python_files: Default::default(),
            reference_files: Default::default(),
            configuration: Default::default(),
            feature_config,
            feature_join_config: job_config,
            feature_gen_config: Default::default(),
        }
    }

    pub(crate) fn new_gen(
        job_name: String,
        input_path: String,
        feature_config: String,
        job_config: String, // feature_join_config or feature_gen_config
    ) -> Self {
        Self {
            job_name,
            input_path,
            main_jar_path: None,
            main_class_name: None,
            output_path: None,
            is_join_job: false,
            python_files: Default::default(),
            reference_files: Default::default(),
            configuration: Default::default(),
            feature_config,
            feature_join_config: Default::default(),
            feature_gen_config: job_config,
        }
    }

    pub fn output_path(&mut self, output_path: &str) -> &mut Self {
        self.output_path = Some(output_path.to_string());
        self
    }

    pub fn build(&self) -> SubmitJobRequest {
        let output = self.output_path.clone().unwrap(); // TODO: Validation
        let job_tags: HashMap<String, String> = [(OUTPUT_PATH_TAG.to_string(), output.clone())]
            .into_iter()
            .collect();
        SubmitJobRequest {
            name: self.job_name.to_owned(),
            input: self.input_path.to_owned(),
            output,
            main_jar_path: self
                .main_jar_path
                .to_owned()
                .unwrap_or_else(|| FEATHR_JOB_JAR_PATH.to_string()),
            main_class_name: self.main_class_name.to_owned().unwrap_or_else(|| {
                if self.is_join_job {
                    JOIN_JOB_MAIN_CLASS_NAME
                } else {
                    GEN_JOB_MAIN_CLASS_NAME
                }
                .to_string()
            }),
            feature_config: self.feature_config.to_owned(),
            join_job_config: self.feature_join_config.to_owned(),
            gen_job_config: self.feature_gen_config.to_owned(),
            python_files: self.python_files.to_owned(),
            reference_files: self.reference_files.to_owned(),
            job_tags,
            configuration: self.configuration.to_owned(),
        }
    }
}
