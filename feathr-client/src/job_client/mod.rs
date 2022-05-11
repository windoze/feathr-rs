mod azure_synapse;
mod databricks;

use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use log::debug;
use reqwest::Url;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::{DateTimeResolution, Error, MaterializationSettingsBuilder, OutputSink, VarSource};

pub use azure_synapse::AzureSynapseClient;
pub use databricks::DatabricksClient;

pub(crate) const OUTPUT_PATH_TAG: &str = "output_path";
pub(crate) const FEATHR_JOB_JAR_PATH: &str =
    "wasbs://public@azurefeathrstorage.blob.core.windows.net/feathr-assembly-LATEST.jar";
pub(crate) const JOIN_JOB_MAIN_CLASS_NAME: &str = "com.linkedin.feathr.offline.job.FeatureJoinJob";
pub(crate) const GEN_JOB_MAIN_CLASS_NAME: &str = "com.linkedin.feathr.offline.job.FeatureGenJob";

#[derive(Clone, Debug, Default)]
pub struct SubmitJobRequest {
    pub name: String,
    pub job_config_file_name: String,
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
    // TODO:
    pub secret_key: Vec<String>,
    pub configuration: HashMap<String, String>,
}

/**
 * Spark Job Id
 */
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JobId(pub u64);

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Starting,
    Running,
    Success,
    Failed,
}

impl JobStatus {
    pub fn is_ended(self) -> bool {
        matches!(self, JobStatus::Success | JobStatus::Failed)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match &self {
                JobStatus::Starting => "Starting",
                JobStatus::Running => "Running",
                JobStatus::Success => "Success",
                JobStatus::Failed => "Failed",
            }
        )
    }
}

/**
 * Spark client trait
 */
#[async_trait]
pub trait JobClient
where
    Self: Sized,
{
    /**
     * Create instance from a variable source
     */
    async fn from_var_source(
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<Self, crate::Error>;

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
    async fn submit_job(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
        request: SubmitJobRequest,
    ) -> Result<JobId, crate::Error>;

    /**
     * Get job status
     */
    async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, crate::Error>;

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
     * Construct remote URL for the filename
     */
    fn get_remote_url(&self, filename: &str) -> String;

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
     * Wait until the job is ended successfully or not
     */
    async fn wait_for_job(
        &self,
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<JobStatus, crate::Error> {
        let wait_until = timeout.map(|d| Instant::now() + d.to_std().unwrap());
        loop {
            let status = self.get_job_status(job_id).await?;
            debug!("Job {}, status: {}", job_id, status);
            if status.is_ended() {
                return Ok(status);
            } else {
                if let Some(t) = wait_until {
                    if Instant::now() > t {
                        break;
                    }
                }
            }
            // Check every few seconds
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
        Ok(if !path_or_url.contains("://") {
            // It's a local path or `dbfs:/path/and/filename`
            let path = std::path::Path::new(path_or_url.trim_start_matches("dbfs:"));
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
        })
    }

    /**
     * Generate arguments for the Spark job
     */
    async fn get_arguments(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
        request: &SubmitJobRequest,
    ) -> Result<Vec<String>, crate::Error> {
        let mut ret: Vec<String> = vec![
            "--s3-config".to_string(),
            self.get_s3_config(var_source.clone()).await?,
            "--adls-config".to_string(),
            self.get_adls_config(var_source.clone()).await?,
            "--blob-config".to_string(),
            self.get_blob_config(var_source.clone()).await?,
            "--sql-config".to_string(),
            self.get_sql_config(var_source.clone()).await?,
            "--snowflake-config".to_string(),
            self.get_snowflake_config(var_source.clone()).await?,
        ];

        let feature_config_url = self.get_remote_url(&format!("{}_features.conf", request.name));
        let feature_config_url = self
            .write_remote_file(&feature_config_url, &request.feature_config.as_bytes())
            .await?;
        ret.extend(vec!["--feature-config".to_string(), feature_config_url].into_iter());

        let job_config_url = self.get_remote_url(&request.job_config_file_name);
        if request.gen_job_config.is_empty() {
            // This is a feature joining job request
            let job_config_url = self
                .write_remote_file(&job_config_url, &request.join_job_config.as_bytes())
                .await?;
            ret.extend(
                vec![
                    "--num-parts".to_string(),
                    self.get_output_num_parts(var_source.clone()).await?,
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
            // This is a feature generation job request
            let job_config_url = self
                .write_remote_file(&job_config_url, &request.gen_job_config.as_bytes())
                .await?;
            ret.extend(
                vec![
                    "--redis-config".to_string(),
                    self.get_redis_config(var_source.clone()).await?,
                    "--generation-config".to_string(),
                    job_config_url,
                ]
                .into_iter(),
            );
        }
        Ok(ret)
    }

    async fn get_output_num_parts(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        Ok(var_source
            .get_environment_variable(&["spark_config", "spark_result_output_parts"])
            .await?)
    }

    async fn get_s3_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            s3_endpoint: String,
            s3_access_key: String,
            s3_secret_key: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            s3_endpoint: var_source
                .get_environment_variable(&["offline_store", "s3", "s3_endpoint"])
                .await?,
            s3_access_key: var_source
                .get_environment_variable(&["S3_ACCESS_KEY"])
                .await
                .ok()
                .unwrap_or_default(),
            s3_secret_key: var_source
                .get_environment_variable(&["S3_SECRET_KEY"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }

    async fn get_adls_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            adls_account: String,
            adls_key: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            adls_account: var_source
                .get_environment_variable(&["ADLS_ACCOUNT"])
                .await
                .ok()
                .unwrap_or_default(),
            adls_key: var_source
                .get_environment_variable(&["ADLS_KEY"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }

    async fn get_blob_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            blob_account: String,
            blob_key: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            blob_account: var_source
                .get_environment_variable(&["BLOB_ACCOUNT"])
                .await
                .ok()
                .unwrap_or_default(),
            blob_key: var_source
                .get_environment_variable(&["BLOB_KEY"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }

    async fn get_sql_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            jdbc_table: String,
            jdbc_user: String,
            jdbc_password: String,
            jdbc_driver: String,
            jdbc_auth_flag: String,
            jdbc_token: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            jdbc_table: var_source
                .get_environment_variable(&["JDBC_TABLE"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_user: var_source
                .get_environment_variable(&["JDBC_USER"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_password: var_source
                .get_environment_variable(&["JDBC_PASSWORD"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_driver: var_source
                .get_environment_variable(&["JDBC_DRIVER"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_auth_flag: var_source
                .get_environment_variable(&["JDBC_AUTH_FLAG"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_token: var_source
                .get_environment_variable(&["JDBC_TOKEN"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }

    async fn get_snowflake_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            jdbc_sf_url: String,
            jdbc_sf_user: String,
            jdbc_sf_role: String,
            jdbc_sf_password: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            jdbc_sf_url: var_source
                .get_environment_variable(&["JDBC_SF_URL"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_sf_user: var_source
                .get_environment_variable(&["JDBC_SF_USER"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_sf_role: var_source
                .get_environment_variable(&["JDBC_SF_ROLE"])
                .await
                .ok()
                .unwrap_or_default(),
            jdbc_sf_password: var_source
                .get_environment_variable(&["JDBC_SF_PASSWORD"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }

    async fn get_redis_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            redis_password: String,
            redis_host: String,
            redis_port: u16,
            redis_ssl_enabled: bool,
        }
        Ok(serde_json::to_string_pretty(&Config {
            redis_password: var_source
                .get_environment_variable(&["REDIS_PASSWORD"])
                .await
                .ok()
                .unwrap_or_default(),
            redis_host: var_source
                .get_environment_variable(&["REDIS_HOST"])
                .await
                .ok()
                .unwrap_or_default(),
            redis_port: var_source
                .get_environment_variable(&["REDIS_PORT"])
                .await
                .ok()
                .unwrap_or_default()
                .parse()
                .unwrap_or(6380),
            redis_ssl_enabled: var_source
                .get_environment_variable(&["REDIS_SSL_ENABLED"])
                .await
                .ok()
                .unwrap_or_default()
                .parse()
                .unwrap_or(true),
        })
        .unwrap())
    }

    async fn get_kafka_config(
        &self,
        var_source: Arc<dyn VarSource + Send + Sync>,
    ) -> Result<String, crate::Error> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Config {
            kafka_sasl_jaas_config: String,
        }
        Ok(serde_json::to_string_pretty(&Config {
            kafka_sasl_jaas_config: var_source
                .get_environment_variable(&["KAFKA_SASL_JAAS_CONFIG"])
                .await
                .ok()
                .unwrap_or_default(),
        })
        .unwrap())
    }
}

/**
 * Builder to build a Spark Job submitting request
 */
pub struct SubmitJoiningJobRequestBuilder {
    job_name: String,
    input_path: String,
    main_jar_path: Option<String>,
    main_class_name: Option<String>,
    output_path: Option<String>,
    python_files: Vec<String>,
    reference_files: Vec<String>,
    configuration: HashMap<String, String>,
    feature_config: String,
    feature_join_config: String,
    secret_keys: Vec<String>,
}

impl SubmitJoiningJobRequestBuilder {
    pub(crate) fn new_join(
        job_name: String,
        input_path: String,
        feature_config: String,
        job_config: String, // feature_join_config or feature_gen_config
        secret_keys: Vec<String>,
    ) -> Self {
        Self {
            job_name,
            input_path,
            main_jar_path: None,
            main_class_name: None,
            output_path: None,
            python_files: Default::default(),
            reference_files: Default::default(),
            configuration: Default::default(),
            feature_config,
            feature_join_config: job_config,
            secret_keys: secret_keys,
        }
    }

    /**
     * Set output path for the Spark job
     */
    pub fn output_path(&mut self, output_path: &str) -> &mut Self {
        self.output_path = Some(output_path.to_string());
        self
    }

    /**
     * Create Spark job request
     */
    pub fn build(&self) -> SubmitJobRequest {
        let output = self.output_path.clone().unwrap(); // TODO: Validation
        let job_tags: HashMap<String, String> = [(OUTPUT_PATH_TAG.to_string(), output.clone())]
            .into_iter()
            .collect();
        SubmitJobRequest {
            name: self.job_name.to_owned(),
            job_config_file_name: format!("{}.conf", self.job_name),
            input: self.input_path.to_owned(),
            output,
            main_jar_path: self
                .main_jar_path
                .to_owned()
                .unwrap_or_else(|| FEATHR_JOB_JAR_PATH.to_string()),
            main_class_name: self
                .main_class_name
                .to_owned()
                .unwrap_or_else(|| JOIN_JOB_MAIN_CLASS_NAME.to_string()),
            feature_config: self.feature_config.to_owned(),
            join_job_config: self.feature_join_config.to_owned(),
            gen_job_config: Default::default(),
            python_files: self.python_files.to_owned(),
            reference_files: self.reference_files.to_owned(),
            job_tags,
            configuration: self.configuration.to_owned(),
            secret_key: self.secret_keys.to_owned(),
        }
    }
}

pub struct SubmitGenerationJobRequestBuilder {
    job_name: String,
    input_path: String,
    main_jar_path: Option<String>,
    main_class_name: Option<String>,
    python_files: Vec<String>,
    reference_files: Vec<String>,
    configuration: HashMap<String, String>,
    feature_config: String,
    secret_keys: Vec<String>,

    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: DateTimeResolution,
    materialization_builder: MaterializationSettingsBuilder,
}

impl SubmitGenerationJobRequestBuilder {
    pub(crate) fn new_gen(
        job_name: String,
        input_path: String,
        feature_config: String,
        secret_keys: Vec<String>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: DateTimeResolution,
    ) -> Self {
        Self {
            job_name: job_name.clone(),
            input_path,
            main_jar_path: None,
            main_class_name: None,
            python_files: Default::default(),
            reference_files: Default::default(),
            configuration: Default::default(),
            feature_config,
            secret_keys: secret_keys,
            start,
            end,
            step,
            materialization_builder: MaterializationSettingsBuilder::new(&job_name),
        }
    }

    pub fn sink<T>(&mut self, sink: T) -> &mut Self
    where
        T: Into<OutputSink>,
    {
        self.materialization_builder.sinks.push(sink.into());
        self
    }

    pub fn sinks<T>(&mut self, sinks: &[T]) -> &mut Self
    where
        T: Clone + Into<OutputSink>,
    {
        self.materialization_builder
            .sinks
            .extend(sinks.into_iter().map(|s| s.to_owned().into()));
        self
    }

    pub fn feature<T>(&mut self, feature: T) -> &mut Self
    where
        T: ToString,
    {
        self.materialization_builder
            .features
            .push(feature.to_string());
        self
    }

    pub fn features<T>(&mut self, features: &[T]) -> &mut Self
    where
        T: ToString,
    {
        self.materialization_builder
            .features
            .extend(features.into_iter().map(|f| f.to_string()));
        self
    }

    /**
     * Create Spark job request
     */
    pub fn build(&self) -> Result<Vec<SubmitJobRequest>, Error> {
        let mat_settings = self
            .materialization_builder
            .build(self.start, self.end, self.step)?;
        Ok(mat_settings
            .into_iter()
            .map(|s| {
                let conf = serde_json::to_string_pretty(&s).unwrap();
                SubmitJobRequest {
                    name: self.job_name.to_owned(),
                    job_config_file_name: format!(
                        "{}_{}.conf",
                        self.job_name,
                        s.operational.end_time.timestamp_millis()
                    ),
                    input: self.input_path.to_owned(),
                    output: Default::default(),
                    main_jar_path: self
                        .main_jar_path
                        .to_owned()
                        .unwrap_or_else(|| FEATHR_JOB_JAR_PATH.to_string()),
                    main_class_name: self
                        .main_class_name
                        .to_owned()
                        .unwrap_or_else(|| GEN_JOB_MAIN_CLASS_NAME.to_string()),
                    feature_config: self.feature_config.to_owned(),
                    join_job_config: Default::default(),
                    gen_job_config: conf,
                    python_files: self.python_files.to_owned(),
                    reference_files: self.reference_files.to_owned(),
                    job_tags: Default::default(),
                    configuration: self.configuration.to_owned(),
                    secret_key: self.secret_keys.to_owned(),
                }
            })
            .collect())
    }
}
