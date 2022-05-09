use async_trait::async_trait;
use azure_identity::token_credentials::DefaultAzureCredential;
use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::clients::{DataLakeClient, PathClient};
use bytes::Bytes;
use livy_client::{
    AadAuthenticator, AzureSynapseClientBuilder, ClusterSize, LivyClient, LivyStates, SparkRequest,
};
use log::debug;
use reqwest::Url;
use tokio::io::AsyncReadExt;

use crate::{JobClient, JobId, Logged};

use super::env_utils::VarSource;

pub struct AzureSynapseClient {
    livy_client: LivyClient<AadAuthenticator>,
    storage_client: DataLakeClient,
    storage_account: String,
    container: String,
    workspace_dir: String,
}

impl AzureSynapseClient {
    pub fn with_credential(
        credential: DefaultAzureCredential,
        url: &str,
        pool: &str,
        storage_account: &str,
        storage_key: &str,
        container: &str,
        workspace_dir: &str,
    ) -> Result<Self, crate::Error> {
        Ok(Self {
            livy_client: AzureSynapseClientBuilder::with_credential(credential)?
                .url(url)
                .pool(pool)
                .build()?,
            storage_client: DataLakeClient::new(
                StorageSharedKeyCredential::new(
                    storage_account.to_string(),
                    storage_key.to_string(),
                ),
                None,
            ),
            storage_account: storage_account.to_string(),
            container: container.to_string(),
            workspace_dir: workspace_dir.to_string(),
        })
    }

    pub fn default() -> Result<Self, crate::Error> {
        let (container, storage_account, workspace_dir) =
            parse_abfs(std::env::var("SYNAPSE_WORKSPACE_DIR")?)?;
        Ok(Self {
            livy_client: AzureSynapseClientBuilder::default()
                .url(std::env::var("SYNAPSE_DEV_URL")?)
                .pool(std::env::var("SYNAPSE_POOL_NAME")?)
                .build()?,
            storage_client: DataLakeClient::new(
                StorageSharedKeyCredential::new(
                    std::env::var("ADLS_ACCOUNT")?,
                    std::env::var("ADLS_KEY")?,
                ),
                None,
            ),
            storage_account,
            container,
            workspace_dir: workspace_dir.trim_start_matches("/").to_string(),
        })
    }
}

#[async_trait]
impl JobClient for AzureSynapseClient {
    type JobStatus = LivyStates;

    fn from_var_source<T>(var_source: &T) -> Result<Self, crate::Error>
    where
        T: VarSource,
    {
        let (container, storage_account, workspace_dir) =
            parse_abfs(var_source.get_environment_variable(&[
                "spark_config",
                "azure_synapse",
                "workspace_dir",
            ])?)?;
        Ok(Self {
            livy_client: AzureSynapseClientBuilder::default()
                .url(var_source.get_environment_variable(&[
                    "spark_config",
                    "azure_synapse",
                    "dev_url",
                ])?)
                .pool(var_source.get_environment_variable(&[
                    "spark_config",
                    "azure_synapse",
                    "pool_name",
                ])?)
                .build()?,
            storage_client: DataLakeClient::new(
                StorageSharedKeyCredential::new(
                    var_source.get_environment_variable(&["ADLS_ACCOUNT"])?,
                    var_source.get_environment_variable(&["ADLS_KEY"])?,
                ),
                None,
            ),
            storage_account,
            container,
            workspace_dir: workspace_dir.trim_start_matches("/").to_string(),
        })
    }

    async fn write_remote_file(&self, path: &str, content: &[u8]) -> Result<String, crate::Error> {
        let (container, _, path) = parse_abfs(path)?;
        debug!("Container: {}", container);
        debug!("Path: {}", path);
        let fs_client = self
            .storage_client
            .clone()
            .into_file_system_client(container);
        // Create file system and ignore error, in case the file system already exists
        fs_client.create().into_future().await.log().ok();
        let file_client = fs_client.get_file_client(path);
        // Delete existing file and ignore error
        file_client.delete().into_future().await.log().ok();
        file_client.create().into_future().await.log()?;
        file_client
            .append(0, bytes::Bytes::from(content.to_owned()))
            .into_future()
            .await
            .log()?;
        file_client
            .flush(content.len() as i64)
            .into_future()
            .await
            .log()?;
        http_to_abfs(file_client.url().log()?)
    }
    async fn submit_job<T>(
        &self,
        var_source: &T,
        request: super::SubmitJobRequest,
    ) -> Result<JobId, crate::Error>
    where
        T: VarSource + Send + Sync,
    {
        let args = self.get_arguments(var_source, &request).await?;
        let main_jar_file = request.main_jar_path;
        let mut orig_files: Vec<String> = vec![];
        let mut orig_jars: Vec<String> = vec![];

        for f in request.reference_files.into_iter() {
            if f.ends_with(".jar") {
                orig_jars.push(f)
            } else {
                orig_files.push(f)
            }
        }
        orig_jars.push(main_jar_file);

        debug!("Uploading JARs: {:#?}", orig_jars);
        let jars = self.multi_upload_or_get_url(&orig_jars).await?;
        debug!("JARs uploaded, URLs: {:#?}", jars);

        debug!("Uploading files: {:#?}", orig_files);
        let files = self.multi_upload_or_get_url(&orig_files).await?;
        debug!("Files uploaded, URLs: {:#?}", files);

        debug!("Uploading Python files: {:#?}", request.python_files);
        let py_files = self.multi_upload_or_get_url(&request.python_files).await?;
        debug!("Python files uploaded, URLs: {:#?}", py_files);

        let executable = if py_files.is_empty() {
            jars[0].clone()
        } else {
            py_files[0].clone()
        };
        debug!("Main executable file: {}", executable);

        let job = SparkRequest {
            args,
            class_name: if py_files.is_empty() {
                request.main_class_name
            } else {
                Default::default()
            },
            conf: request.configuration,
            cluster_size: ClusterSize::MEDIUM(), // TODO:
            file: executable,
            files,
            jars,
            name: request.name,
            py_files,
            tags: request.job_tags,
            ..Default::default()
        };
        debug!("Job request: {:#?}", job);
        let jid = self.livy_client.create_batch_job(job).await?.id;
        debug!("Job submitted, id is {}", jid);
        Ok(JobId(jid))
    }
    fn is_ended_status(&self, status: Self::JobStatus) -> bool {
        matches!(
            status,
            LivyStates::Dead
                | LivyStates::Error
                | LivyStates::Killed
                | LivyStates::Success
                | LivyStates::Busy
        )
    }
    async fn get_job_status(&self, job_id: JobId) -> Result<Self::JobStatus, crate::Error> {
        Ok(self.livy_client.get_batch_job(job_id.0).await?.state)
    }
    async fn get_job_log(&self, job_id: JobId) -> Result<String, crate::Error> {
        Ok(self
            .livy_client
            .get_batch_job_driver_stdout_log(job_id.0)
            .await?)
    }
    async fn get_job_output_url(&self, job_id: JobId) -> Result<Option<String>, crate::Error> {
        let job = self.livy_client.get_batch_job(job_id.0).await?;
        Ok(job
            .tags
            .map(|t| t.get(super::OUTPUT_PATH_TAG).map(|s| s.to_owned()))
            .flatten())
    }
    async fn read_remote_file(&self, url: &str) -> Result<Bytes, crate::Error> {
        let (container, _, dir) = parse_abfs(url)?;
        debug!("Container: {}", container);
        debug!("Path: {}", dir);
        let fs_client = self
            .storage_client
            .clone()
            .into_file_system_client(container);
        let file_client = fs_client.get_file_client(dir);
        Ok(file_client.read().into_future().await?.data)
    }
    async fn upload_or_get_url(&self, path: &str) -> Result<String, crate::Error> {
        let bytes = if path.starts_with("http:") || path.starts_with("https:") {
            // It's a Internet file
            reqwest::Client::new()
                .get(path)
                .send()
                .await?
                .bytes()
                .await?
        } else if path.contains("://") {
            // It's a file on the storage
            // let (container, storage, _) = parse_abfs(path)?;
            // if container == self.container && storage == self.storage_account {
            //     // The file is located in this container of this storage, no need to download and upload again
            //     return Ok(path.to_string());
            // }
            // debug!("Transferring from remote storage {} to local storage {}", storage, self.storage_account);
            // self.read_remote_file(path).await?
            return Ok(path.to_string());
        } else {
            // Local file
            let mut v: Vec<u8> = vec![];
            tokio::fs::File::open(path)
                .await?
                .read_to_end(&mut v)
                .await?;
            Bytes::from(v)
        };
        let url = self.get_remote_url(&self.get_file_name(path)?);
        self.write_remote_file(&url, &bytes).await
    }

    fn get_remote_url(&self, filename: &str) -> String {
        format!(
            "abfss://{}@{}.dfs.core.windows.net/{}",
            self.container,
            self.storage_account,
            [self.workspace_dir.as_str(), filename]
                .join("/")
                .replace("//", "/") // In case workspace_dir is "/"
                .trim_start_matches("/")
                .to_string()
        )
    }
}

/**
 * Convert Storage URL to Spark compatible format:
 * https://storage/container/path -> abfss://container@storage/path
 */
fn http_to_abfs<T: AsRef<str>>(url: T) -> Result<String, crate::Error> {
    let url =
        Url::parse(url.as_ref()).map_err(|_| crate::Error::InvalidUrl(url.as_ref().to_string()))?;
    match url.scheme().to_lowercase().as_str() {
        "http" | "https" => {
            let schema = url.scheme().to_lowercase().replace("http", "abfs");
            let host = url
                .host()
                .ok_or_else(|| crate::Error::InvalidUrl(url.to_string()))?
                .to_string();
            let path: Vec<String> = url
                .path()
                .to_string()
                .split("/")
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect();
            let container = path
                .get(0)
                .ok_or_else(|| crate::Error::InvalidUrl(url.to_string()))?
                .to_owned();
            let dir = path[1..path.len()].join("/");
            Ok(format!("{schema}://{container}@{host}/{dir}"))
        }
        _ => Err(crate::Error::InvalidUrl(url.to_string())),
    }
}

fn parse_abfs<T: AsRef<str>>(abfs_url: T) -> Result<(String, String, String), crate::Error> {
    let url = Url::parse(abfs_url.as_ref())
        .map_err(|_| crate::Error::InvalidUrl(abfs_url.as_ref().to_string()))?;
    let container = url.username().to_string();
    let host: Vec<String> = url
        .host()
        .ok_or_else(|| crate::Error::InvalidUrl(url.to_string()))?
        .to_string()
        .split(".")
        .into_iter()
        .map(|s| s.to_string())
        .take(1)
        .collect();
    let account_name = host
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::InvalidUrl(url.to_string()))?;
    let path = url.path().trim_start_matches("/").to_string();
    Ok((container, account_name, path))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use dotenv;
    use std::sync::Once;
    use tokio::io::AsyncReadExt;

    use crate::*;

    static INIT_ENV_LOGGER: Once = Once::new();

    fn init() -> AzureSynapseClient {
        dotenv::dotenv().ok();
        INIT_ENV_LOGGER.call_once(|| env_logger::init());
        AzureSynapseClient::default().unwrap()
    }

    #[test]
    fn get_file_name() {
        let client = init();
        assert_eq!(
            client
                .get_file_name(
                    "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/abc"
                )
                .unwrap(),
            "abc"
        );

        assert_eq!(
            client
                .get_file_name("../test-script/pyspark-test.py")
                .unwrap(),
            "pyspark-test.py"
        );
    }

    #[tokio::test]
    async fn upload_and_download_file() {
        let client = init();
        let content = Utc::now().format("%+").to_string();
        let url = client
            .write_remote_file(
                "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/abc",
                content.as_bytes(),
            )
            .await
            .unwrap();
        client.download_file(&url, "/tmp").await.unwrap();
        let mut f = tokio::fs::File::open("/tmp/abc").await.unwrap();
        let mut buf: Vec<u8> = vec![];
        f.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, content.as_bytes());
    }

    #[tokio::test]
    async fn multi_upload() {
        let client = init();
        let files = vec![
            String::from("abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/abc"),
            String::from("../test-script/pyspark-test.py"),
        ];
        let ret = client.multi_upload_or_get_url(&files).await.unwrap();
        assert_eq!(
            ret,
            vec![
                "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/abc",
                "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/pyspark-test.py"
            ]
        )
    }
}
