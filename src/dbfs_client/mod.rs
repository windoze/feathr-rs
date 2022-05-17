use std::{
    cmp::min,
    fmt::Display,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use async_trait::async_trait;
use futures::{AsyncRead, Future, FutureExt, AsyncBufRead};
use log::{debug, trace};
use pin_project::pin_project;
use reqwest::multipart::Part;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

const CHUNK_SIZE: usize = 1024 * 1024;

/// Log if `Result` is an error
trait Logged {
    fn log(self) -> Self;
}

impl<T, E> Logged for std::result::Result<T, E>
where
    E: std::fmt::Debug,
{
    fn log(self) -> Self {
        if let Err(e) = &self {
            log::debug!("---TraceError--- {:#?}", e)
        }
        self
    }
}

#[async_trait]
trait LoggedResponse {
    async fn detailed_error_for_status(self) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl LoggedResponse for reqwest::Response {
    async fn detailed_error_for_status(self) -> Result<Self> {
        if self.status().is_client_error() || self.status().is_server_error() {
            let url = self.url().to_string();
            let status = self.status().to_string();
            let text = self.text().await?;
            Err(match serde_json::from_str::<DbfsErrorResponse>(&text) {
                Ok(resp) => DbfsError::DbfsApiError(resp.error_code, resp.message),
                Err(_) => DbfsError::HttpError(url, status, text),
            })
        } else {
            Ok(self)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DbfsErrorCode {
    ResourceAlreadyExists,
    MaxBlockSizeExceeded,
    InvalidParameterValue,
    MaxReadSizeExceeded,
    ResourceDoesNotExist,
}

impl Display for DbfsErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            &serde_json::to_string(&self)
                .unwrap()
                .strip_prefix("\"")
                .unwrap()
                .strip_suffix("\"")
                .unwrap(),
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct DbfsErrorResponse {
    pub error_code: DbfsErrorCode,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum DbfsError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error("HTTP Error, URL: '{0}', Status: {1}, Response: '{2}' ")]
    HttpError(String, String, String),

    #[error(transparent)]
    DecodeError(#[from] base64::DecodeError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error("DBFS Error, Code: {0}, message: {0}")]
    DbfsApiError(DbfsErrorCode, String),

    #[error("Invalid DBFS Path {0}")]
    InvalidDbfsPath(String),
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DbfsApiVersions {
    API_2_0,
}

impl Default for DbfsApiVersions {
    fn default() -> Self {
        Self::API_2_0
    }
}

impl Display for DbfsApiVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match &self {
                DbfsApiVersions::API_2_0 => "api/2.0",
            }
        )
    }
}

pub type Result<T> = std::result::Result<T, DbfsError>;

#[derive(Debug, Deserialize)]
pub struct FileStatus {
    pub path: String,
    pub is_dir: bool,
    pub file_size: usize,
    pub modification_time: u64,
}

#[derive(Clone, Debug)]
pub struct DbfsClient {
    inner: Arc<DbfsClientInner>,
}

impl DbfsClient {
    pub fn new(url_base: &str, token: &str) -> Self {
        Self {
            inner: Arc::new(DbfsClientInner::new(url_base, token)),
        }
    }

    pub fn read(&self, path: &str) -> ReadStreamState {
        let path = path.to_string();
        let inner = self.inner.clone();
        ReadStreamState {
            reader: inner.clone(),
            path: path.clone(),
            step: ReadStreamSteps::Len,
            file_size: 0,
            file_offset: 0,
            current_buf: vec![],
            current_buf_offset: 0,
            len_future: Box::pin(async move {
                inner.get_status(&path)
                    .map(|r| {
                        r.map(|s| s.file_size)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    })
                    .await
            }),
            current_future: None,
        }
    }

    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let path = strip_dbfs_prefix(path)?;
        debug!("Reading DBFS file {}", path);
        let file_size = self.inner.get_status(path).await?.file_size;
        debug!("File size is {}", file_size);
        let mut ret = Vec::with_capacity(file_size);
        let mut offset = 0;
        loop {
            let data = self.inner.read_block(path, offset, CHUNK_SIZE).await?;
            offset += data.len();
            ret.extend(data.into_iter());
            if offset >= file_size {
                break;
            }
        }
        Ok(ret)
    }

    pub async fn write_file<T>(&self, path: &str, data: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        debug!(
            "Writing {} bytes to DBFS file {}",
            data.as_ref().len(),
            path
        );
        let path = strip_dbfs_prefix(path)?;
        if data.as_ref().len() < CHUNK_SIZE {
            return self.inner.put(path, data, true).await;
        }

        let handle = self.inner.create(path, true).await?;
        for chunk in data.as_ref().chunks(CHUNK_SIZE) {
            self.inner.add_block(handle, chunk).await?;
        }
        self.inner.close(handle).await?;
        Ok(())
    }

    pub async fn upload_file<T>(&self, local_path: T, remote_path: &str) -> Result<String>
    where
        T: AsRef<Path>,
    {
        debug!(
            "Uploading local file {} to DBFS file {}",
            local_path.as_ref().to_string_lossy(),
            remote_path
        );
        let remote_path = strip_dbfs_prefix(remote_path)?;
        let filename = local_path.as_ref().to_owned().to_string_lossy().to_string();
        let file = tokio::fs::File::open(local_path).await?;
        let length = file.metadata().await?.len();
        let stream = tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new());
        let body = reqwest::Body::wrap_stream(stream);
        self.inner
            .put_stream(remote_path, &filename, body, length, true)
            .await?;
        Ok(remote_path.to_string())
    }

    pub async fn download_file<T>(&self, remote_path: &str, local_path: T) -> Result<PathBuf>
    where
        T: AsRef<Path>,
    {
        debug!(
            "Downloading DBFS file {} to local file {}",
            remote_path,
            local_path.as_ref().to_string_lossy()
        );
        let remote_path = strip_dbfs_prefix(remote_path)?;
        let file_size = self.inner.get_status(remote_path).await?.file_size;
        let mut offset = 0;
        let mut file = tokio::fs::File::create(local_path.as_ref()).await?;
        loop {
            let data = self.inner.read_block(remote_path, offset, CHUNK_SIZE).await?;
            offset += data.len();
            file.write_all(&data).await?;
            if offset >= file_size {
                break;
            }
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(PathBuf::from(local_path.as_ref()))
    }

    pub async fn get_file_status(&self, path: &str) -> Result<FileStatus> {
        debug!("Getting status of DBFS file {}", path);
        self.inner.get_status(path).await
    }

    pub async fn delete_file(&self, path: &str) -> Result<()> {
        debug!("Deleting DBFS file {}", path);
        self.inner.delete(strip_dbfs_prefix(path)?).await
    }

    pub async fn list(&self, path: &str) -> Result<Vec<FileStatus>> {
        debug!("Listing DBFS directory {}", path);
        self.inner.list(strip_dbfs_prefix(path)?).await
    }

    pub async fn mkdir(&self, path: &str) -> Result<()> {
        debug!("Creating DBFS directory {}", path);
        self.inner.mkdirs(strip_dbfs_prefix(path)?).await
    }

    pub async fn move_file(&self, src_path: &str, dest_path: &str) -> Result<()> {
        debug!("Moving DBFS file from {} to {}", src_path, dest_path);
        self.inner
            .move_(strip_dbfs_prefix(src_path)?, strip_dbfs_prefix(dest_path)?)
            .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Handle(u64);

#[derive(Debug)]
struct DbfsClientInner {
    url_base: String,
    api_version: DbfsApiVersions,
    client: reqwest::Client,
}

impl DbfsClientInner {
    pub fn new(url_base: &str, token: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if !token.is_empty() {
            headers.insert(
                "Authorization",
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }

        Self {
            url_base: url_base
                .trim()
                .strip_suffix("/")
                .unwrap_or(url_base)
                .trim()
                .to_string(),
            api_version: DbfsApiVersions::API_2_0,
            client: reqwest::ClientBuilder::new()
                .default_headers(headers)
                .build()
                .unwrap(),
        }
    }

    fn get_url(&self, api: &str) -> String {
        format!("{}/{}/dbfs/{}", self.url_base, self.api_version, api)
    }

    /// DBFS API

    async fn add_block<T>(&self, handle: Handle, data: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        trace!("Add block to handle {}", handle.0);
        #[derive(Debug, Serialize)]
        struct Request {
            handle: Handle,
            data: String,
        }
        self.client
            .post(self.get_url("add-block"))
            .json(&Request {
                handle,
                data: base64::encode(data),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn close(&self, handle: Handle) -> Result<()> {
        trace!("Close handle {}", handle.0);
        #[derive(Debug, Serialize)]
        struct Request {
            handle: Handle,
        }
        self.client
            .post(self.get_url("close"))
            .json(&Request { handle })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn create(&self, path: &str, overwrite: bool) -> Result<Handle> {
        trace!("Create file {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
            overwrite: bool,
        }
        #[derive(Debug, Deserialize)]
        struct Response {
            handle: Handle,
        }
        let resp: Response = self
            .client
            .post(self.get_url("create"))
            .json(&Request {
                path: path.to_string(),
                overwrite,
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .json()
            .await?;
        Ok(resp.handle)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        trace!("Delete file {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
        }
        self.client
            .post(self.get_url("delete"))
            .json(&Request {
                path: path.to_string(),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn get_status(&self, path: &str) -> Result<FileStatus> {
        trace!("Get status of file {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
        }
        Ok(self
            .client
            .get(self.get_url("get-status"))
            .json(&Request {
                path: path.to_string(),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .json()
            .await?)
    }

    async fn list(&self, path: &str) -> Result<Vec<FileStatus>> {
        trace!("List directory {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
        }
        #[derive(Debug, Deserialize)]
        struct Response {
            files: Vec<FileStatus>,
        }
        let resp: Response = self
            .client
            .get(self.get_url("list"))
            .json(&Request {
                path: path.to_string(),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .json()
            .await?;
        Ok(resp.files)
    }

    async fn mkdirs(&self, path: &str) -> Result<()> {
        trace!("Make directory {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
        }
        self.client
            .post(self.get_url("mkdirs"))
            .json(&Request {
                path: path.to_string(),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn move_(&self, source_path: &str, destination_path: &str) -> Result<()> {
        trace!("Move file from {} to {}", source_path, destination_path);
        #[derive(Debug, Serialize)]
        struct Request {
            source_path: String,
            destination_path: String,
        }
        self.client
            .post(self.get_url("move"))
            .json(&Request {
                source_path: source_path.to_string(),
                destination_path: destination_path.to_string(),
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn put<T>(&self, path: &str, content: T, overwrite: bool) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        trace!(
            "Upload buffer to file {}, length is {}",
            path,
            content.as_ref().len()
        );
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
            contents: String,
            overwrite: bool,
        }
        self.client
            .post(self.get_url("put"))
            .json(&Request {
                path: path.to_string(),
                contents: base64::encode(content),
                overwrite,
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn put_stream<S>(
        &self,
        path: &str,
        filename: &str,
        stream: S,
        length: u64,
        overwrite: bool,
    ) -> Result<()>
    where
        S: Into<reqwest::Body>,
    {
        trace!("Upload stream to file {}, length is {}", path, length);
        let path = path.to_string();
        let form = reqwest::multipart::Form::new()
            .part(
                "contents",
                Part::stream_with_length(stream, length).file_name(filename.to_owned()),
            )
            .text("path", path)
            .text("overwrite", if overwrite { "true" } else { "false" });
        self.client
            .post(self.get_url("put"))
            .multipart(form)
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .text()
            .await?;
        Ok(())
    }

    async fn read_block(&self, path: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        trace!("Read file {}", path);
        #[derive(Debug, Serialize)]
        struct Request {
            path: String,
            offset: usize,
            length: usize,
        }
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct Response {
            bytes_read: usize,
            data: String,
        }
        let resp: Response = self
            .client
            .get(self.get_url("read"))
            .json(&Request {
                path: path.to_string(),
                offset,
                length,
            })
            .send()
            .await?
            .detailed_error_for_status()
            .await
            .log()?
            .json()
            .await?;
        Ok(base64::decode(resp.data)?)
    }
}

#[pin_project]
pub struct ReadStreamState {
    reader: Arc<DbfsClientInner>,
    path: String,
    step: ReadStreamSteps,
    file_size: usize,
    file_offset: usize,
    current_buf: Vec<u8>,
    current_buf_offset: usize,
    len_future: Pin<Box<dyn Future<Output = std::result::Result<usize, std::io::Error>>>>,
    current_future:
        Option<Pin<Box<dyn Future<Output = std::result::Result<Vec<u8>, std::io::Error>>>>>,
}

#[derive(Clone, Copy, Debug)]
enum ReadStreamSteps {
    Len,
    Read,
    End,
}

impl AsyncBufRead for ReadStreamState {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        let mut this = self.project();
        trace!("Current State is {:#?}", *this.step);
        let current_buf = &mut this.current_buf;
        match *this.step {
            ReadStreamSteps::Len => {
                trace!("Polling GetStatus future");
                match this.len_future.poll_unpin(cx) {
                    Poll::Ready(r) => {
                        trace!("GetStatus future ready, result is {:#?}", r);
                        match r {
                            Ok(sz) => {
                                // Got file length, start reading
                                *this.file_size = sz;
                                *this.file_offset = 0;
                                *this.current_buf_offset = 0;
                                // this.current_buf.clear();
                                *this.step = ReadStreamSteps::Read;
                                trace!("State changed to ReadStreamSteps::Read");
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            Err(e) => {
                                // Failed to get file length
                                Poll::Ready(Err(e))
                            }
                        }
                    }
                    Poll::Pending => {
                        // Pending on getting file length
                        Poll::Pending
                    }
                }
            }
            ReadStreamSteps::Read => {
                if *this.file_offset >= *this.file_size {
                    // Reach EOF
                    *this.step = ReadStreamSteps::End;
                    trace!("Reach EOF");
                    Poll::Ready(std::io::Result::Ok(&this.current_buf[0..0]))
                } else if current_buf.len() > *this.current_buf_offset {
                    // There are some data left in the current buffer
                    let end_pos = current_buf.len();
                    Poll::Ready(std::io::Result::Ok(&this.current_buf[*this.current_buf_offset..end_pos]))
                } else if let Some(f) = this.current_future {
                    // Reading operation in progress
                    let p = f.poll_unpin(cx);
                    match p {
                        Poll::Ready(r) => {
                            // Current future completed
                            *this.current_future = None;
                            match r {
                                Ok(b) => {
                                    // Got a buffer
                                    // Reset current buffer and pos
                                    *this.current_buf_offset = 0;
                                    *this.current_buf = b;
                                    *this.step = ReadStreamSteps::Read;
                                    cx.waker().wake_by_ref();
                                    Poll::Pending
                                }
                                Err(e) => {
                                    // Read error
                                    *this.step = ReadStreamSteps::End;
                                    Poll::Ready(Err(e))
                                }
                            }
                        }
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    // Nothing to provide, start reading
                    let path = this.path.clone();
                    let reader = this.reader.clone();
                    let offset = *this.file_offset;
                    let f = async move {
                        reader
                            .read_block(&path, offset, 4096)
                            .await
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    };
                    *this.current_future = Some(Box::pin(f));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            ReadStreamSteps::End => {
                panic!("ReadStreamState must not be polled after it returned `Poll::Ready(Ok(&[]))`")
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        *this.current_buf_offset += amt;
        *this.file_offset += amt;
    }
}

impl AsyncRead for ReadStreamState {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut this = self.project();
        let current_buf = &mut this.current_buf;
        match *this.step {
            ReadStreamSteps::Len => {
                match this.len_future.poll_unpin(cx) {
                    Poll::Ready(r) => {
                        match r {
                            Ok(sz) => {
                                if sz == 0 {
                                    // File is empty
                                    debug!("File is empty");
                                    return Poll::Ready(Ok(0));
                                }
                                // Got file length, start reading
                                debug!("File length is {}", sz);
                                *this.file_size = sz;
                                *this.file_offset = 0;
                                *this.current_buf_offset = 0;
                                this.current_buf.clear();
                                *this.step = ReadStreamSteps::Read;
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            Err(e) => {
                                // Failed to get file length
                                Poll::Ready(Err(e))
                            }
                        }
                    }
                    Poll::Pending => {
                        // Pending on getting file length
                        Poll::Pending
                    }
                }
            }
            ReadStreamSteps::Read => {
                if *this.file_offset >= *this.file_size {
                    // Reach EOF
                    *this.step = ReadStreamSteps::End;
                    Poll::Ready(Ok(0))
                } else if current_buf.len() > *this.current_buf_offset {
                    // There are some data left in the current buffer
                    let existing_sz = current_buf.len() - *this.current_buf_offset;
                    let required_sz = buf.len();
                    let sz = min(existing_sz, required_sz);
                    let end_pos = *this.current_buf_offset + sz;
                    buf[0..sz].copy_from_slice(&current_buf[*this.current_buf_offset..end_pos]);
                    if end_pos >= this.current_buf.len() {
                        // Current buffer exhausted
                        *this.current_buf_offset = 0;
                    } else {
                        // Current buffer still has data
                        *this.current_buf_offset = end_pos;
                    }
                    *this.file_offset += sz;
                    *this.step = ReadStreamSteps::Read;
                    Poll::Ready(std::io::Result::Ok(sz))
                } else if let Some(f) = this.current_future {
                    // Reading operation in progress
                    let p = f.poll_unpin(cx);
                    match p {
                        Poll::Ready(r) => {
                            // Current future completed
                            *this.current_future = None;
                            match r {
                                Ok(b) => {
                                    // Got a buffer
                                    *this.current_buf_offset = 0;
                                    *this.current_buf = b;
                                    *this.step = ReadStreamSteps::Read;
                                    cx.waker().wake_by_ref();
                                    Poll::Pending
                                }
                                Err(e) => {
                                    // Read error
                                    *this.step = ReadStreamSteps::End;
                                    Poll::Ready(Err(e))
                                }
                            }
                        }
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    // Nothing to provide, start reading
                    let path = this.path.clone();
                    let reader = this.reader.clone();
                    let offset = *this.file_offset;
                    let f = async move {
                        reader
                            .read_block(&path, offset, 4096)
                            .await
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    };
                    *this.current_future = Some(Box::pin(f));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            ReadStreamSteps::End => {
                panic!("ReadStreamState must not be polled after it returned `Poll::Ready(Ok(0))`")
            }
        }
    }
}

fn strip_dbfs_prefix(path: &str) -> Result<&str> {
    let ret = path.strip_prefix("dbfs:").unwrap_or(path);
    if ret.starts_with("/") {
        Ok(ret)
    } else {
        Err(DbfsError::InvalidDbfsPath(path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use futures::{AsyncReadExt, AsyncBufReadExt};
    use rand::Rng;

    use super::*;

    fn init() -> DbfsClient {
        crate::tests::init_logger();
        DbfsClient::new(
            &std::env::var("SPARK_CONFIG__DATABRICKS__WORKSPACE_INSTANCE_URL").unwrap(),
            &std::env::var("DATABRICKS_WORKSPACE_TOKEN_VALUE").unwrap(),
        )
    }

    #[test]
    fn test_strip_prefix() {
        assert_eq!(strip_dbfs_prefix("/abc").unwrap(), "/abc");
        assert_ne!(strip_dbfs_prefix("/abc").unwrap(), "/abcd");
        assert_eq!(strip_dbfs_prefix("dbfs:/abc").unwrap(), "/abc");
        assert_ne!(strip_dbfs_prefix("dbfs:/abc").unwrap(), "/abcd");
        assert!(matches!(
            strip_dbfs_prefix("abc"),
            Err(DbfsError::InvalidDbfsPath(..))
        ));
        assert!(matches!(
            strip_dbfs_prefix("dbfs:abc"),
            Err(DbfsError::InvalidDbfsPath(..))
        ));
    }

    #[tokio::test]
    async fn read_write_delete() {
        let client = init();
        let expected = "foo\nbar\nbaz\nspam\n".as_bytes();
        client
            .write_file("/test_read_write_delete", expected)
            .await
            .unwrap();
        let data = client.read_file("/test_read_write_delete").await.unwrap();
        assert_eq!(data, expected);
        assert_eq!(
            client
                .get_file_status("/test_read_write_delete")
                .await
                .unwrap()
                .file_size,
            expected.len()
        );
        client.delete_file("/test_read_write_delete").await.unwrap();
        let ret = client.read_file("/test_read_write_delete").await;
        assert!(matches!(
            ret,
            Err(DbfsError::DbfsApiError(
                DbfsErrorCode::ResourceDoesNotExist,
                ..
            ))
        ));
    }

    #[tokio::test]
    async fn upload_file() {
        let client = init();
        let expected = "foo\nbar\nbaz\nspam\n".as_bytes();
        let mut f = tokio::fs::File::create("/tmp/test_upload_file")
            .await
            .unwrap();
        f.write_all(expected).await.unwrap();
        f.flush().await.unwrap();
        f.sync_all().await.unwrap();
        client
            .upload_file("/tmp/test_upload_file", "/test_upload_file")
            .await
            .unwrap();
        let data = client.read_file("/test_upload_file").await.unwrap();
        assert_eq!(data, expected);
    }

    #[tokio::test]
    async fn large_file() {
        let mut rng = rand::thread_rng();

        // Exceeds CHUNK_SIZE
        let expected: Vec<u8> = (0..1024 * 1024 * 2).map(|_| rng.gen()).collect();

        let client = init();
        client
            .write_file("dbfs:/large_file", &expected)
            .await
            .unwrap();

        let buf = client.read_file("/large_file").await.unwrap();
        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_read() {
        let client = init();
        let mut rng = rand::thread_rng();
        // Exceeds CHUNK_SIZE
        let expected: Vec<u8> = (0..1024 * 1024 * 2 + 997).map(|_| rng.gen()).collect();
        client
            .write_file("dbfs:/test_read", &expected)
            .await
            .unwrap();

        let mut offset = 0;
        let mut buf = [0; 1000];
        let mut s = client.read("dbfs:/test_read");
        while let Ok(sz) = s.read(&mut buf).await {
            debug!("Got {} bytes", sz);
            if sz == 0 {
                break;
            }
            assert_eq!(&buf[0..sz], &expected[offset..offset+sz]);
            offset += sz;
        }
    }

    #[tokio::test]
    async fn test_read_line() {
        let client = init();
        let expected: Vec<String> = (0..10).map(|n| format!("Line {}\n", n)).collect();
        client
            .write_file("dbfs:/test_read_line", expected.join("").as_bytes())
            .await
            .unwrap();

        let mut s = client.read("dbfs:/test_read_line");
        let mut line = String::default();
        let mut counter = 0;
        while let Ok(sz) = s.read_line(&mut line).await {
            debug!("Got {} bytes", sz);
            debug!("Line is  `{}`", line);
            if sz == 0 {
                break;
            }
            assert_eq!(line, format!("Line {}\n", counter));
            counter += 1;
            line.clear();
        }
    }
}

