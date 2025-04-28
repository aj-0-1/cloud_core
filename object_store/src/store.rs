use bytes::Bytes;
use futures::{Stream, StreamExt}; // Added StreamExt
use std::io;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Invalid Path: {0}")]
    InvalidPath(String),
}

pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Store { root: root.into() }
    }

    fn validate_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StoreError> {
        if bucket.is_empty() || key.is_empty() || bucket.contains("..") || key.contains("..") {
            return Err(StoreError::InvalidPath(format!(
                "Invalid bucket or key: {}/{}",
                bucket, key
            )));
        }
        let path = self.root.join(bucket).join(key);
        Ok(path)
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let bucket_path = self.root.join(bucket);
        if !bucket_path.exists() {
            fs::create_dir_all(&bucket_path)
                .await
                .map_err(StoreError::IoError)?;
        }
        Ok(())
    }
}

pub trait Storage {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        data: impl Stream<Item = Result<Bytes, io::Error>> + Send + Unpin,
    ) -> Result<(), StoreError>;

    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<impl Stream<Item = Result<Bytes, io::Error>> + Send, StoreError>;

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StoreError>;
}

impl Storage for Store {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        mut data: impl Stream<Item = Result<Bytes, io::Error>> + Send + Unpin,
    ) -> Result<(), StoreError> {
        let path = self.validate_path(bucket, key)?;
        self.ensure_bucket(bucket).await?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(StoreError::IoError)?;
        }

        // Write stream to file
        let mut file = fs::File::create(&path).await.map_err(StoreError::IoError)?;
        while let Some(result) = data.next().await {
            let chunk = result?;
            file.write_all(&chunk).await.map_err(StoreError::IoError)?;
        }
        file.flush().await.map_err(StoreError::IoError)?;
        Ok(())
    }

    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<impl Stream<Item = Result<Bytes, io::Error>> + Send, StoreError> {
        let path = self.validate_path(bucket, key)?;
        if !path.exists() {
            return Err(StoreError::NotFound(format!(
                "File not found: {}/{}",
                bucket, key
            )));
        }

        let file = fs::File::open(&path).await.map_err(StoreError::IoError)?;
        let (tx, rx) = mpsc::channel(1024);
        let chunk_size = 64 * 1024; // 64KB chunks

        tokio::spawn(async move {
            let mut file = file;
            let mut buffer = vec![0; chunk_size].into_boxed_slice();
            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        let chunk = Bytes::from(buffer[..n].to_vec());
                        if tx.send(Ok(chunk)).await.is_err() {
                            break; // Receiver dropped
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let path = self.validate_path(bucket, key)?;
        if !path.exists() {
            return Err(StoreError::NotFound(format!(
                "File not found: {}/{}",
                bucket, key
            )));
        }
        fs::remove_file(&path).await.map_err(StoreError::IoError)?;
        Ok(())
    }
}
