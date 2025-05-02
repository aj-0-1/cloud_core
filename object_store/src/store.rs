use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    #[error("Sled error: {0}")]
    SledError(#[from] sled::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    HttpError(#[from] axum::http::Error),

    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Invalid Path: {0}")]
    InvalidPath(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    #[serde(rename = "Content-Type")]
    pub content_type: Option<String>,
    #[serde(rename = "Content-Length")]
    pub content_length: Option<u64>,
    #[serde(flatten)]
    pub custom: HashMap<String, String>,
}

pub struct Store {
    root: PathBuf,
    db: sled::Db,
}

impl Store {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, sled::Error> {
        let root = root.into();
        tracing::info!(?root, "Setting up store");
        let db_path = root.join("metadata_db");
        tracing::debug!(?db_path, "Opening sled database");
        let db = sled::open(&db_path)?;
        Ok(Store { root, db })
    }

    fn validate_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StoreError> {
        tracing::debug!(%bucket, %key, "Validating path");
        if bucket.is_empty() || bucket.contains("..") {
            let msg = format!("Invalid bucket: {}", bucket);
            tracing::warn!(%msg, "Path validation failed for bucket");
            return Err(StoreError::InvalidPath(msg));
        }

        if key.is_empty() || key.contains("../") || key.starts_with("..") {
            let msg = format!("Invalid key: {}", key);
            tracing::warn!(%msg, "Path validation failed for key");
            return Err(StoreError::InvalidPath(msg));
        }

        let path = self.root.join(bucket).join(key);
        tracing::debug!(?path, "Constructed valid path");
        Ok(path)
    }

    fn metadata_key(bucket: &str, key: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        tracing::debug!(%bucket, "Ensuring bucket exists");
        let bucket_path = self.root.join(bucket);
        if !bucket_path.exists() {
            tracing::info!(?bucket_path, "Creating bucket directory");
            fs::create_dir_all(&bucket_path).await.map_err(|e| {
                tracing::error!(error = %e, "Failed to create bucket directory");
                StoreError::IoError(e)
            })?;
        }

        let test_file = bucket_path.join(".initial");
        tracing::debug!(?test_file, "Testing write to bucket");
        fs::File::create(&test_file)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to create initial file");
                StoreError::IoError(e)
            })?
            .sync_all()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to sync initial file");
                StoreError::IoError(e)
            })?;
        fs::remove_file(&test_file).await.map_err(|e| {
            tracing::error!(error = %e, "Failed to remove initial file");
            StoreError::IoError(e)
        })?;
        tracing::debug!(?bucket_path, "Bucket directory verified");
        Ok(())
    }
}

pub trait Storage {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        data: impl Stream<Item = Result<Bytes, io::Error>> + Send + Unpin,
        metadata: Metadata,
    ) -> Result<(), StoreError>;

    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        (
            impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
            Metadata,
        ),
        StoreError,
    >;

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StoreError>;
}

impl Storage for Store {
    async fn put(
        &self,
        bucket: &str,
        key: &str,
        mut data: impl Stream<Item = Result<Bytes, io::Error>> + Send + Unpin,
        metadata: Metadata,
    ) -> Result<(), StoreError> {
        tracing::info!(%bucket, %key, "Starting PUT operation");
        let path = self.validate_path(bucket, key)?;
        self.ensure_bucket(bucket).await?;

        if let Some(parent) = path.parent() {
            tracing::debug!(?parent, "Creating parent directory");
            fs::create_dir_all(parent).await.map_err(|e| {
                tracing::error!(error = %e, "Failed to create parent directory");
                StoreError::IoError(e)
            })?;
        }

        tracing::debug!(?path, "Creating file");
        let mut file = fs::File::create(&path).await.map_err(|e| {
            tracing::error!(error = %e, "Failed to create file");
            StoreError::IoError(e)
        })?;

        let mut total_bytes = 0;
        while let Some(result) = data.next().await {
            let chunk = result.map_err(|e| {
                tracing::error!(error = %e, "Stream error");
                StoreError::IoError(e)
            })?;
            total_bytes += chunk.len();
            tracing::debug!(chunk_size = chunk.len(), "Writing file chunk");
            file.write_all(&chunk).await.map_err(|e| {
                tracing::error!(error = %e, "Write error");
                StoreError::IoError(e)
            })?;
        }
        tracing::debug!("Flushing file");
        file.flush().await.map_err(|e| {
            tracing::error!(error = %e, "Flush error");
            StoreError::IoError(e)
        })?;
        tracing::debug!("Syncing file");
        file.sync_all().await.map_err(|e| {
            tracing::error!(error = %e, "Sync error");
            StoreError::IoError(e)
        })?;

        let metadata_key = Self::metadata_key(bucket, key);
        tracing::debug!(%metadata_key, "Storing metadata in sled");
        let metadata_json = serde_json::to_vec(&metadata).map_err(|e| {
            tracing::error!(error = %e, "JSON serialization error");
            StoreError::JsonError(e)
        })?;
        self.db.insert(&metadata_key, metadata_json).map_err(|e| {
            tracing::error!(error = %e, "Failed to insert metadata to sled");
            StoreError::SledError(e)
        })?;

        tracing::info!(%bucket, %key, total_bytes, "PUT operation completed");
        Ok(())
    }

    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        (
            impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
            Metadata,
        ),
        StoreError,
    > {
        tracing::info!(%bucket, %key, "Starting GET operation");
        let path = self.validate_path(bucket, key)?;
        if !path.exists() {
            let msg = format!("File not found: {}/{}", bucket, key);
            tracing::warn!(%msg, "File not found");
            return Err(StoreError::NotFound(msg));
        }

        let metadata_key = Self::metadata_key(bucket, key);
        tracing::debug!(%metadata_key, "Retrieving metadata");
        let metadata = match self.db.get(&metadata_key)? {
            Some(data) => serde_json::from_slice(&data).map_err(|e| {
                tracing::error!(error = %e, "JSON deserialization error");
                StoreError::JsonError(e)
            })?,
            None => {
                let msg = format!("Metadata not found: {}/{}", bucket, key);
                tracing::warn!(%msg, "Metadata not found");
                return Err(StoreError::NotFound(msg));
            }
        };

        tracing::debug!(?path, "Opening file");
        let mut file = fs::File::open(&path).await.map_err(|e| {
            tracing::error!(error = %e,?path, "File open error");
            StoreError::IoError(e)
        })?;
        let (tx, rx) = mpsc::channel(1024);
        const CHUNK_SIZE: usize = 64 * 1024;

        let path_owned = path.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0; CHUNK_SIZE];
            while let Ok(n) = file.read(&mut buffer).await {
                if n == 0 {
                    break;
                }

                let chunk = Bytes::copy_from_slice(&buffer[..n]);
                if tx.send(Ok(chunk)).await.is_err() {
                    tracing::debug!(?path, "Receiver dropped");
                    break;
                }
                tracing::debug!(chunk_size = n, ?path, "Read chunk");
            }

            if let Err(e) = file.read(&mut buffer).await {
                tracing::error!(error = %e,?path, "File read error");
                let _ = tx.send(Err(e)).await;
            }

            drop(path_owned);
        });

        tracing::info!(%bucket, %key, "GET operation completed");
        Ok((ReceiverStream::new(rx), metadata))
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        tracing::info!(%bucket, %key, "Starting DELETE operation");
        let path = self.validate_path(bucket, key)?;
        if !path.exists() {
            let msg = format!("File not found: {}/{}", bucket, key);
            tracing::warn!(%msg, "File not found");
            return Err(StoreError::NotFound(msg));
        }

        tracing::debug!(?path, "Deleting file");
        fs::remove_file(&path).await.map_err(|e| {
            tracing::error!(error = %e, "File remove error");
            StoreError::IoError(e)
        })?;

        let metadata_key = Self::metadata_key(bucket, key);
        tracing::debug!(%metadata_key, "Removing metadata");
        self.db.remove(&metadata_key).map_err(|e| {
            tracing::error!(error = %e, ?metadata_key, "Failed to delete metadata");
            StoreError::SledError(e)
        })?;

        tracing::info!(%bucket, %key, "DELETE operation deleted");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tempfile::TempDir;
    use tracing_subscriber;

    async fn setup_store() -> (Store, TempDir) {
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .try_init()
            .ok();

        let temp_dir = TempDir::new().unwrap();
        tracing::debug!(temp_dir = ?temp_dir.path(), "Created temporary directory");
        let store = Store::new(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get_delete_success() {
        let (store, _temp_dir) = setup_store().await;
        let bucket = "test-bucket";
        let key = "test.txt";
        let data = vec![Bytes::from("Hello, World!")];
        let data_stream = stream::iter(data.into_iter().map(Ok));
        let metadata = Metadata {
            content_type: Some("text/plain".to_string()),
            content_length: Some(13),
            custom: HashMap::from([("x-amz-meta-foo".to_string(), "bar".to_string())]),
        };

        // Test PUT
        tracing::debug!(%bucket, %key, "Testing PUT operation");
        store
            .put(bucket, key, data_stream, metadata.clone())
            .await
            .unwrap();

        // Test GET
        tracing::debug!(%bucket, %key, "Testing GET operation");
        let (mut stream, retrieved_metadata) = store.get(bucket, key).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();
        tracing::debug!(chunk = ?chunk, "Retrieved chunk from GET");
        assert_eq!(chunk, Bytes::from("Hello, World!"));
        assert_eq!(retrieved_metadata.content_type, metadata.content_type);
        assert_eq!(retrieved_metadata.content_length, metadata.content_length);
        assert_eq!(retrieved_metadata.custom, metadata.custom);

        // Test DELETE
        tracing::debug!(%bucket, %key, "Testing DELETE operation");
        store.delete(bucket, key).await.unwrap();
        assert!(store.get(bucket, key).await.is_err());
    }

    #[tokio::test]
    async fn test_put_empty_file() {
        let (store, _temp_dir) = setup_store().await;
        let bucket = "test-bucket";
        let key = "empty.txt";
        let data = vec![];
        let data_stream = stream::iter(data.into_iter().map(Ok));
        let metadata = Metadata {
            content_type: Some("application/octet-stream".to_string()),
            content_length: Some(0),
            custom: HashMap::new(),
        };

        // Test PUT with empty stream
        tracing::debug!(%bucket, %key, "Testing PUT empty file");
        store.put(bucket, key, data_stream, metadata).await.unwrap();

        // Test GET
        tracing::debug!(%bucket, %key, "Testing GET empty file");
        let (mut stream, _) = store.get(bucket, key).await.unwrap();
        assert!(stream.next().await.is_none()); // Empty file should return no chunks
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let (store, _temp_dir) = setup_store().await;
        let data = vec![Bytes::from("Test")];
        let data_stream = stream::iter(data.into_iter().map(Ok));
        let metadata = Metadata {
            content_type: None,
            content_length: None,
            custom: HashMap::new(),
        };

        // Test invalid bucket
        tracing::debug!(bucket = "..", key = "test.txt", "Testing invalid bucket");
        let result = store.put("..", "test.txt", data_stream, metadata).await;
        assert!(matches!(result, Err(StoreError::InvalidPath(_))));

        // Test empty key
        tracing::debug!(bucket = "bucket", key = "", "Testing empty key");
        let result = store.get("bucket", "").await;
        assert!(matches!(result, Err(StoreError::InvalidPath(_))));
    }

    #[tokio::test]
    async fn test_file_not_found() {
        let (store, _temp_dir) = setup_store().await;

        // Test GET for non-existent file
        tracing::debug!(
            bucket = "bucket",
            key = "missing.txt",
            "Testing GET non-existent file"
        );
        let result = store.get("bucket", "missing.txt").await;
        assert!(matches!(result, Err(StoreError::NotFound(_))));

        // Test DELETE for non-existent file
        tracing::debug!(
            bucket = "bucket",
            key = "missing.txt",
            "Testing DELETE non-existent file"
        );
        let result = store.delete("bucket", "missing.txt").await;
        assert!(matches!(result, Err(StoreError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_large_file_streaming() {
        let (store, _temp_dir) = setup_store().await;
        let bucket = "test-bucket";
        let key = "large.txt";
        // Create a 1MB file (16 chunks of 64KB)
        let chunk_size = 64 * 1024;
        let num_chunks = 16;
        let data: Vec<Bytes> = (0..num_chunks)
            .map(|i| Bytes::from(vec![(i % 256) as u8; chunk_size]))
            .collect();
        let data_stream = stream::iter(data.clone().into_iter().map(Ok));
        let metadata = Metadata {
            content_type: Some("application/octet-stream".to_string()),
            content_length: Some((chunk_size * num_chunks) as u64),
            custom: HashMap::new(),
        };

        // Test PUT
        tracing::debug!(%bucket, %key, "Testing PUT large file");
        store.put(bucket, key, data_stream, metadata).await.unwrap();

        // Test GET
        tracing::debug!(%bucket, %key, "Testing GET large file");
        let (mut stream, _) = store.get(bucket, key).await.unwrap();
        let mut received_chunks = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            tracing::debug!(chunk_size = chunk.len(), "Received chunk");
            assert_eq!(chunk.len(), chunk_size);
            assert_eq!(chunk, data[received_chunks]);
            received_chunks += 1;
        }
        assert_eq!(received_chunks, num_chunks);
    }

    #[tokio::test]
    async fn test_complex_key() {
        let (store, _temp_dir) = setup_store().await;
        let bucket = "test-bucket";
        let key = "folder/subfolder/file.txt";
        let data = vec![Bytes::from("Complex path test")];
        let data_stream = stream::iter(data.into_iter().map(Ok));
        let metadata = Metadata {
            content_type: Some("text/plain".to_string()),
            content_length: Some(16),
            custom: HashMap::new(),
        };

        // Test PUT
        tracing::debug!(%bucket, %key, "Testing PUT complex key");
        store
            .put(bucket, key, data_stream, metadata.clone())
            .await
            .unwrap();

        // Test GET
        tracing::debug!(%bucket, %key, "Testing GET complex key");
        let (mut stream, retrieved_metadata) = store.get(bucket, key).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();
        tracing::debug!(chunk = ?chunk, "Retrieved chunk from GET");
        assert_eq!(chunk, Bytes::from("Complex path test"));
        assert_eq!(retrieved_metadata.content_type, metadata.content_type);
        assert_eq!(retrieved_metadata.content_length, metadata.content_length);
    }
}
