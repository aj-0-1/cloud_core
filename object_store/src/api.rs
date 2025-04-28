use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures::stream::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::store::{Metadata, Storage, Store, StoreError};

impl IntoResponse for StoreError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            StoreError::IoError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error: {}", err),
            ),
            StoreError::SledError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", err),
            ),
            StoreError::JsonError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("JSON error: {}", err),
            ),
            StoreError::HttpError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("HTTP error: {}", err),
            ),
            StoreError::NotFound(msg) => {
                tracing::debug!(%msg, "Returning NotFound response");
                (StatusCode::NOT_FOUND, msg)
            }
            StoreError::InvalidPath(msg) => {
                tracing::debug!(%msg, "Returning InvalidPath response");
                (StatusCode::BAD_REQUEST, msg)
            }
        };
        tracing::error!(status = %status, %body, "Error response");
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/plain")
            .header(header::CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .unwrap_or_else(|e| {
                tracing::error!("Failed to build response: {}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("Response build error: {}", e)))
                    .unwrap()
            })
    }
}

fn extract_metadata(headers: &HeaderMap) -> Metadata {
    let mut custom = HashMap::new();
    for (name, value) in headers.iter() {
        if name.as_str().starts_with("x-amz-meta-") {
            if let Ok(value) = value.to_str() {
                custom.insert(name.as_str().to_string(), value.to_string());
            }
        }
    }

    Metadata {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        content_length: headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok()),
        custom,
    }
}

async fn put_handler(
    Path((bucket, key)): Path<(String, String)>,
    State(store): State<Arc<Store>>,
    headers: HeaderMap,
    body: Body,
) -> Result<(), StoreError> {
    tracing::info!(%bucket, %key, "PUT request received");
    if bucket.is_empty() {
        tracing::warn!("Empty bucket provided");
        return Err(StoreError::InvalidPath(
            "Empty bucket is not allowed".to_string(),
        ));
    }
    if key.is_empty() {
        tracing::warn!("Empty key provided");
        return Err(StoreError::InvalidPath(
            "Empty key is not allowed".to_string(),
        ));
    }

    let metadata = extract_metadata(&headers);
    tracing::debug!(?metadata, "Extracted metadata");

    let stream = body.into_data_stream().map_err(|e| {
        tracing::error!("Stream conversion error: {}", e);
        std::io::Error::new(std::io::ErrorKind::Other, e)
    });

    store.put(&bucket, &key, stream, metadata).await?;
    tracing::info!(%bucket, %key, "PUT request completed");
    Ok(())
}

async fn get_handler(
    Path((bucket, key)): Path<(String, String)>,
    State(store): State<Arc<Store>>,
) -> Result<Response, StoreError> {
    tracing::info!(%bucket, %key, "GET request received");
    if bucket.is_empty() {
        tracing::warn!("Empty bucket provided");
        return Err(StoreError::InvalidPath(
            "Empty bucket is not allowed".to_string(),
        ));
    }
    if key.is_empty() {
        tracing::warn!("Empty key provided");
        return Err(StoreError::InvalidPath(
            "Empty key is not allowed".to_string(),
        ));
    }

    let (stream, metadata) = store.get(&bucket, &key).await?;

    let mut response_builder = Response::builder().status(StatusCode::OK);
    if let Some(content_type) = metadata.content_type {
        response_builder = response_builder.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(content_length) = metadata.content_length {
        response_builder =
            response_builder.header(header::CONTENT_LENGTH, content_length.to_string());
    }
    for (key, value) in metadata.custom {
        response_builder = response_builder.header(&key, value);
    }

    let body = Body::from_stream(stream);
    let response = response_builder.body(body).map_err(|e| {
        tracing::error!("Response build error: {}", e);
        StoreError::HttpError(e)
    })?;
    tracing::info!(%bucket, %key, "GET request completed");
    Ok(response)
}

async fn delete_handler(
    Path((bucket, key)): Path<(String, String)>,
    State(store): State<Arc<Store>>,
) -> Result<(), StoreError> {
    tracing::info!(%bucket, %key, "DELETE request received");
    if bucket.is_empty() {
        tracing::warn!("Empty bucket provided");
        return Err(StoreError::InvalidPath(
            "Empty bucket is not allowed".to_string(),
        ));
    }
    if key.is_empty() {
        tracing::warn!("Empty key provided");
        return Err(StoreError::InvalidPath(
            "Empty key is not allowed".to_string(),
        ));
    }

    store.delete(&bucket, &key).await?;
    tracing::info!(%bucket, %key, "DELETE request completed");
    Ok(())
}

pub fn create_router(store: Store) -> Router {
    let store = Arc::new(store);
    Router::new()
        .route(
            "/:bucket/*key",
            get(get_handler).put(put_handler).delete(delete_handler),
        )
        .with_state(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use bytes::Bytes;
    use tempfile::TempDir;
    use tower::ServiceExt;
    use tracing_subscriber;

    async fn setup_app() -> (Router, TempDir) {
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .try_init()
            .ok();

        let temp_dir = TempDir::new().unwrap();
        tracing::debug!(temp_dir = ?temp_dir.path(), "Created temporary directory");
        let test_file = temp_dir.path().join("test.txt");
        tracing::debug!(test_file = ?test_file, "Testing write to temporary file");
        std::fs::write(&test_file, "test").unwrap_or_else(|e| {
            panic!("Failed to write to TempDir {:?}: {}", temp_dir.path(), e);
        });
        std::fs::remove_file(&test_file).unwrap();
        let store = Store::new(temp_dir.path()).unwrap();
        let app = create_router(store);
        (app, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get_delete_success() {
        let (app, _temp_dir) = setup_app().await;

        // Test PUT
        let request = Request::builder()
            .method("PUT")
            .uri("/test-bucket/test.txt")
            .header(header::CONTENT_TYPE, "text/plain")
            .header(header::CONTENT_LENGTH, "13")
            .header("x-amz-meta-foo", "bar")
            .body(Body::from("Hello, World!"))
            .unwrap();
        tracing::debug!(?request, "Sending PUT request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received PUT response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "PUT response body");
        assert_eq!(status, StatusCode::OK, "PUT failed: {}", body_str);

        // Test GET
        let request = Request::builder()
            .method("GET")
            .uri("/test-bucket/test.txt")
            .body(Body::empty())
            .unwrap();
        tracing::debug!(?request, "Sending GET request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received GET response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "GET response body");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(
            headers
                .get(header::CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            "13"
        );
        assert_eq!(headers.get("x-amz-meta-foo").unwrap(), "bar");
        assert_eq!(body, Bytes::from("Hello, World!"));

        // Test DELETE
        let request = Request::builder()
            .method("DELETE")
            .uri("/test-bucket/test.txt")
            .body(Body::empty())
            .unwrap();
        tracing::debug!(?request, "Sending DELETE request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received DELETE response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "DELETE response body");
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (app, _temp_dir) = setup_app().await;

        let request = Request::builder()
            .method("GET")
            .uri("/test-bucket/missing.txt")
            .body(Body::empty())
            .unwrap();
        tracing::debug!(?request, "Sending GET request");
        let response = app.oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received GET not found response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "GET not found response body");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            body_str.contains("File not found"),
            "Expected 'File not found' in body, got: {}",
            body_str
        );
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let (app, _temp_dir) = setup_app().await;

        // Use a valid route format but invalid path content
        let request = Request::builder()
            .method("PUT")
            .uri("/test-bucket/..")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Test"))
            .unwrap();
        tracing::debug!(?request, "Sending PUT request");
        let response = app.oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received invalid path response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "Invalid path response body");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body_str.contains("Invalid key"),
            "Expected 'Invalid key' in body, got: {}",
            body_str
        );
    }

    #[tokio::test]
    async fn test_large_file_streaming() {
        let (app, _temp_dir) = setup_app().await;
        let chunk_size = 64 * 1024;
        let num_chunks = 16;
        let large_data: Bytes = Bytes::from(vec![0u8; chunk_size * num_chunks]);

        // Test PUT
        let request = Request::builder()
            .method("PUT")
            .uri("/test-bucket/large.txt")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(
                header::CONTENT_LENGTH,
                (chunk_size * num_chunks).to_string(),
            )
            .body(Body::from(large_data.clone()))
            .unwrap();
        tracing::debug!(?request, "Sending PUT request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received PUT large response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "PUT large response body");
        assert_eq!(status, StatusCode::OK);

        // Test GET
        let request = Request::builder()
            .method("GET")
            .uri("/test-bucket/large.txt")
            .body(Body::empty())
            .unwrap();
        tracing::debug!(?request, "Sending GET request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received GET large response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "GET large response body");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        assert_eq!(
            headers
                .get(header::CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            (chunk_size * num_chunks).to_string()
        );
        assert_eq!(body.len(), chunk_size * num_chunks);
    }

    #[tokio::test]
    async fn test_complex_key() {
        let (app, _temp_dir) = setup_app().await;

        // Test PUT with complex key
        let request = Request::builder()
            .method("PUT")
            .uri("/test-bucket/folder/subfolder/file.txt")
            .header(header::CONTENT_TYPE, "text/plain")
            .header(header::CONTENT_LENGTH, "16")
            .header("x-amz-meta-foo", "bar")
            .body(Body::from("Complex path test"))
            .unwrap();
        tracing::debug!(?request, "Sending PUT request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received PUT complex response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "PUT complex response body");
        assert_eq!(status, StatusCode::OK);

        // Test GET
        let request = Request::builder()
            .method("GET")
            .uri("/test-bucket/folder/subfolder/file.txt")
            .body(Body::empty())
            .unwrap();
        tracing::debug!(?request, "Sending GET request");
        let response = app.clone().oneshot(request).await.unwrap();
        tracing::debug!(?response, "Received GET complex response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap_or("");
        tracing::debug!(body = %body_str, "GET complex response body");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(
            headers
                .get(header::CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            "16"
        );
        assert_eq!(headers.get("x-amz-meta-foo").unwrap(), "bar");
        assert_eq!(body, Bytes::from("Complex path test"));
    }
}
