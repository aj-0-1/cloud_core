mod api;
mod store;

use axum::middleware;
use std::net::SocketAddr;
use store::Store;
use tracing::{debug, info};
use tracing_subscriber;

async fn log_requests(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    debug!(method = %req.method(), uri = %req.uri(), "Received request");
    next.run(req).await
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt().with_env_filter("info").init();

    let store = Store::new("/tmp/s3_store").unwrap();
    let app = api::create_router(store).layer(middleware::from_fn(log_requests));
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Server running at http://{}", addr);

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .expect("Server failed to start");
}
