use axum::{
    body::Bytes, extract::{Path, State}, routing::get, Router,
    response::{IntoResponse, Response}
};
use my_data::{db::Engine, options::Options};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::signal;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "{}=debug,tower_http=debug,axum=trace",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().without_time())
        .init();

    // 启动 Engine 实例
    let opts = Options {
        dir_path: PathBuf::from("/tmp/bitcask-rs-http"),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(opts).unwrap());

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .layer((
            TraceLayer::new_for_http(),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(engine.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn get_handler(Path(key): Path<String>, State(eng): State<Arc<Engine>>) {
    let value = match eng.get(Bytes::from(key.to_string())) {
        Ok(val) => val,
        Err(e) => {
            if e != my_data::error::Errors::KeyNotFound {
                return ;
            }
        }
    }
}
