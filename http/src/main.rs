mod error;
use axum::routing::delete;
use axum::{
    Json, Router,
    body::Bytes,
    debug_handler,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
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
        .route("/get/{key}", get(get_handler))
        .route("/listkeys", get(list_handler))
        .route("/put", post(put_handler))
        .route("/delete/{key}", delete(delete_handler))
        .route("/stat", get(stat_handler))
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

async fn put_handler(
    State(eng): State<Arc<Engine>>,
    Json(data): Json<HashMap<String, String>>,
) -> Result<Response, error::MyError> {
    for (key, value) in data.iter() {
        if eng
            .put(Bytes::from(key.to_string()), Bytes::from(value.to_string()))
            .is_err()
        {
            return Err(error::MyError::FailToPutKey);
        }
    }

    Ok((StatusCode::OK, "OK").into_response())
}

async fn get_handler(
    Path(key): Path<String>,
    State(eng): State<Arc<Engine>>,
) -> Result<Response, error::MyError> {
    let value = match eng.get(Bytes::from(key.to_string())) {
        Ok(val) => val,
        Err(e) => {
            if e == my_data::error::Errors::KeyNotFound {
                return Ok((StatusCode::OK, "key not found").into_response());
            } else {
                return Err(error::MyError::FailToGetKeyInEngine);
            }
        }
    };
    Ok((StatusCode::OK, value).into_response())
}

async fn list_handler(State(eng): State<Arc<Engine>>) -> Result<Response, error::MyError> {
    let keys = match eng.list_keys() {
        Ok(keys) => keys,
        Err(_) => return Err(error::MyError::FailToListKeys),
    };

    let keys = keys
        .into_iter()
        .map(|key| String::from_utf8(key.to_vec()).unwrap())
        .collect::<Vec<String>>();

    let result = serde_json::to_string(&keys).unwrap();

    Ok((StatusCode::OK, Json(result)).into_response())
}

async fn delete_handler(
    State(eng): State<Arc<Engine>>,
    Path(key): Path<String>,
) -> Result<Response, error::MyError> {
    if let Err(e) = eng.delete(Bytes::from(key.to_string())) {
        if e != my_data::error::Errors::KeyIsEmpty {
            return Err(error::MyError::FailToDelte);
        }
    }
    Ok((StatusCode::OK, "OK").into_response())
}

async fn stat_handler(State(eng): State<Arc<Engine>>) -> Result<Response, error::MyError> {
    let stat = match eng.stat() {
        Ok(stat) => stat,
        Err(_) => return Err(error::MyError::FailToGetState),
    };

    let mut result = HashMap::new();
    result.insert("key_num", stat.key_num);
    result.insert("data_file_num", stat.data_file_num);
    result.insert("reclaim_size", stat.reclaim_size);
    result.insert("disk_size", stat.disk_size as usize);

    Ok((StatusCode::OK, Json(result)).into_response())
}
