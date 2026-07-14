pub mod config;

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    BoxError, Json, Router, error_handling::HandleErrorLayer, extract::State, http::StatusCode,
    response::IntoResponse, routing::get,
};
use serde::Serialize;
use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::fs;
use tower::{ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    limit::RequestBodyLimitLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;
use uuid::Uuid;

use crate::config::Config;

const OWNER_NAMESPACE: &str = "v1";
const STORAGE_DIRECTORIES: [&str; 3] = ["blobs", "uploads", "maintenance"];

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveIdentity {
    pub owner_namespace: String,
    pub archive_instance_id: String,
    pub created_at: String,
}

#[derive(Clone)]
struct AppState {
    database: SqlitePool,
    storage: StorageLayout,
}

#[derive(Clone)]
struct StorageLayout {
    blobs: PathBuf,
    uploads: PathBuf,
    maintenance: PathBuf,
}

pub struct Service {
    state: Arc<AppState>,
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("persistent storage could not be initialized")]
    Storage(#[source] io::Error),
    #[error("metadata store could not be initialized")]
    Database(#[source] sqlx::Error),
    #[error("metadata schema could not be initialized")]
    Migration(#[source] sqlx::migrate::MigrateError),
    #[error("archive identity could not be initialized")]
    Identity(#[source] sqlx::Error),
    #[error("archive timestamp could not be generated")]
    Clock(#[source] time::error::Format),
    #[error("archive listener could not be bound")]
    Bind(#[source] io::Error),
    #[error("archive HTTP service stopped unexpectedly")]
    Serve(#[source] io::Error),
}

impl Service {
    /// Creates the persistent storage layout, applies migrations, and loads archive identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the persistent volume, metadata schema, or identity cannot be used.
    pub async fn bootstrap(config: &Config) -> Result<(Self, ArchiveIdentity), BootstrapError> {
        let storage = StorageLayout::create(&config.data_dir).await?;
        let database_path = config.data_dir.join("patwari.db");
        let connect_options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .disable_statement_logging();
        let database = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(connect_options)
            .await
            .map_err(BootstrapError::Database)?;

        MIGRATOR
            .run(&database)
            .await
            .map_err(BootstrapError::Migration)?;
        let identity = initialize_identity(&database).await?;

        Ok((
            Self {
                state: Arc::new(AppState { database, storage }),
            },
            identity,
        ))
    }

    pub fn router(&self, config: &Config) -> Router {
        let api = Router::new()
            .fallback(api_not_found)
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_timeout))
                    .layer(TimeoutLayer::new(config.request_timeout))
                    .layer(tower::limit::ConcurrencyLimitLayer::new(config.max_concurrency))
                    .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes))
                    .layer(
                        TraceLayer::new_for_http()
                            .make_span_with(|request: &axum::http::Request<_>| {
                                tracing::info_span!("http_request", method = %request.method())
                            })
                            .on_response(DefaultOnResponse::new().level(Level::INFO)),
                    ),
            );

        Router::new()
            .route("/healthz", get(liveness))
            .route("/readyz", get(readiness))
            .nest("/api/v1", api)
            .with_state(self.state.clone())
    }
}

impl StorageLayout {
    async fn create(data_dir: &Path) -> Result<Self, BootstrapError> {
        fs::create_dir_all(data_dir)
            .await
            .map_err(BootstrapError::Storage)?;

        let layout = Self {
            blobs: data_dir.join(STORAGE_DIRECTORIES[0]),
            uploads: data_dir.join(STORAGE_DIRECTORIES[1]),
            maintenance: data_dir.join(STORAGE_DIRECTORIES[2]),
        };
        for directory in [&layout.blobs, &layout.uploads, &layout.maintenance] {
            fs::create_dir_all(directory)
                .await
                .map_err(BootstrapError::Storage)?;
        }
        Ok(layout)
    }

    async fn is_usable(&self) -> bool {
        for directory in [&self.blobs, &self.uploads, &self.maintenance] {
            if !directory_is_writable(directory).await {
                return false;
            }
        }
        true
    }
}

async fn directory_is_writable(directory: &Path) -> bool {
    let probe = directory.join(format!(".patwari-ready-{}", Uuid::new_v4()));
    match fs::write(&probe, []).await {
        Ok(()) => fs::remove_file(probe).await.is_ok(),
        Err(_) => false,
    }
}

async fn initialize_identity(database: &SqlitePool) -> Result<ArchiveIdentity, BootstrapError> {
    let mut transaction = database.begin().await.map_err(BootstrapError::Identity)?;
    let created_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(BootstrapError::Clock)?;
    let archive_instance_id = Uuid::new_v4().to_string();

    sqlx::query(
        "INSERT INTO archive_metadata (singleton, owner_namespace, archive_instance_id, created_at)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(singleton) DO NOTHING",
    )
    .bind(OWNER_NAMESPACE)
    .bind(archive_instance_id)
    .bind(created_at)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapError::Identity)?;

    let row = sqlx::query_as::<_, (String, String, String)>(
        "SELECT owner_namespace, archive_instance_id, created_at
         FROM archive_metadata WHERE singleton = 1",
    )
    .fetch_one(&mut *transaction)
    .await
    .map_err(BootstrapError::Identity)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapError::Identity)?;

    Ok(ArchiveIdentity {
        owner_namespace: row.0,
        archive_instance_id: row.1,
        created_at: row.2,
    })
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "live" })
}

async fn api_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

async fn readiness(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let database_ready = sqlx::query("SELECT 1")
        .execute(&state.database)
        .await
        .is_ok();
    let storage_ready = state.storage.is_usable().await;

    if database_ready && storage_ready {
        (StatusCode::OK, Json(HealthResponse { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "not_ready",
            }),
        )
    }
}

async fn handle_timeout(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::REQUEST_TIMEOUT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// Binds and serves the archive HTTP endpoints.
///
/// # Errors
///
/// Returns an error when bootstrap or binding the configured listener fails.
pub async fn serve(config: Config) -> Result<(), BootstrapError> {
    let (service, identity) = Service::bootstrap(&config).await?;
    tracing::info!(
        archive_instance_id = %identity.archive_instance_id,
        owner_namespace = %identity.owner_namespace,
        "archive service initialized"
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(BootstrapError::Bind)?;
    tracing::info!("archive service listening");
    axum::serve(listener, service.router(&config))
        .await
        .map_err(BootstrapError::Serve)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    struct TestDataDir(PathBuf);

    impl TestDataDir {
        fn new() -> Self {
            let path = std::env::current_dir()
                .expect("current directory exists")
                .join("target")
                .join(format!("patwari-test-{}", Uuid::new_v4()));
            Self(path)
        }
    }

    impl Drop for TestDataDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_config(data_dir: &TestDataDir) -> Config {
        Config {
            data_dir: data_dir.0.clone(),
            ..Config::default()
        }
    }

    async fn response_status(app: Router, path: &'static str) -> (StatusCode, String) {
        let response = app
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request is valid"),
            )
            .await
            .expect("router response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body can be read")
            .to_bytes();
        (
            status,
            String::from_utf8(body.to_vec()).expect("health response is utf-8"),
        )
    }

    #[tokio::test]
    async fn bootstrap_from_empty_volume_creates_layout_and_identity_once() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);

        let (service, identity) = Service::bootstrap(&config)
            .await
            .expect("empty volume bootstraps");

        assert_eq!(identity.owner_namespace, OWNER_NAMESPACE);
        assert!(data_dir.0.join("patwari.db").is_file());
        for directory in STORAGE_DIRECTORIES {
            assert!(data_dir.0.join(directory).is_dir());
        }
        let metadata_rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM archive_metadata")
            .fetch_one(&service.state.database)
            .await
            .expect("metadata is readable");
        assert_eq!(metadata_rows.0, 1);
    }

    #[tokio::test]
    async fn restart_preserves_archive_identity() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);

        let (first, first_identity) = Service::bootstrap(&config).await.expect("first boot");
        drop(first);
        let (_second, second_identity) = Service::bootstrap(&config).await.expect("restart");

        assert_eq!(second_identity, first_identity);
    }

    #[tokio::test]
    async fn liveness_survives_storage_readiness_failure() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        fs::remove_dir(&service.state.storage.uploads).expect("uploads is empty");
        fs::write(&service.state.storage.uploads, "not a directory").expect("create storage fault");

        let (liveness_status, _) = response_status(service.router(&config), "/healthz").await;
        let (readiness_status, readiness_body) =
            response_status(service.router(&config), "/readyz").await;

        assert_eq!(liveness_status, StatusCode::OK);
        assert_eq!(readiness_status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(readiness_body.contains("not_ready"));
    }

    #[tokio::test]
    async fn healthy_archive_is_ready() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");

        let (status, body) = response_status(service.router(&config), "/readyz").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"ready\""));
    }

    #[tokio::test]
    async fn liveness_survives_database_readiness_failure() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        service.state.database.close().await;

        let (liveness_status, _) = response_status(service.router(&config), "/healthz").await;
        let (readiness_status, _) = response_status(service.router(&config), "/readyz").await;

        assert_eq!(liveness_status, StatusCode::OK);
        assert_eq!(readiness_status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
