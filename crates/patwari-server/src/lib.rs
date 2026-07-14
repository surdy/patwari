pub mod config;

use std::{
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    BoxError, Json, Router,
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{Path as AxumPath, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use http_body_util::BodyExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{
    ConnectOptions, FromRow, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
};
use tokio_util::io::ReaderStream;
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
const MAX_LOGICAL_PATH_BYTES: usize = 1024;
const MAX_METADATA_ENTRIES: usize = 64;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 1024;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
const MAX_SOURCE_AGENT_BYTES: usize = 128;
const MAX_SOURCE_SESSION_ID_BYTES: usize = 512;
const MAX_CONTEXT_VALUE_BYTES: usize = 512;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

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
    identity: ArchiveIdentity,
    max_artifact_bytes: u64,
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
    #[error("configured request limit cannot be represented")]
    RequestLimit,
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
        let max_artifact_bytes = u64::try_from(config.max_request_body_bytes)
            .map_err(|_| BootstrapError::RequestLimit)?;
        let state = Arc::new(AppState {
            database,
            storage,
            identity: identity.clone(),
            max_artifact_bytes,
        });

        Ok((Self { state }, identity))
    }

    pub fn router(&self, config: &Config) -> Router {
        let api = Router::new()
            .route("/clients/{client_id}", put(register_client))
            .route("/uploads", post(create_upload))
            .route(
                "/uploads/{upload_id}/artifacts/0/chunks/0",
                put(put_artifact_chunk),
            )
            .route("/uploads/{upload_id}/complete", post(complete_upload))
            .route("/snapshots/{snapshot_id}", get(get_snapshot))
            .route("/artifacts/{artifact_id}/content", get(download_artifact))
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

    fn upload_artifact_path(&self, upload_id: &str) -> PathBuf {
        self.uploads.join(upload_id).join("artifact")
    }

    fn blob_path(&self, stored_sha256: &str) -> PathBuf {
        self.blobs
            .join("sha256")
            .join(&stored_sha256[..2])
            .join(stored_sha256)
    }
}

async fn directory_is_writable(directory: &Path) -> bool {
    let probe = directory.join(format!(".patwari-ready-{}", Uuid::now_v7()));
    match fs::write(&probe, []).await {
        Ok(()) => fs::remove_file(probe).await.is_ok(),
        Err(_) => false,
    }
}

async fn initialize_identity(database: &SqlitePool) -> Result<ArchiveIdentity, BootstrapError> {
    let mut transaction = database.begin().await.map_err(BootstrapError::Identity)?;
    let created_at = now_rfc3339().map_err(BootstrapError::Clock)?;
    let archive_instance_id = Uuid::now_v7().to_string();

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

fn now_rfc3339() -> Result<String, time::error::Format> {
    OffsetDateTime::now_utc().format(&Rfc3339)
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "live" })
}

async fn api_not_found() -> ApiError {
    ApiError::not_found("endpoint_not_found", "API endpoint was not found")
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
        ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "request exceeded the configured time limit",
        )
    } else {
        ApiError::internal()
    }
}

/// Stable HTTP request and response documents for the versioned archive API.
pub mod contract {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RegisterClientRequest {
        pub hostname: Option<String>,
        pub display_name: Option<String>,
        #[serde(default)]
        pub metadata: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ClientResponse {
        pub client_id: String,
        pub hostname: Option<String>,
        pub display_name: Option<String>,
        pub metadata: BTreeMap<String, String>,
        pub created_at: String,
        pub updated_at: String,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CreateUploadRequest {
        pub client_id: String,
        pub idempotency_key: String,
        pub manifest: ManifestInput,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct UploadResponse {
        pub upload_id: String,
        pub session_id: String,
        pub status: UploadStatus,
        pub manifest_sha256: String,
        pub artifact_upload_url: String,
        pub completion_url: String,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ManifestInput {
        pub schema_version: u16,
        pub session: SessionInput,
        pub capture: CaptureInput,
        pub artifact: ArtifactInput,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Manifest {
        pub schema_version: u16,
        pub session: SessionInput,
        pub capture: Capture,
        pub artifact: Artifact,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct SessionInput {
        pub source_agent: String,
        pub source_session_id: String,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CaptureInput {
        pub captured_at: String,
        pub source_cursor: Option<String>,
        pub project: Option<String>,
        pub repository: Option<String>,
        pub branch: Option<String>,
        pub source_agent_version: Option<String>,
        pub munshi_version: Option<String>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Capture {
        pub captured_at: String,
        pub source_cursor: Option<String>,
        pub project: Option<String>,
        pub repository: Option<String>,
        pub branch: Option<String>,
        pub source_agent_version: Option<String>,
        pub munshi_version: Option<String>,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ArtifactInput {
        pub logical_path: String,
        pub media_type: Option<String>,
        pub original_size_bytes: u64,
        pub original_sha256: String,
        pub stored_size_bytes: u64,
        pub stored_sha256: String,
        pub compression: Compression,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Artifact {
        pub logical_path: String,
        pub media_type: Option<String>,
        pub original_size_bytes: u64,
        pub original_sha256: String,
        pub stored_size_bytes: u64,
        pub stored_sha256: String,
        pub compression: Compression,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Compression {
        Identity,
        Zstd,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum UploadStatus {
        Created,
        ArtifactUploaded,
        Completed,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct SnapshotResponse {
        pub snapshot_id: String,
        pub session_id: String,
        pub snapshot_fingerprint: String,
        pub manifest_sha256: String,
        pub completed_at: String,
        pub manifest: Manifest,
        pub artifacts: Vec<ArtifactResponse>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ArtifactResponse {
        pub artifact_id: String,
        pub logical_path: String,
        pub media_type: Option<String>,
        pub original_size_bytes: u64,
        pub original_sha256: String,
        pub stored_size_bytes: u64,
        pub stored_sha256: String,
        pub compression: Compression,
        pub content_url: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Receipt {
        pub receipt_version: u16,
        pub archive_instance_id: String,
        pub owner_namespace: String,
        pub snapshot_id: String,
        pub session_id: String,
        pub snapshot_fingerprint: String,
        pub manifest_sha256: String,
        pub artifact_count: u32,
        pub total_original_bytes: u64,
        pub total_stored_bytes: u64,
        pub completed_at: String,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct ErrorResponse {
        pub error: ErrorDetail,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct ErrorDetail {
        pub code: &'static str,
        pub message: &'static str,
    }
}

use contract::{
    Artifact, ArtifactResponse, Capture, CaptureInput, ClientResponse, Compression,
    CreateUploadRequest, ErrorDetail, ErrorResponse, Manifest, ManifestInput, Receipt,
    RegisterClientRequest, SessionInput, SnapshotResponse, UploadResponse, UploadStatus,
};

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }

    const fn invalid(message: &'static str) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_error",
            message,
        )
    }

    const fn conflict(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    const fn not_found(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    const fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "archive operation could not be completed",
        )
    }

    const fn storage() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            "archive storage could not be used",
        )
    }

    const fn database() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "metadata_error",
            "archive metadata could not be used",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

fn parse_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    payload.map(|Json(value)| value).map_err(|_| {
        ApiError::invalid("request body must be valid JSON with application/json content type")
    })
}

async fn register_client(
    AxumPath(client_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    payload: Result<Json<RegisterClientRequest>, JsonRejection>,
) -> Result<Json<ClientResponse>, ApiError> {
    let client_id = parse_uuid(&client_id, "client identifier is not a UUID")?;
    let request = parse_json(payload)?;
    validate_client_request(&request)?;
    let metadata_json =
        serde_json::to_string(&request.metadata).map_err(|_| ApiError::internal())?;
    let now = now_rfc3339().map_err(|_| ApiError::internal())?;

    sqlx::query(
        "INSERT INTO clients (
            id, owner_namespace, hostname, display_name, metadata_json, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
         ON CONFLICT(id) DO UPDATE SET
            hostname = excluded.hostname,
            display_name = excluded.display_name,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at",
    )
    .bind(client_id.to_string())
    .bind(&state.identity.owner_namespace)
    .bind(&request.hostname)
    .bind(&request.display_name)
    .bind(metadata_json)
    .bind(now)
    .execute(&state.database)
    .await
    .map_err(|_| ApiError::database())?;

    let row = sqlx::query_as::<_, ClientRow>(
        "SELECT id, hostname, display_name, metadata_json, created_at, updated_at
         FROM clients WHERE id = ?1",
    )
    .bind(client_id.to_string())
    .fetch_one(&state.database)
    .await
    .map_err(|_| ApiError::database())?;
    let metadata = serde_json::from_str(&row.metadata_json).map_err(|_| ApiError::internal())?;

    Ok(Json(ClientResponse {
        client_id: row.id,
        hostname: row.hostname,
        display_name: row.display_name,
        metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }))
}

async fn create_upload(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<CreateUploadRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<UploadResponse>), ApiError> {
    let request = parse_json(payload)?;
    let client_id = parse_uuid(&request.client_id, "client identifier is not a UUID")?;
    validate_idempotency_key(&request.idempotency_key)?;
    let manifest = normalize_manifest(request.manifest, state.max_artifact_bytes)?;
    let canonical_json = serde_json::to_string(&manifest).map_err(|_| ApiError::internal())?;
    let manifest_sha256 = sha256_hex(canonical_json.as_bytes());
    let now = now_rfc3339().map_err(|_| ApiError::internal())?;
    let created = persist_upload(
        &state,
        &client_id.to_string(),
        &request.idempotency_key,
        &manifest,
        canonical_json,
        &manifest_sha256,
        &now,
    )
    .await?;
    let response = upload_response(
        created.upload_id,
        created.session_id,
        &manifest_sha256,
        &created.status,
    )?;
    let status = if created.was_created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
}

struct CreatedUpload {
    upload_id: String,
    session_id: String,
    status: String,
    was_created: bool,
}

async fn persist_upload(
    state: &AppState,
    client_id: &str,
    idempotency_key: &str,
    manifest: &Manifest,
    canonical_json: String,
    manifest_sha256: &str,
    now: &str,
) -> Result<CreatedUpload, ApiError> {
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| ApiError::database())?;
    let session_id =
        get_or_create_session(&mut transaction, state, client_id, manifest, now).await?;

    let upload_id = Uuid::now_v7().to_string();
    let insert = sqlx::query(
        "INSERT INTO uploads (
            id, owner_namespace, session_id, client_id, idempotency_key, manifest_sha256, status,
            created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7)
         ON CONFLICT(owner_namespace, client_id, idempotency_key) DO NOTHING",
    )
    .bind(&upload_id)
    .bind(&state.identity.owner_namespace)
    .bind(&session_id)
    .bind(client_id)
    .bind(idempotency_key)
    .bind(manifest_sha256)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    let (upload_id, status, was_created) = if insert.rows_affected() == 1 {
        sqlx::query(
            "INSERT INTO manifests (id, upload_id, canonical_json, sha256, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&upload_id)
        .bind(canonical_json)
        .bind(manifest_sha256)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
        (upload_id, "created".to_owned(), true)
    } else {
        let existing = sqlx::query_as::<_, ExistingUploadRow>(
            "SELECT id, manifest_sha256, status FROM uploads
             WHERE owner_namespace = ?1 AND client_id = ?2 AND idempotency_key = ?3",
        )
        .bind(&state.identity.owner_namespace)
        .bind(client_id)
        .bind(idempotency_key)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
        if existing.manifest_sha256 != manifest_sha256 {
            return Err(ApiError::conflict(
                "idempotency_conflict",
                "idempotency key was already used for a different manifest",
            ));
        }
        (existing.id, existing.status, false)
    };
    transaction
        .commit()
        .await
        .map_err(|_| ApiError::database())?;
    Ok(CreatedUpload {
        upload_id,
        session_id,
        status,
        was_created,
    })
}

async fn get_or_create_session(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    state: &AppState,
    client_id: &str,
    manifest: &Manifest,
    now: &str,
) -> Result<String, ApiError> {
    let client_exists: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM clients WHERE id = ?1 AND owner_namespace = ?2")
            .bind(client_id)
            .bind(&state.identity.owner_namespace)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| ApiError::database())?;
    if client_exists.is_none() {
        return Err(ApiError::not_found(
            "client_not_found",
            "client must be registered first",
        ));
    }
    sqlx::query(
        "INSERT INTO sessions (
            id, owner_namespace, source_agent, source_session_id, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(owner_namespace, source_agent, source_session_id) DO UPDATE SET
            updated_at = excluded.updated_at",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(&state.identity.owner_namespace)
    .bind(&manifest.session.source_agent)
    .bind(&manifest.session.source_session_id)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    let session_id: (String,) = sqlx::query_as(
        "SELECT id FROM sessions
         WHERE owner_namespace = ?1 AND source_agent = ?2 AND source_session_id = ?3",
    )
    .bind(&state.identity.owner_namespace)
    .bind(&manifest.session.source_agent)
    .bind(&manifest.session.source_session_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    Ok(session_id.0)
}

async fn put_artifact_chunk(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    validate_octet_stream(&headers)?;
    let upload = get_upload(&state.database, &upload_id).await?;
    if upload.status == "completed" {
        return Err(ApiError::conflict(
            "upload_completed",
            "completed uploads do not accept artifact bytes",
        ));
    }

    let manifest = get_upload_manifest(&state.database, &upload_id).await?;
    let temporary_path = state
        .storage
        .uploads
        .join(&upload_id)
        .join(format!("artifact.{}.partial", Uuid::now_v7()));
    let final_path = state.storage.upload_artifact_path(&upload_id);
    fs::create_dir_all(
        temporary_path
            .parent()
            .expect("temporary artifact path has a parent"),
    )
    .await
    .map_err(|_| ApiError::storage())?;

    let write_result = write_body(
        body,
        &temporary_path,
        manifest.artifact.stored_size_bytes,
        &manifest.artifact.stored_sha256,
    )
    .await;
    if let Err(error) = write_result {
        remove_temporary_file(&temporary_path).await?;
        return Err(error);
    }

    let linked = match fs::hard_link(&temporary_path, &final_path).await {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(_) => {
            remove_temporary_file(&temporary_path).await?;
            return Err(ApiError::storage());
        }
    };
    if !linked {
        verify_stored_file(
            final_path.clone(),
            manifest.artifact.stored_size_bytes,
            manifest.artifact.stored_sha256.clone(),
        )
        .await?;
    }
    remove_temporary_file(&temporary_path).await?;

    if upload.status == "created" {
        let updated = sqlx::query(
            "UPDATE uploads SET status = 'artifact_uploaded'
             WHERE id = ?1 AND status = 'created'",
        )
        .bind(&upload_id)
        .execute(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
        if updated.rows_affected() == 0 {
            let current = get_upload(&state.database, &upload_id).await?;
            if current.status != "artifact_uploaded" {
                return Err(ApiError::conflict(
                    "upload_state_conflict",
                    "upload state changed while artifact bytes were submitted",
                ));
            }
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn complete_upload(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Receipt>, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    let upload = get_upload(&state.database, &upload_id).await?;
    if upload.status == "completed" {
        let snapshot_id = upload.snapshot_id.ok_or_else(ApiError::internal)?;
        return receipt_for_snapshot(&state, &snapshot_id).await.map(Json);
    }
    if upload.status != "artifact_uploaded" {
        return Err(ApiError::conflict(
            "artifact_not_uploaded",
            "the declared artifact bytes must be uploaded before completion",
        ));
    }

    let manifest = get_upload_manifest(&state.database, &upload_id).await?;
    let source_path = state.storage.upload_artifact_path(&upload_id);
    // Two completion requests can both observe `artifact_uploaded` and race to
    // verify/promote/record the same shared artifact file. Rather than operate
    // on the shared path directly, each request first takes a request-private
    // hard link to it (same filesystem, so this is a cheap metadata-only
    // operation) and verifies/promotes from that link. This keeps the shared
    // inode alive for every concurrent request even if another request commits
    // and removes the shared path first.
    let link_path = completion_link_path(&state.storage, &upload_id);
    if let Err(error) = fs::hard_link(&source_path, &link_path).await {
        if error.kind() == io::ErrorKind::NotFound {
            // The shared artifact is already gone. If a concurrent winner has
            // since completed the upload, hand back its receipt idempotently;
            // otherwise the artifact genuinely disappeared and completion
            // cannot proceed.
            let current = get_upload(&state.database, &upload_id).await?;
            if current.status == "completed" {
                let snapshot_id = current.snapshot_id.ok_or_else(ApiError::internal)?;
                return receipt_for_snapshot(&state, &snapshot_id).await.map(Json);
            }
            return Err(ApiError::conflict(
                "artifact_missing",
                "the uploaded artifact bytes are no longer available for completion",
            ));
        }
        return Err(ApiError::storage());
    }

    let outcome =
        complete_upload_from_link(&state, &upload_id, &upload, &manifest, &link_path).await;
    // Always remove this request's private link, on every success or error
    // path, without letting a cleanup failure mask the primary outcome above.
    if let Err(cleanup_error) = remove_temporary_file(&link_path).await {
        tracing::warn!(
            upload_id = %upload_id,
            error = ?cleanup_error,
            "failed to remove request-private completion link"
        );
    }
    let snapshot_id = outcome?;

    // Best-effort removal of the shared artifact now that every concurrent
    // completion request has taken (or already released) its own private
    // link to the same inode. Whichever request gets here first performs the
    // removal; a later request observes `NotFound`, which is not an error.
    remove_temporary_file(&source_path).await?;
    receipt_for_snapshot(&state, &snapshot_id).await.map(Json)
}

/// Builds a request-private path, on the same filesystem as the shared
/// artifact, for a hard link taken during completion.
fn completion_link_path(storage: &StorageLayout, upload_id: &str) -> PathBuf {
    storage
        .uploads
        .join(upload_id)
        .join(format!("artifact.completing.{}", Uuid::now_v7()))
}

async fn complete_upload_from_link(
    state: &AppState,
    upload_id: &str,
    upload: &UploadRow,
    manifest: &Manifest,
    link_path: &Path,
) -> Result<String, ApiError> {
    verify_artifact(link_path.to_path_buf(), &manifest.artifact).await?;
    promote_blob(
        &state.storage,
        link_path,
        &manifest.artifact.stored_sha256,
        manifest.artifact.stored_size_bytes,
    )
    .await?;
    record_completed_upload(state, upload_id, upload, manifest).await
}

async fn record_completed_upload(
    state: &AppState,
    upload_id: &str,
    upload: &UploadRow,
    manifest: &Manifest,
) -> Result<String, ApiError> {
    let fingerprint = snapshot_fingerprint(manifest)?;
    let now = now_rfc3339().map_err(|_| ApiError::internal())?;
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|error| classify_database_error(&error))?;
    let blob_id = get_or_create_blob(&mut transaction, state, &manifest.artifact, &now).await?;
    let manifest_id: (String,) = sqlx::query_as("SELECT id FROM manifests WHERE upload_id = ?1")
        .bind(upload_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    let candidate_snapshot_id = Uuid::now_v7().to_string();
    let snapshot_insert = sqlx::query(
        "INSERT INTO snapshots (
            id, owner_namespace, session_id, manifest_id, fingerprint_sha256, completed_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(session_id, fingerprint_sha256) DO NOTHING",
    )
    .bind(&candidate_snapshot_id)
    .bind(&state.identity.owner_namespace)
    .bind(&upload.session_id)
    .bind(&manifest_id.0)
    .bind(&fingerprint)
    .bind(&now)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    let snapshot_id: (String,) = sqlx::query_as(
        "SELECT id FROM snapshots WHERE session_id = ?1 AND fingerprint_sha256 = ?2",
    )
    .bind(&upload.session_id)
    .bind(&fingerprint)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;
    if snapshot_insert.rows_affected() == 1 {
        sqlx::query(
            "INSERT INTO artifacts (
                id, snapshot_id, blob_id, logical_path, media_type, original_size_bytes,
                original_sha256, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&snapshot_id.0)
        .bind(&blob_id)
        .bind(&manifest.artifact.logical_path)
        .bind(&manifest.artifact.media_type)
        .bind(to_sqlite_i64(manifest.artifact.original_size_bytes)?)
        .bind(digest_storage_value(&manifest.artifact.original_sha256))
        .bind(&now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    }

    let updated = sqlx::query(
        "UPDATE uploads SET status = 'completed', snapshot_id = ?1, completed_at = ?2
         WHERE id = ?3 AND status = 'artifact_uploaded'",
    )
    .bind(&snapshot_id.0)
    .bind(&now)
    .bind(upload_id)
    .execute(&mut *transaction)
    .await
    .map_err(|error| classify_database_error(&error))?;
    if updated.rows_affected() != 1 {
        // We lost the guarded state transition, most likely to a truly concurrent
        // duplicate completion request for the same upload. That request may have
        // already committed the very same snapshot (fingerprint-deduplicated above),
        // so completion is idempotent: if the upload is now completed and points at
        // the snapshot we computed, treat this as success rather than a conflict.
        let current: UploadRaceRow =
            sqlx::query_as("SELECT status, snapshot_id FROM uploads WHERE id = ?1")
                .bind(upload_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|_| ApiError::database())?;
        let already_completed_with_same_snapshot = current.status == "completed"
            && current.snapshot_id.as_deref() == Some(snapshot_id.0.as_str());
        if !already_completed_with_same_snapshot {
            return Err(ApiError::conflict(
                "upload_state_conflict",
                "upload state changed while completion was requested",
            ));
        }
    }
    transaction
        .commit()
        .await
        .map_err(|error| classify_database_error(&error))?;
    Ok(snapshot_id.0)
}

#[derive(FromRow)]
struct UploadRaceRow {
    status: String,
    snapshot_id: Option<String>,
}

/// Classifies a `sqlx` error, distinguishing transient `SQLite` lock contention
/// (`SQLITE_BUSY` / `SQLITE_LOCKED`) from genuine metadata failures.
///
/// Busy/locked conditions are surfaced as a retryable 409 rather than a 500 so
/// that clients racing to complete the same upload are told to retry instead of
/// receiving a misleading server error; all other database errors keep their
/// existing 500 classification.
fn classify_database_error(error: &sqlx::Error) -> ApiError {
    let is_busy = matches!(
        error,
        sqlx::Error::Database(db_error)
            if matches!(db_error.code().as_deref(), Some("5" | "6"))
    );
    if is_busy {
        ApiError::conflict(
            "upload_completion_contended",
            "archive metadata was busy completing a concurrent request; retry the completion",
        )
    } else {
        ApiError::database()
    }
}

async fn get_snapshot(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    snapshot_response(&state.database, &snapshot_id)
        .await
        .map(Json)
}

async fn download_artifact(
    AxumPath(artifact_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let artifact_id = parse_uuid(&artifact_id, "artifact identifier is not a UUID")?.to_string();
    let row = sqlx::query_as::<_, DownloadRow>(
        "SELECT a.id, b.stored_sha256, b.stored_size_bytes
         FROM artifacts a JOIN blobs b ON b.id = a.blob_id
         WHERE a.id = ?1",
    )
    .bind(&artifact_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("artifact_not_found", "artifact was not found"))?;
    let path = state.storage.blob_path(&row.stored_sha256);
    let file = fs::File::open(path)
        .await
        .map_err(|_| ApiError::storage())?;
    let size = u64::try_from(row.stored_size_bytes).map_err(|_| ApiError::internal())?;
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).map_err(|_| ApiError::internal())?,
    );
    response.headers_mut().insert(
        "x-patwari-stored-sha256",
        HeaderValue::from_str(&digest_document_value(&row.stored_sha256))
            .map_err(|_| ApiError::internal())?,
    );
    Ok(response)
}

fn validate_client_request(request: &RegisterClientRequest) -> Result<(), ApiError> {
    validate_optional_text(request.hostname.as_ref(), MAX_CONTEXT_VALUE_BYTES)?;
    validate_optional_text(request.display_name.as_ref(), MAX_CONTEXT_VALUE_BYTES)?;
    if request.metadata.len() > MAX_METADATA_ENTRIES {
        return Err(ApiError::invalid("metadata has too many entries"));
    }
    for (key, value) in &request.metadata {
        validate_nonempty_text(key, MAX_METADATA_KEY_BYTES, "metadata key is invalid")?;
        validate_text(value, MAX_METADATA_VALUE_BYTES, "metadata value is invalid")?;
    }
    Ok(())
}

fn validate_idempotency_key(key: &str) -> Result<(), ApiError> {
    validate_nonempty_text(key, MAX_IDEMPOTENCY_KEY_BYTES, "idempotency key is invalid")
}

fn normalize_manifest(input: ManifestInput, max_artifact_bytes: u64) -> Result<Manifest, ApiError> {
    if input.schema_version != 1 {
        return Err(ApiError::invalid(
            "manifest schema version is not supported",
        ));
    }
    validate_nonempty_text(
        &input.session.source_agent,
        MAX_SOURCE_AGENT_BYTES,
        "source agent is invalid",
    )?;
    validate_nonempty_text(
        &input.session.source_session_id,
        MAX_SOURCE_SESSION_ID_BYTES,
        "source session identifier is invalid",
    )?;
    let capture = normalize_capture(input.capture)?;
    validate_logical_path(&input.artifact.logical_path)?;
    validate_optional_media_type(input.artifact.media_type.as_ref())?;
    validate_digest(&input.artifact.original_sha256)?;
    validate_digest(&input.artifact.stored_sha256)?;
    validate_size(input.artifact.original_size_bytes, max_artifact_bytes)?;
    validate_size(input.artifact.stored_size_bytes, max_artifact_bytes)?;

    Ok(Manifest {
        schema_version: input.schema_version,
        session: input.session,
        capture,
        artifact: Artifact {
            logical_path: input.artifact.logical_path,
            media_type: input.artifact.media_type,
            original_size_bytes: input.artifact.original_size_bytes,
            original_sha256: input.artifact.original_sha256,
            stored_size_bytes: input.artifact.stored_size_bytes,
            stored_sha256: input.artifact.stored_sha256,
            compression: input.artifact.compression,
        },
    })
}

fn normalize_capture(input: CaptureInput) -> Result<Capture, ApiError> {
    let captured_at = OffsetDateTime::parse(&input.captured_at, &Rfc3339)
        .map_err(|_| ApiError::invalid("capture timestamp must be RFC 3339"))?
        .format(&Rfc3339)
        .map_err(|_| ApiError::internal())?;
    for value in [
        &input.source_cursor,
        &input.project,
        &input.repository,
        &input.branch,
        &input.source_agent_version,
        &input.munshi_version,
    ] {
        validate_optional_text(value.as_ref(), MAX_CONTEXT_VALUE_BYTES)?;
    }
    Ok(Capture {
        captured_at,
        source_cursor: input.source_cursor,
        project: input.project,
        repository: input.repository,
        branch: input.branch,
        source_agent_version: input.source_agent_version,
        munshi_version: input.munshi_version,
    })
}

fn validate_size(size: u64, max_artifact_bytes: u64) -> Result<(), ApiError> {
    if size > max_artifact_bytes || i64::try_from(size).is_err() {
        return Err(ApiError::invalid(
            "artifact size exceeds the configured bounded limit",
        ));
    }
    Ok(())
}

fn validate_logical_path(path: &str) -> Result<(), ApiError> {
    if path.is_empty()
        || path.len() > MAX_LOGICAL_PATH_BYTES
        || path.starts_with('/')
        || path.contains('\\')
        || path.bytes().any(|byte| byte.is_ascii_control())
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ApiError::invalid(
            "logical path must be a normalized relative regular-file path",
        ));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), ApiError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ApiError::invalid("hash must be a lowercase sha256 digest"));
    };
    if hex.len() != 64
        || !hex.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        })
    {
        return Err(ApiError::invalid("hash must be a lowercase sha256 digest"));
    }
    Ok(())
}

fn validate_optional_text(value: Option<&String>, maximum: usize) -> Result<(), ApiError> {
    if let Some(value) = value {
        validate_text(value, maximum, "text value is invalid")?;
    }
    Ok(())
}

fn validate_optional_media_type(value: Option<&String>) -> Result<(), ApiError> {
    if let Some(value) = value {
        validate_text(value, MAX_CONTEXT_VALUE_BYTES, "media type is invalid")?;
        value
            .parse::<mime::Mime>()
            .map_err(|_| ApiError::invalid("media type is invalid"))?;
    }
    Ok(())
}

fn validate_nonempty_text(
    value: &str,
    maximum: usize,
    message: &'static str,
) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::invalid(message));
    }
    validate_text(value, maximum, message)
}

fn validate_text(value: &str, maximum: usize, message: &'static str) -> Result<(), ApiError> {
    if value.len() > maximum || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ApiError::invalid(message));
    }
    Ok(())
}

fn validate_octet_stream(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if content_type != Some("application/octet-stream") {
        return Err(ApiError::invalid(
            "artifact upload requires application/octet-stream content type",
        ));
    }
    Ok(())
}

fn parse_uuid(value: &str, message: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| ApiError::invalid(message))
}

fn upload_response(
    upload_id: String,
    session_id: String,
    manifest_sha256: &str,
    status: &str,
) -> Result<UploadResponse, ApiError> {
    Ok(UploadResponse {
        artifact_upload_url: format!("/api/v1/uploads/{upload_id}/artifacts/0/chunks/0"),
        completion_url: format!("/api/v1/uploads/{upload_id}/complete"),
        upload_id,
        session_id,
        status: upload_status(status)?,
        manifest_sha256: digest_document_value(manifest_sha256),
    })
}

fn upload_status(status: &str) -> Result<UploadStatus, ApiError> {
    match status {
        "created" => Ok(UploadStatus::Created),
        "artifact_uploaded" => Ok(UploadStatus::ArtifactUploaded),
        "completed" => Ok(UploadStatus::Completed),
        _ => Err(ApiError::internal()),
    }
}

async fn write_body(
    mut body: Body,
    temporary_path: &Path,
    expected_size: u64,
    expected_digest: &str,
) -> Result<(), ApiError> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary_path)
        .await
        .map_err(|_| ApiError::storage())?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    while let Some(frame) = body.frame().await {
        let frame =
            frame.map_err(|_| ApiError::invalid("artifact request body could not be read"))?;
        if let Ok(data) = frame.into_data() {
            let chunk_len = u64::try_from(data.len()).map_err(|_| ApiError::internal())?;
            size = size
                .checked_add(chunk_len)
                .ok_or_else(|| ApiError::invalid("artifact size is invalid"))?;
            if size > expected_size {
                return Err(ApiError::invalid(
                    "artifact size does not match the manifest",
                ));
            }
            hasher.update(&data);
            writer
                .write_all(&data)
                .await
                .map_err(|_| ApiError::storage())?;
        }
    }
    writer.flush().await.map_err(|_| ApiError::storage())?;
    writer
        .into_inner()
        .sync_all()
        .await
        .map_err(|_| ApiError::storage())?;
    if size != expected_size
        || hex_digest(hasher.finalize()) != digest_storage_value(expected_digest)
    {
        return Err(ApiError::invalid(
            "artifact stored size or hash does not match the manifest",
        ));
    }
    Ok(())
}

async fn remove_temporary_file(path: &Path) -> Result<(), ApiError> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ApiError::storage()),
    }
}

async fn verify_artifact(path: PathBuf, artifact: &Artifact) -> Result<(), ApiError> {
    let artifact = artifact.clone();
    tokio::task::spawn_blocking(move || verify_artifact_blocking(&path, &artifact))
        .await
        .map_err(|_| ApiError::internal())?
}

fn verify_artifact_blocking(path: &Path, artifact: &Artifact) -> Result<(), ApiError> {
    let mut file = std::fs::File::open(path).map_err(|_| ApiError::storage())?;
    let (stored_size, stored_digest) =
        hash_reader(&mut file, artifact.stored_size_bytes, ApiError::storage)?;
    if stored_size != artifact.stored_size_bytes
        || stored_digest != digest_storage_value(&artifact.stored_sha256)
    {
        return Err(ApiError::invalid(
            "artifact stored size or hash does not match the manifest",
        ));
    }

    let file = std::fs::File::open(path).map_err(|_| ApiError::storage())?;
    // Genuine file open/storage failures (e.g. disk I/O errors) remain 500s, but a
    // valid-header zstd stream that is corrupt or truncated mid-frame is a client
    // validation failure (422), not a server storage failure. `io_error` selects
    // the classification appropriate to the underlying reader.
    let (mut original, io_error): (Box<dyn Read>, fn() -> ApiError) = match artifact.compression {
        Compression::Identity => (Box::new(file), ApiError::storage),
        Compression::Zstd => (
            Box::new(
                zstd::stream::read::Decoder::new(file)
                    .map_err(|_| ApiError::invalid("artifact is not a valid zstd stream"))?,
            ),
            zstd_decode_error,
        ),
    };
    let (original_size, original_digest) =
        hash_reader(&mut original, artifact.original_size_bytes, io_error)?;
    if original_size != artifact.original_size_bytes
        || original_digest != digest_storage_value(&artifact.original_sha256)
    {
        return Err(ApiError::invalid(
            "artifact original size or hash does not match the manifest",
        ));
    }
    Ok(())
}

fn zstd_decode_error() -> ApiError {
    ApiError::invalid("artifact zstd stream is corrupt or truncated")
}

fn hash_reader(
    reader: &mut dyn Read,
    maximum_size: u64,
    io_error: fn() -> ApiError,
) -> Result<(u64, String), ApiError> {
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    loop {
        let read = reader.read(&mut buffer).map_err(|_| io_error())?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).expect("buffer length fits in u64"))
            .ok_or_else(|| ApiError::invalid("artifact size is invalid"))?;
        if total > maximum_size {
            return Err(ApiError::invalid(
                "artifact size exceeds its declared bound",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    Ok((total, hex_digest(hasher.finalize())))
}

async fn verify_stored_file(
    path: PathBuf,
    expected_size: u64,
    expected_digest: String,
) -> Result<(), ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(path).map_err(|_| ApiError::storage())?;
        let (size, digest) = hash_reader(&mut file, expected_size, ApiError::storage)?;
        if size == expected_size && digest == digest_storage_value(&expected_digest) {
            Ok(())
        } else {
            Err(ApiError::storage())
        }
    })
    .await
    .map_err(|_| ApiError::internal())?
}

async fn promote_blob(
    storage: &StorageLayout,
    source_path: &Path,
    stored_sha256: &str,
    stored_size: u64,
) -> Result<(), ApiError> {
    let destination = storage.blob_path(digest_storage_value(stored_sha256));
    let parent = destination.parent().expect("blob path has a parent");
    fs::create_dir_all(parent)
        .await
        .map_err(|_| ApiError::storage())?;
    match fs::hard_link(source_path, &destination).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            verify_stored_file(destination, stored_size, stored_sha256.to_owned()).await
        }
        Err(_) => Err(ApiError::storage()),
    }
}

async fn get_or_create_blob(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    state: &AppState,
    artifact: &Artifact,
    now: &str,
) -> Result<String, ApiError> {
    let blob_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO blobs (
            id, owner_namespace, stored_sha256, stored_size_bytes, compression, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(owner_namespace, stored_sha256) DO NOTHING",
    )
    .bind(&blob_id)
    .bind(&state.identity.owner_namespace)
    .bind(digest_storage_value(&artifact.stored_sha256))
    .bind(to_sqlite_i64(artifact.stored_size_bytes)?)
    .bind(compression_name(artifact.compression))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    let row = sqlx::query_as::<_, BlobRow>(
        "SELECT id, stored_size_bytes, compression FROM blobs
         WHERE owner_namespace = ?1 AND stored_sha256 = ?2",
    )
    .bind(&state.identity.owner_namespace)
    .bind(digest_storage_value(&artifact.stored_sha256))
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    if row.stored_size_bytes != to_sqlite_i64(artifact.stored_size_bytes)?
        || row.compression != compression_name(artifact.compression)
    {
        return Err(ApiError::conflict(
            "blob_representation_conflict",
            "stored bytes are already associated with a different representation",
        ));
    }
    Ok(row.id)
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::Identity => "identity",
        Compression::Zstd => "zstd",
    }
}

/// Computes the identity fingerprint of a snapshot's content.
///
/// The fingerprint intentionally covers only schema/session identity, the stable
/// capture context (`project`, `repository`, `branch`, `source_agent_version`), and
/// the original artifact's semantic fields (`logical_path`, original size/hash).
/// It deliberately excludes provenance and representation details that can differ
/// between otherwise-identical uploads without changing the snapshot's meaning:
/// `captured_at` (client-supplied time), `source_cursor` (client cursor), and
/// `munshi_version` (client software version) from `Capture`, and the stored
/// representation (`stored_size_bytes`, `stored_sha256`, `compression`) from
/// `Artifact`.
fn snapshot_fingerprint(manifest: &Manifest) -> Result<String, ApiError> {
    #[derive(Serialize)]
    struct Fingerprint<'a> {
        schema_version: u16,
        session: &'a SessionInput,
        capture: StableCapture<'a>,
        artifact: OriginalArtifact<'a>,
    }
    #[derive(Serialize)]
    struct StableCapture<'a> {
        project: &'a Option<String>,
        repository: &'a Option<String>,
        branch: &'a Option<String>,
        source_agent_version: &'a Option<String>,
    }
    #[derive(Serialize)]
    struct OriginalArtifact<'a> {
        logical_path: &'a str,
        original_size_bytes: u64,
        original_sha256: &'a str,
    }

    let fingerprint = Fingerprint {
        schema_version: manifest.schema_version,
        session: &manifest.session,
        capture: StableCapture {
            project: &manifest.capture.project,
            repository: &manifest.capture.repository,
            branch: &manifest.capture.branch,
            source_agent_version: &manifest.capture.source_agent_version,
        },
        artifact: OriginalArtifact {
            logical_path: &manifest.artifact.logical_path,
            original_size_bytes: manifest.artifact.original_size_bytes,
            original_sha256: &manifest.artifact.original_sha256,
        },
    };
    serde_json::to_vec(&fingerprint)
        .map(|value| sha256_hex(&value))
        .map_err(|_| ApiError::internal())
}

async fn get_upload(database: &SqlitePool, upload_id: &str) -> Result<UploadRow, ApiError> {
    sqlx::query_as("SELECT id, session_id, status, snapshot_id FROM uploads WHERE id = ?1")
        .bind(upload_id)
        .fetch_optional(database)
        .await
        .map_err(|_| ApiError::database())?
        .ok_or_else(|| ApiError::not_found("upload_not_found", "upload was not found"))
}

async fn get_upload_manifest(database: &SqlitePool, upload_id: &str) -> Result<Manifest, ApiError> {
    let row: (String,) =
        sqlx::query_as("SELECT canonical_json FROM manifests WHERE upload_id = ?1")
            .bind(upload_id)
            .fetch_one(database)
            .await
            .map_err(|_| ApiError::database())?;
    serde_json::from_str(&row.0).map_err(|_| ApiError::internal())
}

async fn receipt_for_snapshot(state: &AppState, snapshot_id: &str) -> Result<Receipt, ApiError> {
    let row = sqlx::query_as::<_, ReceiptRow>(
        "SELECT s.id, s.session_id, s.fingerprint_sha256, s.completed_at, m.sha256 AS manifest_sha256,
                a.original_size_bytes, b.stored_size_bytes
         FROM snapshots s
         JOIN manifests m ON m.id = s.manifest_id
         JOIN artifacts a ON a.snapshot_id = s.id
         JOIN blobs b ON b.id = a.blob_id
         WHERE s.id = ?1",
    )
    .bind(snapshot_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    Ok(Receipt {
        receipt_version: 1,
        archive_instance_id: state.identity.archive_instance_id.clone(),
        owner_namespace: state.identity.owner_namespace.clone(),
        snapshot_id: row.id,
        session_id: row.session_id,
        snapshot_fingerprint: digest_document_value(&row.fingerprint_sha256),
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        artifact_count: 1,
        total_original_bytes: u64::try_from(row.original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        total_stored_bytes: u64::try_from(row.stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        completed_at: row.completed_at,
    })
}

async fn snapshot_response(
    database: &SqlitePool,
    snapshot_id: &str,
) -> Result<SnapshotResponse, ApiError> {
    let row = sqlx::query_as::<_, SnapshotRow>(
        "SELECT s.id, s.session_id, s.fingerprint_sha256, s.completed_at, m.sha256 AS manifest_sha256,
                m.canonical_json
         FROM snapshots s JOIN manifests m ON m.id = s.manifest_id
         WHERE s.id = ?1",
    )
    .bind(snapshot_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    let manifest: Manifest =
        serde_json::from_str(&row.canonical_json).map_err(|_| ApiError::internal())?;
    let artifacts = sqlx::query_as::<_, ArtifactRow>(
        "SELECT a.id, a.logical_path, a.media_type, a.original_size_bytes, a.original_sha256,
                b.stored_size_bytes, b.stored_sha256, b.compression
         FROM artifacts a JOIN blobs b ON b.id = a.blob_id WHERE a.snapshot_id = ?1
         ORDER BY a.logical_path",
    )
    .bind(snapshot_id)
    .fetch_all(database)
    .await
    .map_err(|_| ApiError::database())?
    .into_iter()
    .map(artifact_response)
    .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotResponse {
        snapshot_id: row.id,
        session_id: row.session_id,
        snapshot_fingerprint: digest_document_value(&row.fingerprint_sha256),
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        completed_at: row.completed_at,
        manifest,
        artifacts,
    })
}

fn artifact_response(row: ArtifactRow) -> Result<ArtifactResponse, ApiError> {
    Ok(ArtifactResponse {
        content_url: format!("/api/v1/artifacts/{}/content", row.id),
        artifact_id: row.id,
        logical_path: row.logical_path,
        media_type: row.media_type,
        original_size_bytes: u64::try_from(row.original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        original_sha256: digest_document_value(&row.original_sha256),
        stored_size_bytes: u64::try_from(row.stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        stored_sha256: digest_document_value(&row.stored_sha256),
        compression: match row.compression.as_str() {
            "identity" => Compression::Identity,
            "zstd" => Compression::Zstd,
            _ => return Err(ApiError::internal()),
        },
    })
}

fn to_sqlite_i64(value: u64) -> Result<i64, ApiError> {
    i64::try_from(value)
        .map_err(|_| ApiError::invalid("artifact size exceeds the configured bounded limit"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let bytes = digest.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn digest_storage_value(value: &str) -> &str {
    value.strip_prefix("sha256:").unwrap_or(value)
}

fn digest_document_value(value: &str) -> String {
    format!("sha256:{}", digest_storage_value(value))
}

#[derive(FromRow)]
struct ClientRow {
    id: String,
    hostname: Option<String>,
    display_name: Option<String>,
    metadata_json: String,
    created_at: String,
    updated_at: String,
}

#[derive(FromRow)]
struct ExistingUploadRow {
    id: String,
    manifest_sha256: String,
    status: String,
}

#[derive(FromRow)]
struct UploadRow {
    session_id: String,
    status: String,
    snapshot_id: Option<String>,
}

#[derive(FromRow)]
struct BlobRow {
    id: String,
    stored_size_bytes: i64,
    compression: String,
}

#[derive(FromRow)]
struct ReceiptRow {
    id: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_sha256: String,
    completed_at: String,
    original_size_bytes: i64,
    stored_size_bytes: i64,
}

#[derive(FromRow)]
struct SnapshotRow {
    id: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_sha256: String,
    completed_at: String,
    canonical_json: String,
}

#[derive(FromRow)]
struct ArtifactRow {
    id: String,
    logical_path: String,
    media_type: Option<String>,
    original_size_bytes: i64,
    original_sha256: String,
    stored_size_bytes: i64,
    stored_sha256: String,
    compression: String,
}

#[derive(FromRow)]
struct DownloadRow {
    stored_sha256: String,
    stored_size_bytes: i64,
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

    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    struct TestDataDir(PathBuf);

    impl TestDataDir {
        fn new() -> Self {
            let path = std::env::current_dir()
                .expect("current directory exists")
                .join("target")
                .join(format!("patwari-test-{}", Uuid::now_v7()));
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

    async fn call(app: Router, request: Request<Body>) -> (StatusCode, HeaderMap, Vec<u8>) {
        let response = app.oneshot(request).await.expect("router response");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body can be read")
            .to_bytes()
            .to_vec();
        (status, headers, bytes)
    }

    fn json_request<T: Serialize + ?Sized>(method: &str, uri: &str, body: &T) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_string(body).expect("test JSON serializes"),
            ))
            .expect("request is valid")
    }

    fn digest(bytes: &[u8]) -> String {
        digest_document_value(&sha256_hex(bytes))
    }

    fn manifest(original: &[u8], stored: &[u8], compression: Compression) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "session": {
                "source_agent": "copilot-cli",
                "source_session_id": "source-session-1"
            },
            "capture": {
                "captured_at": "2026-07-13T20:00:00Z",
                "source_cursor": "1",
                "project": "patwari",
                "repository": "surdy/patwari",
                "branch": "main",
                "source_agent_version": "1.0",
                "munshi_version": "1.0"
            },
            "artifact": {
                "logical_path": "events.jsonl",
                "media_type": "application/x-ndjson",
                "original_size_bytes": original.len(),
                "original_sha256": digest(original),
                "stored_size_bytes": stored.len(),
                "stored_sha256": digest(stored),
                "compression": compression
            }
        })
    }

    async fn register(app: Router, client_id: Uuid) {
        let (status, _, _) = call(
            app,
            json_request(
                "PUT",
                &format!("/api/v1/clients/{client_id}"),
                &serde_json::json!({
                    "hostname": "developer-host",
                    "display_name": "Developer",
                    "metadata": {"munshi": "1.0"}
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn bootstrap_from_empty_volume_creates_layout_and_identity_once() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);

        let (service, identity) = Service::bootstrap(&config)
            .await
            .expect("empty volume bootstraps");

        assert_eq!(identity.owner_namespace, OWNER_NAMESPACE);
        assert!(
            Uuid::parse_str(&identity.archive_instance_id)
                .expect("archive identity is a UUID")
                .get_version()
                .is_some_and(|version| version == uuid::Version::SortRand)
        );
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
    async fn client_registration_is_idempotent_and_metadata_is_mutable() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        let uri = format!("/api/v1/clients/{client_id}");

        let (first_status, _, _) = call(
            service.router(&config),
            json_request(
                "PUT",
                &uri,
                &serde_json::json!({
                    "hostname": "first-host",
                    "display_name": "First",
                    "metadata": {"version": "1"}
                }),
            ),
        )
        .await;
        assert_eq!(first_status, StatusCode::OK);
        let (second_status, _, body) = call(
            service.router(&config),
            json_request(
                "PUT",
                &uri,
                &serde_json::json!({
                    "hostname": "second-host",
                    "display_name": "Second",
                    "metadata": {"version": "2"}
                }),
            ),
        )
        .await;
        assert_eq!(second_status, StatusCode::OK);
        let client: ClientResponse = serde_json::from_slice(&body).expect("client response parses");
        assert_eq!(client.client_id, client_id.to_string());
        assert_eq!(client.hostname.as_deref(), Some("second-host"));
        assert_eq!(client.metadata["version"], "2");
        let rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM clients")
            .fetch_one(&service.state.database)
            .await
            .expect("clients are readable");
        assert_eq!(rows.0, 1);
    }

    #[tokio::test]
    async fn one_artifact_archive_survives_restart_and_completion_retry() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, identity) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        register(service.router(&config), client_id).await;

        let bytes = b"{\"event\":\"archived\"}\n";
        let create = serde_json::json!({
            "client_id": client_id.to_string(),
            "idempotency_key": "capture-1",
            "manifest": manifest(bytes, bytes, Compression::Identity)
        });
        let (status, _, body) = call(
            service.router(&config),
            json_request("POST", "/api/v1/uploads", &create),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let upload: UploadResponse = serde_json::from_slice(&body).expect("upload response parses");
        let upload_id = upload.upload_id.clone();

        let (repeated_status, _, repeated_body) = call(
            service.router(&config),
            json_request("POST", "/api/v1/uploads", &create),
        )
        .await;
        assert_eq!(repeated_status, StatusCode::OK);
        let repeated: UploadResponse =
            serde_json::from_slice(&repeated_body).expect("idempotent upload parses");
        assert_eq!(repeated.upload_id, upload_id);

        let (upload_status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("PUT")
                .uri(&upload.artifact_upload_url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(bytes.as_slice()))
                .expect("request is valid"),
        )
        .await;
        assert_eq!(upload_status, StatusCode::NO_CONTENT);

        let (complete_status, _, receipt_bytes) = call(
            service.router(&config),
            Request::builder()
                .method("POST")
                .uri(&upload.completion_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(complete_status, StatusCode::OK);
        let receipt: Receipt = serde_json::from_slice(&receipt_bytes).expect("receipt parses");
        assert_eq!(receipt.archive_instance_id, identity.archive_instance_id);
        assert_eq!(receipt.artifact_count, 1);

        let (retry_status, _, retry_receipt_bytes) = call(
            service.router(&config),
            Request::builder()
                .method("POST")
                .uri(&upload.completion_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(retry_status, StatusCode::OK);
        assert_eq!(retry_receipt_bytes, receipt_bytes);
        drop(service);

        let (restarted, restarted_identity) = Service::bootstrap(&config).await.expect("restarts");
        assert_eq!(restarted_identity, identity);
        let (snapshot_status, _, snapshot_bytes) = call(
            restarted.router(&config),
            Request::builder()
                .uri(format!("/api/v1/snapshots/{}", receipt.snapshot_id))
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(snapshot_status, StatusCode::OK);
        let snapshot: SnapshotResponse =
            serde_json::from_slice(&snapshot_bytes).expect("snapshot response parses");
        assert_eq!(snapshot.manifest.artifact.logical_path, "events.jsonl");
        assert_eq!(snapshot.artifacts.len(), 1);

        let (download_status, headers, downloaded) = call(
            restarted.router(&config),
            Request::builder()
                .uri(&snapshot.artifacts[0].content_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(download_status, StatusCode::OK);
        assert_eq!(downloaded, bytes);
        assert_eq!(headers["x-patwari-stored-sha256"], digest(bytes));
    }

    #[tokio::test]
    async fn zstd_completion_verifies_decompressed_original_bytes() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        register(service.router(&config), client_id).await;

        let original = b"repeated source event\nrepeated source event\n";
        let stored = zstd::stream::encode_all(&original[..], 1).expect("compresses");
        let (status, _, body) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "idempotency_key": "capture-zstd",
                    "manifest": manifest(original, &stored, Compression::Zstd)
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let upload: UploadResponse = serde_json::from_slice(&body).expect("upload parses");
        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("PUT")
                .uri(&upload.artifact_upload_url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(stored))
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("POST")
                .uri(&upload.completion_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_non_normalized_path_and_corrupt_original_claim() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        register(service.router(&config), client_id).await;
        let bytes = b"contents";
        let mut invalid_manifest = manifest(bytes, bytes, Compression::Identity);
        invalid_manifest["artifact"]["logical_path"] = serde_json::json!("../source");
        let (status, _, _) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "idempotency_key": "bad-path",
                    "manifest": invalid_manifest
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let mut invalid_original = manifest(bytes, bytes, Compression::Identity);
        invalid_original["artifact"]["original_sha256"] = serde_json::json!(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        );
        let (status, _, body) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "idempotency_key": "bad-original",
                    "manifest": invalid_original
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let upload: UploadResponse = serde_json::from_slice(&body).expect("upload parses");
        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("PUT")
                .uri(&upload.artifact_upload_url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(bytes.as_slice()))
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("POST")
                .uri(&upload.completion_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    fn sample_manifest_for_fingerprint(
        captured_at: &str,
        source_cursor: Option<&str>,
        munshi_version: Option<&str>,
    ) -> Manifest {
        Manifest {
            schema_version: 1,
            session: SessionInput {
                source_agent: "copilot-cli".to_string(),
                source_session_id: "source-session-1".to_string(),
            },
            capture: Capture {
                captured_at: captured_at.to_string(),
                source_cursor: source_cursor.map(str::to_string),
                project: Some("patwari".to_string()),
                repository: Some("surdy/patwari".to_string()),
                branch: Some("main".to_string()),
                source_agent_version: Some("1.0".to_string()),
                munshi_version: munshi_version.map(str::to_string),
            },
            artifact: Artifact {
                logical_path: "events.jsonl".to_string(),
                media_type: Some("application/x-ndjson".to_string()),
                original_size_bytes: 42,
                original_sha256: digest(b"original-content"),
                stored_size_bytes: 42,
                stored_sha256: digest(b"stored-content"),
                compression: Compression::Identity,
            },
        }
    }

    #[test]
    fn snapshot_fingerprint_ignores_provenance_but_not_stable_context_or_content() {
        let baseline =
            sample_manifest_for_fingerprint("2026-07-13T20:00:00Z", Some("1"), Some("1.0"));
        // Differs only in provenance: captured_at (client time), source_cursor (client
        // cursor), and munshi_version (client software version).
        let different_provenance =
            sample_manifest_for_fingerprint("2026-07-14T09:30:00Z", Some("999"), Some("2.5"));
        assert_eq!(
            snapshot_fingerprint(&baseline).expect("fingerprint computes"),
            snapshot_fingerprint(&different_provenance).expect("fingerprint computes"),
            "manifests differing only in captured_at/source_cursor/munshi_version must share a fingerprint"
        );

        let mut different_branch = baseline.clone();
        different_branch.capture.branch = Some("feature".to_string());
        assert_ne!(
            snapshot_fingerprint(&baseline).expect("fingerprint computes"),
            snapshot_fingerprint(&different_branch).expect("fingerprint computes"),
            "a change to stable capture context (branch) must change the fingerprint"
        );

        let mut different_content = baseline.clone();
        different_content.artifact.original_sha256 = digest(b"different-original-content");
        assert_ne!(
            snapshot_fingerprint(&baseline).expect("fingerprint computes"),
            snapshot_fingerprint(&different_content).expect("fingerprint computes"),
            "a change to the original artifact content must change the fingerprint"
        );

        // Stored representation and compression are not part of the artifact's
        // semantic identity, so they must not affect the fingerprint either.
        let mut different_storage = baseline.clone();
        different_storage.artifact.stored_sha256 = digest(b"different-stored-content");
        different_storage.artifact.compression = Compression::Zstd;
        assert_eq!(
            snapshot_fingerprint(&baseline).expect("fingerprint computes"),
            snapshot_fingerprint(&different_storage).expect("fingerprint computes"),
            "stored representation/compression must not affect the fingerprint"
        );
    }

    #[tokio::test]
    async fn corrupt_zstd_stream_with_matching_hash_is_rejected_as_validation_error() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        register(service.router(&config), client_id).await;

        let original = b"repeated source event\n".repeat(64);
        let full_stream = zstd::stream::encode_all(&original[..], 3).expect("compresses");
        assert!(
            full_stream.len() > 16,
            "compressed stream must be long enough to truncate past its header"
        );
        // Truncate a well-formed zstd stream so its magic number and frame header
        // remain intact but the compressed block data is incomplete. The stored
        // hash below is computed over these exact truncated bytes, so the stored
        // size/hash check in `verify_artifact` passes; only decompression fails.
        let corrupt_stored = full_stream[..full_stream.len() - 4].to_vec();

        let (status, _, body) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "idempotency_key": "capture-corrupt-zstd",
                    "manifest": {
                        "schema_version": 1,
                        "session": {
                            "source_agent": "copilot-cli",
                            "source_session_id": "source-session-1"
                        },
                        "capture": {
                            "captured_at": "2026-07-13T20:00:00Z",
                            "source_cursor": "1",
                            "project": "patwari",
                            "repository": "surdy/patwari",
                            "branch": "main",
                            "source_agent_version": "1.0",
                            "munshi_version": "1.0"
                        },
                        "artifact": {
                            "logical_path": "events.jsonl",
                            "media_type": "application/x-ndjson",
                            "original_size_bytes": original.len(),
                            "original_sha256": digest(&original),
                            "stored_size_bytes": corrupt_stored.len(),
                            "stored_sha256": digest(&corrupt_stored),
                            "compression": "zstd"
                        }
                    }
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let upload: UploadResponse = serde_json::from_slice(&body).expect("upload parses");

        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("PUT")
                .uri(&upload.artifact_upload_url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(corrupt_stored))
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("POST")
                .uri(&upload.completion_url)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "a valid-header but corrupt/truncated zstd stream must be a 422, not a 500"
        );

        let snapshot_rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&service.state.database)
            .await
            .expect("snapshots are readable");
        assert_eq!(
            snapshot_rows.0, 0,
            "rejected completion must not produce a snapshot"
        );
        let upload_row = get_upload(&service.state.database, &upload.upload_id)
            .await
            .expect("upload is readable");
        assert_eq!(upload_row.status, "artifact_uploaded");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_duplicate_completion_is_idempotent() {
        const CONCURRENT_COMPLETIONS: usize = 8;

        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        let client_id = Uuid::new_v4();
        register(service.router(&config), client_id).await;

        let bytes = b"{\"event\":\"archived\"}\n";
        let (status, _, body) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "idempotency_key": "capture-race",
                    "manifest": manifest(bytes, bytes, Compression::Identity)
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let upload: UploadResponse = serde_json::from_slice(&body).expect("upload parses");

        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .method("PUT")
                .uri(&upload.artifact_upload_url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(bytes.as_slice()))
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Widen the interleaving beyond a single pair of duplicate completions:
        // fire many concurrent completion requests for the same upload so the
        // "both read `artifact_uploaded`, one wins and removes the shared
        // artifact before the other opens/links it" window is exercised
        // repeatedly across worker threads, not just once.
        let completion_url = upload.completion_url.clone();
        let mut tasks = Vec::with_capacity(CONCURRENT_COMPLETIONS);
        for _ in 0..CONCURRENT_COMPLETIONS {
            let router = service.router(&config);
            let url = completion_url.clone();
            tasks.push(tokio::spawn(async move {
                call(
                    router,
                    Request::builder()
                        .method("POST")
                        .uri(url)
                        .body(Body::empty())
                        .expect("request is valid"),
                )
                .await
            }));
        }

        let mut receipts = Vec::with_capacity(CONCURRENT_COMPLETIONS);
        for task in tasks {
            let (status, _, body) = task.await.expect("completion task runs to completion");
            assert_eq!(
                status,
                StatusCode::OK,
                "every concurrent duplicate completion must be idempotent, not a 409/500"
            );
            let receipt: Receipt = serde_json::from_slice(&body).expect("receipt parses");
            receipts.push(receipt);
        }
        for receipt in &receipts[1..] {
            assert_eq!(
                receipt.snapshot_id, receipts[0].snapshot_id,
                "every concurrent completion must resolve to the same snapshot"
            );
        }

        let snapshot_rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&service.state.database)
            .await
            .expect("snapshots are readable");
        assert_eq!(snapshot_rows.0, 1, "only one snapshot must be created");
        let artifact_rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM artifacts")
            .fetch_one(&service.state.database)
            .await
            .expect("artifacts are readable");
        assert_eq!(artifact_rows.0, 1, "only one artifact must be created");

        let upload_dir = service.state.storage.uploads.join(&upload.upload_id);
        let leaked_entries: Vec<_> = fs::read_dir(&upload_dir)
            .expect("upload directory is readable")
            .map(|entry| entry.expect("directory entry is readable").file_name())
            .collect();
        assert!(
            leaked_entries.is_empty(),
            "no request-private completion links or shared artifact should remain, found: {leaked_entries:?}"
        );
    }

    #[tokio::test]
    async fn liveness_survives_storage_readiness_failure() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        fs::remove_dir(&service.state.storage.uploads).expect("uploads is empty");
        fs::write(&service.state.storage.uploads, "not a directory").expect("create storage fault");

        let (liveness_status, _, _) = call(
            service.router(&config),
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        let (readiness_status, _, _) = call(
            service.router(&config),
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;

        assert_eq!(liveness_status, StatusCode::OK);
        assert_eq!(readiness_status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn healthy_archive_is_ready() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");

        let (status, _, body) = call(
            service.router(&config),
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(
            String::from_utf8(body)
                .expect("health body is utf-8")
                .contains("\"ready\"")
        );
    }

    #[tokio::test]
    async fn liveness_survives_database_readiness_failure() {
        let data_dir = TestDataDir::new();
        let config = test_config(&data_dir);
        let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
        service.state.database.close().await;

        let (liveness_status, _, _) = call(
            service.router(&config),
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        let (readiness_status, _, _) = call(
            service.router(&config),
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;

        assert_eq!(liveness_status, StatusCode::OK);
        assert_eq!(readiness_status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
