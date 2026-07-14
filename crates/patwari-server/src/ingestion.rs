use std::{
    collections::HashSet,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use http_body_util::BodyExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, SqlitePool};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    config::MAX_CHUNK_COUNT,
    contract::{
        Artifact, ArtifactResponse, ClientResponse, Compression, CreateUploadRequest, Manifest,
        Receipt, RegisterClientRequest, SessionInput, SnapshotResponse, UploadArtifactStatus,
        UploadResponse, UploadStatus, UploadStatusResponse,
    },
    database::{self, format_time, now_rfc3339},
    error::{ApiError, classify_database_error, parse_json},
    service::{AppState, MaintenanceError},
    storage::StorageLayout,
    validation::{
        normalize_manifest, parse_uuid, to_sqlite_i64, validate_client_request, validate_digest,
        validate_idempotency_key, validate_octet_stream,
    },
};

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const CHUNK_SHA256_HEADER: &str = "x-patwari-chunk-sha256";
const CHUNK_LENGTH_HEADER: &str = "x-patwari-chunk-length";
const LEGACY_EXPIRY_MARKER: &str = "1970-01-01T00:00:00Z";

pub(crate) async fn register_client(
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

pub(crate) async fn create_upload(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<CreateUploadRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<UploadResponse>), ApiError> {
    let request = parse_json(payload)?;
    let client_id = parse_uuid(&request.client_id, "client identifier is not a UUID")?;
    validate_idempotency_key(&request.idempotency_key)?;
    let manifest = normalize_manifest(
        request.manifest,
        state.max_artifact_stored_bytes,
        state.max_artifact_original_bytes,
    )?;
    let chunk_count = chunk_count(manifest.artifact.stored_size_bytes, state.chunk_size_bytes)?;
    let canonical_json = serde_json::to_string(&manifest).map_err(|_| ApiError::internal())?;
    let manifest_sha256 = sha256_hex(canonical_json.as_bytes());
    let now = OffsetDateTime::now_utc();
    let now_text = format_time(now).map_err(|_| ApiError::internal())?;
    let expires_at =
        database::expiration_at(now, state.upload_expiry).map_err(|_| ApiError::internal())?;

    // A caller may resume with the same idempotency key after a prior upload has
    // expired. Expire first so an obsolete active row cannot pin that key.
    expire_uploads_at(&state, now)
        .await
        .map_err(|_| ApiError::internal())?;
    let created = persist_upload(
        &state,
        &client_id.to_string(),
        &request.idempotency_key,
        &manifest,
        canonical_json,
        &manifest_sha256,
        chunk_count,
        &now_text,
        &expires_at,
    )
    .await?;
    let response = active_upload_response(&state, &created.upload_id).await?;
    let status = if created.was_created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
}

struct CreatedUpload {
    upload_id: String,
    was_created: bool,
}

#[allow(clippy::too_many_arguments)]
async fn persist_upload(
    state: &AppState,
    client_id: &str,
    idempotency_key: &str,
    manifest: &Manifest,
    canonical_json: String,
    manifest_sha256: &str,
    chunk_count: u64,
    now: &str,
    expires_at: &str,
) -> Result<CreatedUpload, ApiError> {
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| ApiError::database())?;
    let session_id =
        get_or_create_session(&mut transaction, state, client_id, manifest, now).await?;

    let upload_id = Uuid::now_v7().to_string();
    let initial_status = if chunk_count == 0 {
        "artifact_uploaded"
    } else {
        "created"
    };
    let insert = sqlx::query(
        "INSERT INTO uploads (
            id, owner_namespace, session_id, client_id, idempotency_key, manifest_sha256, status,
            created_at, chunk_size_bytes, chunk_count, declared_stored_size_bytes,
            declared_original_size_bytes, expires_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(owner_namespace, client_id, idempotency_key) DO NOTHING",
    )
    .bind(&upload_id)
    .bind(&state.identity.owner_namespace)
    .bind(&session_id)
    .bind(client_id)
    .bind(idempotency_key)
    .bind(manifest_sha256)
    .bind(initial_status)
    .bind(now)
    .bind(to_sqlite_i64(state.chunk_size_bytes)?)
    .bind(to_sqlite_i64(chunk_count)?)
    .bind(to_sqlite_i64(manifest.artifact.stored_size_bytes)?)
    .bind(to_sqlite_i64(manifest.artifact.original_size_bytes)?)
    .bind(expires_at)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    let (persisted_id, was_created) = if insert.rows_affected() == 1 {
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
        (upload_id, true)
    } else {
        let existing = sqlx::query_as::<_, ExistingUploadRow>(
            "SELECT id, manifest_sha256 FROM uploads
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
        (existing.id, false)
    };
    transaction
        .commit()
        .await
        .map_err(|_| ApiError::database())?;
    Ok(CreatedUpload {
        upload_id: persisted_id,
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

pub(crate) async fn get_upload_status(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<UploadStatusResponse>, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    let lock = state.upload_lock(&upload_id);
    let _guard = lock.lock().await;
    if let Some(upload) = get_active_upload(&state.database, &upload_id).await? {
        if is_expired(&upload, OffsetDateTime::now_utc())? {
            terminalize_upload_locked(&state, &upload, TerminalReason::Expired).await?;
            return audit_upload_response(&state.database, &upload_id)
                .await
                .map(Json);
        }
        return active_upload_status_response(&state, &upload)
            .await
            .map(Json);
    }
    audit_upload_response(&state.database, &upload_id)
        .await
        .map(Json)
}

pub(crate) async fn put_artifact_chunk(
    AxumPath((upload_id, chunk_index)): AxumPath<(String, String)>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    let chunk_index = chunk_index
        .parse::<u64>()
        .map_err(|_| ApiError::invalid("chunk index is not a non-negative integer"))?;
    validate_octet_stream(&headers)?;

    let lock = state.upload_lock(&upload_id);
    let _guard = lock.lock().await;
    let upload = require_active_upload(&state, &upload_id).await?;
    if upload.status == "completed" {
        return Err(ApiError::conflict(
            "upload_completed",
            "completed uploads do not accept artifact bytes",
        ));
    }
    let request_chunk = match parse_chunk_headers(&headers)? {
        Some(request) => request,
        None => legacy_single_chunk_request(&state.database, &upload, chunk_index).await?,
    };
    if let Some(existing) = get_chunk(&state.database, &upload_id, chunk_index).await? {
        if existing.byte_length != to_sqlite_i64(request_chunk.byte_length)?
            || existing.sha256 != digest_storage_value(&request_chunk.sha256)
        {
            return Err(ApiError::conflict(
                "chunk_conflict",
                "this chunk index was already accepted with a different length or checksum",
            ));
        }
        verify_stored_file(
            state.storage.chunk_path(&upload_id, chunk_index),
            request_chunk.byte_length,
            &request_chunk.sha256,
        )
        .await?;
        return Ok(StatusCode::NO_CONTENT);
    }
    let expected_length = expected_chunk_length(&upload, chunk_index)?;
    if request_chunk.byte_length != expected_length {
        return Err(ApiError::invalid(
            "chunk length does not match the negotiated chunk layout",
        ));
    }

    state
        .storage
        .ensure_chunk_dir(&upload_id)
        .await
        .map_err(|_| ApiError::storage())?;
    let temporary_path = state.storage.staged_chunk_path(&upload_id);
    let write_result = write_chunk_body(body, &temporary_path, &request_chunk).await;
    if let Err(error) = write_result {
        let _ = StorageLayout::remove_file(&temporary_path).await;
        return Err(error);
    }

    let final_path = state.storage.chunk_path(&upload_id, chunk_index);
    // Files precede metadata: an interrupted write can leave only an orphan
    // file, which restart recovery removes. Reversing this order could make a
    // committed accepted record point to bytes that never became durable.
    let created_file = match fs::hard_link(&temporary_path, &final_path).await {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(_) => {
            let _ = StorageLayout::remove_file(&temporary_path).await;
            return Err(ApiError::storage());
        }
    };
    if !created_file
        && verify_stored_file(
            final_path.clone(),
            request_chunk.byte_length,
            &request_chunk.sha256,
        )
        .await
        .is_err()
    {
        let _ = StorageLayout::remove_file(&temporary_path).await;
        return Err(ApiError::conflict(
            "chunk_conflict",
            "this chunk index was already accepted with a different length or checksum",
        ));
    }

    let now = now_rfc3339().map_err(|_| ApiError::internal())?;
    let accepted = persist_chunk_record(
        &state,
        &upload,
        chunk_index,
        request_chunk.byte_length,
        &request_chunk.sha256,
        &now,
    )
    .await;
    let cleanup = StorageLayout::remove_file(&temporary_path).await;
    if cleanup.is_err() && accepted.is_ok() {
        return Err(ApiError::storage());
    }
    accepted?;
    refresh_upload_status(&state.database, &upload).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Clone)]
struct ChunkRequest {
    byte_length: u64,
    sha256: String,
}

fn parse_chunk_headers(headers: &HeaderMap) -> Result<Option<ChunkRequest>, ApiError> {
    let sha256 = optional_header(headers, CHUNK_SHA256_HEADER)?;
    let byte_length = optional_header(headers, CHUNK_LENGTH_HEADER)?;
    match (sha256, byte_length) {
        (None, None) => Ok(None),
        (Some(sha256), Some(byte_length)) => {
            validate_digest(sha256)?;
            let byte_length = byte_length.parse().map_err(|_| {
                ApiError::invalid("chunk length header must be a non-negative integer")
            })?;
            Ok(Some(ChunkRequest {
                byte_length,
                sha256: sha256.to_owned(),
            }))
        }
        _ => Err(ApiError::invalid(
            "chunk checksum and length headers must be supplied together",
        )),
    }
}

fn optional_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a str>, ApiError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| ApiError::invalid("chunk headers must be valid text"))
        })
        .transpose()
}

async fn legacy_single_chunk_request(
    database: &SqlitePool,
    upload: &ActiveUploadRow,
    chunk_index: u64,
) -> Result<ChunkRequest, ApiError> {
    if upload.chunk_count != 1 || chunk_index != 0 {
        return Err(ApiError::invalid(
            "chunk checksum and length headers are required",
        ));
    }
    let manifest = get_upload_manifest(database, &upload.id).await?;
    Ok(ChunkRequest {
        byte_length: manifest.artifact.stored_size_bytes,
        sha256: manifest.artifact.stored_sha256,
    })
}

async fn persist_chunk_record(
    state: &AppState,
    upload: &ActiveUploadRow,
    chunk_index: u64,
    byte_length: u64,
    sha256: &str,
    accepted_at: &str,
) -> Result<(), ApiError> {
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| ApiError::database())?;
    let current: Option<(String,)> = sqlx::query_as("SELECT status FROM uploads WHERE id = ?1")
        .bind(&upload.id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    if !matches!(
        current.as_ref().map(|row| row.0.as_str()),
        Some("created" | "artifact_uploaded")
    ) {
        return Err(ApiError::conflict(
            "upload_state_conflict",
            "upload state changed while the chunk was submitted",
        ));
    }
    let inserted = sqlx::query(
        "INSERT INTO upload_chunks (
            upload_id, artifact_index, chunk_index, byte_length, sha256, accepted_at
         ) VALUES (?1, 0, ?2, ?3, ?4, ?5)
         ON CONFLICT(upload_id, artifact_index, chunk_index) DO NOTHING",
    )
    .bind(&upload.id)
    .bind(to_sqlite_i64(chunk_index)?)
    .bind(to_sqlite_i64(byte_length)?)
    .bind(digest_storage_value(sha256))
    .bind(accepted_at)
    .execute(&mut *transaction)
    .await
    .map_err(|error| classify_database_error(&error))?;

    if inserted.rows_affected() == 0 {
        let existing = sqlx::query_as::<_, ChunkRow>(
            "SELECT chunk_index, byte_length, sha256 FROM upload_chunks
             WHERE upload_id = ?1 AND artifact_index = 0 AND chunk_index = ?2",
        )
        .bind(&upload.id)
        .bind(to_sqlite_i64(chunk_index)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
        if existing.byte_length != to_sqlite_i64(byte_length)?
            || existing.sha256 != digest_storage_value(sha256)
        {
            return Err(ApiError::conflict(
                "chunk_conflict",
                "this chunk index was already accepted with a different length or checksum",
            ));
        }
    }
    transaction
        .commit()
        .await
        .map_err(|error| classify_database_error(&error))?;
    Ok(())
}

async fn refresh_upload_status(
    database: &SqlitePool,
    upload: &ActiveUploadRow,
) -> Result<(), ApiError> {
    let accepted: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM upload_chunks WHERE upload_id = ?1 AND artifact_index = 0",
    )
    .bind(&upload.id)
    .fetch_one(database)
    .await
    .map_err(|_| ApiError::database())?;
    let complete = accepted.0 == upload.chunk_count;
    let desired_status = if complete {
        "artifact_uploaded"
    } else {
        "created"
    };
    sqlx::query(
        "UPDATE uploads SET status = ?1
         WHERE id = ?2 AND status IN ('created', 'artifact_uploaded')",
    )
    .bind(desired_status)
    .bind(&upload.id)
    .execute(database)
    .await
    .map_err(|_| ApiError::database())?;
    Ok(())
}

pub(crate) async fn abandon_upload(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<UploadStatusResponse>, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    let lock = state.upload_lock(&upload_id);
    let _guard = lock.lock().await;

    if let Some(upload) = get_active_upload(&state.database, &upload_id).await? {
        if upload.status == "completed" {
            return Err(ApiError::conflict(
                "upload_completed",
                "completed uploads cannot be abandoned",
            ));
        }
        let reason = if is_expired(&upload, OffsetDateTime::now_utc())? {
            TerminalReason::Expired
        } else {
            TerminalReason::Abandoned
        };
        terminalize_upload_locked(&state, &upload, reason).await?;
    }
    audit_upload_response(&state.database, &upload_id)
        .await
        .map(Json)
}

#[derive(Clone, Copy)]
enum TerminalReason {
    Abandoned,
    Expired,
}

impl TerminalReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Abandoned => "abandoned",
            Self::Expired => "expired",
        }
    }

    const fn error_code(self) -> &'static str {
        match self {
            Self::Abandoned => "upload_abandoned",
            Self::Expired => "upload_expired",
        }
    }
}

async fn terminalize_upload_locked(
    state: &AppState,
    upload: &ActiveUploadRow,
    reason: TerminalReason,
) -> Result<(), ApiError> {
    let terminal_at = now_rfc3339().map_err(|_| ApiError::internal())?;
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| ApiError::database())?;
    let current: Option<(String,)> = sqlx::query_as("SELECT status FROM uploads WHERE id = ?1")
        .bind(&upload.id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    if current.is_none() {
        transaction
            .commit()
            .await
            .map_err(|_| ApiError::database())?;
        return Ok(());
    }
    if matches!(
        current.as_ref().map(|row| row.0.as_str()),
        Some("completed")
    ) {
        return Err(ApiError::conflict(
            "upload_completed",
            "completed uploads cannot be abandoned",
        ));
    }
    sqlx::query(
        "INSERT INTO upload_audits (
            upload_id, owner_namespace, client_id, session_id, declared_original_size_bytes,
            declared_stored_size_bytes, chunk_size_bytes, chunk_count, created_at, terminal_at,
            terminal_reason, error_code
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(upload_id) DO NOTHING",
    )
    .bind(&upload.id)
    .bind(&upload.owner_namespace)
    .bind(&upload.client_id)
    .bind(&upload.session_id)
    .bind(upload.declared_original_size_bytes)
    .bind(upload.declared_stored_size_bytes)
    .bind(upload.chunk_size_bytes)
    .bind(upload.chunk_count)
    .bind(&upload.created_at)
    .bind(&terminal_at)
    .bind(reason.as_str())
    .bind(reason.error_code())
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    // Delete database detail before deleting bytes. If the process stops after
    // this commit, recovery sees the audit record and removes the orphaned
    // upload directory. The reverse order would lose the durable cleanup work.
    sqlx::query("DELETE FROM upload_chunks WHERE upload_id = ?1")
        .bind(&upload.id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    sqlx::query("DELETE FROM manifests WHERE upload_id = ?1")
        .bind(&upload.id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    sqlx::query("DELETE FROM uploads WHERE id = ?1")
        .bind(&upload.id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;
    transaction
        .commit()
        .await
        .map_err(|_| ApiError::database())?;

    state
        .storage
        .remove_upload_dir(&upload.id)
        .await
        .map_err(|_| ApiError::storage())
}

pub(crate) async fn expire_uploads_at(
    state: &AppState,
    now: OffsetDateTime,
) -> Result<usize, MaintenanceError> {
    let now_text = format_time(now).map_err(|_| MaintenanceError::Clock)?;
    let candidates = sqlx::query_as::<_, (String,)>(
        "SELECT id FROM uploads
         WHERE status IN ('created', 'artifact_uploaded') AND expires_at <= ?1",
    )
    .bind(now_text)
    .fetch_all(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    let mut expired = 0;
    for (upload_id,) in candidates {
        let lock = state.upload_lock(&upload_id);
        let _guard = lock.lock().await;
        let Some(upload) = get_active_upload(&state.database, &upload_id)
            .await
            .map_err(|_| MaintenanceError::Operation)?
        else {
            continue;
        };
        if upload.status != "completed"
            && is_expired(&upload, now).map_err(|_| MaintenanceError::Operation)?
        {
            terminalize_upload_locked(state, &upload, TerminalReason::Expired)
                .await
                .map_err(|_| MaintenanceError::Operation)?;
            expired += 1;
        }
    }
    Ok(expired)
}

async fn require_active_upload(
    state: &AppState,
    upload_id: &str,
) -> Result<ActiveUploadRow, ApiError> {
    let upload = get_active_upload(&state.database, upload_id)
        .await?
        .ok_or_else(|| ApiError::not_found("upload_not_found", "upload was not found"))?;
    if upload.status != "completed" && is_expired(&upload, OffsetDateTime::now_utc())? {
        terminalize_upload_locked(state, &upload, TerminalReason::Expired).await?;
        return Err(ApiError::conflict(
            "upload_expired",
            "upload expired before this request could be accepted",
        ));
    }
    Ok(upload)
}

fn is_expired(upload: &ActiveUploadRow, now: OffsetDateTime) -> Result<bool, ApiError> {
    let expires_at =
        OffsetDateTime::parse(&upload.expires_at, &Rfc3339).map_err(|_| ApiError::database())?;
    Ok(expires_at <= now)
}

async fn active_upload_response(
    state: &AppState,
    upload_id: &str,
) -> Result<UploadResponse, ApiError> {
    let upload = get_active_upload(&state.database, upload_id)
        .await?
        .ok_or_else(|| ApiError::not_found("upload_not_found", "upload was not found"))?;
    let artifact = upload_artifact_status(&state.database, &upload).await?;
    Ok(UploadResponse {
        artifact_upload_url: format!("/api/v1/uploads/{upload_id}/artifacts/0/chunks/0"),
        status_url: format!("/api/v1/uploads/{upload_id}"),
        abandon_url: format!("/api/v1/uploads/{upload_id}/abandon"),
        completion_url: format!("/api/v1/uploads/{upload_id}/complete"),
        upload_id: upload.id,
        session_id: upload.session_id,
        status: upload_status(&upload.status)?,
        manifest_sha256: digest_document_value(&upload.manifest_sha256),
        chunk_size_bytes: u64::try_from(upload.chunk_size_bytes)
            .map_err(|_| ApiError::internal())?,
        artifacts: vec![artifact],
    })
}

async fn active_upload_status_response(
    state: &AppState,
    upload: &ActiveUploadRow,
) -> Result<UploadStatusResponse, ApiError> {
    let artifact = upload_artifact_status(&state.database, upload).await?;
    Ok(UploadStatusResponse {
        status_url: format!("/api/v1/uploads/{}", upload.id),
        abandon_url: format!("/api/v1/uploads/{}/abandon", upload.id),
        completion_url: format!("/api/v1/uploads/{}/complete", upload.id),
        upload_id: upload.id.clone(),
        session_id: upload.session_id.clone(),
        status: upload_status(&upload.status)?,
        manifest_sha256: Some(digest_document_value(&upload.manifest_sha256)),
        chunk_size_bytes: u64::try_from(upload.chunk_size_bytes)
            .map_err(|_| ApiError::internal())?,
        artifacts: vec![artifact],
    })
}

async fn audit_upload_response(
    database: &SqlitePool,
    upload_id: &str,
) -> Result<UploadStatusResponse, ApiError> {
    let audit = sqlx::query_as::<_, AuditUploadRow>(
        "SELECT upload_id, session_id, chunk_size_bytes, terminal_reason
         FROM upload_audits WHERE upload_id = ?1",
    )
    .bind(upload_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("upload_not_found", "upload was not found"))?;
    let status = match audit.terminal_reason.as_str() {
        "abandoned" => UploadStatus::Abandoned,
        "expired" => UploadStatus::Expired,
        _ => return Err(ApiError::internal()),
    };
    Ok(UploadStatusResponse {
        status_url: format!("/api/v1/uploads/{upload_id}"),
        abandon_url: format!("/api/v1/uploads/{upload_id}/abandon"),
        completion_url: format!("/api/v1/uploads/{upload_id}/complete"),
        upload_id: audit.upload_id,
        session_id: audit.session_id,
        status,
        manifest_sha256: None,
        chunk_size_bytes: u64::try_from(audit.chunk_size_bytes)
            .map_err(|_| ApiError::internal())?,
        // Terminal audit rows intentionally expose no chunk detail.
        artifacts: Vec::new(),
    })
}

async fn upload_artifact_status(
    database: &SqlitePool,
    upload: &ActiveUploadRow,
) -> Result<UploadArtifactStatus, ApiError> {
    let chunk_count = u64::try_from(upload.chunk_count).map_err(|_| ApiError::internal())?;
    let rows = sqlx::query_as::<_, ChunkRow>(
        "SELECT chunk_index, byte_length, sha256 FROM upload_chunks
         WHERE upload_id = ?1 AND artifact_index = 0 ORDER BY chunk_index",
    )
    .bind(&upload.id)
    .fetch_all(database)
    .await
    .map_err(|_| ApiError::database())?;
    let bitmap_bytes =
        usize::try_from(chunk_count.div_ceil(8)).map_err(|_| ApiError::internal())?;
    let mut bitmap = vec![0_u8; bitmap_bytes];
    for row in rows {
        let index = u64::try_from(row.chunk_index).map_err(|_| ApiError::internal())?;
        if index >= chunk_count {
            return Err(ApiError::conflict(
                "chunk_layout_invalid",
                "accepted chunk records do not match the negotiated layout",
            ));
        }
        let byte = usize::try_from(index / 8).map_err(|_| ApiError::internal())?;
        let bit = u32::try_from(index % 8).map_err(|_| ApiError::internal())?;
        bitmap[byte] |= 1_u8 << bit;
    }
    let missing_chunk_indexes = (0..chunk_count)
        .filter(|index| {
            let byte = usize::try_from(*index / 8).expect("configured chunk count fits usize");
            let bit = u32::try_from(*index % 8).expect("bit index fits u32");
            bitmap[byte] & (1_u8 << bit) == 0
        })
        .collect();
    Ok(UploadArtifactStatus {
        artifact_index: 0,
        stored_size_bytes: u64::try_from(upload.declared_stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        chunk_count,
        accepted_chunk_bitmap: hex_digest(bitmap),
        missing_chunk_indexes,
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

pub(crate) async fn complete_upload(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Receipt>, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    let lock = state.upload_lock(&upload_id);
    let _guard = lock.lock().await;
    let upload = require_active_upload(&state, &upload_id).await?;
    if upload.status == "completed" {
        let snapshot_id = upload.snapshot_id.ok_or_else(ApiError::internal)?;
        return receipt_for_snapshot(&state, &snapshot_id).await.map(Json);
    }

    let manifest = get_upload_manifest(&state.database, &upload_id).await?;
    validate_current_artifact_limits(&state, &manifest)?;
    let assembled_path = state.storage.assembled_path(&upload_id);
    let assembled = assemble_upload(&state, &upload, &manifest, &assembled_path).await;
    if let Err(error) = assembled {
        let _ = StorageLayout::remove_file(&assembled_path).await;
        return Err(error);
    }
    if let Err(error) = verify_artifact(&assembled_path, &manifest.artifact).await {
        let _ = StorageLayout::remove_file(&assembled_path).await;
        return Err(error);
    }

    let fingerprint = snapshot_fingerprint(&manifest)?;

    // Resolve any already-committed snapshot for this session/fingerprint
    // before promoting a permanent blob. A duplicate completion that
    // assembled a different (but equally valid) stored representation of
    // the same original bytes must never promote that alternate
    // representation to permanent storage or create a blob row for it: the
    // fingerprint depends only on stable session/capture/original-artifact
    // fields, so a match here always already has a durable artifact+blob.
    if let Some(snapshot_id) =
        find_snapshot_by_fingerprint(&state, &upload.session_id, &fingerprint).await?
    {
        let _ = StorageLayout::remove_file(&assembled_path).await;
        finalize_upload_for_existing_snapshot(&state, &upload_id, &snapshot_id).await?;
        state
            .storage
            .remove_upload_dir(&upload_id)
            .await
            .map_err(|_| ApiError::storage())?;
        return receipt_for_snapshot(&state, &snapshot_id).await.map(Json);
    }

    // Every operation that promotes, verifies/reuses, creates/references, or
    // conditionally deletes the canonical blob file for this digest must run
    // under this same lock for its whole critical section: otherwise a
    // third party racing on the same `stored_sha256` could commit a new
    // blob/artifact reference in the gap between this task discovering it
    // lost a snapshot race and deleting the file it had just promoted,
    // leaving that new reference dangling. Acquired after the upload lock
    // (never the reverse) so the two lock families cannot deadlock; see the
    // ordering note on `AppState::blob_lock`.
    let blob_lock = state.blob_lock(
        &state.identity.owner_namespace,
        digest_storage_value(&manifest.artifact.stored_sha256),
    );
    // Test-only: lets a test confirm this task has reached the exact point
    // where it attempts to acquire the per-digest lock, without any
    // dependence on scheduling timing.
    #[cfg(test)]
    if let Some(checkpoint) = state.test_hooks.before_blob_lock_attempt() {
        checkpoint.mark_reached();
    }
    let blob_guard = blob_lock.lock().await;
    if let Err(error) = promote_blob(
        &state.storage,
        &assembled_path,
        &manifest.artifact.stored_sha256,
        manifest.artifact.stored_size_bytes,
    )
    .await
    {
        let _ = StorageLayout::remove_file(&assembled_path).await;
        return Err(error);
    }
    let snapshot_id =
        record_completed_upload(&state, &upload_id, &upload, &manifest, &fingerprint).await?;
    drop(blob_guard);

    // Metadata commits before cleanup. A crash here leaves only recoverable
    // upload-scoped temporary files; bootstrap removes them for completed rows.
    state
        .storage
        .remove_upload_dir(&upload_id)
        .await
        .map_err(|_| ApiError::storage())?;
    receipt_for_snapshot(&state, &snapshot_id).await.map(Json)
}

fn validate_current_artifact_limits(state: &AppState, manifest: &Manifest) -> Result<(), ApiError> {
    if manifest.artifact.stored_size_bytes > state.max_artifact_stored_bytes {
        return Err(ApiError::invalid(
            "artifact stored size exceeds the configured bounded limit",
        ));
    }
    if manifest.artifact.original_size_bytes > state.max_artifact_original_bytes {
        return Err(ApiError::invalid(
            "artifact original size exceeds the configured bounded limit",
        ));
    }
    Ok(())
}

async fn assemble_upload(
    state: &AppState,
    upload: &ActiveUploadRow,
    manifest: &Manifest,
    destination: &Path,
) -> Result<(), ApiError> {
    let chunks = accepted_chunk_layout(&state.database, upload).await?;
    let parent = destination.parent().ok_or_else(ApiError::storage)?;
    fs::create_dir_all(parent)
        .await
        .map_err(|_| ApiError::storage())?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .await
        .map_err(|_| ApiError::storage())?;
    let mut writer = BufWriter::new(file);
    let mut artifact_hasher = Sha256::new();
    let mut artifact_size = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];

    for chunk in chunks {
        let expected_length = u64::try_from(chunk.byte_length).map_err(|_| ApiError::internal())?;
        let mut file = fs::File::open(state.storage.chunk_path(
            &upload.id,
            u64::try_from(chunk.chunk_index).map_err(|_| ApiError::internal())?,
        ))
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "chunk_missing",
                "an accepted chunk is no longer available",
            )
        })?;
        let mut chunk_hasher = Sha256::new();
        let mut chunk_size = 0_u64;
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|_| ApiError::storage())?;
            if read == 0 {
                break;
            }
            let read = u64::try_from(read).map_err(|_| ApiError::internal())?;
            chunk_size = chunk_size
                .checked_add(read)
                .ok_or_else(|| ApiError::invalid("chunk size is invalid"))?;
            if chunk_size > expected_length {
                return Err(chunk_checksum_error());
            }
            artifact_size = artifact_size
                .checked_add(read)
                .ok_or_else(|| ApiError::invalid("artifact size is invalid"))?;
            if artifact_size > manifest.artifact.stored_size_bytes {
                return Err(ApiError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "artifact_stored_checksum_mismatch",
                    "assembled artifact exceeds its declared stored size",
                ));
            }
            let read_usize = usize::try_from(read).map_err(|_| ApiError::internal())?;
            chunk_hasher.update(&buffer[..read_usize]);
            artifact_hasher.update(&buffer[..read_usize]);
            writer
                .write_all(&buffer[..read_usize])
                .await
                .map_err(|_| ApiError::storage())?;
        }
        if chunk_size != expected_length || hex_digest(chunk_hasher.finalize()) != chunk.sha256 {
            return Err(chunk_checksum_error());
        }
    }
    writer.flush().await.map_err(|_| ApiError::storage())?;
    writer
        .into_inner()
        .sync_all()
        .await
        .map_err(|_| ApiError::storage())?;
    if artifact_size != manifest.artifact.stored_size_bytes
        || hex_digest(artifact_hasher.finalize())
            != digest_storage_value(&manifest.artifact.stored_sha256)
    {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "artifact_stored_checksum_mismatch",
            "assembled artifact stored size or checksum does not match the manifest",
        ));
    }
    Ok(())
}

async fn accepted_chunk_layout(
    database: &SqlitePool,
    upload: &ActiveUploadRow,
) -> Result<Vec<ChunkRow>, ApiError> {
    let rows = sqlx::query_as::<_, ChunkRow>(
        "SELECT chunk_index, byte_length, sha256 FROM upload_chunks
         WHERE upload_id = ?1 AND artifact_index = 0 ORDER BY chunk_index",
    )
    .bind(&upload.id)
    .fetch_all(database)
    .await
    .map_err(|_| ApiError::database())?;
    let expected_count = u64::try_from(upload.chunk_count).map_err(|_| ApiError::internal())?;
    if rows.len() != usize::try_from(expected_count).map_err(|_| ApiError::internal())? {
        return Err(ApiError::conflict(
            "artifact_incomplete",
            "all negotiated chunks must be accepted before completion",
        ));
    }
    for (expected_index, row) in rows.iter().enumerate() {
        let expected_index = u64::try_from(expected_index).map_err(|_| ApiError::internal())?;
        let actual_index = u64::try_from(row.chunk_index).map_err(|_| ApiError::internal())?;
        if actual_index != expected_index {
            return Err(ApiError::conflict(
                "artifact_incomplete",
                "accepted chunks contain a gap or overlap",
            ));
        }
        let expected_length = expected_chunk_length(upload, actual_index)?;
        if row.byte_length != to_sqlite_i64(expected_length)? {
            return Err(ApiError::conflict(
                "chunk_layout_invalid",
                "accepted chunk length does not match the negotiated layout",
            ));
        }
        validate_digest(&digest_document_value(&row.sha256)).map_err(|_| {
            ApiError::conflict(
                "chunk_layout_invalid",
                "accepted chunk checksum does not match the negotiated layout",
            )
        })?;
    }
    Ok(rows)
}

fn chunk_checksum_error() -> ApiError {
    ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "chunk_checksum_mismatch",
        "accepted chunk bytes no longer match their checksum or length",
    )
}

async fn find_snapshot_by_fingerprint(
    state: &AppState,
    session_id: &str,
    fingerprint: &str,
) -> Result<Option<String>, ApiError> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM snapshots WHERE session_id = ?1 AND fingerprint_sha256 = ?2",
    )
    .bind(session_id)
    .bind(fingerprint)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?;
    Ok(row.map(|row| row.0))
}

/// Marks `upload_id` completed against an already-committed `snapshot_id`
/// without touching blobs or artifacts, which already exist for that
/// snapshot. Tolerates being called more than once for the same outcome
/// (idempotent retries, or two racing completions that both resolve to the
/// same winning snapshot).
async fn finalize_upload_for_existing_snapshot(
    state: &AppState,
    upload_id: &str,
    snapshot_id: &str,
) -> Result<(), ApiError> {
    let now = now_rfc3339().map_err(|_| ApiError::internal())?;
    let updated = sqlx::query(
        "UPDATE uploads SET status = 'completed', snapshot_id = ?1, completed_at = ?2
         WHERE id = ?3 AND status IN ('created', 'artifact_uploaded')",
    )
    .bind(snapshot_id)
    .bind(&now)
    .bind(upload_id)
    .execute(&state.database)
    .await
    .map_err(|error| classify_database_error(&error))?;
    if updated.rows_affected() == 1 {
        return Ok(());
    }
    let current: Option<UploadRaceRow> =
        sqlx::query_as("SELECT status, snapshot_id FROM uploads WHERE id = ?1")
            .bind(upload_id)
            .fetch_optional(&state.database)
            .await
            .map_err(|_| ApiError::database())?;
    let already_completed_with_same_snapshot = current.is_some_and(|current| {
        current.status == "completed" && current.snapshot_id.as_deref() == Some(snapshot_id)
    });
    if already_completed_with_same_snapshot {
        Ok(())
    } else {
        Err(ApiError::conflict(
            "upload_state_conflict",
            "upload state changed while completion was requested",
        ))
    }
}

async fn record_completed_upload(
    state: &AppState,
    upload_id: &str,
    upload: &ActiveUploadRow,
    manifest: &Manifest,
    fingerprint: &str,
) -> Result<String, ApiError> {
    // Test-only: lets concurrency tests force a specific winner/loser
    // ordering between two completions racing on the same session +
    // fingerprint, by pausing one side here (before it opens its
    // transaction) while the other runs to completion.
    #[cfg(test)]
    if let Some(checkpoint) = state.test_hooks.before_snapshot_commit() {
        checkpoint.arrive_and_wait().await;
    }
    let now = now_rfc3339().map_err(|_| ApiError::internal())?;
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|error| classify_database_error(&error))?;
    let (blob_id, blob_freshly_inserted) =
        get_or_create_blob(&mut transaction, state, &manifest.artifact, &now).await?;
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
    .bind(fingerprint)
    .bind(&now)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    if snapshot_insert.rows_affected() == 0 {
        return resolve_losing_snapshot_race(
            state,
            upload_id,
            upload,
            manifest,
            fingerprint,
            transaction,
            blob_freshly_inserted,
        )
        .await;
    }

    finalize_winning_snapshot(
        &mut transaction,
        upload_id,
        &candidate_snapshot_id,
        &blob_id,
        manifest,
        &now,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|error| classify_database_error(&error))?;
    Ok(candidate_snapshot_id)
}

/// Handles the losing side of a snapshot creation race: a different upload
/// racing on the same session/fingerprint committed a snapshot first. Rolls
/// back before any of our work becomes visible, discarding the blob row we
/// may have just inserted, and cleans up the file we had promoted if it is
/// now guaranteed unreferenced. No restart is required to reclaim it.
///
/// The caller must hold `state.blob_lock` for this artifact's
/// `(owner_namespace, stored_sha256)` for the entire call. Without it, a
/// third party could commit a fresh blob/artifact reference to the same
/// digest in the gap between the rollback below (which releases the
/// `SQLite` write lock) and the conditional file deletion, leaving that
/// party's reference dangling once this task deletes the file.
async fn resolve_losing_snapshot_race(
    state: &AppState,
    upload_id: &str,
    upload: &ActiveUploadRow,
    manifest: &Manifest,
    fingerprint: &str,
    transaction: sqlx::Transaction<'_, sqlx::Sqlite>,
    blob_freshly_inserted: bool,
) -> Result<String, ApiError> {
    transaction
        .rollback()
        .await
        .map_err(|error| classify_database_error(&error))?;
    // Test-only: lets concurrency tests deterministically land a third
    // party's completion inside this exact historical vulnerable window
    // (see the doc comment above) instead of relying on scheduling luck.
    // The blob lock held by our caller makes this a no-op race in
    // production: nothing else touching this digest can run concurrently.
    #[cfg(test)]
    if let Some(checkpoint) = state.test_hooks.after_losing_rollback() {
        checkpoint.arrive_and_wait().await;
    }
    let winner: (String,) = sqlx::query_as(
        "SELECT id FROM snapshots WHERE session_id = ?1 AND fingerprint_sha256 = ?2",
    )
    .bind(&upload.session_id)
    .bind(fingerprint)
    .fetch_one(&state.database)
    .await
    .map_err(|_| ApiError::database())?;
    if blob_freshly_inserted {
        // Our blob row was rolled back, so the file we had just promoted is
        // guaranteed unreferenced by any committed row.
        let _ = StorageLayout::remove_file(
            &state
                .storage
                .blob_path(digest_storage_value(&manifest.artifact.stored_sha256)),
        )
        .await;
    }
    finalize_upload_for_existing_snapshot(state, upload_id, &winner.0).await?;
    Ok(winner.0)
}

/// Links the winning snapshot to its artifact and marks the upload
/// completed, all within the caller's still-open transaction.
async fn finalize_winning_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    upload_id: &str,
    snapshot_id: &str,
    blob_id: &str,
    manifest: &Manifest,
    now: &str,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO artifacts (
            id, snapshot_id, blob_id, logical_path, media_type, original_size_bytes,
            original_sha256, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(snapshot_id)
    .bind(blob_id)
    .bind(&manifest.artifact.logical_path)
    .bind(&manifest.artifact.media_type)
    .bind(to_sqlite_i64(manifest.artifact.original_size_bytes)?)
    .bind(digest_storage_value(&manifest.artifact.original_sha256))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;

    let updated = sqlx::query(
        "UPDATE uploads SET status = 'completed', snapshot_id = ?1, completed_at = ?2
         WHERE id = ?3 AND status IN ('created', 'artifact_uploaded')",
    )
    .bind(snapshot_id)
    .bind(now)
    .bind(upload_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| classify_database_error(&error))?;
    if updated.rows_affected() == 1 {
        return Ok(());
    }
    let current: Option<UploadRaceRow> =
        sqlx::query_as("SELECT status, snapshot_id FROM uploads WHERE id = ?1")
            .bind(upload_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| ApiError::database())?;
    let already_completed_with_same_snapshot = current.is_some_and(|current| {
        current.status == "completed" && current.snapshot_id.as_deref() == Some(snapshot_id)
    });
    if already_completed_with_same_snapshot {
        Ok(())
    } else {
        Err(ApiError::conflict(
            "upload_state_conflict",
            "upload state changed while completion was requested",
        ))
    }
}

/// Inserts a blob row for `artifact`'s stored bytes if one does not already
/// exist, returning its ID and whether this call is the one that created it.
/// The `bool` lets a caller that later loses a snapshot race and rolls back
/// know whether the file it promoted is safe to delete: a freshly-inserted
/// row never became visible outside this transaction, so after a rollback no
/// *committed* blob row references the underlying file. That alone is not
/// enough to delete the file safely, because the file path is addressed by
/// digest rather than by this row's ID: a different transaction could insert
/// a new row for the same digest and promote/reuse the same path before the
/// caller deletes it. Callers must hold `state.blob_lock` for this digest
/// across the insert, the eventual commit or rollback, and any conditional
/// deletion so no such transaction can run concurrently.
async fn get_or_create_blob(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    state: &AppState,
    artifact: &Artifact,
    now: &str,
) -> Result<(String, bool), ApiError> {
    let blob_id = Uuid::now_v7().to_string();
    let insert = sqlx::query(
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
    let freshly_inserted = insert.rows_affected() == 1;
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
    Ok((row.id, freshly_inserted))
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::Identity => "identity",
        Compression::Zstd => "zstd",
    }
}

async fn write_chunk_body(
    mut body: Body,
    temporary_path: &Path,
    expected: &ChunkRequest,
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
        let frame = frame.map_err(|_| ApiError::invalid("chunk request body could not be read"))?;
        if let Ok(data) = frame.into_data() {
            let chunk_len = u64::try_from(data.len()).map_err(|_| ApiError::internal())?;
            size = size
                .checked_add(chunk_len)
                .ok_or_else(|| ApiError::invalid("chunk size is invalid"))?;
            if size > expected.byte_length {
                return Err(ApiError::invalid(
                    "chunk body exceeds its declared negotiated length",
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
    if size != expected.byte_length
        || hex_digest(hasher.finalize()) != digest_storage_value(&expected.sha256)
    {
        return Err(ApiError::invalid(
            "chunk body length or checksum does not match its headers",
        ));
    }
    Ok(())
}

async fn verify_artifact(path: &Path, artifact: &Artifact) -> Result<(), ApiError> {
    let path = path.to_path_buf();
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
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "artifact_stored_checksum_mismatch",
            "artifact stored size or checksum does not match the manifest",
        ));
    }

    let file = std::fs::File::open(path).map_err(|_| ApiError::storage())?;
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
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "artifact_original_checksum_mismatch",
            "artifact original size or checksum does not match the manifest",
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
    expected_digest: &str,
) -> Result<(), ApiError> {
    let expected_digest = expected_digest.to_owned();
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
    let parent = destination.parent().ok_or_else(ApiError::storage)?;
    fs::create_dir_all(parent)
        .await
        .map_err(|_| ApiError::storage())?;
    match fs::hard_link(source_path, &destination).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            verify_stored_file(destination, stored_size, stored_sha256).await
        }
        Err(_) => Err(ApiError::storage()),
    }
}

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

pub(crate) async fn get_snapshot(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    snapshot_response(&state.database, &snapshot_id)
        .await
        .map(Json)
}

pub(crate) async fn download_artifact(
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

fn chunk_count(stored_size: u64, chunk_size: u64) -> Result<u64, ApiError> {
    if chunk_size == 0 {
        return Err(ApiError::internal());
    }
    let count = if stored_size == 0 {
        0
    } else {
        stored_size
            .checked_sub(1)
            .and_then(|size| size.checked_div(chunk_size))
            .and_then(|quotient| quotient.checked_add(1))
            .ok_or_else(|| ApiError::invalid("artifact chunk count is invalid"))?
    };
    if count > MAX_CHUNK_COUNT {
        return Err(ApiError::invalid(
            "artifact requires more chunks than the configured bounded limit",
        ));
    }
    Ok(count)
}

fn expected_chunk_length(upload: &ActiveUploadRow, chunk_index: u64) -> Result<u64, ApiError> {
    let chunk_count = u64::try_from(upload.chunk_count).map_err(|_| ApiError::internal())?;
    if chunk_index >= chunk_count {
        return Err(ApiError::invalid(
            "chunk index is outside the negotiated artifact range",
        ));
    }
    let chunk_size = u64::try_from(upload.chunk_size_bytes).map_err(|_| ApiError::internal())?;
    let stored_size =
        u64::try_from(upload.declared_stored_size_bytes).map_err(|_| ApiError::internal())?;
    let offset = chunk_index
        .checked_mul(chunk_size)
        .ok_or_else(ApiError::internal)?;
    if chunk_index + 1 == chunk_count {
        stored_size
            .checked_sub(offset)
            .ok_or_else(ApiError::internal)
    } else {
        Ok(chunk_size)
    }
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

async fn get_active_upload(
    database: &SqlitePool,
    upload_id: &str,
) -> Result<Option<ActiveUploadRow>, ApiError> {
    sqlx::query_as(
        "SELECT id, owner_namespace, session_id, client_id, manifest_sha256, status, snapshot_id,
                created_at, chunk_size_bytes, chunk_count, declared_stored_size_bytes,
                declared_original_size_bytes, expires_at
         FROM uploads WHERE id = ?1",
    )
    .bind(upload_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())
}

async fn get_chunk(
    database: &SqlitePool,
    upload_id: &str,
    chunk_index: u64,
) -> Result<Option<ChunkRow>, ApiError> {
    sqlx::query_as(
        "SELECT chunk_index, byte_length, sha256 FROM upload_chunks
         WHERE upload_id = ?1 AND artifact_index = 0 AND chunk_index = ?2",
    )
    .bind(upload_id)
    .bind(to_sqlite_i64(chunk_index)?)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())
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
}

#[derive(Clone, FromRow)]
struct ActiveUploadRow {
    id: String,
    owner_namespace: String,
    session_id: String,
    client_id: String,
    manifest_sha256: String,
    status: String,
    snapshot_id: Option<String>,
    created_at: String,
    chunk_size_bytes: i64,
    chunk_count: i64,
    declared_stored_size_bytes: i64,
    declared_original_size_bytes: i64,
    expires_at: String,
}

#[derive(FromRow)]
struct ChunkRow {
    chunk_index: i64,
    byte_length: i64,
    sha256: String,
}

#[derive(FromRow)]
struct AuditUploadRow {
    upload_id: String,
    session_id: String,
    chunk_size_bytes: i64,
    terminal_reason: String,
}

#[derive(FromRow)]
struct UploadRaceRow {
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

/// Reconciles file-first chunk persistence after a restart.
///
/// A durable chunk file is linked and synced before its record is committed.
/// Therefore orphan files can be discarded safely; accepted records without a
/// file are removed so a client can retry the affected chunk. Completed and
/// terminal upload directories are always disposable after their metadata state
/// has committed.
pub(crate) async fn recover_uploads(state: &AppState) -> Result<(), MaintenanceError> {
    upgrade_legacy_uploads(state).await?;
    let uploads =
        sqlx::query_as::<_, RecoveryUploadRow>("SELECT id, status, chunk_count FROM uploads")
            .fetch_all(&state.database)
            .await
            .map_err(|_| MaintenanceError::Operation)?;
    let audits = sqlx::query_as::<_, (String,)>("SELECT upload_id FROM upload_audits")
        .fetch_all(&state.database)
        .await
        .map_err(|_| MaintenanceError::Operation)?;

    let known_uploads: HashSet<String> = uploads
        .iter()
        .map(|upload| upload.id.clone())
        .chain(audits.iter().map(|audit| audit.0.clone()))
        .collect();
    remove_unknown_upload_directories(state, &known_uploads).await?;

    for upload in uploads {
        if upload.status == "completed" {
            state
                .storage
                .remove_upload_dir(&upload.id)
                .await
                .map_err(|_| MaintenanceError::Operation)?;
        } else {
            recover_active_upload(state, &upload).await?;
        }
    }
    for (upload_id,) in audits {
        state
            .storage
            .remove_upload_dir(&upload_id)
            .await
            .map_err(|_| MaintenanceError::Operation)?;
    }
    recover_unreferenced_blobs(state).await?;
    Ok(())
}

/// Migrates the pre-chunk v1 temporary `artifact` file when a volume created by
/// the previous server is first opened. The migration keeps the old file until
/// its new chunk record commits, so either crash ordering is retryable.
async fn upgrade_legacy_uploads(state: &AppState) -> Result<(), MaintenanceError> {
    let legacy = sqlx::query_as::<_, LegacyUploadRow>(
        "SELECT u.id, m.canonical_json
         FROM uploads u JOIN manifests m ON m.upload_id = u.id
         WHERE u.status IN ('created', 'artifact_uploaded') AND u.expires_at = ?1",
    )
    .bind(LEGACY_EXPIRY_MARKER)
    .fetch_all(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    if legacy.is_empty() {
        return Ok(());
    }
    let now = OffsetDateTime::now_utc();
    let now_text = format_time(now).map_err(|_| MaintenanceError::Clock)?;
    let expires_at =
        database::expiration_at(now, state.upload_expiry).map_err(|_| MaintenanceError::Clock)?;
    for upload in legacy {
        upgrade_legacy_upload(state, upload, &now_text, &expires_at).await?;
    }
    Ok(())
}

async fn upgrade_legacy_upload(
    state: &AppState,
    upload: LegacyUploadRow,
    now: &str,
    expires_at: &str,
) -> Result<(), MaintenanceError> {
    let Ok(manifest) = serde_json::from_str::<Manifest>(&upload.canonical_json) else {
        return Ok(());
    };
    let stored_size = manifest.artifact.stored_size_bytes;
    let chunk_size = if stored_size == 0 {
        state.chunk_size_bytes
    } else {
        stored_size
    };
    let source = state.storage.upload_dir(&upload.id).join("artifact");
    let accepted = migrate_legacy_file(state, &upload.id, &manifest, &source).await?;
    let layout = LegacyLayout {
        upload_id: &upload.id,
        manifest: &manifest,
        chunk_size,
        accepted,
        now,
        expires_at,
    };
    persist_legacy_layout(state, &layout).await?;
    if accepted {
        StorageLayout::remove_file(&source)
            .await
            .map_err(|_| MaintenanceError::Operation)?;
    }
    Ok(())
}

async fn migrate_legacy_file(
    state: &AppState,
    upload_id: &str,
    manifest: &Manifest,
    source: &Path,
) -> Result<bool, MaintenanceError> {
    let stored_size = manifest.artifact.stored_size_bytes;
    if stored_size == 0
        || !fs::try_exists(source)
            .await
            .map_err(|_| MaintenanceError::Operation)?
        || verify_stored_file(
            source.to_path_buf(),
            stored_size,
            &manifest.artifact.stored_sha256,
        )
        .await
        .is_err()
    {
        return Ok(false);
    }
    state
        .storage
        .ensure_chunk_dir(upload_id)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
    let destination = state.storage.chunk_path(upload_id, 0);
    match fs::hard_link(source, &destination).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            Ok(
                verify_stored_file(destination, stored_size, &manifest.artifact.stored_sha256)
                    .await
                    .is_ok(),
            )
        }
        Err(_) => Err(MaintenanceError::Operation),
    }
}

struct LegacyLayout<'a> {
    upload_id: &'a str,
    manifest: &'a Manifest,
    chunk_size: u64,
    accepted: bool,
    now: &'a str,
    expires_at: &'a str,
}

async fn persist_legacy_layout(
    state: &AppState,
    layout: &LegacyLayout<'_>,
) -> Result<(), MaintenanceError> {
    let stored_size = layout.manifest.artifact.stored_size_bytes;
    let status = if stored_size == 0 || layout.accepted {
        "artifact_uploaded"
    } else {
        "created"
    };
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| MaintenanceError::Operation)?;
    sqlx::query(
        "UPDATE uploads SET
            chunk_size_bytes = ?1, chunk_count = ?2, declared_stored_size_bytes = ?3,
            declared_original_size_bytes = ?4, expires_at = ?5, status = ?6
         WHERE id = ?7",
    )
    .bind(i64::try_from(layout.chunk_size).map_err(|_| MaintenanceError::Operation)?)
    .bind(i64::from(stored_size != 0))
    .bind(i64::try_from(stored_size).map_err(|_| MaintenanceError::Operation)?)
    .bind(
        i64::try_from(layout.manifest.artifact.original_size_bytes)
            .map_err(|_| MaintenanceError::Operation)?,
    )
    .bind(layout.expires_at)
    .bind(status)
    .bind(layout.upload_id)
    .execute(&mut *transaction)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    if layout.accepted {
        sqlx::query(
            "INSERT INTO upload_chunks (
                upload_id, artifact_index, chunk_index, byte_length, sha256, accepted_at
             ) VALUES (?1, 0, 0, ?2, ?3, ?4)
             ON CONFLICT(upload_id, artifact_index, chunk_index) DO NOTHING",
        )
        .bind(layout.upload_id)
        .bind(i64::try_from(stored_size).map_err(|_| MaintenanceError::Operation)?)
        .bind(digest_storage_value(
            &layout.manifest.artifact.stored_sha256,
        ))
        .bind(layout.now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
    }
    transaction
        .commit()
        .await
        .map_err(|_| MaintenanceError::Operation)
}

async fn recover_unreferenced_blobs(state: &AppState) -> Result<(), MaintenanceError> {
    let unreferenced = sqlx::query_as::<_, OrphanedBlobRow>(
        "SELECT b.id, b.stored_sha256
         FROM blobs b LEFT JOIN artifacts a ON a.blob_id = b.id
         WHERE a.id IS NULL",
    )
    .fetch_all(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    if !unreferenced.is_empty() {
        let mut transaction = state
            .database
            .begin()
            .await
            .map_err(|_| MaintenanceError::Operation)?;
        for blob in &unreferenced {
            sqlx::query("DELETE FROM blobs WHERE id = ?1")
                .bind(&blob.id)
                .execute(&mut *transaction)
                .await
                .map_err(|_| MaintenanceError::Operation)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| MaintenanceError::Operation)?;
        for blob in unreferenced {
            StorageLayout::remove_file(&state.storage.blob_path(&blob.stored_sha256))
                .await
                .map_err(|_| MaintenanceError::Operation)?;
        }
    }

    let known: HashSet<String> = sqlx::query_as::<_, (String,)>("SELECT stored_sha256 FROM blobs")
        .fetch_all(&state.database)
        .await
        .map_err(|_| MaintenanceError::Operation)?
        .into_iter()
        .map(|row| row.0)
        .collect();
    let sha256_root = state.storage.blobs.join("sha256");
    match fs::read_dir(&sha256_root).await {
        Ok(mut shard_entries) => {
            while let Some(shard) = shard_entries
                .next_entry()
                .await
                .map_err(|_| MaintenanceError::Operation)?
            {
                if !shard
                    .file_type()
                    .await
                    .map_err(|_| MaintenanceError::Operation)?
                    .is_dir()
                {
                    continue;
                }
                let mut blob_entries = fs::read_dir(shard.path())
                    .await
                    .map_err(|_| MaintenanceError::Operation)?;
                while let Some(blob) = blob_entries
                    .next_entry()
                    .await
                    .map_err(|_| MaintenanceError::Operation)?
                {
                    let Ok(name) = blob.file_name().into_string() else {
                        continue;
                    };
                    if !known.contains(&name) {
                        remove_recovery_entry(&blob.path()).await?;
                    }
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(MaintenanceError::Operation),
    }
    Ok(())
}

async fn remove_unknown_upload_directories(
    state: &AppState,
    known_uploads: &HashSet<String>,
) -> Result<(), MaintenanceError> {
    let mut entries = fs::read_dir(&state.storage.uploads)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| MaintenanceError::Operation)?
    {
        let name = entry.file_name();
        let Ok(name) = name.into_string() else {
            continue;
        };
        if Uuid::parse_str(&name).is_ok() && !known_uploads.contains(&name) {
            fs::remove_dir_all(entry.path())
                .await
                .map_err(|_| MaintenanceError::Operation)?;
        }
    }
    Ok(())
}

async fn recover_active_upload(
    state: &AppState,
    upload: &RecoveryUploadRow,
) -> Result<(), MaintenanceError> {
    let rows = sqlx::query_as::<_, ChunkRow>(
        "SELECT chunk_index, byte_length, sha256 FROM upload_chunks
         WHERE upload_id = ?1 AND artifact_index = 0",
    )
    .bind(&upload.id)
    .fetch_all(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    let accepted: HashSet<u64> = rows
        .iter()
        .map(|row| u64::try_from(row.chunk_index))
        .collect::<Result<_, _>>()
        .map_err(|_| MaintenanceError::Operation)?;
    for row in rows {
        let chunk_index =
            u64::try_from(row.chunk_index).map_err(|_| MaintenanceError::Operation)?;
        let path = state.storage.chunk_path(&upload.id, chunk_index);
        if !fs::try_exists(&path)
            .await
            .map_err(|_| MaintenanceError::Operation)?
        {
            sqlx::query(
                "DELETE FROM upload_chunks
                 WHERE upload_id = ?1 AND artifact_index = 0 AND chunk_index = ?2",
            )
            .bind(&upload.id)
            .bind(row.chunk_index)
            .execute(&state.database)
            .await
            .map_err(|_| MaintenanceError::Operation)?;
        }
    }

    let chunk_dir = state.storage.chunk_dir(&upload.id);
    match fs::read_dir(&chunk_dir).await {
        Ok(mut entries) => {
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|_| MaintenanceError::Operation)?
            {
                let name = entry.file_name();
                let Ok(name) = name.into_string() else {
                    continue;
                };
                let parsed_index = name.parse::<u64>().ok();
                if !parsed_index.is_some_and(|index| accepted.contains(&index)) {
                    remove_recovery_entry(&entry.path()).await?;
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(MaintenanceError::Operation),
    }

    let accepted_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM upload_chunks WHERE upload_id = ?1 AND artifact_index = 0",
    )
    .bind(&upload.id)
    .fetch_one(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;
    let status = if accepted_count.0 == upload.chunk_count {
        "artifact_uploaded"
    } else {
        "created"
    };
    sqlx::query(
        "UPDATE uploads SET status = ?1
         WHERE id = ?2 AND status IN ('created', 'artifact_uploaded')",
    )
    .bind(status)
    .bind(&upload.id)
    .execute(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;

    let upload_dir = state.storage.upload_dir(&upload.id);
    match fs::read_dir(&upload_dir).await {
        Ok(mut entries) => {
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|_| MaintenanceError::Operation)?
            {
                let name = entry.file_name();
                let Ok(name) = name.into_string() else {
                    continue;
                };
                if name.starts_with(".assembled-") || name == "artifact" {
                    remove_recovery_entry(&entry.path()).await?;
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(MaintenanceError::Operation),
    }
    Ok(())
}

async fn remove_recovery_entry(path: &Path) -> Result<(), MaintenanceError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
    if metadata.is_dir() {
        fs::remove_dir_all(path)
            .await
            .map_err(|_| MaintenanceError::Operation)
    } else {
        StorageLayout::remove_file(path)
            .await
            .map_err(|_| MaintenanceError::Operation)
    }
}

#[derive(FromRow)]
struct RecoveryUploadRow {
    id: String,
    status: String,
    chunk_count: i64,
}

#[derive(FromRow)]
struct OrphanedBlobRow {
    id: String,
    stored_sha256: String,
}

#[derive(FromRow)]
struct LegacyUploadRow {
    id: String,
    canonical_json: String,
}
