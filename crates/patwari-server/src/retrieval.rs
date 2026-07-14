//! Read-only archive browsing resources and their stable pagination rules.

use std::{
    future::Future,
    io,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path as AxumPath, Query, State, rejection::QueryRejection},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, SqlitePool};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::OwnedSemaphorePermit,
    time::{Instant, Sleep},
};
use tokio_util::io::ReaderStream;

use crate::{
    contract::{
        ArtifactMetadataResponse, ArtifactResponse, CanonicalManifestResponse,
        CanonicalManifestSummary, CaptureProvenance, CompletionResponse, CompletionTransfer,
        Compression, Manifest, PaginatedResponse, Receipt, SessionLatestSnapshot, SessionResponse,
        SnapshotCapturesResponse, SnapshotResponse, SnapshotSummary,
    },
    database,
    error::ApiError,
    ingestion,
    pagination::{
        PageRequest, SortBoundary, append_descending_bounds, bind_descending_bounds, filter_hash,
        page_from_rows, parse_page,
    },
    service::AppState,
    validation::{parse_uuid, validate_capture_identifier},
};

const MAX_CONTEXT_FILTER_BYTES: usize = 512;
const MAX_SOURCE_AGENT_FILTER_BYTES: usize = 128;

const SESSION_CURSOR_KIND: &str = "sessions";
const SESSION_CAPTURE_CURSOR_KIND: &str = "session_captures";
const SESSION_SNAPSHOT_CURSOR_KIND: &str = "session_snapshots";
const SNAPSHOT_CAPTURE_CURSOR_KIND: &str = "snapshot_captures";
const CAPTURE_CURSOR_KIND: &str = "captures";
const SNAPSHOT_CURSOR_KIND: &str = "snapshots";
const MANIFEST_CURSOR_KIND: &str = "manifests";
const ARTIFACT_CURSOR_KIND: &str = "artifacts";
const DOWNLOAD_BUFFER_BYTES: usize = 64 * 1024;

fn normalize_optional_filter(
    value: Option<String>,
    maximum: usize,
    message: &'static str,
) -> Result<Option<String>, ApiError> {
    if let Some(value) = &value
        && (value.len() > maximum || value.bytes().any(|byte| byte.is_ascii_control()))
    {
        return Err(ApiError::invalid(message));
    }
    Ok(value)
}

fn normalize_optional_nonempty_filter(
    value: Option<String>,
    maximum: usize,
    message: &'static str,
) -> Result<Option<String>, ApiError> {
    let value = normalize_optional_filter(value, maximum, message)?;
    if value.as_deref() == Some("") {
        return Err(ApiError::invalid(message));
    }
    Ok(value)
}

fn normalize_activity_range(
    activity_from: Option<&str>,
    activity_to: Option<&str>,
) -> Result<(Option<i64>, Option<i64>), ApiError> {
    let activity_from = activity_from
        .map(|value| activity_sort_key(value, "activity timestamp must be RFC 3339"))
        .transpose()?;
    let activity_to = activity_to
        .map(|value| activity_sort_key(value, "activity timestamp must be RFC 3339"))
        .transpose()?;
    if activity_from
        .zip(activity_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(ApiError::invalid(
            "activity_from must not be later than activity_to",
        ));
    }
    Ok((activity_from, activity_to))
}

/// Parses a client-supplied activity filter into the same numeric ordering
/// key stored alongside the RFC 3339 columns it is compared against, so the
/// filter's chronological meaning matches its SQL comparison exactly (see
/// `SortBoundary`).
fn activity_sort_key(value: &str, message: &'static str) -> Result<i64, ApiError> {
    database::sort_key_from_rfc3339(value).map_err(|_| ApiError::invalid(message))
}

fn normalize_client_id(value: Option<&str>) -> Result<Option<String>, ApiError> {
    value
        .map(|value| parse_uuid(value, "client identifier is not a UUID").map(|id| id.to_string()))
        .transpose()
}

fn normalize_resource_id(
    value: Option<&str>,
    message: &'static str,
) -> Result<Option<String>, ApiError> {
    value
        .map(|value| parse_uuid(value, message).map(|id| id.to_string()))
        .transpose()
}

fn normalize_artifact_set_version(value: Option<u16>) -> Result<Option<u16>, ApiError> {
    if value == Some(0) {
        return Err(ApiError::invalid(
            "artifact set version must be a non-zero adapter contract version",
        ));
    }
    Ok(value)
}

fn query_error<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    query
        .map(|Query(value)| value)
        .map_err(|_| ApiError::invalid("query parameters are invalid"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    client_id: Option<String>,
    activity_from: Option<String>,
    activity_to: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SessionFilters {
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    client_id: Option<String>,
    activity_from: Option<i64>,
    activity_to: Option<i64>,
}

fn session_filters(query: &SessionListQuery) -> Result<SessionFilters, ApiError> {
    let (activity_from, activity_to) =
        normalize_activity_range(query.activity_from.as_deref(), query.activity_to.as_deref())?;
    Ok(SessionFilters {
        source_agent: normalize_optional_nonempty_filter(
            query.source_agent.clone(),
            MAX_SOURCE_AGENT_FILTER_BYTES,
            "source agent filter is invalid",
        )?,
        repository: normalize_optional_filter(
            query.repository.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "repository filter is invalid",
        )?,
        project: normalize_optional_filter(
            query.project.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "project filter is invalid",
        )?,
        branch: normalize_optional_filter(
            query.branch.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "branch filter is invalid",
        )?,
        source_agent_version: normalize_optional_filter(
            query.source_agent_version.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "source agent version filter is invalid",
        )?,
        artifact_set_version: normalize_artifact_set_version(query.artifact_set_version)?,
        client_id: normalize_client_id(query.client_id.as_deref())?,
        activity_from,
        activity_to,
    })
}

#[derive(FromRow)]
struct SessionRow {
    session_id: String,
    source_agent: String,
    source_session_id: String,
    created_at: String,
    updated_at: String,
    snapshot_id: String,
    completed_at: String,
    completed_at_seq: i64,
    project: Option<String>,
    repository: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: i64,
}

fn session_response(row: SessionRow) -> Result<SessionResponse, ApiError> {
    let artifact_set_version =
        u16::try_from(row.artifact_set_version).map_err(|_| ApiError::internal())?;
    let session_id = row.session_id;
    let snapshot_id = row.snapshot_id;
    Ok(SessionResponse {
        source_agent: row.source_agent,
        source_session_id: row.source_session_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
        captures_url: format!("/api/v1/sessions/{session_id}/captures"),
        snapshots_url: format!("/api/v1/sessions/{session_id}/snapshots"),
        session_id,
        latest_snapshot: SessionLatestSnapshot {
            manifest_url: format!("/api/v1/snapshots/{snapshot_id}/manifest"),
            snapshot_url: format!("/api/v1/snapshots/{snapshot_id}"),
            snapshot_id,
            completed_at: row.completed_at,
            project: row.project,
            repository: row.repository,
            branch: row.branch,
            source_agent_version: row.source_agent_version,
            artifact_set_version,
        },
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn list_sessions(
    State(state): State<Arc<AppState>>,
    query: Result<Query<SessionListQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<SessionResponse>>, ApiError> {
    let query = query_error(query)?;
    let filters = session_filters(&query)?;
    let filter_hash = filter_hash(SESSION_CURSOR_KIND, &filters)?;
    let page = parse_page(query.limit, query.cursor, SESSION_CURSOR_KIND, &filter_hash)?;

    let mut sql = String::from(
        "SELECT s.id AS session_id, s.source_agent, s.source_session_id, s.created_at, s.updated_at,
                p.snapshot_id, p.completed_at, p.completed_at_seq, p.project, p.repository, p.branch,
                p.source_agent_version, p.artifact_set_version
         FROM session_latest_context p
         JOIN sessions s ON s.id = p.session_id
         JOIN snapshots snapshot ON snapshot.id = p.snapshot_id
         WHERE snapshot.deleted_at IS NULL",
    );
    if filters.source_agent.is_some() {
        sql.push_str(" AND p.source_agent = ?");
    }
    if filters.repository.is_some() {
        sql.push_str(" AND p.repository = ?");
    }
    if filters.project.is_some() {
        sql.push_str(" AND p.project = ?");
    }
    if filters.branch.is_some() {
        sql.push_str(" AND p.branch = ?");
    }
    if filters.source_agent_version.is_some() {
        sql.push_str(" AND p.source_agent_version = ?");
    }
    if filters.artifact_set_version.is_some() {
        sql.push_str(" AND p.artifact_set_version = ?");
    }
    if filters.client_id.is_some() {
        sql.push_str(
            " AND EXISTS (
                SELECT 1 FROM captures client_capture
                WHERE client_capture.snapshot_id = p.snapshot_id
                  AND client_capture.client_id = ?
            )",
        );
    }
    if filters.activity_from.is_some() {
        sql.push_str(" AND p.completed_at_seq >= ?");
    }
    if filters.activity_to.is_some() {
        sql.push_str(" AND p.completed_at_seq <= ?");
    }
    append_descending_bounds(&mut sql, "p.completed_at_seq", "p.session_id", &page);
    sql.push_str(" ORDER BY p.completed_at_seq DESC, p.session_id DESC LIMIT ?");

    let mut request = sqlx::query_as::<_, SessionRow>(&sql);
    if let Some(value) = &filters.source_agent {
        request = request.bind(value);
    }
    if let Some(value) = &filters.repository {
        request = request.bind(value);
    }
    if let Some(value) = &filters.project {
        request = request.bind(value);
    }
    if let Some(value) = &filters.branch {
        request = request.bind(value);
    }
    if let Some(value) = &filters.source_agent_version {
        request = request.bind(value);
    }
    if let Some(value) = filters.artifact_set_version {
        request = request.bind(i64::from(value));
    }
    if let Some(value) = &filters.client_id {
        request = request.bind(value);
    }
    if let Some(value) = filters.activity_from {
        request = request.bind(value);
    }
    if let Some(value) = filters.activity_to {
        request = request.bind(value);
    }
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.completed_at_seq,
                timestamp: row.completed_at.clone(),
                id: row.session_id.clone(),
            };
            session_response(row).map(|response| (response, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        SESSION_CURSOR_KIND,
        filter_hash,
    )?))
}

pub(crate) async fn get_session(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SessionResponse>, ApiError> {
    let session_id = parse_uuid(&session_id, "session identifier is not a UUID")?.to_string();
    let row = sqlx::query_as::<_, SessionRow>(
        "SELECT s.id AS session_id, s.source_agent, s.source_session_id, s.created_at, s.updated_at,
                p.snapshot_id, p.completed_at, p.completed_at_seq, p.project, p.repository, p.branch,
                p.source_agent_version, p.artifact_set_version
         FROM session_latest_context p
         JOIN sessions s ON s.id = p.session_id
         JOIN snapshots snapshot ON snapshot.id = p.snapshot_id
         WHERE p.session_id = ? AND snapshot.deleted_at IS NULL",
    )
    .bind(session_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("session_not_found", "session was not found"))?;
    session_response(row).map(Json)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionCollectionQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

pub(crate) async fn list_session_captures(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    query: Result<Query<SessionCollectionQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<CaptureProvenance>>, ApiError> {
    let session_id = parse_uuid(&session_id, "session identifier is not a UUID")?.to_string();
    let query = query_error(query)?;
    let filters = SessionOnlyFilter {
        session_id: session_id.clone(),
    };
    let filter_hash = filter_hash(SESSION_CAPTURE_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        SESSION_CAPTURE_CURSOR_KIND,
        &filter_hash,
    )?;
    let mut sql = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         WHERE c.session_id = ? AND snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    append_descending_bounds(&mut sql, "c.server_completed_at_seq", "c.id", &page);
    sql.push_str(" ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT ?");
    let mut request = sqlx::query_as::<_, CaptureRow>(&sql).bind(session_id);
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.server_completed_at_seq,
                timestamp: row.server_completed_at.clone(),
                id: row.id.clone(),
            };
            capture_from_row(row).map(|capture| (capture, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        SESSION_CAPTURE_CURSOR_KIND,
        filter_hash,
    )?))
}

#[derive(Clone, Debug, Serialize)]
struct SessionOnlyFilter {
    session_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct SnapshotOnlyFilter {
    snapshot_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    client_id: Option<String>,
    capture_id: Option<String>,
    session_id: Option<String>,
    snapshot_id: Option<String>,
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    activity_from: Option<String>,
    activity_to: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct CaptureFilters {
    client_id: Option<String>,
    session_id: Option<String>,
    snapshot_id: Option<String>,
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    activity_from: Option<i64>,
    activity_to: Option<i64>,
}

fn capture_filters(query: &CaptureListQuery) -> Result<CaptureFilters, ApiError> {
    let (activity_from, activity_to) =
        normalize_activity_range(query.activity_from.as_deref(), query.activity_to.as_deref())?;
    Ok(CaptureFilters {
        client_id: normalize_client_id(query.client_id.as_deref())?,
        session_id: normalize_resource_id(
            query.session_id.as_deref(),
            "session identifier is not a UUID",
        )?,
        snapshot_id: normalize_resource_id(
            query.snapshot_id.as_deref(),
            "snapshot identifier is not a UUID",
        )?,
        source_agent: normalize_optional_nonempty_filter(
            query.source_agent.clone(),
            MAX_SOURCE_AGENT_FILTER_BYTES,
            "source agent filter is invalid",
        )?,
        repository: normalize_optional_filter(
            query.repository.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "repository filter is invalid",
        )?,
        project: normalize_optional_filter(
            query.project.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "project filter is invalid",
        )?,
        branch: normalize_optional_filter(
            query.branch.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "branch filter is invalid",
        )?,
        source_agent_version: normalize_optional_filter(
            query.source_agent_version.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "source agent version filter is invalid",
        )?,
        artifact_set_version: normalize_artifact_set_version(query.artifact_set_version)?,
        activity_from,
        activity_to,
    })
}

/// Lists capture provenance, or preserves the original exact
/// `(client_id, capture_id)` lookup when both fields are supplied alone.
pub(crate) async fn captures(
    State(state): State<Arc<AppState>>,
    query: Result<Query<CaptureListQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let query = query_error(query)?;
    if let Some(capture_id) = &query.capture_id {
        let client_id = query
            .client_id
            .as_deref()
            .ok_or_else(|| ApiError::invalid("capture_id requires a client_id exact lookup"))?;
        if query.limit.is_some()
            || query.cursor.is_some()
            || query.session_id.is_some()
            || query.snapshot_id.is_some()
            || query.source_agent.is_some()
            || query.repository.is_some()
            || query.project.is_some()
            || query.branch.is_some()
            || query.source_agent_version.is_some()
            || query.artifact_set_version.is_some()
            || query.activity_from.is_some()
            || query.activity_to.is_some()
        {
            return Err(ApiError::invalid(
                "capture exact lookup cannot be combined with list filters",
            ));
        }
        let client_id = parse_uuid(client_id, "client identifier is not a UUID")?.to_string();
        validate_capture_identifier(capture_id)?;
        return capture_by_client(&state.database, &client_id, capture_id)
            .await
            .map(|capture| Json(capture).into_response());
    }
    list_captures(state, query)
        .await
        .map(IntoResponse::into_response)
}

#[allow(clippy::too_many_lines)]
async fn list_captures(
    state: Arc<AppState>,
    query: CaptureListQuery,
) -> Result<Json<PaginatedResponse<CaptureProvenance>>, ApiError> {
    let filters = capture_filters(&query)?;
    let filter_hash = filter_hash(CAPTURE_CURSOR_KIND, &filters)?;
    let page = parse_page(query.limit, query.cursor, CAPTURE_CURSOR_KIND, &filter_hash)?;
    let mut sql = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         JOIN sessions session ON session.id = c.session_id
         WHERE snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    if filters.client_id.is_some() {
        sql.push_str(" AND c.client_id = ?");
    }
    if filters.session_id.is_some() {
        sql.push_str(" AND c.session_id = ?");
    }
    if filters.snapshot_id.is_some() {
        sql.push_str(" AND c.snapshot_id = ?");
    }
    if filters.source_agent.is_some() {
        sql.push_str(" AND session.source_agent = ?");
    }
    if filters.repository.is_some() {
        sql.push_str(" AND c.repository = ?");
    }
    if filters.project.is_some() {
        sql.push_str(" AND c.project = ?");
    }
    if filters.branch.is_some() {
        sql.push_str(" AND c.branch = ?");
    }
    if filters.source_agent_version.is_some() {
        sql.push_str(" AND c.source_agent_version = ?");
    }
    if filters.artifact_set_version.is_some() {
        sql.push_str(" AND c.artifact_set_version = ?");
    }
    if filters.activity_from.is_some() {
        sql.push_str(" AND c.server_completed_at_seq >= ?");
    }
    if filters.activity_to.is_some() {
        sql.push_str(" AND c.server_completed_at_seq <= ?");
    }
    append_descending_bounds(&mut sql, "c.server_completed_at_seq", "c.id", &page);
    sql.push_str(" ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT ?");
    let mut request = sqlx::query_as::<_, CaptureRow>(&sql);
    if let Some(value) = &filters.client_id {
        request = request.bind(value);
    }
    if let Some(value) = &filters.session_id {
        request = request.bind(value);
    }
    if let Some(value) = &filters.snapshot_id {
        request = request.bind(value);
    }
    if let Some(value) = &filters.source_agent {
        request = request.bind(value);
    }
    if let Some(value) = &filters.repository {
        request = request.bind(value);
    }
    if let Some(value) = &filters.project {
        request = request.bind(value);
    }
    if let Some(value) = &filters.branch {
        request = request.bind(value);
    }
    if let Some(value) = &filters.source_agent_version {
        request = request.bind(value);
    }
    if let Some(value) = filters.artifact_set_version {
        request = request.bind(i64::from(value));
    }
    if let Some(value) = filters.activity_from {
        request = request.bind(value);
    }
    if let Some(value) = filters.activity_to {
        request = request.bind(value);
    }
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.server_completed_at_seq,
                timestamp: row.server_completed_at.clone(),
                id: row.id.clone(),
            };
            capture_from_row(row).map(|capture| (capture, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        CAPTURE_CURSOR_KIND,
        filter_hash,
    )?))
}

pub(crate) async fn get_capture_by_upload(
    AxumPath(upload_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CaptureProvenance>, ApiError> {
    let upload_id = parse_uuid(&upload_id, "upload identifier is not a UUID")?.to_string();
    capture_for_upload(&state.database, &upload_id)
        .await
        .map(Json)
}

pub(crate) async fn get_capture(
    AxumPath(capture_record_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CaptureProvenance>, ApiError> {
    let capture_record_id = parse_uuid(
        &capture_record_id,
        "capture record identifier is not a UUID",
    )?
    .to_string();
    let query = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         WHERE c.id = ? AND snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    let row = sqlx::query_as::<_, CaptureRow>(&query)
        .bind(capture_record_id)
        .fetch_optional(&state.database)
        .await
        .map_err(|_| ApiError::database())?
        .ok_or_else(|| ApiError::not_found("capture_not_found", "capture was not found"))?;
    capture_from_row(row).map(Json)
}

pub(crate) async fn get_snapshot_captures(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    query: Result<Query<SessionCollectionQuery>, QueryRejection>,
) -> Result<Json<SnapshotCapturesResponse>, ApiError> {
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    let query = query_error(query)?;
    let exists: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM snapshots WHERE id = ? AND deleted_at IS NULL")
            .bind(&snapshot_id)
            .fetch_optional(&state.database)
            .await
            .map_err(|_| ApiError::database())?;
    if exists.is_none() {
        return Err(ApiError::not_found(
            "snapshot_not_found",
            "snapshot was not found",
        ));
    }
    let filters = SnapshotOnlyFilter {
        snapshot_id: snapshot_id.clone(),
    };
    let filter_hash = filter_hash(SNAPSHOT_CAPTURE_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        SNAPSHOT_CAPTURE_CURSOR_KIND,
        &filter_hash,
    )?;
    let query = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         WHERE c.snapshot_id = ? AND snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    let mut sql = query;
    append_descending_bounds(&mut sql, "c.server_completed_at_seq", "c.id", &page);
    sql.push_str(" ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT ?");
    let mut request = sqlx::query_as::<_, CaptureRow>(&sql).bind(&snapshot_id);
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.server_completed_at_seq,
                timestamp: row.server_completed_at.clone(),
                id: row.id.clone(),
            };
            capture_from_row(row).map(|capture| (capture, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let page = page_from_rows(rows, &page, SNAPSHOT_CAPTURE_CURSOR_KIND, filter_hash)?;
    Ok(Json(SnapshotCapturesResponse {
        snapshot_id,
        captures: page.items,
        next_cursor: page.next_cursor,
        high_watermark: page.high_watermark,
    }))
}

async fn capture_by_client(
    database: &SqlitePool,
    client_id: &str,
    capture_id: &str,
) -> Result<CaptureProvenance, ApiError> {
    let query = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         WHERE c.client_id = ? AND c.capture_id = ? AND snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    let row = sqlx::query_as::<_, CaptureRow>(&query)
        .bind(client_id)
        .bind(capture_id)
        .fetch_optional(database)
        .await
        .map_err(|_| ApiError::database())?
        .ok_or_else(|| ApiError::not_found("capture_not_found", "capture was not found"))?;
    capture_from_row(row)
}

pub(crate) async fn capture_for_upload(
    database: &SqlitePool,
    upload_id: &str,
) -> Result<CaptureProvenance, ApiError> {
    let query = format!(
        "{} JOIN snapshots snapshot ON snapshot.id = c.snapshot_id
         WHERE c.upload_id = ? AND snapshot.deleted_at IS NULL",
        capture_select_sql()
    );
    let row = sqlx::query_as::<_, CaptureRow>(&query)
        .bind(upload_id)
        .fetch_optional(database)
        .await
        .map_err(|_| ApiError::database())?
        .ok_or_else(|| ApiError::not_found("capture_not_found", "capture was not found"))?;
    capture_from_row(row)
}

fn capture_select_sql() -> &'static str {
    "SELECT c.id, c.capture_id, c.client_id, c.session_id, c.upload_id, c.snapshot_id, c.manifest_id,
            m.sha256 AS manifest_sha256, c.source_captured_at, c.source_cursor,
            c.source_state_hash, c.source_metadata_json, c.project, c.repository, c.branch,
            c.source_agent_version, c.artifact_set_version, c.munshi_version,
            c.server_received_at, c.server_completed_at, c.server_completed_at_seq
     FROM captures c JOIN manifests m ON m.id = c.manifest_id"
}

fn capture_from_row(row: CaptureRow) -> Result<CaptureProvenance, ApiError> {
    let source_metadata =
        serde_json::from_str(&row.source_metadata_json).map_err(|_| ApiError::internal())?;
    Ok(CaptureProvenance {
        capture_url: format!("/api/v1/captures/{}", row.id),
        manifest_url: format!("/api/v1/manifests/{}", row.manifest_id),
        capture_record_id: row.id,
        capture_id: row.capture_id,
        client_id: row.client_id,
        session_id: row.session_id,
        upload_id: row.upload_id,
        snapshot_id: row.snapshot_id,
        manifest_id: row.manifest_id,
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        source_captured_at: row.source_captured_at,
        source_cursor: row.source_cursor,
        source_state_hash: row.source_state_hash,
        source_metadata,
        project: row.project,
        repository: row.repository,
        branch: row.branch,
        source_agent_version: row.source_agent_version,
        artifact_set_version: u16::try_from(row.artifact_set_version)
            .map_err(|_| ApiError::internal())?,
        munshi_version: row.munshi_version,
        server_received_at: row.server_received_at,
        server_completed_at: row.server_completed_at,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    session_id: Option<String>,
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    client_id: Option<String>,
    activity_from: Option<String>,
    activity_to: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SnapshotFilters {
    session_id: Option<String>,
    source_agent: Option<String>,
    repository: Option<String>,
    project: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: Option<u16>,
    client_id: Option<String>,
    activity_from: Option<i64>,
    activity_to: Option<i64>,
}

fn snapshot_filters(query: &SnapshotListQuery) -> Result<SnapshotFilters, ApiError> {
    let (activity_from, activity_to) =
        normalize_activity_range(query.activity_from.as_deref(), query.activity_to.as_deref())?;
    Ok(SnapshotFilters {
        session_id: normalize_resource_id(
            query.session_id.as_deref(),
            "session identifier is not a UUID",
        )?,
        source_agent: normalize_optional_nonempty_filter(
            query.source_agent.clone(),
            MAX_SOURCE_AGENT_FILTER_BYTES,
            "source agent filter is invalid",
        )?,
        repository: normalize_optional_filter(
            query.repository.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "repository filter is invalid",
        )?,
        project: normalize_optional_filter(
            query.project.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "project filter is invalid",
        )?,
        branch: normalize_optional_filter(
            query.branch.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "branch filter is invalid",
        )?,
        source_agent_version: normalize_optional_filter(
            query.source_agent_version.clone(),
            MAX_CONTEXT_FILTER_BYTES,
            "source agent version filter is invalid",
        )?,
        artifact_set_version: normalize_artifact_set_version(query.artifact_set_version)?,
        client_id: normalize_client_id(query.client_id.as_deref())?,
        activity_from,
        activity_to,
    })
}

#[derive(FromRow)]
struct SnapshotListRow {
    id: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_id: String,
    manifest_sha256: String,
    completed_at: String,
    completed_at_seq: i64,
    artifact_count: i64,
    total_original_size_bytes: i64,
    total_stored_size_bytes: i64,
    capture_count: i64,
}

fn snapshot_summary(row: SnapshotListRow) -> Result<SnapshotSummary, ApiError> {
    let snapshot_id = row.id;
    let manifest_id = row.manifest_id;
    Ok(SnapshotSummary {
        snapshot_url: format!("/api/v1/snapshots/{snapshot_id}"),
        captures_url: format!("/api/v1/snapshots/{snapshot_id}/captures"),
        manifest_url: format!("/api/v1/manifests/{manifest_id}"),
        snapshot_id,
        session_id: row.session_id,
        snapshot_fingerprint: digest_document_value(&row.fingerprint_sha256),
        manifest_id,
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        completed_at: row.completed_at,
        artifact_count: u32::try_from(row.artifact_count).map_err(|_| ApiError::internal())?,
        total_original_bytes: u64::try_from(row.total_original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        total_stored_bytes: u64::try_from(row.total_stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        capture_count: u64::try_from(row.capture_count).map_err(|_| ApiError::internal())?,
    })
}

pub(crate) async fn list_snapshots(
    State(state): State<Arc<AppState>>,
    query: Result<Query<SnapshotListQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<SnapshotSummary>>, ApiError> {
    let query = query_error(query)?;
    let filters = snapshot_filters(&query)?;
    let filter_hash = filter_hash(SNAPSHOT_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        SNAPSHOT_CURSOR_KIND,
        &filter_hash,
    )?;
    let rows = query_snapshot_rows(&state.database, &filters, &page).await?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.completed_at_seq,
                timestamp: row.completed_at.clone(),
                id: row.id.clone(),
            };
            snapshot_summary(row).map(|snapshot| (snapshot, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        SNAPSHOT_CURSOR_KIND,
        filter_hash,
    )?))
}

pub(crate) async fn list_session_snapshots(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    query: Result<Query<SessionCollectionQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<SnapshotSummary>>, ApiError> {
    let session_id = parse_uuid(&session_id, "session identifier is not a UUID")?.to_string();
    let query = query_error(query)?;
    let filters = SnapshotFilters {
        session_id: Some(session_id),
        source_agent: None,
        repository: None,
        project: None,
        branch: None,
        source_agent_version: None,
        artifact_set_version: None,
        client_id: None,
        activity_from: None,
        activity_to: None,
    };
    let filter_hash = filter_hash(SESSION_SNAPSHOT_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        SESSION_SNAPSHOT_CURSOR_KIND,
        &filter_hash,
    )?;
    let rows = query_snapshot_rows(&state.database, &filters, &page).await?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.completed_at_seq,
                timestamp: row.completed_at.clone(),
                id: row.id.clone(),
            };
            snapshot_summary(row).map(|snapshot| (snapshot, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        SESSION_SNAPSHOT_CURSOR_KIND,
        filter_hash,
    )?))
}

async fn query_snapshot_rows(
    database: &SqlitePool,
    filters: &SnapshotFilters,
    page: &PageRequest,
) -> Result<Vec<SnapshotListRow>, ApiError> {
    let mut sql = String::from(
        "SELECT s.id, s.session_id, s.fingerprint_sha256, m.id AS manifest_id,
                m.sha256 AS manifest_sha256, s.completed_at, s.completed_at_seq, s.artifact_count,
                s.total_original_size_bytes, s.total_stored_size_bytes,
                (SELECT COUNT(*) FROM captures c WHERE c.snapshot_id = s.id) AS capture_count
         FROM snapshots s
         JOIN manifests m ON m.id = s.manifest_id
         JOIN sessions session ON session.id = s.session_id
         WHERE s.deleted_at IS NULL",
    );
    if filters.session_id.is_some() {
        sql.push_str(" AND s.session_id = ?");
    }
    if filters.source_agent.is_some() {
        sql.push_str(" AND session.source_agent = ?");
    }
    let has_capture_context = filters.repository.is_some()
        || filters.project.is_some()
        || filters.branch.is_some()
        || filters.source_agent_version.is_some()
        || filters.artifact_set_version.is_some()
        || filters.client_id.is_some();
    if has_capture_context {
        sql.push_str(" AND EXISTS (SELECT 1 FROM captures capture_context WHERE capture_context.snapshot_id = s.id");
        if filters.repository.is_some() {
            sql.push_str(" AND capture_context.repository = ?");
        }
        if filters.project.is_some() {
            sql.push_str(" AND capture_context.project = ?");
        }
        if filters.branch.is_some() {
            sql.push_str(" AND capture_context.branch = ?");
        }
        if filters.source_agent_version.is_some() {
            sql.push_str(" AND capture_context.source_agent_version = ?");
        }
        if filters.artifact_set_version.is_some() {
            sql.push_str(" AND capture_context.artifact_set_version = ?");
        }
        if filters.client_id.is_some() {
            sql.push_str(" AND capture_context.client_id = ?");
        }
        sql.push(')');
    }
    if filters.activity_from.is_some() {
        sql.push_str(" AND s.completed_at_seq >= ?");
    }
    if filters.activity_to.is_some() {
        sql.push_str(" AND s.completed_at_seq <= ?");
    }
    append_descending_bounds(&mut sql, "s.completed_at_seq", "s.id", page);
    sql.push_str(" ORDER BY s.completed_at_seq DESC, s.id DESC LIMIT ?");

    let mut request = sqlx::query_as::<_, SnapshotListRow>(&sql);
    if let Some(value) = &filters.session_id {
        request = request.bind(value);
    }
    if let Some(value) = &filters.source_agent {
        request = request.bind(value);
    }
    if let Some(value) = &filters.repository {
        request = request.bind(value);
    }
    if let Some(value) = &filters.project {
        request = request.bind(value);
    }
    if let Some(value) = &filters.branch {
        request = request.bind(value);
    }
    if let Some(value) = &filters.source_agent_version {
        request = request.bind(value);
    }
    if let Some(value) = filters.artifact_set_version {
        request = request.bind(i64::from(value));
    }
    if let Some(value) = &filters.client_id {
        request = request.bind(value);
    }
    if let Some(value) = filters.activity_from {
        request = request.bind(value);
    }
    if let Some(value) = filters.activity_to {
        request = request.bind(value);
    }
    request = bind_descending_bounds(request, page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    request
        .bind(limit)
        .fetch_all(database)
        .await
        .map_err(|_| ApiError::database())
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

async fn snapshot_response(
    database: &SqlitePool,
    snapshot_id: &str,
) -> Result<SnapshotResponse, ApiError> {
    let row = sqlx::query_as::<_, SnapshotRow>(
        "SELECT s.id, s.session_id, s.fingerprint_sha256, m.id AS manifest_id,
                m.sha256 AS manifest_sha256, s.completed_at, s.artifact_count,
                s.total_original_size_bytes, s.total_stored_size_bytes, m.canonical_json,
                (SELECT COUNT(*) FROM captures c WHERE c.snapshot_id = s.id) AS capture_count
         FROM snapshots s JOIN manifests m ON m.id = s.manifest_id
         WHERE s.id = ? AND s.deleted_at IS NULL",
    )
    .bind(snapshot_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    ingestion::reconcile_snapshot(database, snapshot_id)
        .await
        .map_err(|error| match error {
            ingestion::ReconciliationError::NotFound => {
                ApiError::not_found("snapshot_not_found", "snapshot was not found")
            }
            ingestion::ReconciliationError::Drift | ingestion::ReconciliationError::Metadata => {
                ApiError::internal()
            }
        })?;
    let manifest: Manifest =
        serde_json::from_str(&row.canonical_json).map_err(|_| ApiError::internal())?;
    let artifacts = sqlx::query_as::<_, ArtifactRow>(
        "SELECT a.id, a.artifact_index, a.logical_path, a.media_type, a.original_size_bytes,
                a.original_sha256, b.stored_size_bytes, b.stored_sha256, b.compression
         FROM artifacts a JOIN blobs b ON b.id = a.blob_id
         WHERE a.snapshot_id = ? ORDER BY a.artifact_index",
    )
    .bind(snapshot_id)
    .fetch_all(database)
    .await
    .map_err(|_| ApiError::database())?
    .into_iter()
    .map(artifact_response)
    .collect::<Result<Vec<_>, _>>()?;
    let manifest_id = row.manifest_id;
    let response_snapshot_id = row.id;
    Ok(SnapshotResponse {
        captures_url: format!("/api/v1/snapshots/{response_snapshot_id}/captures"),
        manifest_url: format!("/api/v1/manifests/{manifest_id}"),
        snapshot_id: response_snapshot_id,
        session_id: row.session_id,
        snapshot_fingerprint: digest_document_value(&row.fingerprint_sha256),
        manifest_id,
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        completed_at: row.completed_at,
        artifact_count: u32::try_from(row.artifact_count).map_err(|_| ApiError::internal())?,
        total_original_bytes: u64::try_from(row.total_original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        total_stored_bytes: u64::try_from(row.total_stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        capture_count: u64::try_from(row.capture_count).map_err(|_| ApiError::internal())?,
        manifest,
        artifacts,
    })
}

fn artifact_response(row: ArtifactRow) -> Result<ArtifactResponse, ApiError> {
    Ok(ArtifactResponse {
        content_url: format!("/api/v1/artifacts/{}/content", row.id),
        metadata_url: format!("/api/v1/artifacts/{}", row.id),
        artifact_id: row.id,
        artifact_index: u32::try_from(row.artifact_index).map_err(|_| ApiError::internal())?,
        logical_path: row.logical_path,
        media_type: row.media_type,
        original_size_bytes: u64::try_from(row.original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        original_sha256: digest_document_value(&row.original_sha256),
        stored_size_bytes: u64::try_from(row.stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        stored_sha256: digest_document_value(&row.stored_sha256),
        compression: parse_compression(&row.compression).map_err(|()| ApiError::internal())?,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ManifestFilters {
    session_id: Option<String>,
}

pub(crate) async fn list_manifests(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ManifestListQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<CanonicalManifestSummary>>, ApiError> {
    let query = query_error(query)?;
    let filters = ManifestFilters {
        session_id: normalize_resource_id(
            query.session_id.as_deref(),
            "session identifier is not a UUID",
        )?,
    };
    let filter_hash = filter_hash(MANIFEST_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        MANIFEST_CURSOR_KIND,
        &filter_hash,
    )?;
    let mut sql = String::from(
        "SELECT m.id AS manifest_id, s.id AS snapshot_id, c.session_id,
                c.id AS capture_record_id, m.sha256, m.created_at,
                c.server_completed_at AS completed_at, c.server_completed_at_seq AS completed_at_seq
         FROM captures c
         JOIN snapshots s ON s.id = c.snapshot_id
         JOIN manifests m ON m.id = c.manifest_id
         WHERE s.deleted_at IS NULL",
    );
    if filters.session_id.is_some() {
        sql.push_str(" AND c.session_id = ?");
    }
    append_descending_bounds(&mut sql, "c.server_completed_at_seq", "c.id", &page);
    sql.push_str(" ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT ?");
    let mut request = sqlx::query_as::<_, ManifestSummaryRow>(&sql);
    if let Some(value) = &filters.session_id {
        request = request.bind(value);
    }
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.completed_at_seq,
                timestamp: row.completed_at.clone(),
                id: row.capture_record_id.clone(),
            };
            (manifest_summary(row), boundary)
        })
        .collect::<Vec<_>>();
    Ok(Json(page_from_rows(
        rows,
        &page,
        MANIFEST_CURSOR_KIND,
        filter_hash,
    )?))
}

pub(crate) async fn get_manifest(
    AxumPath(manifest_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CanonicalManifestResponse>, ApiError> {
    let manifest_id = parse_uuid(&manifest_id, "manifest identifier is not a UUID")?.to_string();
    canonical_manifest_by_id(&state.database, &manifest_id)
        .await
        .map(Json)
}

pub(crate) async fn get_snapshot_manifest(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<CanonicalManifestResponse>, ApiError> {
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    let row = sqlx::query_as::<_, ManifestRow>(
        "SELECT m.id AS manifest_id, s.id AS snapshot_id, c.session_id,
                c.id AS capture_record_id, m.sha256, m.created_at,
                c.server_completed_at AS completed_at, m.canonical_json
         FROM snapshots s
         JOIN manifests m ON m.id = s.manifest_id
         JOIN captures c ON c.manifest_id = m.id
         WHERE s.id = ? AND s.deleted_at IS NULL",
    )
    .bind(snapshot_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    canonical_manifest_response(row).map(Json)
}

async fn canonical_manifest_by_id(
    database: &SqlitePool,
    manifest_id: &str,
) -> Result<CanonicalManifestResponse, ApiError> {
    let row = sqlx::query_as::<_, ManifestRow>(
        "SELECT m.id AS manifest_id, s.id AS snapshot_id, c.session_id,
                c.id AS capture_record_id, m.sha256, m.created_at,
                c.server_completed_at AS completed_at, m.canonical_json
         FROM manifests m
         JOIN captures c ON c.manifest_id = m.id
         JOIN snapshots s ON s.id = c.snapshot_id
         WHERE m.id = ? AND s.deleted_at IS NULL",
    )
    .bind(manifest_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("manifest_not_found", "manifest was not found"))?;
    canonical_manifest_response(row)
}

fn canonical_manifest_response(row: ManifestRow) -> Result<CanonicalManifestResponse, ApiError> {
    let manifest = serde_json::from_str(&row.canonical_json).map_err(|_| ApiError::internal())?;
    let snapshot_id = row.snapshot_id;
    let manifest_id = row.manifest_id;
    let capture_record_id = row.capture_record_id;
    Ok(CanonicalManifestResponse {
        snapshot_url: format!("/api/v1/snapshots/{snapshot_id}"),
        capture_url: format!("/api/v1/captures/{capture_record_id}"),
        manifest_url: format!("/api/v1/manifests/{manifest_id}"),
        manifest_id,
        snapshot_id,
        session_id: row.session_id,
        capture_record_id,
        sha256: digest_document_value(&row.sha256),
        created_at: row.created_at,
        completed_at: row.completed_at,
        manifest,
    })
}

fn manifest_summary(row: ManifestSummaryRow) -> CanonicalManifestSummary {
    let snapshot_id = row.snapshot_id;
    let manifest_id = row.manifest_id;
    let capture_record_id = row.capture_record_id;
    CanonicalManifestSummary {
        snapshot_url: format!("/api/v1/snapshots/{snapshot_id}"),
        capture_url: format!("/api/v1/captures/{capture_record_id}"),
        manifest_url: format!("/api/v1/manifests/{manifest_id}"),
        manifest_id,
        snapshot_id,
        session_id: row.session_id,
        capture_record_id,
        sha256: digest_document_value(&row.sha256),
        created_at: row.created_at,
        completed_at: row.completed_at,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    snapshot_id: Option<String>,
    session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ArtifactFilters {
    snapshot_id: Option<String>,
    session_id: Option<String>,
}

pub(crate) async fn list_artifacts(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ArtifactListQuery>, QueryRejection>,
) -> Result<Json<PaginatedResponse<ArtifactMetadataResponse>>, ApiError> {
    let query = query_error(query)?;
    let filters = ArtifactFilters {
        snapshot_id: normalize_resource_id(
            query.snapshot_id.as_deref(),
            "snapshot identifier is not a UUID",
        )?,
        session_id: normalize_resource_id(
            query.session_id.as_deref(),
            "session identifier is not a UUID",
        )?,
    };
    let filter_hash = filter_hash(ARTIFACT_CURSOR_KIND, &filters)?;
    let page = parse_page(
        query.limit,
        query.cursor,
        ARTIFACT_CURSOR_KIND,
        &filter_hash,
    )?;
    let mut sql = String::from(
        "SELECT a.id AS artifact_id, a.snapshot_id, a.artifact_index, a.logical_path,
                a.media_type, a.original_size_bytes, a.original_sha256,
                b.stored_size_bytes, b.stored_sha256, b.compression, a.created_at, a.created_at_seq
         FROM artifacts a
         JOIN snapshots s ON s.id = a.snapshot_id
         JOIN blobs b ON b.id = a.blob_id
         WHERE s.deleted_at IS NULL",
    );
    if filters.snapshot_id.is_some() {
        sql.push_str(" AND a.snapshot_id = ?");
    }
    if filters.session_id.is_some() {
        sql.push_str(" AND s.session_id = ?");
    }
    append_descending_bounds(&mut sql, "a.created_at_seq", "a.id", &page);
    sql.push_str(" ORDER BY a.created_at_seq DESC, a.id DESC LIMIT ?");
    let mut request = sqlx::query_as::<_, ArtifactMetadataRow>(&sql);
    if let Some(value) = &filters.snapshot_id {
        request = request.bind(value);
    }
    if let Some(value) = &filters.session_id {
        request = request.bind(value);
    }
    request = bind_descending_bounds(request, &page);
    let limit = i64::try_from(page.limit + 1).map_err(|_| ApiError::internal())?;
    let rows = request
        .bind(limit)
        .fetch_all(&state.database)
        .await
        .map_err(|_| ApiError::database())?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let boundary = SortBoundary {
                sort_key: row.created_at_seq,
                timestamp: row.created_at.clone(),
                id: row.artifact_id.clone(),
            };
            artifact_metadata_response(row).map(|artifact| (artifact, boundary))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(page_from_rows(
        rows,
        &page,
        ARTIFACT_CURSOR_KIND,
        filter_hash,
    )?))
}

pub(crate) async fn get_artifact_metadata(
    AxumPath(artifact_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<ArtifactMetadataResponse>, ApiError> {
    let artifact_id = parse_uuid(&artifact_id, "artifact identifier is not a UUID")?.to_string();
    let row = artifact_metadata_row(&state.database, &artifact_id)
        .await?
        .ok_or_else(|| ApiError::not_found("artifact_not_found", "artifact was not found"))?;
    artifact_metadata_response(row).map(Json)
}

async fn artifact_metadata_row(
    database: &SqlitePool,
    artifact_id: &str,
) -> Result<Option<ArtifactMetadataRow>, ApiError> {
    sqlx::query_as(
        "SELECT a.id AS artifact_id, a.snapshot_id, a.artifact_index, a.logical_path,
                a.media_type, a.original_size_bytes, a.original_sha256,
                b.stored_size_bytes, b.stored_sha256, b.compression, a.created_at, a.created_at_seq
         FROM artifacts a
         JOIN snapshots s ON s.id = a.snapshot_id
         JOIN blobs b ON b.id = a.blob_id
         WHERE a.id = ? AND s.deleted_at IS NULL",
    )
    .bind(artifact_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())
}

fn artifact_metadata_response(
    row: ArtifactMetadataRow,
) -> Result<ArtifactMetadataResponse, ApiError> {
    let artifact_id = row.artifact_id;
    Ok(ArtifactMetadataResponse {
        metadata_url: format!("/api/v1/artifacts/{artifact_id}"),
        content_url: format!("/api/v1/artifacts/{artifact_id}/content"),
        artifact_id,
        snapshot_id: row.snapshot_id,
        artifact_index: u32::try_from(row.artifact_index).map_err(|_| ApiError::internal())?,
        logical_path: row.logical_path,
        media_type: row.media_type,
        original_size_bytes: u64::try_from(row.original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        original_sha256: digest_document_value(&row.original_sha256),
        stored_size_bytes: u64::try_from(row.stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        stored_sha256: digest_document_value(&row.stored_sha256),
        compression: parse_compression(&row.compression).map_err(|()| ApiError::internal())?,
        created_at: row.created_at,
    })
}

pub(crate) async fn download_artifact(
    AxumPath(artifact_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let artifact_id = parse_uuid(&artifact_id, "artifact identifier is not a UUID")?.to_string();
    let download_deadline = Instant::now() + state.download_timeout;
    let permit = state
        .download_permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal())?;
    let metadata = verified_download_metadata(&state.database, &artifact_id)
        .await?
        .ok_or_else(|| ApiError::not_found("artifact_not_found", "artifact was not found"))?;
    let path = state.storage.blob_path(&metadata.stored_sha256);
    let (file, stored_digest_base64) =
        preflight_blob(&path, metadata.stored_size_bytes, &metadata.stored_sha256).await?;
    let stream = VerifiedDownloadStream::new(file, download_deadline, permit);
    let mut response = Response::new(Body::from_stream(stream));
    add_download_headers(&mut response, &metadata, &stored_digest_base64)?;
    Ok(response)
}

async fn verified_download_metadata(
    database: &SqlitePool,
    artifact_id: &str,
) -> Result<Option<DownloadMetadata>, ApiError> {
    let row = sqlx::query_as::<_, DownloadRow>(
        "SELECT a.snapshot_id, a.artifact_index, a.logical_path, a.media_type, a.original_size_bytes,
                a.original_sha256, b.stored_size_bytes, b.stored_sha256, b.compression,
                m.canonical_json, m.sha256 AS manifest_sha256
         FROM artifacts a
         JOIN snapshots s ON s.id = a.snapshot_id
         JOIN manifests m ON m.id = s.manifest_id
         JOIN blobs b ON b.id = a.blob_id
         WHERE a.id = ?1 AND s.deleted_at IS NULL",
    )
    .bind(artifact_id)
    .fetch_optional(database)
    .await
    .map_err(|_| ApiError::database())?;
    let Some(row) = row else {
        return Ok(None);
    };
    let snapshot_id = row.snapshot_id.clone();
    let metadata = download_metadata_from_row(&row)?;
    ingestion::reconcile_snapshot(database, &snapshot_id)
        .await
        .map_err(|error| match error {
            ingestion::ReconciliationError::Metadata => ApiError::database(),
            ingestion::ReconciliationError::NotFound | ingestion::ReconciliationError::Drift => {
                ApiError::artifact_integrity()
            }
        })?;
    Ok(Some(metadata))
}

fn download_metadata_from_row(row: &DownloadRow) -> Result<DownloadMetadata, ApiError> {
    if sha256_hex(row.canonical_json.as_bytes()) != row.manifest_sha256 {
        return Err(ApiError::artifact_integrity());
    }
    let manifest: Manifest =
        serde_json::from_str(&row.canonical_json).map_err(|_| ApiError::artifact_integrity())?;
    let artifact_index =
        usize::try_from(row.artifact_index).map_err(|_| ApiError::artifact_integrity())?;
    let artifact = manifest
        .artifacts
        .get(artifact_index)
        .ok_or_else(ApiError::artifact_integrity)?;
    let original_sha256 =
        canonical_digest(&artifact.original_sha256).ok_or_else(ApiError::artifact_integrity)?;
    let stored_sha256 =
        canonical_digest(&artifact.stored_sha256).ok_or_else(ApiError::artifact_integrity)?;
    let original_size_bytes =
        u64::try_from(row.original_size_bytes).map_err(|_| ApiError::artifact_integrity())?;
    let stored_size_bytes =
        u64::try_from(row.stored_size_bytes).map_err(|_| ApiError::artifact_integrity())?;
    let compression =
        parse_compression(&row.compression).map_err(|()| ApiError::artifact_integrity())?;

    if row.logical_path != artifact.logical_path
        || row.media_type != artifact.media_type
        || original_size_bytes != artifact.original_size_bytes
        || storage_digest(&row.original_sha256) != Some(original_sha256)
        || stored_size_bytes != artifact.stored_size_bytes
        || storage_digest(&row.stored_sha256) != Some(stored_sha256)
        || compression != artifact.compression
    {
        return Err(ApiError::artifact_integrity());
    }

    Ok(DownloadMetadata {
        logical_path: artifact.logical_path.clone(),
        media_type: artifact.media_type.clone(),
        original_size_bytes: artifact.original_size_bytes,
        original_sha256: digest_document_value(original_sha256),
        stored_size_bytes: artifact.stored_size_bytes,
        stored_sha256: stored_sha256.to_owned(),
        compression: artifact.compression,
    })
}

fn add_download_headers(
    response: &mut Response,
    metadata: &DownloadMetadata,
    stored_digest_base64: &str,
) -> Result<(), ApiError> {
    let headers = response.headers_mut();
    if let Some(media_type) = &metadata.media_type {
        media_type
            .parse::<mime::Mime>()
            .map_err(|_| ApiError::artifact_integrity())?;
    }
    let media_type = metadata
        .media_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    headers.insert(header::CONTENT_TYPE, header_value(media_type)?);
    headers.insert(
        header::CONTENT_LENGTH,
        header_value(&metadata.stored_size_bytes.to_string())?,
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-transform"),
    );
    if metadata.compression == Compression::Zstd {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("zstd"));
    }
    headers.insert(
        "digest",
        header_value(&format!("SHA-256={stored_digest_base64}"))?,
    );
    headers.insert(
        "content-digest",
        header_value(&format!("sha-256=:{stored_digest_base64}:"))?,
    );
    headers.insert(
        "x-patwari-logical-path",
        header_value(&URL_SAFE_NO_PAD.encode(metadata.logical_path.as_bytes()))?,
    );
    headers.insert(
        "x-patwari-logical-path-encoding",
        HeaderValue::from_static("base64url"),
    );
    if let Some(media_type) = &metadata.media_type {
        headers.insert("x-patwari-media-type", header_value(media_type)?);
    }
    headers.insert(
        "x-patwari-compression",
        HeaderValue::from_static(match metadata.compression {
            Compression::Identity => "identity",
            Compression::Zstd => "zstd",
        }),
    );
    headers.insert(
        "x-patwari-original-size-bytes",
        header_value(&metadata.original_size_bytes.to_string())?,
    );
    headers.insert(
        "x-patwari-original-sha256",
        header_value(&metadata.original_sha256)?,
    );
    headers.insert(
        "x-patwari-stored-size-bytes",
        header_value(&metadata.stored_size_bytes.to_string())?,
    );
    headers.insert(
        "x-patwari-stored-sha256",
        header_value(&digest_document_value(&metadata.stored_sha256))?,
    );
    Ok(())
}

fn header_value(value: &str) -> Result<HeaderValue, ApiError> {
    HeaderValue::from_str(value).map_err(|_| ApiError::artifact_integrity())
}

async fn preflight_blob(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(fs::File, String), ApiError> {
    if storage_digest(expected_sha256).is_none() {
        return Err(ApiError::artifact_integrity());
    }
    let path_metadata = fs::symlink_metadata(path)
        .await
        .map_err(|_| ApiError::artifact_integrity())?;
    if !path_metadata.file_type().is_file() {
        return Err(ApiError::artifact_integrity());
    }

    let mut file = open_blob_without_following_symlinks(path)
        .await
        .map_err(|_| ApiError::artifact_integrity())?;
    let before = file
        .metadata()
        .await
        .map_err(|_| ApiError::artifact_integrity())?;
    if !before.is_file() || before.len() != expected_size {
        return Err(ApiError::artifact_integrity());
    }

    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|_| ApiError::artifact_integrity())?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| ApiError::artifact_integrity())?)
            .ok_or_else(ApiError::artifact_integrity)?;
        if total > expected_size {
            return Err(ApiError::artifact_integrity());
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let actual_sha256 = hex_digest(digest);
    let after = file
        .metadata()
        .await
        .map_err(|_| ApiError::artifact_integrity())?;
    if total != expected_size
        || actual_sha256 != expected_sha256
        || metadata_changed_during_preflight(&before, &after)
    {
        return Err(ApiError::artifact_integrity());
    }
    file.seek(SeekFrom::Start(0))
        .await
        .map_err(|_| ApiError::artifact_integrity())?;
    Ok((file, STANDARD.encode(&digest[..])))
}

async fn open_blob_without_following_symlinks(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path).await
}

fn metadata_changed_during_preflight(
    before: &std::fs::Metadata,
    after: &std::fs::Metadata,
) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return true;
    }
    #[cfg(unix)]
    {
        before.dev() != after.dev()
            || before.ino() != after.ino()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn storage_digest(value: &str) -> Option<&str> {
    (value.len() == 64
        && value.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        }))
    .then_some(value)
}

fn canonical_digest(value: &str) -> Option<&str> {
    value.strip_prefix("sha256:").and_then(storage_digest)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = digest.as_ref();
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

/// Keeps the download permit until the client finishes, errors, times out,
/// or drops the body. `ReaderStream` pulls at most one fixed-size chunk per
/// poll, so downstream HTTP backpressure bounds the in-memory payload.
struct VerifiedDownloadStream {
    reader: ReaderStream<fs::File>,
    deadline: Pin<Box<Sleep>>,
    permit: Option<OwnedSemaphorePermit>,
    timed_out: bool,
}

impl VerifiedDownloadStream {
    fn new(file: fs::File, deadline: Instant, permit: OwnedSemaphorePermit) -> Self {
        Self {
            reader: ReaderStream::with_capacity(file, DOWNLOAD_BUFFER_BYTES),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            permit: Some(permit),
            timed_out: false,
        }
    }
}

impl Stream for VerifiedDownloadStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.as_mut().get_mut();
        if stream.timed_out {
            return Poll::Ready(None);
        }
        if stream.deadline.as_mut().poll(context).is_ready() {
            stream.timed_out = true;
            stream.permit.take();
            return Poll::Ready(Some(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "artifact download exceeded the configured time limit",
            ))));
        }
        match Pin::new(&mut stream.reader).poll_next(context) {
            Poll::Ready(None) => {
                stream.permit.take();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                stream.permit.take();
                Poll::Ready(Some(Err(error)))
            }
            result => result,
        }
    }
}

pub(crate) async fn completion_for_upload(
    state: &AppState,
    upload_id: &str,
) -> Result<CompletionResponse, ApiError> {
    let receipt = receipt_for_upload(state, upload_id).await?;
    let transfer = sqlx::query_as::<_, CompletionTransferRow>(
        "SELECT id, capture_id, transfer_bytes, newly_persisted_bytes
         FROM uploads WHERE id = ? AND status = 'completed'",
    )
    .bind(upload_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("upload_not_found", "upload was not found"))?;
    let capture = capture_for_upload(&state.database, upload_id).await?;
    Ok(CompletionResponse {
        receipt,
        transfer: CompletionTransfer {
            upload_id: transfer.id,
            capture_id: transfer.capture_id,
            upload_transfer_bytes: u64::try_from(transfer.transfer_bytes)
                .map_err(|_| ApiError::internal())?,
            newly_persisted_physical_bytes: u64::try_from(transfer.newly_persisted_bytes)
                .map_err(|_| ApiError::internal())?,
        },
        capture,
    })
}

async fn receipt_for_upload(state: &AppState, upload_id: &str) -> Result<Receipt, ApiError> {
    let row = sqlx::query_as::<_, ReceiptRow>(
        "SELECT s.id, s.session_id, s.fingerprint_sha256, s.completed_at,
                m.sha256 AS manifest_sha256, s.artifact_count,
                s.total_original_size_bytes, s.total_stored_size_bytes
         FROM uploads u
         JOIN snapshots s ON s.id = u.snapshot_id
         JOIN manifests m ON m.id = s.manifest_id
         WHERE u.id = ? AND u.status = 'completed' AND s.deleted_at IS NULL",
    )
    .bind(upload_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    Ok(Receipt {
        receipt_version: 2,
        archive_instance_id: state.identity.archive_instance_id.clone(),
        owner_namespace: state.identity.owner_namespace.clone(),
        snapshot_id: row.id,
        session_id: row.session_id,
        snapshot_fingerprint: digest_document_value(&row.fingerprint_sha256),
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        artifact_count: u32::try_from(row.artifact_count).map_err(|_| ApiError::internal())?,
        total_original_bytes: u64::try_from(row.total_original_size_bytes)
            .map_err(|_| ApiError::internal())?,
        total_stored_bytes: u64::try_from(row.total_stored_size_bytes)
            .map_err(|_| ApiError::internal())?,
        completed_at: row.completed_at,
    })
}

fn digest_document_value(value: &str) -> String {
    format!("sha256:{}", value.strip_prefix("sha256:").unwrap_or(value))
}

fn parse_compression(value: &str) -> Result<Compression, ()> {
    match value {
        "identity" => Ok(Compression::Identity),
        "zstd" => Ok(Compression::Zstd),
        _ => Err(()),
    }
}

#[derive(FromRow)]
struct CaptureRow {
    id: String,
    capture_id: String,
    client_id: String,
    session_id: String,
    upload_id: String,
    snapshot_id: String,
    manifest_id: String,
    manifest_sha256: String,
    source_captured_at: String,
    source_cursor: Option<String>,
    source_state_hash: Option<String>,
    source_metadata_json: String,
    project: Option<String>,
    repository: Option<String>,
    branch: Option<String>,
    source_agent_version: Option<String>,
    artifact_set_version: i64,
    munshi_version: Option<String>,
    server_received_at: String,
    server_completed_at: String,
    server_completed_at_seq: i64,
}

#[derive(FromRow)]
struct SnapshotRow {
    id: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_id: String,
    manifest_sha256: String,
    completed_at: String,
    artifact_count: i64,
    total_original_size_bytes: i64,
    total_stored_size_bytes: i64,
    canonical_json: String,
    capture_count: i64,
}

#[derive(FromRow)]
struct ArtifactRow {
    id: String,
    artifact_index: i64,
    logical_path: String,
    media_type: Option<String>,
    original_size_bytes: i64,
    original_sha256: String,
    stored_size_bytes: i64,
    stored_sha256: String,
    compression: String,
}

#[derive(FromRow)]
struct ManifestSummaryRow {
    manifest_id: String,
    snapshot_id: String,
    session_id: String,
    capture_record_id: String,
    sha256: String,
    created_at: String,
    completed_at: String,
    completed_at_seq: i64,
}

#[derive(FromRow)]
struct ManifestRow {
    manifest_id: String,
    snapshot_id: String,
    session_id: String,
    capture_record_id: String,
    sha256: String,
    created_at: String,
    completed_at: String,
    canonical_json: String,
}

#[derive(FromRow)]
struct ArtifactMetadataRow {
    artifact_id: String,
    snapshot_id: String,
    artifact_index: i64,
    logical_path: String,
    media_type: Option<String>,
    original_size_bytes: i64,
    original_sha256: String,
    stored_size_bytes: i64,
    stored_sha256: String,
    compression: String,
    created_at: String,
    created_at_seq: i64,
}

#[derive(FromRow)]
struct DownloadRow {
    snapshot_id: String,
    artifact_index: i64,
    logical_path: String,
    media_type: Option<String>,
    original_size_bytes: i64,
    original_sha256: String,
    stored_sha256: String,
    stored_size_bytes: i64,
    compression: String,
    canonical_json: String,
    manifest_sha256: String,
}

struct DownloadMetadata {
    logical_path: String,
    media_type: Option<String>,
    original_size_bytes: u64,
    original_sha256: String,
    stored_size_bytes: u64,
    /// Stored as bare lowercase hexadecimal to match the blob pathname.
    stored_sha256: String,
    compression: Compression,
}

#[derive(FromRow)]
struct ReceiptRow {
    id: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_sha256: String,
    completed_at: String,
    artifact_count: i64,
    total_original_size_bytes: i64,
    total_stored_size_bytes: i64,
}

#[derive(FromRow)]
struct CompletionTransferRow {
    id: String,
    capture_id: String,
    transfer_bytes: i64,
    newly_persisted_bytes: i64,
}
