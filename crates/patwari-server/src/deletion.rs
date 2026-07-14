//! Trusted-boundary snapshot deletion and relationship-authoritative blob GC.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    http::HeaderMap,
};
use serde::Deserialize;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    contract::{
        BlobGcResponse, DeleteSnapshotRequest, PaginatedResponse, Receipt, TombstoneResponse,
    },
    database::{self, format_time},
    error::ApiError,
    pagination::{
        SortBoundary, append_descending_bounds, bind_descending_bounds, filter_hash,
        page_from_rows, parse_page,
    },
    service::{AppState, MaintenanceError},
    storage::StorageLayout,
    validation::parse_uuid,
};

const DELETE_CONFIRMATION_HEADER: &str = "x-patwari-delete-confirmation";
const DELETE_CONFIRMATION_PREFIX: &str = "delete-snapshot";
const MAX_DELETION_REASON_BYTES: usize = 512;
const TOMBSTONE_CURSOR_KIND: &str = "tombstones";
const GC_BATCH_SIZE: i64 = 256;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TombstoneListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(FromRow)]
struct SnapshotDeletionRow {
    id: String,
    owner_namespace: String,
    session_id: String,
    fingerprint_sha256: String,
    manifest_sha256: String,
    completed_at: String,
    deleted_at: Option<String>,
}

#[derive(FromRow)]
struct ArtifactBlobRow {
    id: String,
    stored_sha256: String,
}

#[derive(FromRow)]
struct TombstoneListRow {
    snapshot_id: String,
    deleted_at: String,
    deleted_at_seq: i64,
    tombstone_id: String,
}

#[derive(FromRow)]
struct TombstoneRow {
    tombstone_id: String,
    deletion_audit_id: String,
    owner_namespace: String,
    session_id: String,
    snapshot_id: String,
    snapshot_fingerprint_sha256: String,
    manifest_sha256: String,
    snapshot_completed_at: String,
    deleted_at: String,
    deleted_at_seq: i64,
    reason: Option<String>,
    rearchived_snapshot_id: Option<String>,
    artifact_count: i64,
    total_original_size_bytes: i64,
    total_stored_size_bytes: i64,
    capture_count: i64,
}

#[derive(FromRow)]
struct GcCandidateRow {
    id: String,
    stored_sha256: String,
}

fn require_admin_enabled(state: &AppState) -> Result<(), ApiError> {
    if state.admin_deletion_enabled {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "admin_deletion_disabled",
            "administrative deletion is disabled by server configuration",
        ))
    }
}

/// Deletes one live snapshot after a fingerprint-bound explicit confirmation.
/// The route is deliberately separate from normal archive reads and remains
/// unavailable unless an operator enables the trusted-boundary configuration.
#[allow(clippy::too_many_lines)]
pub(crate) async fn delete_snapshot(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Option<Json<DeleteSnapshotRequest>>,
) -> Result<Json<TombstoneResponse>, ApiError> {
    require_admin_enabled(&state)?;
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    let payload = payload.map(|Json(payload)| payload);
    let reason = deletion_reason(payload.as_ref())?;
    let initial = snapshot_deletion_row(&state, &snapshot_id)
        .await?
        .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    confirm_snapshot_deletion(
        &headers,
        payload
            .as_ref()
            .and_then(|request| request.confirmation.as_deref()),
        &initial.id,
        &initial.fingerprint_sha256,
    )?;

    // Repeating a fully confirmed delete is intentionally idempotent. The
    // tombstone/audit pair is not duplicated.
    if initial.deleted_at.is_some() {
        return tombstone_response(&state, &snapshot_id).await.map(Json);
    }

    // A completion that finds a live semantic match takes this same identity
    // lock before recording provenance. See the lock ordering in `service`.
    let snapshot_lock = state.snapshot_lock(&initial.session_id, &initial.fingerprint_sha256);
    let _snapshot_guard = snapshot_lock.lock_owned().await;
    let current = snapshot_deletion_row(&state, &snapshot_id)
        .await?
        .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    confirm_snapshot_deletion(
        &headers,
        payload
            .as_ref()
            .and_then(|request| request.confirmation.as_deref()),
        &current.id,
        &current.fingerprint_sha256,
    )?;
    if current.deleted_at.is_some() {
        return tombstone_response(&state, &snapshot_id).await.map(Json);
    }

    // Acquire all digest stripes before opening the write transaction. A
    // completion, deletion, or GC pass can never delete a file while another
    // operation is about to add an Artifact reference to it.
    let initial_artifacts = artifact_blobs(&state, &snapshot_id).await?;
    let digests = initial_artifacts
        .iter()
        .map(|blob| blob.stored_sha256.clone())
        .collect::<Vec<_>>();
    let locks = state.blob_locks_for_digests(&state.identity.owner_namespace, &digests);
    let mut blob_guards = Vec::with_capacity(locks.len());
    for lock in locks {
        blob_guards.push(lock.lock_owned().await);
    }

    let now = OffsetDateTime::now_utc();
    let deleted_at = format_time(now).map_err(|_| ApiError::internal())?;
    let deleted_at_seq = database::sort_key_from_timestamp(now);
    let eligible_at = database::expiration_time(now, state.blob_gc_grace);
    let eligible_after = format_time(eligible_at).map_err(|_| ApiError::internal())?;
    let eligible_after_seq = database::sort_key_from_timestamp(eligible_at);
    let tombstone_id = Uuid::now_v7().to_string();
    let audit_id = Uuid::now_v7().to_string();
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|_| ApiError::database())?;

    // Re-read the relationship rows while holding every relevant digest lock.
    // These rows—not an optional cache—are the authority for GC eligibility.
    let live = snapshot_deletion_row_in_transaction(&mut transaction, &snapshot_id)
        .await?
        .ok_or_else(|| ApiError::not_found("snapshot_not_found", "snapshot was not found"))?;
    if live.deleted_at.is_some() {
        transaction
            .commit()
            .await
            .map_err(|_| ApiError::database())?;
        drop(blob_guards);
        return tombstone_response(&state, &snapshot_id).await.map(Json);
    }
    let artifacts = artifact_blobs_in_transaction(&mut transaction, &snapshot_id).await?;

    sqlx::query(
        "INSERT INTO tombstones (
            id, owner_namespace, session_id, snapshot_id, snapshot_fingerprint_sha256,
            manifest_sha256, snapshot_completed_at, deleted_at, deleted_at_seq, reason
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind(&tombstone_id)
    .bind(&live.owner_namespace)
    .bind(&live.session_id)
    .bind(&live.id)
    .bind(&live.fingerprint_sha256)
    .bind(&live.manifest_sha256)
    .bind(&live.completed_at)
    .bind(&deleted_at)
    .bind(deleted_at_seq)
    .bind(&reason)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    sqlx::query(
        "INSERT INTO deletion_audits (
            id, tombstone_id, owner_namespace, session_id, snapshot_id,
            snapshot_fingerprint_sha256, manifest_sha256, occurred_at,
            occurred_at_seq, reason
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )
    .bind(&audit_id)
    .bind(&tombstone_id)
    .bind(&live.owner_namespace)
    .bind(&live.session_id)
    .bind(&live.id)
    .bind(&live.fingerprint_sha256)
    .bind(&live.manifest_sha256)
    .bind(&deleted_at)
    .bind(deleted_at_seq)
    .bind(&reason)
    .execute(&mut *transaction)
    .await
    .map_err(|_| ApiError::database())?;

    let marked =
        sqlx::query("UPDATE snapshots SET deleted_at = ?1 WHERE id = ?2 AND deleted_at IS NULL")
            .bind(&deleted_at)
            .bind(&live.id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| ApiError::database())?;
    if marked.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "snapshot_deletion_contended",
            "snapshot state changed while deletion was requested",
        ));
    }

    // The reference removal and durable tombstone/audit event become visible
    // in the same transaction. Captures and canonical manifests remain
    // linked to the tombstoned snapshot as historical internals only.
    sqlx::query("DELETE FROM artifacts WHERE snapshot_id = ?1")
        .bind(&live.id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ApiError::database())?;

    for blob in &artifacts {
        schedule_or_clear_blob_candidate(
            &mut transaction,
            &blob.id,
            &deleted_at,
            &eligible_after,
            eligible_after_seq,
        )
        .await?;
    }
    rebuild_session_latest_context(&mut transaction, &live.session_id).await?;
    #[cfg(test)]
    if let Some(checkpoint) = state.test_hooks.before_snapshot_deletion_commit() {
        checkpoint.arrive_and_wait().await;
    }
    transaction
        .commit()
        .await
        .map_err(|_| ApiError::database())?;
    drop(blob_guards);

    tombstone_response(&state, &snapshot_id).await.map(Json)
}

pub(crate) async fn get_tombstone(
    AxumPath(snapshot_id): AxumPath<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<TombstoneResponse>, ApiError> {
    require_admin_enabled(&state)?;
    let snapshot_id = parse_uuid(&snapshot_id, "snapshot identifier is not a UUID")?.to_string();
    tombstone_response(&state, &snapshot_id).await.map(Json)
}

pub(crate) async fn list_tombstones(
    State(state): State<Arc<AppState>>,
    query: Query<TombstoneListQuery>,
) -> Result<Json<PaginatedResponse<TombstoneResponse>>, ApiError> {
    require_admin_enabled(&state)?;
    // Tombstones have no filters, so every request shares one filter hash;
    // this only changes how cursors are tagged, never what admin data a
    // cursor can unlock.
    let filter_hash = filter_hash(TOMBSTONE_CURSOR_KIND, &())?;
    let page = parse_page(
        query.limit,
        query.cursor.clone(),
        TOMBSTONE_CURSOR_KIND,
        &filter_hash,
    )?;
    let mut sql = String::from(
        "SELECT id AS tombstone_id, snapshot_id, deleted_at, deleted_at_seq
         FROM tombstones
         WHERE 1 = 1",
    );
    append_descending_bounds(&mut sql, "deleted_at_seq", "id", &page);
    sql.push_str(" ORDER BY deleted_at_seq DESC, id DESC LIMIT ?");
    let request = sqlx::query_as::<_, TombstoneListRow>(&sql);
    let request = bind_descending_bounds(request, &page);
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
                sort_key: row.deleted_at_seq,
                timestamp: row.deleted_at.clone(),
                id: row.tombstone_id.clone(),
            };
            (row, boundary)
        })
        .collect::<Vec<_>>();
    // `page_from_rows` establishes the high watermark on the first page and
    // carries/enforces it via `append_descending_bounds`/
    // `bind_descending_bounds` on every later page, exactly as the
    // non-admin retrieval collections do. Tombstone responses require an
    // extra per-row database call, so enrichment happens after truncation
    // to the requested page (never for the discarded look-ahead row).
    let truncated = page_from_rows(rows, &page, TOMBSTONE_CURSOR_KIND, filter_hash)?;
    let mut items = Vec::with_capacity(truncated.items.len());
    for row in truncated.items {
        items.push(tombstone_response(&state, &row.snapshot_id).await?);
    }
    Ok(Json(PaginatedResponse {
        items,
        next_cursor: truncated.next_cursor,
        high_watermark: truncated.high_watermark,
    }))
}

pub(crate) async fn run_blob_gc(
    State(state): State<Arc<AppState>>,
) -> Result<Json<BlobGcResponse>, ApiError> {
    require_admin_enabled(&state)?;
    collect_orphaned_blobs_at(&state, OffsetDateTime::now_utc())
        .await
        .map(Json)
        .map_err(|_| ApiError::internal())
}

/// Runs one bounded, relationship-authoritative GC pass. This is callable by
/// trusted in-process maintenance even if the remotely reachable admin API is
/// disabled, because the caller already owns the server boundary.
#[allow(clippy::too_many_lines)]
pub(crate) async fn collect_orphaned_blobs_at(
    state: &AppState,
    now: OffsetDateTime,
) -> Result<BlobGcResponse, MaintenanceError> {
    let now_seq = database::sort_key_from_timestamp(now);
    let candidates = sqlx::query_as::<_, GcCandidateRow>(
        "SELECT id, stored_sha256 FROM blobs
         WHERE eligible_after_seq IS NOT NULL AND eligible_after_seq <= ?1
         ORDER BY eligible_after_seq ASC, id ASC
         LIMIT ?2",
    )
    .bind(now_seq)
    .bind(GC_BATCH_SIZE)
    .fetch_all(&state.database)
    .await
    .map_err(|_| MaintenanceError::Operation)?;

    let mut deleted_blobs = 0_u32;
    for candidate in &candidates {
        let locks = state.blob_locks_for_digests(
            &state.identity.owner_namespace,
            std::slice::from_ref(&candidate.stored_sha256),
        );
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        let mut transaction = state
            .database
            .begin()
            .await
            .map_err(|_| MaintenanceError::Operation)?;

        let current: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM blobs
             WHERE id = ?1 AND eligible_after_seq IS NOT NULL AND eligible_after_seq <= ?2",
        )
        .bind(&candidate.id)
        .bind(now_seq)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
        let Some((blob_id,)) = current else {
            transaction
                .commit()
                .await
                .map_err(|_| MaintenanceError::Operation)?;
            drop(guards);
            continue;
        };

        // This is the destructive authorization check. It deliberately joins
        // the live relationship rows rather than trusting an optional count
        // cache or the candidate timestamp.
        let has_live_reference: Option<(i64,)> = sqlx::query_as(
            "SELECT 1
             FROM artifacts artifact
             JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
             WHERE artifact.blob_id = ?1 AND snapshot.deleted_at IS NULL
             LIMIT 1",
        )
        .bind(&blob_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
        if has_live_reference.is_some() {
            sqlx::query(
                "UPDATE blobs
                 SET orphaned_at = NULL, eligible_after = NULL, eligible_after_seq = NULL
                 WHERE id = ?1",
            )
            .bind(&blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| MaintenanceError::Operation)?;
            transaction
                .commit()
                .await
                .map_err(|_| MaintenanceError::Operation)?;
            drop(guards);
            continue;
        }

        // Remove metadata only while the no-live-reference condition still
        // holds in this transaction. Keep that delete uncommitted until the
        // file removal succeeds; completion cannot gain a reference between
        // the check and file removal because it must first obtain this digest
        // lock.
        let removed = sqlx::query(
            "DELETE FROM blobs
             WHERE id = ?1
               AND NOT EXISTS (
                   SELECT 1
                   FROM artifacts artifact
                   JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
                   WHERE artifact.blob_id = ?1 AND snapshot.deleted_at IS NULL
               )",
        )
        .bind(&blob_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| MaintenanceError::Operation)?;
        if removed.rows_affected() == 1 {
            StorageLayout::remove_file(&state.storage.blob_path(&candidate.stored_sha256))
                .await
                .map_err(|_| MaintenanceError::Operation)?;
            transaction
                .commit()
                .await
                .map_err(|_| MaintenanceError::Operation)?;
            deleted_blobs = deleted_blobs
                .checked_add(1)
                .ok_or(MaintenanceError::Operation)?;
        } else {
            transaction
                .commit()
                .await
                .map_err(|_| MaintenanceError::Operation)?;
        }
        drop(guards);
    }

    Ok(BlobGcResponse {
        inspected_blobs: u32::try_from(candidates.len())
            .map_err(|_| MaintenanceError::Operation)?,
        deleted_blobs,
    })
}

fn deletion_reason(request: Option<&DeleteSnapshotRequest>) -> Result<Option<String>, ApiError> {
    let Some(reason) = request.and_then(|request| request.reason.as_ref()) else {
        return Ok(None);
    };
    if reason.trim().is_empty()
        || reason.len() > MAX_DELETION_REASON_BYTES
        || reason.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(ApiError::invalid("deletion reason is invalid"));
    }
    Ok(Some(reason.clone()))
}

fn confirm_snapshot_deletion(
    headers: &HeaderMap,
    body_confirmation: Option<&str>,
    snapshot_id: &str,
    fingerprint_sha256: &str,
) -> Result<(), ApiError> {
    let header_confirmation = headers
        .get(DELETE_CONFIRMATION_HEADER)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ApiError::invalid("deletion confirmation is invalid"))
        })
        .transpose()?;
    if header_confirmation
        .as_deref()
        .zip(body_confirmation)
        .is_some_and(|(header, body)| header != body)
    {
        return Err(ApiError::invalid(
            "header and body deletion confirmations must match",
        ));
    }
    let confirmation = header_confirmation
        .as_deref()
        .or(body_confirmation)
        .ok_or_else(|| {
            ApiError::invalid("a snapshot-specific deletion confirmation is required")
        })?;
    let expected =
        format!("{DELETE_CONFIRMATION_PREFIX}:{snapshot_id}:sha256:{fingerprint_sha256}");
    if confirmation != expected {
        return Err(ApiError::conflict(
            "deletion_confirmation_mismatch",
            "deletion confirmation does not match this snapshot fingerprint",
        ));
    }
    Ok(())
}

async fn snapshot_deletion_row(
    state: &AppState,
    snapshot_id: &str,
) -> Result<Option<SnapshotDeletionRow>, ApiError> {
    sqlx::query_as(
        "SELECT s.id, s.owner_namespace, s.session_id, s.fingerprint_sha256,
                m.sha256 AS manifest_sha256, s.completed_at, s.deleted_at
         FROM snapshots s JOIN manifests m ON m.id = s.manifest_id
         WHERE s.id = ?1",
    )
    .bind(snapshot_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())
}

async fn snapshot_deletion_row_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    snapshot_id: &str,
) -> Result<Option<SnapshotDeletionRow>, ApiError> {
    sqlx::query_as(
        "SELECT s.id, s.owner_namespace, s.session_id, s.fingerprint_sha256,
                m.sha256 AS manifest_sha256, s.completed_at, s.deleted_at
         FROM snapshots s JOIN manifests m ON m.id = s.manifest_id
         WHERE s.id = ?1",
    )
    .bind(snapshot_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())
}

async fn artifact_blobs(
    state: &AppState,
    snapshot_id: &str,
) -> Result<Vec<ArtifactBlobRow>, ApiError> {
    sqlx::query_as(
        "SELECT DISTINCT b.id, b.stored_sha256
         FROM artifacts artifact
         JOIN blobs b ON b.id = artifact.blob_id
         JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
         WHERE artifact.snapshot_id = ?1 AND snapshot.deleted_at IS NULL",
    )
    .bind(snapshot_id)
    .fetch_all(&state.database)
    .await
    .map_err(|_| ApiError::database())
}

async fn artifact_blobs_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    snapshot_id: &str,
) -> Result<Vec<ArtifactBlobRow>, ApiError> {
    sqlx::query_as(
        "SELECT DISTINCT b.id, b.stored_sha256
         FROM artifacts artifact
         JOIN blobs b ON b.id = artifact.blob_id
         JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
         WHERE artifact.snapshot_id = ?1 AND snapshot.deleted_at IS NULL",
    )
    .bind(snapshot_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())
}

async fn schedule_or_clear_blob_candidate(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    blob_id: &str,
    orphaned_at: &str,
    eligible_after: &str,
    eligible_after_seq: i64,
) -> Result<(), ApiError> {
    sqlx::query(
        "UPDATE blobs
         SET orphaned_at = CASE
                 WHEN NOT EXISTS (
                     SELECT 1
                     FROM artifacts artifact
                     JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
                     WHERE artifact.blob_id = blobs.id AND snapshot.deleted_at IS NULL
                 ) THEN ?1
                 ELSE NULL
             END,
             eligible_after = CASE
                 WHEN NOT EXISTS (
                     SELECT 1
                     FROM artifacts artifact
                     JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
                     WHERE artifact.blob_id = blobs.id AND snapshot.deleted_at IS NULL
                 ) THEN ?2
                 ELSE NULL
             END,
             eligible_after_seq = CASE
                 WHEN NOT EXISTS (
                     SELECT 1
                     FROM artifacts artifact
                     JOIN snapshots snapshot ON snapshot.id = artifact.snapshot_id
                     WHERE artifact.blob_id = blobs.id AND snapshot.deleted_at IS NULL
                 ) THEN ?3
                 ELSE NULL
             END
         WHERE id = ?4",
    )
    .bind(orphaned_at)
    .bind(eligible_after)
    .bind(eligible_after_seq)
    .bind(blob_id)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    Ok(())
}

async fn rebuild_session_latest_context(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session_id: &str,
) -> Result<(), ApiError> {
    sqlx::query("DELETE FROM session_latest_context WHERE session_id = ?1")
        .bind(session_id)
        .execute(&mut **transaction)
        .await
        .map_err(|_| ApiError::database())?;
    sqlx::query(
        "INSERT INTO session_latest_context (
            session_id, snapshot_id, completed_at, completed_at_seq, source_agent,
            project, repository, branch, source_agent_version, artifact_set_version
         )
         SELECT snapshot.session_id, snapshot.id, snapshot.completed_at,
                snapshot.completed_at_seq, session.source_agent, capture.project,
                capture.repository, capture.branch, capture.source_agent_version,
                capture.artifact_set_version
         FROM snapshots snapshot
         JOIN sessions session ON session.id = snapshot.session_id
         JOIN captures capture ON capture.snapshot_id = snapshot.id
         WHERE snapshot.session_id = ?1 AND snapshot.deleted_at IS NULL
         ORDER BY snapshot.completed_at_seq DESC, snapshot.id DESC, capture.id ASC
         LIMIT 1",
    )
    .bind(session_id)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ApiError::database())?;
    Ok(())
}

async fn tombstone_response(
    state: &AppState,
    snapshot_id: &str,
) -> Result<TombstoneResponse, ApiError> {
    let row = sqlx::query_as::<_, TombstoneRow>(
        "SELECT t.id AS tombstone_id, audit.id AS deletion_audit_id,
                t.owner_namespace, t.session_id, t.snapshot_id,
                t.snapshot_fingerprint_sha256, t.manifest_sha256,
                t.snapshot_completed_at, t.deleted_at, t.deleted_at_seq, t.reason,
                t.rearchived_snapshot_id, snapshot.artifact_count,
                snapshot.total_original_size_bytes, snapshot.total_stored_size_bytes,
                (SELECT COUNT(*) FROM captures capture
                 WHERE capture.snapshot_id = t.snapshot_id) AS capture_count
         FROM tombstones t
         JOIN deletion_audits audit ON audit.tombstone_id = t.id
         JOIN snapshots snapshot ON snapshot.id = t.snapshot_id
         WHERE t.snapshot_id = ?1",
    )
    .bind(snapshot_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| ApiError::database())?
    .ok_or_else(|| ApiError::not_found("tombstone_not_found", "tombstone was not found"))?;
    let rearchived_snapshot_url = row
        .rearchived_snapshot_id
        .as_ref()
        .map(|snapshot_id| format!("/api/v1/snapshots/{snapshot_id}"));
    Ok(TombstoneResponse {
        tombstone_id: row.tombstone_id,
        deletion_audit_id: row.deletion_audit_id,
        owner_namespace: row.owner_namespace.clone(),
        session_id: row.session_id.clone(),
        snapshot_id: row.snapshot_id.clone(),
        snapshot_fingerprint: digest_document_value(&row.snapshot_fingerprint_sha256),
        manifest_sha256: digest_document_value(&row.manifest_sha256),
        snapshot_completed_at: row.snapshot_completed_at.clone(),
        deleted_at: row.deleted_at,
        deleted_at_sort_key: row.deleted_at_seq,
        reason: row.reason,
        capture_count: u64::try_from(row.capture_count).map_err(|_| ApiError::internal())?,
        rearchived_snapshot_id: row.rearchived_snapshot_id,
        rearchived_snapshot_url,
        historical_receipt: Receipt {
            receipt_version: 2,
            archive_instance_id: state.identity.archive_instance_id.clone(),
            owner_namespace: row.owner_namespace,
            snapshot_id: row.snapshot_id,
            session_id: row.session_id,
            snapshot_fingerprint: digest_document_value(&row.snapshot_fingerprint_sha256),
            manifest_sha256: digest_document_value(&row.manifest_sha256),
            artifact_count: u32::try_from(row.artifact_count).map_err(|_| ApiError::internal())?,
            total_original_bytes: u64::try_from(row.total_original_size_bytes)
                .map_err(|_| ApiError::internal())?,
            total_stored_bytes: u64::try_from(row.total_stored_size_bytes)
                .map_err(|_| ApiError::internal())?,
            completed_at: row.snapshot_completed_at,
        },
    })
}

fn digest_document_value(value: &str) -> String {
    format!("sha256:{}", value.strip_prefix("sha256:").unwrap_or(value))
}
