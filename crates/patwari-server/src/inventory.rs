//! Archive-wide inventory: aggregate counters and the registered clients.
//!
//! These two resources answer "how much is in the archive" and "which
//! clients have written to it" without paging the browsing collections 50
//! rows at a time. They stay inside the ADR 0001 boundary deliberately:
//! every value is an integer, a timestamp, or an identifier the archive
//! already stores. Nothing here reads, ranks, or interprets artifact
//! content.

use std::sync::Arc;

use axum::{Json, extract::State};
use sqlx::FromRow;

use crate::{
    contract::{ArchiveStats, ClientInventoryEntry, PaginatedResponse},
    database,
    error::ApiError,
    service::AppState,
};

/// The version of the [`ArchiveStats`] document. Consumers pin this the way
/// they pin the manifest schema version.
const STATS_SCHEMA_VERSION: u16 = 1;

/// Counted rows are non-negative by table constraint, so a negative value
/// here would mean the metadata schema itself has drifted.
fn counter(value: i64) -> Result<u64, ApiError> {
    u64::try_from(value).map_err(|_| ApiError::internal())
}

#[derive(Debug, FromRow)]
struct StatsRow {
    sessions: i64,
    snapshots: i64,
    captures: i64,
    artifacts: i64,
    blobs: i64,
    stored_bytes: i64,
    original_bytes: i64,
    blob_stored_bytes: i64,
    clients: i64,
    tombstones: i64,
    last_ingest_at: Option<String>,
    oldest_activity_at: Option<String>,
    newest_activity_at: Option<String>,
}

/// Returns archive-wide totals as of the moment the query ran.
///
/// Counts cover live rows: a tombstoned snapshot leaves `snapshots`,
/// `sessions`, `captures`, and `artifacts` and appears in `tombstones`
/// instead. `blobs` counts authoritative blob rows, which are shared across
/// snapshots and survive a tombstone until blob GC collects them, so the
/// byte sums and the blob count answer different questions on purpose.
pub(crate) async fn get_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ArchiveStats>, ApiError> {
    let generated_at = database::now_rfc3339().map_err(|_| ApiError::internal())?;
    // Ordering by the numeric sort key rather than MIN/MAX over the RFC 3339
    // text is what makes these bounds chronological: the text column has a
    // variable-precision fractional-second component (see
    // `database::sort_key_from_rfc3339`), and each of these subqueries walks
    // one row of an existing index.
    let row = sqlx::query_as::<_, StatsRow>(
        "SELECT
            (SELECT COUNT(*) FROM session_latest_context p
                JOIN snapshots s ON s.id = p.snapshot_id
              WHERE s.deleted_at IS NULL) AS sessions,
            (SELECT COUNT(*) FROM snapshots WHERE deleted_at IS NULL) AS snapshots,
            (SELECT COUNT(*) FROM captures c
                JOIN snapshots s ON s.id = c.snapshot_id
              WHERE s.deleted_at IS NULL) AS captures,
            (SELECT COUNT(*) FROM artifacts a
                JOIN snapshots s ON s.id = a.snapshot_id
              WHERE s.deleted_at IS NULL) AS artifacts,
            (SELECT COUNT(*) FROM blobs) AS blobs,
            (SELECT COALESCE(SUM(total_stored_size_bytes), 0) FROM snapshots
              WHERE deleted_at IS NULL) AS stored_bytes,
            (SELECT COALESCE(SUM(total_original_size_bytes), 0) FROM snapshots
              WHERE deleted_at IS NULL) AS original_bytes,
            (SELECT COALESCE(SUM(stored_size_bytes), 0) FROM blobs) AS blob_stored_bytes,
            (SELECT COUNT(*) FROM clients) AS clients,
            (SELECT COUNT(*) FROM tombstones) AS tombstones,
            (SELECT c.server_completed_at FROM captures c
                JOIN snapshots s ON s.id = c.snapshot_id
              WHERE s.deleted_at IS NULL
              ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT 1) AS last_ingest_at,
            (SELECT p.completed_at FROM session_latest_context p
                JOIN snapshots s ON s.id = p.snapshot_id
              WHERE s.deleted_at IS NULL
              ORDER BY p.completed_at_seq ASC, p.session_id ASC LIMIT 1) AS oldest_activity_at,
            (SELECT p.completed_at FROM session_latest_context p
                JOIN snapshots s ON s.id = p.snapshot_id
              WHERE s.deleted_at IS NULL
              ORDER BY p.completed_at_seq DESC, p.session_id DESC LIMIT 1) AS newest_activity_at",
    )
    .fetch_one(&state.database)
    .await
    .map_err(|_| ApiError::database())?;

    Ok(Json(ArchiveStats {
        schema_version: STATS_SCHEMA_VERSION,
        generated_at,
        archive_instance_id: state.identity.archive_instance_id.clone(),
        sessions: counter(row.sessions)?,
        snapshots: counter(row.snapshots)?,
        captures: counter(row.captures)?,
        artifacts: counter(row.artifacts)?,
        blobs: counter(row.blobs)?,
        stored_bytes: counter(row.stored_bytes)?,
        original_bytes: counter(row.original_bytes)?,
        blob_stored_bytes: counter(row.blob_stored_bytes)?,
        clients: counter(row.clients)?,
        tombstones: counter(row.tombstones)?,
        last_ingest_at: row.last_ingest_at,
        oldest_activity_at: row.oldest_activity_at,
        newest_activity_at: row.newest_activity_at,
    }))
}

#[derive(Debug, FromRow)]
struct ClientRow {
    client_id: String,
    hostname: Option<String>,
    display_name: Option<String>,
    first_seen_at: String,
    last_seen_at: String,
    capture_count: i64,
    last_capture_at: Option<String>,
}

/// Lists every registered client with the identity fields
/// `PUT /clients/{client_id}` stores and its live capture count.
///
/// The registry is bounded by the number of machines an owner runs, so this
/// is deliberately unpaginated; `next_cursor` is always `null` and reserved
/// for a future page boundary. Client metadata is not returned here: it is
/// an opaque per-client document, and this resource is an inventory.
pub(crate) async fn list_clients(
    State(state): State<Arc<AppState>>,
) -> Result<Json<PaginatedResponse<ClientInventoryEntry>>, ApiError> {
    let rows = sqlx::query_as::<_, ClientRow>(
        "SELECT client.id AS client_id, client.hostname, client.display_name,
                client.created_at AS first_seen_at, client.updated_at AS last_seen_at,
                (SELECT COUNT(*) FROM captures c
                    JOIN snapshots s ON s.id = c.snapshot_id
                  WHERE c.client_id = client.id AND s.deleted_at IS NULL) AS capture_count,
                (SELECT c.server_completed_at FROM captures c
                    JOIN snapshots s ON s.id = c.snapshot_id
                  WHERE c.client_id = client.id AND s.deleted_at IS NULL
                  ORDER BY c.server_completed_at_seq DESC, c.id DESC LIMIT 1) AS last_capture_at
         FROM clients client
         ORDER BY client.id ASC",
    )
    .fetch_all(&state.database)
    .await
    .map_err(|_| ApiError::database())?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok(ClientInventoryEntry {
                client_id: row.client_id,
                hostname: row.hostname,
                display_name: row.display_name,
                first_seen_at: row.first_seen_at,
                last_seen_at: Some(row.last_seen_at),
                capture_count: counter(row.capture_count)?,
                last_capture_at: row.last_capture_at,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(Json(PaginatedResponse {
        items,
        next_cursor: None,
        high_watermark: None,
    }))
}
