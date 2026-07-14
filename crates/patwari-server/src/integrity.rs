//! Bounded, observational archive-integrity scanning.
//!
//! Scans deliberately persist observations instead of changing Snapshot
//! completion evidence. The scanner snapshots metadata a page at a time,
//! never holds a database transaction while hashing, and uses the same
//! per-digest locks as promotion and GC to avoid reporting in-process races
//! as corruption.

use std::{
    collections::BTreeMap,
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read},
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use sha2::{Digest, Sha256};
use sqlx::{FromRow, SqlitePool};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{fs as tokio_fs, sync::Semaphore, task::JoinHandle};
use uuid::Uuid;

use crate::{
    contract::{
        Compression, IntegrityFindingCounts, IntegrityFindingKind, IntegrityFindingSummary,
        IntegrityReport, IntegrityRunStatus, IntegrityRunSummary, IntegritySeverity, Manifest,
        ManifestInput,
    },
    database,
    ingestion::{self, ReconciliationError},
    service::AppState,
    validation::{ManifestLimits, normalize_manifest},
};

const PAGE_SIZE: i64 = 64;
const REPORT_FINDING_LIMIT: usize = 256;
const HISTORY_LIMIT: usize = 256;

#[derive(Debug, Error)]
pub enum IntegrityScanError {
    #[error("integrity scan could not enter the archive maintenance gate")]
    Maintenance,
    #[error("integrity scan could not generate a server timestamp")]
    Clock,
    #[error("integrity scan metadata operation failed")]
    Metadata,
    #[error("integrity scan filesystem inventory failed")]
    Storage,
    #[error("integrity scan worker failed")]
    Worker,
    #[error("integrity scan result could not be persisted")]
    Persistence,
}

async fn load_manifest_document(
    state: &AppState,
    snapshot_id: &str,
    max_manifest_bytes: u64,
) -> Result<Option<ManifestDocumentRow>, IntegrityScanError> {
    sqlx::query_as(
        "SELECT m.canonical_json, m.sha256 AS manifest_sha256
         FROM snapshots s
         JOIN manifests m ON m.id = s.manifest_id
         WHERE s.id = ?1 AND length(CAST(m.canonical_json AS BLOB)) <= ?2",
    )
    .bind(snapshot_id)
    .bind(i64::try_from(max_manifest_bytes).map_err(|_| IntegrityScanError::Metadata)?)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)
}

#[derive(FromRow)]
struct SnapshotScanRow {
    id: String,
    deleted_at: Option<String>,
    manifest_row_id: Option<String>,
    manifest_size_bytes: Option<i64>,
    tombstone_id: Option<String>,
}

#[derive(FromRow)]
struct ManifestDocumentRow {
    canonical_json: String,
    manifest_sha256: String,
}

#[derive(FromRow)]
struct ArtifactProjectionRow {
    artifact_id: String,
    blob_id: String,
    artifact_index: i64,
    blob_row_id: Option<String>,
    stored_size_bytes: Option<i64>,
    stored_sha256: Option<String>,
    compression: Option<String>,
}

#[derive(FromRow)]
struct DanglingArtifactRow {
    artifact: String,
    snapshot: String,
    blob: String,
}

#[derive(FromRow)]
struct BlobScanRow {
    id: String,
    stored_sha256: String,
    stored_size_bytes: i64,
    compression: String,
    orphaned_at: Option<String>,
    eligible_after: Option<String>,
    eligible_after_seq: Option<i64>,
    has_live_reference: i64,
}

#[derive(FromRow)]
struct RunRow {
    id: String,
    owner_namespace: String,
    status: String,
    started_at: String,
    completed_at: Option<String>,
    finding_count: i64,
    info_count: i64,
    warning_count: i64,
    error_count: i64,
}

#[derive(FromRow)]
struct FindingRow {
    id: String,
    run_id: String,
    kind: String,
    severity: String,
    snapshot_id: Option<String>,
    artifact_id: Option<String>,
    blob_id: Option<String>,
    detected_at: String,
    detail_code: String,
}

struct ScanRecorder {
    database: SqlitePool,
    run_id: String,
    owner_namespace: String,
    started_at: String,
    counts: IntegrityFindingCounts,
    report_findings: Vec<IntegrityFindingSummary>,
}

impl ScanRecorder {
    async fn begin(state: &AppState) -> Result<Self, IntegrityScanError> {
        let now = OffsetDateTime::now_utc();
        let started_at = database::format_time(now).map_err(|_| IntegrityScanError::Clock)?;
        let run_id = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO integrity_runs (
                id, owner_namespace, started_at, started_at_seq, status
             ) VALUES (?1, ?2, ?3, ?4, 'running')",
        )
        .bind(&run_id)
        .bind(&state.identity.owner_namespace)
        .bind(&started_at)
        .bind(database::sort_key_from_timestamp(now))
        .execute(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Persistence)?;

        Ok(Self {
            database: state.database.clone(),
            run_id,
            owner_namespace: state.identity.owner_namespace.clone(),
            started_at,
            counts: IntegrityFindingCounts::default(),
            report_findings: Vec::new(),
        })
    }

    async fn record(
        &mut self,
        kind: IntegrityFindingKind,
        severity: IntegritySeverity,
        snapshot_id: Option<&str>,
        artifact_id: Option<&str>,
        blob_id: Option<&str>,
        detail_code: &'static str,
    ) -> Result<(), IntegrityScanError> {
        let now = OffsetDateTime::now_utc();
        let detected_at = database::format_time(now).map_err(|_| IntegrityScanError::Clock)?;
        let finding_id = Uuid::now_v7().to_string();
        let inserted = sqlx::query(
            "INSERT INTO integrity_findings (
                id, run_id, owner_namespace, kind, severity, snapshot_id,
                artifact_id, blob_id, detected_at, detected_at_seq, detail_code
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )
        .bind(&finding_id)
        .bind(&self.run_id)
        .bind(&self.owner_namespace)
        .bind(kind.as_str())
        .bind(severity.as_str())
        .bind(snapshot_id)
        .bind(artifact_id)
        .bind(blob_id)
        .bind(&detected_at)
        .bind(database::sort_key_from_timestamp(now))
        .bind(detail_code)
        .execute(&self.database)
        .await
        .map_err(|_| IntegrityScanError::Persistence)?;
        if inserted.rows_affected() != 1 {
            return Err(IntegrityScanError::Persistence);
        }

        self.counts.total = self
            .counts
            .total
            .checked_add(1)
            .ok_or(IntegrityScanError::Persistence)?;
        match severity {
            IntegritySeverity::Info => {
                self.counts.info = self
                    .counts
                    .info
                    .checked_add(1)
                    .ok_or(IntegrityScanError::Persistence)?;
            }
            IntegritySeverity::Warning => {
                self.counts.warning = self
                    .counts
                    .warning
                    .checked_add(1)
                    .ok_or(IntegrityScanError::Persistence)?;
            }
            IntegritySeverity::Error => {
                self.counts.error = self
                    .counts
                    .error
                    .checked_add(1)
                    .ok_or(IntegrityScanError::Persistence)?;
            }
        }
        let count = self
            .counts
            .by_kind
            .entry(kind.as_str().to_owned())
            .or_default();
        *count = count
            .checked_add(1)
            .ok_or(IntegrityScanError::Persistence)?;

        if self.report_findings.len() < REPORT_FINDING_LIMIT {
            self.report_findings.push(IntegrityFindingSummary {
                finding_id,
                run_id: self.run_id.clone(),
                kind,
                severity,
                snapshot_id: snapshot_id.map(str::to_owned),
                artifact_id: artifact_id.map(str::to_owned),
                blob_id: blob_id.map(str::to_owned),
                detected_at,
                detail_code: detail_code.to_owned(),
            });
        }
        Ok(())
    }

    fn final_status(&self) -> IntegrityRunStatus {
        if self.counts.warning != 0 || self.counts.error != 0 {
            IntegrityRunStatus::ActionRequired
        } else {
            IntegrityRunStatus::Healthy
        }
    }

    async fn complete(self) -> Result<IntegrityReport, IntegrityScanError> {
        let now = OffsetDateTime::now_utc();
        let completed_at = database::format_time(now).map_err(|_| IntegrityScanError::Clock)?;
        let status = self.final_status();
        let updated = sqlx::query(
            "UPDATE integrity_runs
             SET completed_at = ?1, completed_at_seq = ?2, status = ?3,
                 finding_count = ?4, info_count = ?5, warning_count = ?6, error_count = ?7
             WHERE id = ?8 AND status = 'running'",
        )
        .bind(&completed_at)
        .bind(database::sort_key_from_timestamp(now))
        .bind(status.as_str())
        .bind(i64::try_from(self.counts.total).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.info).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.warning).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.error).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(&self.run_id)
        .execute(&self.database)
        .await
        .map_err(|_| IntegrityScanError::Persistence)?;
        if updated.rows_affected() != 1 {
            return Err(IntegrityScanError::Persistence);
        }

        let findings_truncated = u64::try_from(self.report_findings.len())
            .map_err(|_| IntegrityScanError::Persistence)?
            < self.counts.total;
        Ok(IntegrityReport {
            run_id: self.run_id,
            owner_namespace: self.owner_namespace,
            status,
            started_at: self.started_at,
            completed_at,
            findings_truncated,
            counts: self.counts,
            findings: self.report_findings,
        })
    }

    async fn fail(&self) -> Result<(), IntegrityScanError> {
        let now = OffsetDateTime::now_utc();
        let completed_at = database::format_time(now).map_err(|_| IntegrityScanError::Clock)?;
        let updated = sqlx::query(
            "UPDATE integrity_runs
             SET completed_at = ?1, completed_at_seq = ?2, status = 'failed',
                 finding_count = ?3, info_count = ?4, warning_count = ?5, error_count = ?6
             WHERE id = ?7 AND status = 'running'",
        )
        .bind(&completed_at)
        .bind(database::sort_key_from_timestamp(now))
        .bind(i64::try_from(self.counts.total).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.info).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.warning).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(i64::try_from(self.counts.error).map_err(|_| IntegrityScanError::Persistence)?)
        .bind(&self.run_id)
        .execute(&self.database)
        .await
        .map_err(|_| IntegrityScanError::Persistence)?;
        if updated.rows_affected() != 1 {
            return Err(IntegrityScanError::Persistence);
        }
        Ok(())
    }
}

/// Runs one complete archive scan and persists an immutable result.
pub(crate) async fn scan_archive(
    state: &Arc<AppState>,
) -> Result<IntegrityReport, IntegrityScanError> {
    let mut recorder = ScanRecorder::begin(state).await?;
    match scan_all(state, &mut recorder).await {
        Ok(()) => recorder.complete().await,
        Err(error) => {
            if recorder.fail().await.is_err() {
                return Err(IntegrityScanError::Persistence);
            }
            Err(error)
        }
    }
}

async fn scan_all(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    scan_database_integrity(state, recorder).await?;
    scan_dangling_artifact_references(state, recorder).await?;
    scan_tombstoned_artifact_references(state, recorder).await?;
    scan_snapshots(state, recorder).await?;
    scan_blob_rows(state, recorder).await?;
    inventory_blob_files(state, recorder).await
}

async fn scan_database_integrity(
    state: &AppState,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let quick_check: (String,) = sqlx::query_as("PRAGMA quick_check(1)")
        .fetch_one(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
    if quick_check.0 != "ok" {
        recorder
            .record(
                IntegrityFindingKind::DatabaseIntegrityFailure,
                IntegritySeverity::Error,
                None,
                None,
                None,
                "sqlite_quick_check_failed",
            )
            .await?;
    }
    let foreign_key_failure = sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
    if foreign_key_failure.is_some() {
        recorder
            .record(
                IntegrityFindingKind::DatabaseIntegrityFailure,
                IntegritySeverity::Error,
                None,
                None,
                None,
                "sqlite_foreign_key_check_failed",
            )
            .await?;
    }
    Ok(())
}

async fn scan_dangling_artifact_references(
    state: &AppState,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let mut after = String::new();
    loop {
        let rows = sqlx::query_as::<_, DanglingArtifactRow>(
            "SELECT a.id AS artifact, a.snapshot_id AS snapshot, a.blob_id AS blob
             FROM artifacts a
             JOIN snapshots s ON s.id = a.snapshot_id
             LEFT JOIN blobs b ON b.id = a.blob_id
             WHERE s.deleted_at IS NULL AND b.id IS NULL AND a.id > ?1
             ORDER BY a.id ASC
             LIMIT ?2",
        )
        .bind(&after)
        .bind(PAGE_SIZE)
        .fetch_all(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
        if rows.is_empty() {
            break;
        }

        after = rows
            .last()
            .map(|row| row.artifact.clone())
            .expect("nonempty rows have a final ID");
        for row in rows {
            recorder
                .record(
                    IntegrityFindingKind::ArtifactBlobReferenceMissing,
                    IntegritySeverity::Error,
                    Some(&row.snapshot),
                    Some(&row.artifact),
                    Some(&row.blob),
                    "artifact_references_missing_blob_row",
                )
                .await?;
        }
    }
    Ok(())
}

async fn scan_tombstoned_artifact_references(
    state: &AppState,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let mut after = String::new();
    loop {
        let rows = sqlx::query_as::<_, DanglingArtifactRow>(
            "SELECT a.id AS artifact, a.snapshot_id AS snapshot, a.blob_id AS blob
             FROM artifacts a
             JOIN snapshots s ON s.id = a.snapshot_id
             WHERE s.deleted_at IS NOT NULL AND a.id > ?1
             ORDER BY a.id ASC
             LIMIT ?2",
        )
        .bind(&after)
        .bind(PAGE_SIZE)
        .fetch_all(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
        if rows.is_empty() {
            break;
        }
        after = rows
            .last()
            .map(|row| row.artifact.clone())
            .expect("nonempty rows have a final ID");
        for row in rows {
            recorder
                .record(
                    IntegrityFindingKind::SnapshotProjectionDrift,
                    IntegritySeverity::Error,
                    Some(&row.snapshot),
                    Some(&row.artifact),
                    Some(&row.blob),
                    "tombstoned_snapshot_has_artifact_reference",
                )
                .await?;
        }
    }
    Ok(())
}

async fn scan_snapshots(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let mut after = String::new();
    loop {
        let rows = sqlx::query_as::<_, SnapshotScanRow>(
            "SELECT s.id, s.deleted_at, m.id AS manifest_row_id,
                    length(CAST(m.canonical_json AS BLOB)) AS manifest_size_bytes,
                    t.id AS tombstone_id
             FROM snapshots s
             LEFT JOIN manifests m ON m.id = s.manifest_id
             LEFT JOIN tombstones t ON t.snapshot_id = s.id
             WHERE s.id > ?1
             ORDER BY s.id ASC
             LIMIT ?2",
        )
        .bind(&after)
        .bind(PAGE_SIZE)
        .fetch_all(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
        if rows.is_empty() {
            break;
        }
        after = rows
            .last()
            .map(|row| row.id.clone())
            .expect("nonempty rows have a final ID");
        for row in rows {
            scan_snapshot(state, recorder, row).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn scan_snapshot(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    row: SnapshotScanRow,
) -> Result<(), IntegrityScanError> {
    let tombstoned = row.deleted_at.is_some();
    if tombstoned {
        let (severity, detail_code) = if row.tombstone_id.is_some() {
            (IntegritySeverity::Info, "snapshot_tombstoned")
        } else {
            (IntegritySeverity::Error, "tombstone_record_missing")
        };
        recorder
            .record(
                IntegrityFindingKind::TombstonedSnapshot,
                severity,
                Some(&row.id),
                None,
                None,
                detail_code,
            )
            .await?;
    }
    if row.manifest_row_id.is_none() {
        recorder
            .record(
                IntegrityFindingKind::ManifestMissing,
                IntegritySeverity::Error,
                Some(&row.id),
                None,
                None,
                "snapshot_manifest_row_missing",
            )
            .await?;
        return Ok(());
    }

    let Some(manifest_size_bytes) = row.manifest_size_bytes else {
        recorder
            .record(
                IntegrityFindingKind::ManifestMissing,
                IntegritySeverity::Error,
                Some(&row.id),
                None,
                None,
                "snapshot_manifest_metadata_missing",
            )
            .await?;
        return Ok(());
    };
    let manifest_size_bytes =
        u64::try_from(manifest_size_bytes).map_err(|_| IntegrityScanError::Metadata)?;
    let max_manifest_bytes =
        u64::try_from(state.max_request_body_bytes).map_err(|_| IntegrityScanError::Metadata)?;
    if manifest_size_bytes > max_manifest_bytes {
        recorder
            .record(
                IntegrityFindingKind::ManifestUnparseable,
                IntegritySeverity::Error,
                Some(&row.id),
                None,
                None,
                "canonical_manifest_exceeds_scan_limit",
            )
            .await?;
        return Ok(());
    }
    let Some(document) = load_manifest_document(state, &row.id, max_manifest_bytes).await? else {
        recorder
            .record(
                IntegrityFindingKind::TransientChange,
                IntegritySeverity::Info,
                Some(&row.id),
                None,
                None,
                "snapshot_changed_during_scan",
            )
            .await?;
        return Ok(());
    };
    let limits = ManifestLimits {
        artifact_count: state.max_artifact_count,
        artifact_stored_bytes: state.max_artifact_stored_bytes,
        artifact_original_bytes: state.max_artifact_original_bytes,
        snapshot_stored_bytes: state.max_snapshot_stored_bytes,
        snapshot_original_bytes: state.max_snapshot_original_bytes,
    };
    let analysis = analyze_manifest(document.canonical_json, limits).await?;
    if analysis.computed_hash != document.manifest_sha256 {
        recorder
            .record(
                IntegrityFindingKind::ManifestHashMismatch,
                IntegritySeverity::Error,
                Some(&row.id),
                None,
                None,
                "canonical_manifest_hash_mismatch",
            )
            .await?;
    }
    let Some(manifest) = analysis.manifest else {
        recorder
            .record(
                IntegrityFindingKind::ManifestUnparseable,
                IntegritySeverity::Error,
                Some(&row.id),
                None,
                None,
                analysis.invalid_detail_code,
            )
            .await?;
        return Ok(());
    };

    if tombstoned {
        return Ok(());
    }
    match ingestion::reconcile_snapshot(&state.database, &row.id).await {
        Ok(()) => {}
        Err(ReconciliationError::Drift) => {
            recorder
                .record(
                    IntegrityFindingKind::SnapshotProjectionDrift,
                    IntegritySeverity::Error,
                    Some(&row.id),
                    None,
                    None,
                    "normalized_snapshot_projection_mismatch",
                )
                .await?;
        }
        Err(ReconciliationError::NotFound) => {
            recorder
                .record(
                    IntegrityFindingKind::TransientChange,
                    IntegritySeverity::Info,
                    Some(&row.id),
                    None,
                    None,
                    "snapshot_changed_during_scan",
                )
                .await?;
            return Ok(());
        }
        Err(ReconciliationError::Metadata) => return Err(IntegrityScanError::Metadata),
    }

    let jobs = artifact_jobs(state, recorder, &row.id, &manifest).await?;
    scan_artifact_jobs(state, recorder, jobs).await
}

struct ManifestAnalysis {
    computed_hash: String,
    manifest: Option<Manifest>,
    invalid_detail_code: &'static str,
}

async fn analyze_manifest(
    canonical_json: String,
    limits: ManifestLimits,
) -> Result<ManifestAnalysis, IntegrityScanError> {
    tokio::task::spawn_blocking(move || {
        let computed_hash = hex_digest(Sha256::digest(canonical_json.as_bytes()));
        let parsed = serde_json::from_str::<Manifest>(&canonical_json);
        let manifest = parsed
            .ok()
            .and_then(|manifest| validate_manifest_schema(manifest, limits));
        let invalid_detail_code = if manifest.is_some() {
            "canonical_manifest_valid"
        } else if serde_json::from_str::<Manifest>(&canonical_json).is_err() {
            "canonical_manifest_unparseable"
        } else {
            "canonical_manifest_schema_invalid"
        };
        ManifestAnalysis {
            computed_hash,
            manifest,
            invalid_detail_code,
        }
    })
    .await
    .map_err(|_| IntegrityScanError::Worker)
}

fn validate_manifest_schema(manifest: Manifest, limits: ManifestLimits) -> Option<Manifest> {
    let input: ManifestInput = serde_json::to_value(manifest)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())?;
    normalize_manifest(input, limits).ok()
}

#[derive(Clone)]
struct ArtifactJob {
    snapshot_id: String,
    artifact_id: String,
    blob_id: String,
    stored_sha256: String,
    stored_size_bytes: u64,
    original_sha256: String,
    original_size_bytes: u64,
    compression: Compression,
}

async fn artifact_jobs(
    state: &AppState,
    recorder: &mut ScanRecorder,
    snapshot_id: &str,
    manifest: &Manifest,
) -> Result<Vec<ArtifactJob>, IntegrityScanError> {
    let rows = sqlx::query_as::<_, ArtifactProjectionRow>(
        "SELECT a.id AS artifact_id, a.blob_id, a.artifact_index,
                b.id AS blob_row_id, b.stored_size_bytes, b.stored_sha256, b.compression
         FROM artifacts a
         LEFT JOIN blobs b ON b.id = a.blob_id
         WHERE a.snapshot_id = ?1
         ORDER BY a.artifact_index ASC, a.id ASC",
    )
    .bind(snapshot_id)
    .fetch_all(&state.database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    let mut jobs = Vec::new();
    for (index, artifact) in manifest.artifacts.iter().enumerate() {
        let expected_index = i64::try_from(index).map_err(|_| IntegrityScanError::Metadata)?;
        let Some(row) = rows.iter().find(|row| row.artifact_index == expected_index) else {
            continue;
        };
        if row.blob_row_id.is_none() {
            continue;
        }
        let Some(stored_sha256) = document_digest(&artifact.stored_sha256) else {
            recorder
                .record(
                    IntegrityFindingKind::ManifestUnparseable,
                    IntegritySeverity::Error,
                    Some(snapshot_id),
                    Some(&row.artifact_id),
                    Some(&row.blob_id),
                    "canonical_manifest_stored_digest_invalid",
                )
                .await?;
            continue;
        };
        let Some(original_sha256) = document_digest(&artifact.original_sha256) else {
            recorder
                .record(
                    IntegrityFindingKind::ManifestUnparseable,
                    IntegritySeverity::Error,
                    Some(snapshot_id),
                    Some(&row.artifact_id),
                    Some(&row.blob_id),
                    "canonical_manifest_original_digest_invalid",
                )
                .await?;
            continue;
        };
        let stored_size_bytes = artifact.stored_size_bytes;
        if stored_size_bytes > state.max_artifact_stored_bytes
            || artifact.original_size_bytes > state.max_artifact_original_bytes
        {
            recorder
                .record(
                    IntegrityFindingKind::ArtifactOriginalMismatch,
                    IntegritySeverity::Error,
                    Some(snapshot_id),
                    Some(&row.artifact_id),
                    Some(&row.blob_id),
                    "artifact_declared_size_exceeds_scan_limit",
                )
                .await?;
            continue;
        }
        if row.stored_size_bytes != i64::try_from(stored_size_bytes).ok()
            || row.stored_sha256.as_deref() != Some(stored_sha256)
            || row.compression.as_deref() != Some(compression_name(artifact.compression))
        {
            continue;
        }
        jobs.push(ArtifactJob {
            snapshot_id: snapshot_id.to_owned(),
            artifact_id: row.artifact_id.clone(),
            blob_id: row.blob_id.clone(),
            stored_sha256: stored_sha256.to_owned(),
            stored_size_bytes,
            original_sha256: original_sha256.to_owned(),
            original_size_bytes: artifact.original_size_bytes,
            compression: artifact.compression,
        });
    }
    Ok(jobs)
}

async fn scan_artifact_jobs(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    jobs: Vec<ArtifactJob>,
) -> Result<(), IntegrityScanError> {
    let semaphore = Arc::new(Semaphore::new(state.integrity_scan_concurrency));
    let mut handles = Vec::with_capacity(jobs.len());
    for job in jobs {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IntegrityScanError::Worker)?;
        let state = Arc::clone(state);
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            scan_artifact_job(state, job).await
        }));
    }
    for handle in handles {
        let result = join_scan_task(handle).await?;
        record_artifact_result(recorder, result).await?;
    }
    Ok(())
}

enum ArtifactScanResult {
    Healthy,
    StoredInvalid,
    OriginalMismatch {
        snapshot_id: String,
        artifact_id: String,
        blob_id: String,
        detail_code: &'static str,
    },
    Transient {
        snapshot_id: String,
        artifact_id: String,
        blob_id: String,
    },
}

async fn scan_artifact_job(
    state: Arc<AppState>,
    job: ArtifactJob,
) -> Result<ArtifactScanResult, IntegrityScanError> {
    let digest = vec![job.stored_sha256.clone()];
    let locks = state.blob_locks_for_digests(&state.identity.owner_namespace, &digest);
    let lock = locks
        .into_iter()
        .next()
        .expect("one digest always maps to one lock");
    let _guard = lock.lock_owned().await;
    if !artifact_is_current(&state.database, &job).await? {
        return Ok(ArtifactScanResult::Transient {
            snapshot_id: job.snapshot_id,
            artifact_id: job.artifact_id,
            blob_id: job.blob_id,
        });
    }
    let path = state.storage.blob_path(&job.stored_sha256);
    let buffer_size = state.integrity_scan_buffer_bytes;
    let job_for_worker = job.clone();
    let result = tokio::task::spawn_blocking(move || {
        scan_artifact_original_file(&path, &job_for_worker, buffer_size)
    })
    .await
    .map_err(|_| IntegrityScanError::Worker)?;
    if !artifact_is_current(&state.database, &job).await? {
        return Ok(ArtifactScanResult::Transient {
            snapshot_id: job.snapshot_id,
            artifact_id: job.artifact_id,
            blob_id: job.blob_id,
        });
    }
    Ok(match result {
        OriginalFileCheck::Healthy => ArtifactScanResult::Healthy,
        OriginalFileCheck::StoredInvalid => ArtifactScanResult::StoredInvalid,
        OriginalFileCheck::Mismatch(detail_code) => ArtifactScanResult::OriginalMismatch {
            snapshot_id: job.snapshot_id,
            artifact_id: job.artifact_id,
            blob_id: job.blob_id,
            detail_code,
        },
        OriginalFileCheck::Transient => ArtifactScanResult::Transient {
            snapshot_id: job.snapshot_id,
            artifact_id: job.artifact_id,
            blob_id: job.blob_id,
        },
    })
}

async fn artifact_is_current(
    database: &SqlitePool,
    job: &ArtifactJob,
) -> Result<bool, IntegrityScanError> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT 1
         FROM artifacts a
         JOIN snapshots s ON s.id = a.snapshot_id
         JOIN blobs b ON b.id = a.blob_id
         WHERE a.id = ?1 AND a.snapshot_id = ?2 AND a.blob_id = ?3
           AND s.deleted_at IS NULL
           AND b.stored_sha256 = ?4 AND b.stored_size_bytes = ?5
         LIMIT 1",
    )
    .bind(&job.artifact_id)
    .bind(&job.snapshot_id)
    .bind(&job.blob_id)
    .bind(&job.stored_sha256)
    .bind(i64::try_from(job.stored_size_bytes).map_err(|_| IntegrityScanError::Metadata)?)
    .fetch_optional(database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    Ok(row.is_some())
}

async fn record_artifact_result(
    recorder: &mut ScanRecorder,
    result: ArtifactScanResult,
) -> Result<(), IntegrityScanError> {
    match result {
        ArtifactScanResult::Healthy | ArtifactScanResult::StoredInvalid => Ok(()),
        ArtifactScanResult::OriginalMismatch {
            snapshot_id,
            artifact_id,
            blob_id,
            detail_code,
        } => {
            recorder
                .record(
                    IntegrityFindingKind::ArtifactOriginalMismatch,
                    IntegritySeverity::Error,
                    Some(&snapshot_id),
                    Some(&artifact_id),
                    Some(&blob_id),
                    detail_code,
                )
                .await
        }
        ArtifactScanResult::Transient {
            snapshot_id,
            artifact_id,
            blob_id,
        } => {
            recorder
                .record(
                    IntegrityFindingKind::TransientChange,
                    IntegritySeverity::Info,
                    Some(&snapshot_id),
                    Some(&artifact_id),
                    Some(&blob_id),
                    "artifact_changed_during_scan",
                )
                .await
        }
    }
}

async fn scan_blob_rows(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let now_seq = database::sort_key_from_timestamp(OffsetDateTime::now_utc());
    let mut after = String::new();
    loop {
        let rows = sqlx::query_as::<_, BlobScanRow>(
            "SELECT b.id, b.stored_sha256, b.stored_size_bytes, b.compression,
                    b.orphaned_at, b.eligible_after, b.eligible_after_seq,
                    EXISTS(
                        SELECT 1
                        FROM artifacts a
                        JOIN snapshots s ON s.id = a.snapshot_id
                        WHERE a.blob_id = b.id AND s.deleted_at IS NULL
                    ) AS has_live_reference
             FROM blobs b
             WHERE b.id > ?1
             ORDER BY b.id ASC
             LIMIT ?2",
        )
        .bind(&after)
        .bind(PAGE_SIZE)
        .fetch_all(&state.database)
        .await
        .map_err(|_| IntegrityScanError::Metadata)?;
        if rows.is_empty() {
            break;
        }
        after = rows
            .last()
            .map(|row| row.id.clone())
            .expect("nonempty rows have a final ID");
        let mut jobs = Vec::new();
        for row in rows {
            classify_blob_liveness(state, recorder, &row, now_seq).await?;
            let valid_size = u64::try_from(row.stored_size_bytes)
                .ok()
                .filter(|size| *size <= state.max_artifact_stored_bytes);
            if !storage_digest(&row.stored_sha256)
                || valid_size.is_none()
                || !matches!(row.compression.as_str(), "identity" | "zstd")
            {
                recorder
                    .record(
                        IntegrityFindingKind::BlobMetadataInvalid,
                        IntegritySeverity::Error,
                        None,
                        None,
                        Some(&row.id),
                        "blob_row_metadata_invalid",
                    )
                    .await?;
                continue;
            }
            jobs.push(BlobJob {
                blob_id: row.id,
                stored_sha256: row.stored_sha256,
                stored_size_bytes: valid_size.expect("validated size"),
            });
        }
        scan_blob_jobs(state, recorder, jobs).await?;
    }
    Ok(())
}

async fn classify_blob_liveness(
    state: &AppState,
    recorder: &mut ScanRecorder,
    row: &BlobScanRow,
    now_seq: i64,
) -> Result<(), IntegrityScanError> {
    if storage_digest(&row.stored_sha256) {
        let digests = vec![row.stored_sha256.clone()];
        let locks = state.blob_locks_for_digests(&state.identity.owner_namespace, &digests);
        let lock = locks
            .into_iter()
            .next()
            .expect("one digest always maps to one lock");
        let _guard = lock.lock_owned().await;
        let Some(current) = current_blob_row(&state.database, &row.id).await? else {
            return recorder
                .record(
                    IntegrityFindingKind::TransientChange,
                    IntegritySeverity::Info,
                    None,
                    None,
                    Some(&row.id),
                    "blob_changed_during_scan",
                )
                .await;
        };
        return record_blob_liveness(recorder, &current, now_seq).await;
    }
    record_blob_liveness(recorder, row, now_seq).await
}

async fn current_blob_row(
    database: &SqlitePool,
    blob_id: &str,
) -> Result<Option<BlobScanRow>, IntegrityScanError> {
    sqlx::query_as::<_, BlobScanRow>(
        "SELECT b.id, b.stored_sha256, b.stored_size_bytes, b.compression,
                b.orphaned_at, b.eligible_after, b.eligible_after_seq,
                EXISTS(
                    SELECT 1
                    FROM artifacts a
                    JOIN snapshots s ON s.id = a.snapshot_id
                    WHERE a.blob_id = b.id AND s.deleted_at IS NULL
                ) AS has_live_reference
         FROM blobs b
         WHERE b.id = ?1",
    )
    .bind(blob_id)
    .fetch_optional(database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)
}

async fn record_blob_liveness(
    recorder: &mut ScanRecorder,
    row: &BlobScanRow,
    now_seq: i64,
) -> Result<(), IntegrityScanError> {
    let has_candidate = row.orphaned_at.is_some()
        || row.eligible_after.is_some()
        || row.eligible_after_seq.is_some();
    if row.has_live_reference != 0 {
        if has_candidate {
            recorder
                .record(
                    IntegrityFindingKind::BlobStaleCandidate,
                    IntegritySeverity::Warning,
                    None,
                    None,
                    Some(&row.id),
                    "candidate_has_live_artifact_reference",
                )
                .await?;
        }
        return Ok(());
    }
    match (
        row.orphaned_at.is_some(),
        row.eligible_after.is_some(),
        row.eligible_after_seq,
    ) {
        (true, true, Some(eligible_after_seq)) if eligible_after_seq > now_seq => {
            recorder
                .record(
                    IntegrityFindingKind::BlobGraceCandidate,
                    IntegritySeverity::Info,
                    None,
                    None,
                    Some(&row.id),
                    "orphan_within_gc_grace",
                )
                .await
        }
        (true, true, Some(_)) => {
            recorder
                .record(
                    IntegrityFindingKind::BlobGcEligibleCandidate,
                    IntegritySeverity::Warning,
                    None,
                    None,
                    Some(&row.id),
                    "orphan_gc_eligible",
                )
                .await
        }
        (false, false, None) => {
            recorder
                .record(
                    IntegrityFindingKind::BlobOrphan,
                    IntegritySeverity::Error,
                    None,
                    None,
                    Some(&row.id),
                    "blob_has_no_live_artifact_reference",
                )
                .await
        }
        _ => {
            recorder
                .record(
                    IntegrityFindingKind::BlobOrphan,
                    IntegritySeverity::Error,
                    None,
                    None,
                    Some(&row.id),
                    "orphan_candidate_metadata_inconsistent",
                )
                .await
        }
    }
}

#[derive(Clone)]
struct BlobJob {
    blob_id: String,
    stored_sha256: String,
    stored_size_bytes: u64,
}

async fn scan_blob_jobs(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    jobs: Vec<BlobJob>,
) -> Result<(), IntegrityScanError> {
    let semaphore = Arc::new(Semaphore::new(state.integrity_scan_concurrency));
    let mut handles = Vec::with_capacity(jobs.len());
    for job in jobs {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IntegrityScanError::Worker)?;
        let state = Arc::clone(state);
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            scan_blob_job(state, job).await
        }));
    }
    for handle in handles {
        let result = join_scan_task(handle).await?;
        record_blob_result(recorder, result).await?;
    }
    Ok(())
}

enum BlobScanResult {
    Healthy,
    Finding {
        blob_id: String,
        kind: IntegrityFindingKind,
        detail_code: &'static str,
    },
    Transient {
        blob_id: String,
    },
}

async fn scan_blob_job(
    state: Arc<AppState>,
    job: BlobJob,
) -> Result<BlobScanResult, IntegrityScanError> {
    let digest = vec![job.stored_sha256.clone()];
    let locks = state.blob_locks_for_digests(&state.identity.owner_namespace, &digest);
    let lock = locks
        .into_iter()
        .next()
        .expect("one digest always maps to one lock");
    let _guard = lock.lock_owned().await;
    if !blob_is_current(&state.database, &job).await? {
        return Ok(BlobScanResult::Transient {
            blob_id: job.blob_id,
        });
    }
    let path = state.storage.blob_path(&job.stored_sha256);
    let buffer_size = state.integrity_scan_buffer_bytes;
    let job_for_worker = job.clone();
    let result =
        tokio::task::spawn_blocking(move || scan_stored_file(&path, &job_for_worker, buffer_size))
            .await
            .map_err(|_| IntegrityScanError::Worker)?;
    if !blob_is_current(&state.database, &job).await? {
        return Ok(BlobScanResult::Transient {
            blob_id: job.blob_id,
        });
    }
    Ok(match result {
        StoredFileCheck::Healthy => BlobScanResult::Healthy,
        StoredFileCheck::Missing => BlobScanResult::Finding {
            blob_id: job.blob_id,
            kind: IntegrityFindingKind::BlobFileMissing,
            detail_code: "canonical_blob_file_missing",
        },
        StoredFileCheck::NonRegular => BlobScanResult::Finding {
            blob_id: job.blob_id,
            kind: IntegrityFindingKind::BlobFileNonRegular,
            detail_code: "canonical_blob_file_not_regular",
        },
        StoredFileCheck::SizeMismatch => BlobScanResult::Finding {
            blob_id: job.blob_id,
            kind: IntegrityFindingKind::BlobFileSizeMismatch,
            detail_code: "canonical_blob_file_size_mismatch",
        },
        StoredFileCheck::HashMismatch => BlobScanResult::Finding {
            blob_id: job.blob_id,
            kind: IntegrityFindingKind::BlobFileHashMismatch,
            detail_code: "canonical_blob_file_hash_mismatch",
        },
        StoredFileCheck::Unreadable => BlobScanResult::Finding {
            blob_id: job.blob_id,
            kind: IntegrityFindingKind::BlobFileMissing,
            detail_code: "canonical_blob_file_unreadable",
        },
        StoredFileCheck::Transient => BlobScanResult::Transient {
            blob_id: job.blob_id,
        },
    })
}

async fn blob_is_current(database: &SqlitePool, job: &BlobJob) -> Result<bool, IntegrityScanError> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT 1 FROM blobs
         WHERE id = ?1 AND stored_sha256 = ?2 AND stored_size_bytes = ?3
         LIMIT 1",
    )
    .bind(&job.blob_id)
    .bind(&job.stored_sha256)
    .bind(i64::try_from(job.stored_size_bytes).map_err(|_| IntegrityScanError::Metadata)?)
    .fetch_optional(database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    Ok(row.is_some())
}

async fn record_blob_result(
    recorder: &mut ScanRecorder,
    result: BlobScanResult,
) -> Result<(), IntegrityScanError> {
    match result {
        BlobScanResult::Healthy => Ok(()),
        BlobScanResult::Finding {
            blob_id,
            kind,
            detail_code,
        } => {
            recorder
                .record(
                    kind,
                    IntegritySeverity::Error,
                    None,
                    None,
                    Some(&blob_id),
                    detail_code,
                )
                .await
        }
        BlobScanResult::Transient { blob_id } => {
            recorder
                .record(
                    IntegrityFindingKind::TransientChange,
                    IntegritySeverity::Info,
                    None,
                    None,
                    Some(&blob_id),
                    "blob_changed_during_scan",
                )
                .await
        }
    }
}

async fn join_scan_task<T>(
    handle: JoinHandle<Result<T, IntegrityScanError>>,
) -> Result<T, IntegrityScanError> {
    handle.await.map_err(|_| IntegrityScanError::Worker)?
}

enum StoredFileCheck {
    Healthy,
    Missing,
    NonRegular,
    SizeMismatch,
    HashMismatch,
    Unreadable,
    Transient,
}

fn scan_stored_file(path: &Path, job: &BlobJob, buffer_size: usize) -> StoredFileCheck {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return StoredFileCheck::Missing,
        Err(_) => return StoredFileCheck::Unreadable,
    };
    if !metadata.file_type().is_file() {
        return StoredFileCheck::NonRegular;
    }
    let mut file = match open_without_following_symlinks(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return StoredFileCheck::Missing,
        Err(_) => return StoredFileCheck::Unreadable,
    };
    let Ok(before) = file.metadata() else {
        return StoredFileCheck::Unreadable;
    };
    if !before.file_type().is_file() {
        return StoredFileCheck::NonRegular;
    }
    if before.len() != job.stored_size_bytes {
        return StoredFileCheck::SizeMismatch;
    }
    let (size, digest) = match hash_reader(&mut file, job.stored_size_bytes, buffer_size) {
        Ok(value) => value,
        Err(HashReadError::TooLarge) => return StoredFileCheck::SizeMismatch,
        Err(HashReadError::Io) => return StoredFileCheck::Unreadable,
    };
    let Ok(after) = file.metadata() else {
        return StoredFileCheck::Unreadable;
    };
    if file_changed(&before, &after) {
        return StoredFileCheck::Transient;
    }
    if size != job.stored_size_bytes {
        return StoredFileCheck::SizeMismatch;
    }
    if digest != job.stored_sha256 {
        return StoredFileCheck::HashMismatch;
    }
    StoredFileCheck::Healthy
}

enum OriginalFileCheck {
    Healthy,
    StoredInvalid,
    Mismatch(&'static str),
    Transient,
}

fn scan_artifact_original_file(
    path: &Path,
    job: &ArtifactJob,
    buffer_size: usize,
) -> OriginalFileCheck {
    let stored_job = BlobJob {
        blob_id: job.blob_id.clone(),
        stored_sha256: job.stored_sha256.clone(),
        stored_size_bytes: job.stored_size_bytes,
    };
    if !matches!(
        scan_stored_file(path, &stored_job, buffer_size),
        StoredFileCheck::Healthy
    ) {
        return OriginalFileCheck::StoredInvalid;
    }

    let Ok(file) = open_without_following_symlinks(path) else {
        return OriginalFileCheck::StoredInvalid;
    };
    let before = match file.metadata() {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) | Err(_) => return OriginalFileCheck::StoredInvalid,
    };
    let mut reader: Box<dyn Read> = match job.compression {
        Compression::Identity => Box::new(file),
        Compression::Zstd => match zstd::stream::read::Decoder::new(file) {
            Ok(decoder) => Box::new(decoder),
            Err(_) => return OriginalFileCheck::Mismatch("zstd_decode_failed"),
        },
    };
    let (size, digest) = match hash_reader(&mut reader, job.original_size_bytes, buffer_size) {
        Ok(value) => value,
        Err(HashReadError::TooLarge) => {
            return OriginalFileCheck::Mismatch("original_size_mismatch");
        }
        Err(HashReadError::Io) => {
            return OriginalFileCheck::Mismatch(match job.compression {
                Compression::Identity => "original_content_unreadable",
                Compression::Zstd => "zstd_decode_failed",
            });
        }
    };
    drop(reader);
    let Ok(after) = fs::symlink_metadata(path) else {
        return OriginalFileCheck::Transient;
    };
    if !after.file_type().is_file() {
        return OriginalFileCheck::Transient;
    }
    if file_changed(&before, &after) {
        return OriginalFileCheck::Transient;
    }
    if size != job.original_size_bytes {
        return OriginalFileCheck::Mismatch("original_size_mismatch");
    }
    if digest != job.original_sha256 {
        return OriginalFileCheck::Mismatch("original_sha256_mismatch");
    }
    OriginalFileCheck::Healthy
}

enum HashReadError {
    TooLarge,
    Io,
}

fn hash_reader(
    reader: &mut dyn Read,
    maximum_size: u64,
    buffer_size: usize,
) -> Result<(u64, String), HashReadError> {
    let mut buffer = vec![0_u8; buffer_size];
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    loop {
        let read = reader.read(&mut buffer).map_err(|_| HashReadError::Io)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).expect("buffer length fits in u64"))
            .ok_or(HashReadError::TooLarge)?;
        if total > maximum_size {
            return Err(HashReadError::TooLarge);
        }
        hasher.update(&buffer[..read]);
    }
    Ok((total, hex_digest(hasher.finalize())))
}

#[derive(PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    mtime: i64,
    #[cfg(unix)]
    mtime_nsec: i64,
    #[cfg(unix)]
    ctime: i64,
    #[cfg(unix)]
    ctime_nsec: i64,
}

fn file_stamp(metadata: &Metadata) -> FileStamp {
    FileStamp {
        len: metadata.len(),
        modified: metadata.modified().ok(),
        #[cfg(unix)]
        dev: metadata.dev(),
        #[cfg(unix)]
        ino: metadata.ino(),
        #[cfg(unix)]
        mtime: metadata.mtime(),
        #[cfg(unix)]
        mtime_nsec: metadata.mtime_nsec(),
        #[cfg(unix)]
        ctime: metadata.ctime(),
        #[cfg(unix)]
        ctime_nsec: metadata.ctime_nsec(),
    }
}

fn file_changed(before: &Metadata, after: &Metadata) -> bool {
    file_stamp(before) != file_stamp(after)
}

fn open_without_following_symlinks(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path)
}

async fn inventory_blob_files(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
) -> Result<(), IntegrityScanError> {
    let root = &state.storage.blobs;
    let root_metadata = tokio_fs::symlink_metadata(root)
        .await
        .map_err(|_| IntegrityScanError::Storage)?;
    if !root_metadata.file_type().is_dir() {
        recorder
            .record(
                IntegrityFindingKind::UnexpectedBlobFile,
                IntegritySeverity::Error,
                None,
                None,
                None,
                "blob_root_not_directory",
            )
            .await?;
        return Ok(());
    }
    let mut entries = tokio_fs::read_dir(root)
        .await
        .map_err(|_| IntegrityScanError::Storage)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| IntegrityScanError::Storage)?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| IntegrityScanError::Storage)?;
        if name == "sha256" {
            if file_type.is_dir() {
                inventory_sha256_directory(state, recorder, &entry.path()).await?;
            } else {
                recorder
                    .record(
                        IntegrityFindingKind::UnexpectedBlobFile,
                        IntegritySeverity::Error,
                        None,
                        None,
                        None,
                        "blob_sha256_root_not_directory",
                    )
                    .await?;
            }
        } else if !temporary_storage_name(&name) {
            recorder
                .record(
                    IntegrityFindingKind::UnexpectedBlobFile,
                    IntegritySeverity::Error,
                    None,
                    None,
                    None,
                    "noncanonical_blob_root_entry",
                )
                .await?;
        }
    }
    // `sha256/` is created lazily on the first promotion. Its absence is
    // healthy for an empty archive; Blob-row checks report every missing
    // canonical file once metadata says one should exist.
    Ok(())
}

async fn inventory_sha256_directory(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    root: &Path,
) -> Result<(), IntegrityScanError> {
    let mut entries = tokio_fs::read_dir(root)
        .await
        .map_err(|_| IntegrityScanError::Storage)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| IntegrityScanError::Storage)?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if temporary_storage_name(&name) {
            continue;
        }
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| IntegrityScanError::Storage)?;
        if file_type.is_dir() && shard_name(&name) {
            inventory_blob_shard(state, recorder, &entry.path(), &name).await?;
        } else {
            recorder
                .record(
                    IntegrityFindingKind::UnexpectedBlobFile,
                    IntegritySeverity::Error,
                    None,
                    None,
                    None,
                    "invalid_canonical_blob_shard",
                )
                .await?;
        }
    }
    Ok(())
}

async fn inventory_blob_shard(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    directory: &Path,
    shard: &str,
) -> Result<(), IntegrityScanError> {
    let mut entries = tokio_fs::read_dir(directory)
        .await
        .map_err(|_| IntegrityScanError::Storage)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| IntegrityScanError::Storage)?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if temporary_storage_name(&name) {
            continue;
        }
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| IntegrityScanError::Storage)?;
        if storage_digest(&name) && name.starts_with(shard) {
            record_unowned_blob_file_if_current(state, recorder, &name, &entry.path()).await?;
        } else if !file_type.is_file() || !temporary_storage_name(&name) {
            recorder
                .record(
                    IntegrityFindingKind::UnexpectedBlobFile,
                    IntegritySeverity::Error,
                    None,
                    None,
                    None,
                    "invalid_canonical_blob_entry",
                )
                .await?;
        }
    }
    Ok(())
}

async fn record_unowned_blob_file_if_current(
    state: &Arc<AppState>,
    recorder: &mut ScanRecorder,
    digest: &str,
    path: &Path,
) -> Result<(), IntegrityScanError> {
    let digests = vec![digest.to_owned()];
    let locks = state.blob_locks_for_digests(&state.identity.owner_namespace, &digests);
    let lock = locks
        .into_iter()
        .next()
        .expect("one digest always maps to one lock");
    let _guard = lock.lock_owned().await;
    let known: Option<(String,)> =
        sqlx::query_as("SELECT id FROM blobs WHERE owner_namespace = ?1 AND stored_sha256 = ?2")
            .bind(&state.identity.owner_namespace)
            .bind(digest)
            .fetch_optional(&state.database)
            .await
            .map_err(|_| IntegrityScanError::Metadata)?;
    if known.is_none() && tokio_fs::symlink_metadata(path).await.is_ok() {
        recorder
            .record(
                IntegrityFindingKind::UnexpectedBlobFile,
                IntegritySeverity::Error,
                None,
                None,
                None,
                "canonical_blob_file_has_no_blob_row",
            )
            .await?;
    }
    Ok(())
}

fn temporary_storage_name(name: &str) -> bool {
    name.starts_with('.')
        || Path::new(name).extension().is_some_and(|extension| {
            extension.eq_ignore_ascii_case("partial")
                || extension.eq_ignore_ascii_case("tmp")
                || extension.eq_ignore_ascii_case("temp")
        })
}

fn shard_name(value: &str) -> bool {
    value.len() == 2 && value.bytes().all(is_lower_hex)
}

fn storage_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(is_lower_hex)
}

fn document_digest(value: &str) -> Option<&str> {
    value
        .strip_prefix("sha256:")
        .filter(|value| storage_digest(value))
}

const fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
}

const fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::Identity => "identity",
        Compression::Zstd => "zstd",
    }
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

/// Reads the latest completed run as the current integrity-health projection.
pub(crate) async fn latest_health(
    state: &AppState,
) -> Result<Option<IntegrityRunSummary>, IntegrityScanError> {
    let id: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM integrity_runs
         WHERE owner_namespace = ?1 AND completed_at_seq IS NOT NULL
         ORDER BY completed_at_seq DESC, id DESC
         LIMIT 1",
    )
    .bind(&state.identity.owner_namespace)
    .fetch_optional(&state.database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    let Some((id,)) = id else {
        return Ok(None);
    };
    Ok(Some(run_summary(&state.database, &id).await?))
}

pub(crate) async fn list_runs(
    state: &AppState,
    limit: usize,
) -> Result<Vec<IntegrityRunSummary>, IntegrityScanError> {
    let limit =
        i64::try_from(limit.min(HISTORY_LIMIT)).map_err(|_| IntegrityScanError::Metadata)?;
    let ids = sqlx::query_as::<_, (String,)>(
        "SELECT id FROM integrity_runs
         WHERE owner_namespace = ?1 AND completed_at_seq IS NOT NULL
         ORDER BY completed_at_seq DESC, id DESC
         LIMIT ?2",
    )
    .bind(&state.identity.owner_namespace)
    .bind(limit)
    .fetch_all(&state.database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    let mut runs = Vec::with_capacity(ids.len());
    for (id,) in ids {
        runs.push(run_summary(&state.database, &id).await?);
    }
    Ok(runs)
}

pub(crate) async fn list_findings(
    state: &AppState,
    run_id: &str,
    limit: usize,
) -> Result<Vec<IntegrityFindingSummary>, IntegrityScanError> {
    let limit =
        i64::try_from(limit.min(HISTORY_LIMIT)).map_err(|_| IntegrityScanError::Metadata)?;
    let rows = sqlx::query_as::<_, FindingRow>(
        "SELECT id, run_id, kind, severity, snapshot_id, artifact_id, blob_id,
                detected_at, detail_code
         FROM integrity_findings
         WHERE run_id = ?1
         ORDER BY detected_at_seq ASC, id ASC
         LIMIT ?2",
    )
    .bind(run_id)
    .bind(limit)
    .fetch_all(&state.database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    rows.into_iter().map(finding_summary).collect()
}

async fn run_summary(
    database: &SqlitePool,
    run_id: &str,
) -> Result<IntegrityRunSummary, IntegrityScanError> {
    let row = sqlx::query_as::<_, RunRow>(
        "SELECT id, owner_namespace, status, started_at, completed_at,
                finding_count, info_count, warning_count, error_count
         FROM integrity_runs WHERE id = ?1",
    )
    .bind(run_id)
    .fetch_one(database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    let kind_rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT kind, COUNT(*) FROM integrity_findings
         WHERE run_id = ?1
         GROUP BY kind
         ORDER BY kind ASC",
    )
    .bind(run_id)
    .fetch_all(database)
    .await
    .map_err(|_| IntegrityScanError::Metadata)?;
    let mut by_kind = BTreeMap::new();
    for (kind, count) in kind_rows {
        by_kind.insert(
            kind,
            u64::try_from(count).map_err(|_| IntegrityScanError::Metadata)?,
        );
    }
    Ok(IntegrityRunSummary {
        run_id: row.id,
        owner_namespace: row.owner_namespace,
        status: IntegrityRunStatus::parse(&row.status).ok_or(IntegrityScanError::Metadata)?,
        started_at: row.started_at,
        completed_at: row.completed_at,
        counts: IntegrityFindingCounts {
            total: u64::try_from(row.finding_count).map_err(|_| IntegrityScanError::Metadata)?,
            info: u64::try_from(row.info_count).map_err(|_| IntegrityScanError::Metadata)?,
            warning: u64::try_from(row.warning_count).map_err(|_| IntegrityScanError::Metadata)?,
            error: u64::try_from(row.error_count).map_err(|_| IntegrityScanError::Metadata)?,
            by_kind,
        },
    })
}

fn finding_summary(row: FindingRow) -> Result<IntegrityFindingSummary, IntegrityScanError> {
    Ok(IntegrityFindingSummary {
        finding_id: row.id,
        run_id: row.run_id,
        kind: IntegrityFindingKind::parse(&row.kind).ok_or(IntegrityScanError::Metadata)?,
        severity: IntegritySeverity::parse(&row.severity).ok_or(IntegrityScanError::Metadata)?,
        snapshot_id: row.snapshot_id,
        artifact_id: row.artifact_id,
        blob_id: row.blob_id,
        detected_at: row.detected_at,
        detail_code: row.detail_code,
    })
}
