//! Stable HTTP request and response documents for the versioned archive API.

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
    /// A client-generated identifier for one durable capture observation.
    ///
    /// This is deliberately unrelated to the server-derived snapshot
    /// fingerprint. Repeating a capture ID for the same client is
    /// idempotent only when the canonical manifest is unchanged.
    #[serde(default)]
    pub capture_id: Option<String>,
    /// Deprecated compatibility alias for `capture_id`.
    ///
    /// Supplying both names is allowed only when their values are identical,
    /// so old and new clients cannot create an ambiguous capture identity.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub manifest: ManifestInput,
}

/// The upload negotiation response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadResponse {
    pub upload_id: String,
    pub capture_id: String,
    pub session_id: String,
    pub status: UploadStatus,
    pub manifest_sha256: String,
    pub chunk_size_bytes: u64,
    pub artifacts: Vec<UploadArtifactStatus>,
    /// Compatibility shortcut for the first chunk of artifact zero.
    ///
    /// Clients uploading more than one artifact must use the per-artifact
    /// `chunk_upload_url` in `artifacts`.
    pub artifact_upload_url: String,
    pub status_url: String,
    pub abandon_url: String,
    pub completion_url: String,
    /// Becomes retrievable after successful completion.
    pub capture_url: String,
}

/// The resumable-upload status document returned by `GET /uploads/{upload_id}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadStatusResponse {
    pub upload_id: String,
    pub capture_id: String,
    pub session_id: String,
    pub status: UploadStatus,
    pub manifest_sha256: Option<String>,
    pub chunk_size_bytes: u64,
    pub artifacts: Vec<UploadArtifactStatus>,
    pub status_url: String,
    pub abandon_url: String,
    pub completion_url: String,
    /// Becomes retrievable after successful completion.
    pub capture_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadArtifactStatus {
    pub artifact_index: u32,
    pub logical_path: String,
    pub media_type: Option<String>,
    pub original_size_bytes: u64,
    pub original_sha256: String,
    pub stored_size_bytes: u64,
    pub stored_sha256: String,
    pub compression: Compression,
    pub chunk_count: u64,
    /// A URL template ending in `{chunk_index}` for this artifact only.
    pub chunk_upload_url: String,
    /// Lowercase hexadecimal bytes, with bit 0 of byte 0 representing chunk 0.
    pub accepted_chunk_bitmap: String,
    pub missing_chunk_indexes: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestInput {
    pub schema_version: u16,
    pub session: SessionInput,
    pub capture: CaptureInput,
    /// The complete artifact set. New manifests must use this field.
    #[serde(default)]
    pub artifacts: Option<Vec<ArtifactInput>>,
    /// Legacy singleton input accepted only for migration compatibility.
    #[serde(default)]
    pub artifact: Option<ArtifactInput>,
}

/// Canonical v1 manifest. Serialization always emits the ordered
/// multi-artifact form, even when it contains a single artifact.
#[derive(Clone, Debug, Serialize)]
pub struct Manifest {
    pub schema_version: u16,
    pub session: SessionInput,
    pub capture: Capture,
    pub artifacts: Vec<Artifact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    schema_version: u16,
    session: SessionInput,
    capture: Capture,
    #[serde(default)]
    artifacts: Option<Vec<Artifact>>,
    #[serde(default)]
    artifact: Option<Artifact>,
}

impl<'de> Deserialize<'de> for Manifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ManifestWire::deserialize(deserializer)?;
        let artifacts = match (wire.artifacts, wire.artifact) {
            (Some(artifacts), None) => artifacts,
            (None, Some(artifact)) => vec![artifact],
            _ => {
                return Err(serde::de::Error::custom(
                    "manifest must contain exactly one of artifacts or legacy artifact",
                ));
            }
        };
        Ok(Self {
            schema_version: wire.schema_version,
            session: wire.session,
            capture: wire.capture,
            artifacts,
        })
    }
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
    pub source_state_hash: Option<String>,
    #[serde(default)]
    pub source_metadata: BTreeMap<String, String>,
    pub project: Option<String>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub source_agent_version: Option<String>,
    /// Versioned source-adapter artifact contract. New manifests must state
    /// this explicitly because it participates in snapshot identity.
    pub artifact_set_version: u16,
    pub munshi_version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub captured_at: String,
    pub source_cursor: Option<String>,
    pub source_state_hash: Option<String>,
    #[serde(default)]
    pub source_metadata: BTreeMap<String, String>,
    pub project: Option<String>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub source_agent_version: Option<String>,
    /// Historical persisted manifests predate this required input field.
    /// They are interpreted as the original adapter contract version.
    #[serde(default = "default_artifact_set_version")]
    pub artifact_set_version: u16,
    pub munshi_version: Option<String>,
}

/// The original source-adapter artifact contract version, implied for every
/// manifest recorded before `artifact_set_version` became a required input
/// field. Legacy upgrade and terminal-audit compatibility logic key off this
/// constant rather than a bare literal.
pub(crate) const LEGACY_ARTIFACT_SET_VERSION: u16 = 1;

const fn default_artifact_set_version() -> u16 {
    LEGACY_ARTIFACT_SET_VERSION
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
    Abandoned,
    Expired,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub snapshot_id: String,
    pub session_id: String,
    pub snapshot_fingerprint: String,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub completed_at: String,
    pub artifact_count: u32,
    pub total_original_bytes: u64,
    pub total_stored_bytes: u64,
    pub capture_count: u64,
    pub captures_url: String,
    pub manifest_url: String,
    pub manifest: Manifest,
    pub artifacts: Vec<ArtifactResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactResponse {
    pub artifact_id: String,
    pub artifact_index: u32,
    pub logical_path: String,
    pub media_type: Option<String>,
    pub original_size_bytes: u64,
    pub original_sha256: String,
    pub stored_size_bytes: u64,
    pub stored_sha256: String,
    pub compression: Compression,
    pub metadata_url: String,
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

/// Per-upload transfer facts. These intentionally are not receipt fields:
/// distinct captures can resolve to one snapshot while transferring different
/// stored representations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionTransfer {
    pub upload_id: String,
    pub capture_id: String,
    pub upload_transfer_bytes: u64,
    pub newly_persisted_physical_bytes: u64,
}

/// Durable provenance for one successfully archived client observation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureProvenance {
    pub capture_record_id: String,
    pub capture_id: String,
    pub client_id: String,
    pub session_id: String,
    pub upload_id: String,
    pub snapshot_id: String,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub source_captured_at: String,
    pub source_cursor: Option<String>,
    pub source_state_hash: Option<String>,
    pub source_metadata: BTreeMap<String, String>,
    pub project: Option<String>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub source_agent_version: Option<String>,
    pub artifact_set_version: u16,
    pub munshi_version: Option<String>,
    pub server_received_at: String,
    pub server_completed_at: String,
    pub capture_url: String,
    pub manifest_url: String,
}

/// Completion keeps immutable snapshot evidence separate from mutable
/// transfer facts and the successful capture's provenance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub receipt: Receipt,
    pub transfer: CompletionTransfer,
    pub capture: CaptureProvenance,
}

/// Focused paginated provenance relation for one snapshot. The field name is
/// retained for compatibility with the original focused relation response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotCapturesResponse {
    pub snapshot_id: String,
    pub captures: Vec<CaptureProvenance>,
    pub next_cursor: Option<String>,
    pub high_watermark: Option<PageHighWatermark>,
}

/// An immutable boundary from the fixed descending order used by a paginated
/// archive collection. It is informational; clients must use `next_cursor`
/// rather than construct a cursor from these fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageHighWatermark {
    pub timestamp: String,
    pub id: String,
}

/// A page from an archive collection. Collections are ordered descending by
/// their documented server timestamp and then ID. A cursor carries the first
/// page's high-watermark, so records completed after that boundary are not
/// interleaved into an in-progress traversal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub high_watermark: Option<PageHighWatermark>,
}

/// Immutable context from the latest completed snapshot projected onto a
/// session for archive browsing. It does not replace historical capture or
/// snapshot context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLatestSnapshot {
    pub snapshot_id: String,
    pub completed_at: String,
    pub project: Option<String>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub source_agent_version: Option<String>,
    pub artifact_set_version: u16,
    pub snapshot_url: String,
    pub manifest_url: String,
}

/// A logical source session with context from its latest completed snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_id: String,
    pub source_agent: String,
    pub source_session_id: String,
    pub created_at: String,
    pub updated_at: String,
    pub latest_snapshot: SessionLatestSnapshot,
    pub captures_url: String,
    pub snapshots_url: String,
}

/// Lightweight immutable evidence for one completed snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub snapshot_id: String,
    pub session_id: String,
    pub snapshot_fingerprint: String,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub completed_at: String,
    pub artifact_count: u32,
    pub total_original_bytes: u64,
    pub total_stored_bytes: u64,
    pub capture_count: u64,
    pub snapshot_url: String,
    pub captures_url: String,
    pub manifest_url: String,
}

/// An immutable canonical manifest retained for one completed capture.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalManifestResponse {
    pub manifest_id: String,
    pub snapshot_id: String,
    pub session_id: String,
    pub capture_record_id: String,
    pub sha256: String,
    pub created_at: String,
    pub completed_at: String,
    pub snapshot_url: String,
    pub capture_url: String,
    pub manifest_url: String,
    pub manifest: Manifest,
}

/// Metadata for a canonical manifest without duplicating its document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalManifestSummary {
    pub manifest_id: String,
    pub snapshot_id: String,
    pub session_id: String,
    pub capture_record_id: String,
    pub sha256: String,
    pub created_at: String,
    pub completed_at: String,
    pub snapshot_url: String,
    pub capture_url: String,
    pub manifest_url: String,
}

/// Inspectable immutable metadata for a stored artifact. Content remains a
/// separate streaming resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMetadataResponse {
    pub artifact_id: String,
    pub snapshot_id: String,
    pub artifact_index: u32,
    pub logical_path: String,
    pub media_type: Option<String>,
    pub original_size_bytes: u64,
    pub original_sha256: String,
    pub stored_size_bytes: u64,
    pub stored_sha256: String,
    pub compression: Compression,
    pub created_at: String,
    pub metadata_url: String,
    pub content_url: String,
}

/// Optional JSON document for an administrative snapshot deletion request.
///
/// The confirmation may instead be supplied in the
/// `X-Patwari-Delete-Confirmation` header. At least one location is
/// required, and any supplied values must agree exactly.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSnapshotRequest {
    #[serde(default)]
    pub confirmation: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Minimal durable history for a deliberately deleted snapshot. This response
/// intentionally does not include artifact paths, artifact metadata, or
/// manifest content. The linked capture count describes retained provenance
/// without making deleted capture documents normally retrievable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TombstoneResponse {
    pub tombstone_id: String,
    pub deletion_audit_id: String,
    pub owner_namespace: String,
    pub session_id: String,
    pub snapshot_id: String,
    pub snapshot_fingerprint: String,
    pub manifest_sha256: String,
    pub snapshot_completed_at: String,
    pub deleted_at: String,
    pub deleted_at_sort_key: i64,
    pub reason: Option<String>,
    pub capture_count: u64,
    /// Set when an identical later capture created a distinct live snapshot.
    pub rearchived_snapshot_id: Option<String>,
    pub rearchived_snapshot_url: Option<String>,
    /// Receipts for tombstoned snapshots are available only through this
    /// administrative representation.
    pub historical_receipt: Receipt,
}

/// Bounded result from one explicit administrative blob-GC pass.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobGcResponse {
    pub inspected_blobs: u32,
    pub deleted_blobs: u32,
}

/// The final outcome of one immutable integrity observation run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityRunStatus {
    Running,
    Healthy,
    ActionRequired,
    Failed,
}

impl IntegrityRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Healthy => "healthy",
            Self::ActionRequired => "action_required",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "healthy" => Some(Self::Healthy),
            "action_required" => Some(Self::ActionRequired),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A stable severity used to determine whether an operator must act.
///
/// `info` documents intentional archive states. `warning` and `error` are
/// actionable and therefore make an integrity run `action_required`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegritySeverity {
    Info,
    Warning,
    Error,
}

impl IntegritySeverity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "info" => Some(Self::Info),
            "warning" => Some(Self::Warning),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// Stable categories emitted by the archive integrity scanner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityFindingKind {
    ManifestMissing,
    ManifestUnparseable,
    ManifestHashMismatch,
    SnapshotProjectionDrift,
    DatabaseIntegrityFailure,
    ArtifactBlobReferenceMissing,
    BlobMetadataInvalid,
    BlobFileMissing,
    BlobFileNonRegular,
    BlobFileSizeMismatch,
    BlobFileHashMismatch,
    ArtifactOriginalMismatch,
    UnexpectedBlobFile,
    BlobOrphan,
    BlobGraceCandidate,
    BlobGcEligibleCandidate,
    BlobStaleCandidate,
    TombstonedSnapshot,
    TransientChange,
}

impl IntegrityFindingKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestMissing => "manifest_missing",
            Self::ManifestUnparseable => "manifest_unparseable",
            Self::ManifestHashMismatch => "manifest_hash_mismatch",
            Self::SnapshotProjectionDrift => "snapshot_projection_drift",
            Self::DatabaseIntegrityFailure => "database_integrity_failure",
            Self::ArtifactBlobReferenceMissing => "artifact_blob_reference_missing",
            Self::BlobMetadataInvalid => "blob_metadata_invalid",
            Self::BlobFileMissing => "blob_file_missing",
            Self::BlobFileNonRegular => "blob_file_non_regular",
            Self::BlobFileSizeMismatch => "blob_file_size_mismatch",
            Self::BlobFileHashMismatch => "blob_file_hash_mismatch",
            Self::ArtifactOriginalMismatch => "artifact_original_mismatch",
            Self::UnexpectedBlobFile => "unexpected_blob_file",
            Self::BlobOrphan => "blob_orphan",
            Self::BlobGraceCandidate => "blob_grace_candidate",
            Self::BlobGcEligibleCandidate => "blob_gc_eligible_candidate",
            Self::BlobStaleCandidate => "blob_stale_candidate",
            Self::TombstonedSnapshot => "tombstoned_snapshot",
            Self::TransientChange => "transient_change",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "manifest_missing" => Some(Self::ManifestMissing),
            "manifest_unparseable" => Some(Self::ManifestUnparseable),
            "manifest_hash_mismatch" => Some(Self::ManifestHashMismatch),
            "snapshot_projection_drift" => Some(Self::SnapshotProjectionDrift),
            "database_integrity_failure" => Some(Self::DatabaseIntegrityFailure),
            "artifact_blob_reference_missing" => Some(Self::ArtifactBlobReferenceMissing),
            "blob_metadata_invalid" => Some(Self::BlobMetadataInvalid),
            "blob_file_missing" => Some(Self::BlobFileMissing),
            "blob_file_non_regular" => Some(Self::BlobFileNonRegular),
            "blob_file_size_mismatch" => Some(Self::BlobFileSizeMismatch),
            "blob_file_hash_mismatch" => Some(Self::BlobFileHashMismatch),
            "artifact_original_mismatch" => Some(Self::ArtifactOriginalMismatch),
            "unexpected_blob_file" => Some(Self::UnexpectedBlobFile),
            "blob_orphan" => Some(Self::BlobOrphan),
            "blob_grace_candidate" => Some(Self::BlobGraceCandidate),
            "blob_gc_eligible_candidate" => Some(Self::BlobGcEligibleCandidate),
            "blob_stale_candidate" => Some(Self::BlobStaleCandidate),
            "tombstoned_snapshot" => Some(Self::TombstonedSnapshot),
            "transient_change" => Some(Self::TransientChange),
            _ => None,
        }
    }
}

/// Aggregated counts for a run. `by_kind` is keyed by stable snake-case
/// finding names, making it convenient for command-line automation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityFindingCounts {
    pub total: u64,
    pub info: u64,
    pub warning: u64,
    pub error: u64,
    pub by_kind: BTreeMap<String, u64>,
}

/// Redacted, machine-safe evidence for one detected condition.
///
/// The scanner never persists paths, manifest bodies, artifact bytes, or raw
/// capture metadata in a finding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityFindingSummary {
    pub finding_id: String,
    pub run_id: String,
    pub kind: IntegrityFindingKind,
    pub severity: IntegritySeverity,
    pub snapshot_id: Option<String>,
    pub artifact_id: Option<String>,
    pub blob_id: Option<String>,
    pub detected_at: String,
    pub detail_code: String,
}

/// Immutable summary of one integrity scan run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityRunSummary {
    pub run_id: String,
    pub owner_namespace: String,
    pub status: IntegrityRunStatus,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub counts: IntegrityFindingCounts,
}

/// The machine-readable result returned by a newly completed scan.
///
/// Findings are intentionally capped so a very damaged archive cannot make a
/// maintenance command load all historical evidence into memory. Counts
/// always cover the full run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub run_id: String,
    pub owner_namespace: String,
    pub status: IntegrityRunStatus,
    pub started_at: String,
    pub completed_at: String,
    pub counts: IntegrityFindingCounts,
    pub findings: Vec<IntegrityFindingSummary>,
    pub findings_truncated: bool,
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
