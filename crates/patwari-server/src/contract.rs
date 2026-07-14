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
    pub manifest_sha256: String,
    pub completed_at: String,
    pub artifact_count: u32,
    pub total_original_bytes: u64,
    pub total_stored_bytes: u64,
    pub capture_count: u64,
    pub captures_url: String,
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
}

/// Completion keeps immutable snapshot evidence separate from mutable
/// transfer facts and the successful capture's provenance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub receipt: Receipt,
    pub transfer: CompletionTransfer,
    pub capture: CaptureProvenance,
}

/// Focused provenance relation for one snapshot. This is intentionally not a
/// general capture listing or pagination surface.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotCapturesResponse {
    pub snapshot_id: String,
    pub captures: Vec<CaptureProvenance>,
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
