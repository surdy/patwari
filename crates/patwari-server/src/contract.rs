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
    pub idempotency_key: String,
    pub manifest: ManifestInput,
}

/// The initial upload negotiation response.
///
/// Version 1 has one artifact, but exposes it as an array so a later
/// multi-artifact manifest can retain this status shape.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadResponse {
    pub upload_id: String,
    pub session_id: String,
    pub status: UploadStatus,
    pub manifest_sha256: String,
    pub chunk_size_bytes: u64,
    pub artifacts: Vec<UploadArtifactStatus>,
    /// Compatibility shortcut for the first chunk of the sole v1 artifact.
    pub artifact_upload_url: String,
    pub status_url: String,
    pub abandon_url: String,
    pub completion_url: String,
}

/// The resumable-upload status document returned by `GET /uploads/{upload_id}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadStatusResponse {
    pub upload_id: String,
    pub session_id: String,
    pub status: UploadStatus,
    pub manifest_sha256: Option<String>,
    pub chunk_size_bytes: u64,
    pub artifacts: Vec<UploadArtifactStatus>,
    pub status_url: String,
    pub abandon_url: String,
    pub completion_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadArtifactStatus {
    pub artifact_index: u32,
    pub stored_size_bytes: u64,
    pub chunk_count: u64,
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
