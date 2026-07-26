use std::collections::{BTreeMap, HashSet};

use axum::http::{HeaderMap, header};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    contract::{
        Artifact, ArtifactInput, Capture, CaptureInput, Compression, CreateUploadRequest, Manifest,
        ManifestInput, RegisterClientRequest,
    },
    error::ApiError,
};

const MAX_LOGICAL_PATH_BYTES: usize = 1024;
const MAX_METADATA_ENTRIES: usize = 64;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 1024;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
const MAX_SOURCE_AGENT_BYTES: usize = 128;
const MAX_SOURCE_SESSION_ID_BYTES: usize = 512;
const MAX_CONTEXT_VALUE_BYTES: usize = 512;
const WINDOWS_RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

#[derive(Clone, Copy)]
pub(crate) struct ManifestLimits {
    pub(crate) artifact_count: usize,
    pub(crate) artifact_stored_bytes: u64,
    pub(crate) artifact_original_bytes: u64,
    pub(crate) snapshot_stored_bytes: u64,
    pub(crate) snapshot_original_bytes: u64,
}

pub(crate) fn validate_client_request(request: &RegisterClientRequest) -> Result<(), ApiError> {
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

pub(crate) fn capture_id(request: &CreateUploadRequest) -> Result<&str, ApiError> {
    let capture_id = match (&request.capture_id, &request.idempotency_key) {
        (Some(capture_id), None | Some(_)) => capture_id,
        (None, Some(idempotency_key)) => idempotency_key,
        (None, None) => {
            return Err(ApiError::invalid("capture identifier is required"));
        }
    };
    if let (Some(capture_id), Some(idempotency_key)) =
        (&request.capture_id, &request.idempotency_key)
        && capture_id != idempotency_key
    {
        return Err(ApiError::invalid(
            "capture_id and idempotency_key must be identical when both are supplied",
        ));
    }
    validate_capture_identifier(capture_id)?;
    Ok(capture_id)
}

pub(crate) fn validate_capture_identifier(capture_id: &str) -> Result<(), ApiError> {
    validate_nonempty_text(
        capture_id,
        MAX_IDEMPOTENCY_KEY_BYTES,
        "capture identifier is invalid",
    )
}

pub(crate) fn normalize_manifest(
    input: ManifestInput,
    limits: ManifestLimits,
) -> Result<Manifest, ApiError> {
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
    let artifacts = match (input.artifacts, input.artifact) {
        (Some(artifacts), None) => artifacts,
        (None, Some(artifact)) => vec![artifact],
        _ => {
            return Err(ApiError::invalid(
                "manifest must declare exactly one artifacts array or legacy artifact",
            ));
        }
    };
    if artifacts.is_empty() || artifacts.len() > limits.artifact_count {
        return Err(ApiError::invalid(
            "manifest artifact count exceeds the configured bounded limit",
        ));
    }

    let mut normalized = Vec::with_capacity(artifacts.len());
    let mut original_total = 0_u64;
    let mut stored_total = 0_u64;
    let mut portable_paths = HashSet::with_capacity(artifacts.len());
    let mut representations = BTreeMap::<String, (u64, Compression)>::new();
    for artifact in artifacts {
        normalize_artifact(
            artifact,
            limits,
            &mut original_total,
            &mut stored_total,
            &mut portable_paths,
            &mut representations,
            &mut normalized,
        )?;
    }
    if portable_paths.iter().any(|path| {
        let mut parent = path.as_str();
        while let Some((prefix, _)) = parent.rsplit_once('/') {
            if portable_paths.contains(prefix) {
                return true;
            }
            parent = prefix;
        }
        false
    }) {
        return Err(ApiError::invalid(
            "manifest contains conflicting regular-file logical paths",
        ));
    }
    normalized.sort_unstable_by(|left, right| left.logical_path.cmp(&right.logical_path));
    Ok(Manifest {
        schema_version: input.schema_version,
        session: input.session,
        capture,
        artifacts: normalized,
    })
}

#[allow(clippy::too_many_arguments)]
fn normalize_artifact(
    input: ArtifactInput,
    limits: ManifestLimits,
    original_total: &mut u64,
    stored_total: &mut u64,
    portable_paths: &mut HashSet<String>,
    representations: &mut BTreeMap<String, (u64, Compression)>,
    normalized: &mut Vec<Artifact>,
) -> Result<(), ApiError> {
    validate_logical_path(&input.logical_path)?;
    let portable_path = input.logical_path.to_ascii_lowercase();
    if !portable_paths.insert(portable_path) {
        return Err(ApiError::invalid(
            "manifest contains duplicate normalized logical paths",
        ));
    }
    validate_optional_media_type(input.media_type.as_ref())?;
    validate_digest(&input.original_sha256)?;
    validate_digest(&input.stored_sha256)?;
    validate_size(
        input.original_size_bytes,
        limits.artifact_original_bytes,
        "artifact original size exceeds the configured bounded limit",
    )?;
    validate_size(
        input.stored_size_bytes,
        limits.artifact_stored_bytes,
        "artifact stored size exceeds the configured bounded limit",
    )?;
    *original_total = original_total
        .checked_add(input.original_size_bytes)
        .ok_or_else(|| ApiError::invalid("snapshot original size is invalid"))?;
    if *original_total > limits.snapshot_original_bytes {
        return Err(ApiError::invalid(
            "snapshot original size exceeds the configured bounded limit",
        ));
    }
    *stored_total = stored_total
        .checked_add(input.stored_size_bytes)
        .ok_or_else(|| ApiError::invalid("snapshot stored size is invalid"))?;
    if *stored_total > limits.snapshot_stored_bytes {
        return Err(ApiError::invalid(
            "snapshot stored size exceeds the configured bounded limit",
        ));
    }
    let stored_digest = input
        .stored_sha256
        .strip_prefix("sha256:")
        .expect("digest validation requires sha256 prefix");
    if let Some((size, compression)) = representations.get(stored_digest) {
        if *size != input.stored_size_bytes || *compression != input.compression {
            return Err(ApiError::conflict(
                "blob_integrity_conflict",
                "stored digest conflicts with immutable blob size or compression metadata",
            ));
        }
    } else {
        representations.insert(
            stored_digest.to_owned(),
            (input.stored_size_bytes, input.compression),
        );
    }
    normalized.push(Artifact {
        logical_path: input.logical_path,
        media_type: input.media_type,
        original_size_bytes: input.original_size_bytes,
        original_sha256: input.original_sha256,
        stored_size_bytes: input.stored_size_bytes,
        stored_sha256: input.stored_sha256,
        compression: input.compression,
    });
    Ok(())
}

pub(crate) fn manifest_totals(manifest: &Manifest) -> Result<(u64, u64), ApiError> {
    manifest
        .artifacts
        .iter()
        .try_fold((0_u64, 0_u64), |(original, stored), artifact| {
            let original = original
                .checked_add(artifact.original_size_bytes)
                .ok_or_else(|| ApiError::invalid("snapshot original size is invalid"))?;
            let stored = stored
                .checked_add(artifact.stored_size_bytes)
                .ok_or_else(|| ApiError::invalid("snapshot stored size is invalid"))?;
            Ok((original, stored))
        })
}

fn normalize_capture(input: CaptureInput) -> Result<Capture, ApiError> {
    let captured_at = OffsetDateTime::parse(&input.captured_at, &Rfc3339)
        .map_err(|_| ApiError::invalid("capture timestamp must be RFC 3339"))?
        .format(&Rfc3339)
        .map_err(|_| ApiError::internal())?;
    for value in [
        &input.source_cursor,
        &input.source_state_hash,
        &input.project,
        &input.repository,
        &input.branch,
        &input.source_agent_version,
        &input.munshi_version,
    ] {
        validate_optional_text(value.as_ref(), MAX_CONTEXT_VALUE_BYTES)?;
    }
    if input.artifact_set_version == 0 {
        return Err(ApiError::invalid(
            "artifact set version must be a non-zero adapter contract version",
        ));
    }
    if input.source_metadata.len() > MAX_METADATA_ENTRIES {
        return Err(ApiError::invalid("source metadata has too many entries"));
    }
    for (key, value) in &input.source_metadata {
        validate_nonempty_text(
            key,
            MAX_METADATA_KEY_BYTES,
            "source metadata key is invalid",
        )?;
        validate_text(
            value,
            MAX_METADATA_VALUE_BYTES,
            "source metadata value is invalid",
        )?;
    }
    Ok(Capture {
        captured_at,
        source_cursor: input.source_cursor,
        source_state_hash: input.source_state_hash,
        source_metadata: input.source_metadata,
        project: input.project,
        repository: input.repository,
        branch: input.branch,
        source_agent_version: input.source_agent_version,
        artifact_set_version: input.artifact_set_version,
        munshi_version: input.munshi_version,
    })
}

fn validate_size(size: u64, maximum: u64, message: &'static str) -> Result<(), ApiError> {
    if size > maximum || i64::try_from(size).is_err() {
        return Err(ApiError::invalid(message));
    }
    Ok(())
}

fn validate_logical_path(path: &str) -> Result<(), ApiError> {
    if path.is_empty()
        || path.len() > MAX_LOGICAL_PATH_BYTES
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains(':')
        || !path.is_ascii()
        || path.bytes().any(|byte| byte.is_ascii_control())
        || path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.ends_with('.')
                || part.ends_with(' ')
                || is_windows_reserved_component(part)
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
    {
        return Err(ApiError::invalid(
            "logical path must be a portable normalized relative regular-file path",
        ));
    }

    Ok(())
}

fn is_windows_reserved_component(component: &str) -> bool {
    let basename = component.split('.').next().unwrap_or(component);
    WINDOWS_RESERVED_NAMES.contains(&basename.to_ascii_uppercase().as_str())
}

pub(crate) fn validate_digest(value: &str) -> Result<(), ApiError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ApiError::invalid("hash must be a lowercase sha256 digest"));
    };
    validate_content_hash(hex, "hash must be a lowercase sha256 digest")
}

pub(crate) fn validate_content_hash(value: &str, message: &'static str) -> Result<(), ApiError> {
    if value.len() != 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        })
    {
        return Err(ApiError::invalid(message));
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

pub(crate) fn validate_octet_stream(headers: &HeaderMap) -> Result<(), ApiError> {
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

pub(crate) fn parse_uuid(value: &str, message: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| ApiError::invalid(message))
}

pub(crate) fn to_sqlite_i64(value: u64) -> Result<i64, ApiError> {
    i64::try_from(value).map_err(|_| ApiError::invalid("artifact size exceeds configured bounds"))
}
