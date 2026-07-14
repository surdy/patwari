use axum::http::{HeaderMap, header};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    contract::{Artifact, Capture, CaptureInput, Manifest, ManifestInput, RegisterClientRequest},
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

pub(crate) fn validate_idempotency_key(key: &str) -> Result<(), ApiError> {
    validate_nonempty_text(key, MAX_IDEMPOTENCY_KEY_BYTES, "idempotency key is invalid")
}

pub(crate) fn normalize_manifest(
    input: ManifestInput,
    max_stored_bytes: u64,
    max_original_bytes: u64,
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
    validate_logical_path(&input.artifact.logical_path)?;
    validate_optional_media_type(input.artifact.media_type.as_ref())?;
    validate_digest(&input.artifact.original_sha256)?;
    validate_digest(&input.artifact.stored_sha256)?;
    validate_size(
        input.artifact.original_size_bytes,
        max_original_bytes,
        "artifact original size exceeds the configured bounded limit",
    )?;
    validate_size(
        input.artifact.stored_size_bytes,
        max_stored_bytes,
        "artifact stored size exceeds the configured bounded limit",
    )?;

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

pub(crate) fn validate_digest(value: &str) -> Result<(), ApiError> {
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
