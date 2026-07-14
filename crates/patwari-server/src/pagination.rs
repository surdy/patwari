//! Shared opaque keyset-pagination primitives.
//!
//! Every paginated collection - both the public read-only archive browsing
//! endpoints and the trusted-boundary administrative tombstone history -
//! must apply identical high-watermark/after semantics: the first page
//! establishes an immutable high watermark, later cursors carry and enforce
//! it so newer rows never interleave mid-traversal, and tampered or
//! mismatched cursors are rejected consistently. This module owns only that
//! generic cursor logic; callers supply their own SQL column names, cursor
//! "kind", and filter hash, so no collection-specific (including
//! administrative) data is exposed here.

use std::fmt::Write;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    contract::{PageHighWatermark, PaginatedResponse},
    database,
    error::ApiError,
};

pub(crate) const DEFAULT_PAGE_LIMIT: usize = 50;
pub(crate) const MAX_PAGE_LIMIT: usize = 100;
const MAX_CURSOR_BYTES: usize = 2048;
const CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SortBoundary {
    /// Signed 64-bit microseconds since the Unix epoch (UTC); see
    /// `database::sort_key_from_rfc3339`. This is the only field ever
    /// compared in SQL. RFC 3339 timestamps are not chronologically
    /// lexicographic TEXT (`.12Z` sorts after `.123Z` even though it is
    /// earlier), so ordering, high-watermarks, and keyset bounds all use
    /// this numeric key instead.
    pub(crate) sort_key: i64,
    /// The row's documented RFC 3339 timestamp, carried only for display in
    /// `PageHighWatermark`; never used for ordering or comparisons.
    pub(crate) timestamp: String,
    pub(crate) id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpaqueCursor {
    version: u8,
    kind: String,
    filter_hash: String,
    high_watermark: SortBoundary,
    after: SortBoundary,
}

#[derive(Debug)]
pub(crate) struct PageRequest {
    pub(crate) limit: usize,
    cursor: Option<OpaqueCursor>,
}

pub(crate) fn parse_page(
    limit: Option<usize>,
    cursor: Option<String>,
    kind: &'static str,
    filter_hash: &str,
) -> Result<PageRequest, ApiError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return Err(ApiError::invalid("page limit must be between 1 and 100"));
    }
    let cursor = match cursor {
        None => None,
        Some(cursor) => {
            if cursor.len() > MAX_CURSOR_BYTES {
                return Err(ApiError::invalid("cursor is invalid"));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ApiError::invalid("cursor is invalid"))?;
            let cursor: OpaqueCursor = serde_json::from_slice(&bytes)
                .map_err(|_| ApiError::invalid("cursor is invalid"))?;
            if cursor.version != CURSOR_VERSION
                || cursor.kind != kind
                || cursor.filter_hash != filter_hash
            {
                return Err(ApiError::invalid(
                    "cursor does not match this collection and filters",
                ));
            }
            validate_boundary(&cursor.high_watermark)?;
            validate_boundary(&cursor.after)?;
            if !is_at_or_before(&cursor.after, &cursor.high_watermark) {
                return Err(ApiError::invalid("cursor is invalid"));
            }
            Some(cursor)
        }
    };
    Ok(PageRequest { limit, cursor })
}

fn validate_boundary(boundary: &SortBoundary) -> Result<(), ApiError> {
    if normalize_timestamp(&boundary.timestamp, "cursor is invalid")? != boundary.timestamp {
        return Err(ApiError::invalid("cursor is invalid"));
    }
    let expected_sort_key = database::sort_key_from_rfc3339(&boundary.timestamp)
        .map_err(|_| ApiError::invalid("cursor is invalid"))?;
    if boundary.sort_key != expected_sort_key {
        return Err(ApiError::invalid("cursor is invalid"));
    }
    let id = Uuid::parse_str(&boundary.id).map_err(|_| ApiError::invalid("cursor is invalid"))?;
    if id.to_string() != boundary.id || id.get_version_num() != 7 {
        return Err(ApiError::invalid("cursor is invalid"));
    }
    Ok(())
}

fn is_at_or_before(left: &SortBoundary, right: &SortBoundary) -> bool {
    left.sort_key < right.sort_key || (left.sort_key == right.sort_key && left.id <= right.id)
}

pub(crate) fn filter_hash<T: Serialize>(kind: &str, filters: &T) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(&(kind, filters)).map_err(|_| ApiError::internal())?;
    let digest = Sha256::digest(bytes);
    let mut hash = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hash, "{byte:02x}").map_err(|_| ApiError::internal())?;
    }
    Ok(hash)
}

fn encode_cursor(
    kind: &'static str,
    filter_hash: String,
    high_watermark: SortBoundary,
    after: SortBoundary,
) -> Result<String, ApiError> {
    let cursor = OpaqueCursor {
        version: CURSOR_VERSION,
        kind: kind.to_owned(),
        filter_hash,
        high_watermark,
        after,
    };
    let bytes = serde_json::to_vec(&cursor).map_err(|_| ApiError::internal())?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn page_from_rows<T>(
    mut rows: Vec<(T, SortBoundary)>,
    page: &PageRequest,
    kind: &'static str,
    filter_hash: String,
) -> Result<PaginatedResponse<T>, ApiError> {
    let high_watermark = page
        .cursor
        .as_ref()
        .map(|cursor| cursor.high_watermark.clone())
        .or_else(|| rows.first().map(|(_, boundary)| boundary.clone()));
    let has_next = rows.len() > page.limit;
    rows.truncate(page.limit);
    let next_cursor = if has_next {
        let after = rows
            .last()
            .map(|(_, boundary)| boundary.clone())
            .ok_or_else(ApiError::internal)?;
        Some(encode_cursor(
            kind,
            filter_hash,
            high_watermark.clone().ok_or_else(ApiError::internal)?,
            after,
        )?)
    } else {
        None
    };
    Ok(PaginatedResponse {
        items: rows.into_iter().map(|(item, _)| item).collect(),
        next_cursor,
        high_watermark: high_watermark.map(|boundary| PageHighWatermark {
            timestamp: boundary.timestamp,
            id: boundary.id,
        }),
    })
}

fn normalize_timestamp(value: &str, message: &'static str) -> Result<String, ApiError> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| ApiError::invalid(message))?
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|_| ApiError::internal())
}

pub(crate) fn append_descending_bounds(
    sql: &mut String,
    sort_key_column: &str,
    id_column: &str,
    page: &PageRequest,
) {
    if page.cursor.is_some() {
        write!(
            sql,
            " AND ({sort_key_column} < ? OR ({sort_key_column} = ? AND {id_column} <= ?))"
        )
        .expect("writing SQL to string succeeds");
        write!(
            sql,
            " AND ({sort_key_column} < ? OR ({sort_key_column} = ? AND {id_column} < ?))"
        )
        .expect("writing SQL to string succeeds");
    }
}

pub(crate) fn bind_descending_bounds<'q, O>(
    mut request: sqlx::query::QueryAs<'q, sqlx::Sqlite, O, sqlx::sqlite::SqliteArguments<'q>>,
    page: &PageRequest,
) -> sqlx::query::QueryAs<'q, sqlx::Sqlite, O, sqlx::sqlite::SqliteArguments<'q>>
where
    O: Send + Unpin,
{
    if let Some(cursor) = &page.cursor {
        request = request
            .bind(cursor.high_watermark.sort_key)
            .bind(cursor.high_watermark.sort_key)
            .bind(cursor.high_watermark.id.clone())
            .bind(cursor.after.sort_key)
            .bind(cursor.after.sort_key)
            .bind(cursor.after.id.clone());
    }
    request
}
