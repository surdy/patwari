use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use thiserror::Error;

pub const DEFAULT_DATA_DIR: &str = "data";
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENCY: usize = 64;
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_ARTIFACT_STORED_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_ARTIFACT_ORIGINAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_ARTIFACT_COUNT: usize = 128;
pub const DEFAULT_MAX_SNAPSHOT_STORED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_SNAPSHOT_ORIGINAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const DEFAULT_UPLOAD_EXPIRY: Duration = Duration::from_hours(24);
pub const MAX_CHUNK_COUNT: u64 = 65_536;

const MIN_REQUEST_BODY_BYTES: usize = 1024;
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 256;
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
const MIN_CHUNK_SIZE_BYTES: usize = 1024;
const MAX_CHUNK_SIZE_BYTES: usize = 32 * 1024 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_ARTIFACT_COUNT: usize = 1_024;
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MIN_UPLOAD_EXPIRY: Duration = Duration::from_mins(1);
const MAX_UPLOAD_EXPIRY: Duration = Duration::from_hours(720);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind_addr: SocketAddr,
    pub max_request_body_bytes: usize,
    pub max_concurrency: usize,
    pub request_timeout: Duration,
    pub chunk_size_bytes: usize,
    pub max_artifact_stored_bytes: u64,
    pub max_artifact_original_bytes: u64,
    pub max_artifact_count: usize,
    pub max_snapshot_stored_bytes: u64,
    pub max_snapshot_original_bytes: u64,
    pub upload_expiry: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("PATWARI_DATA_DIR must not be empty")]
    EmptyDataDir,
    #[error("PATWARI_BIND_ADDR must be a socket address")]
    InvalidBindAddr,
    #[error("PATWARI_MAX_REQUEST_BODY_BYTES must be between 1024 and 67108864 bytes")]
    InvalidRequestBodyLimit,
    #[error("PATWARI_MAX_CONCURRENCY must be between 1 and 256")]
    InvalidConcurrencyLimit,
    #[error("PATWARI_REQUEST_TIMEOUT must be a duration between 1s and 5m")]
    InvalidRequestTimeout,
    #[error("PATWARI_UPLOAD_CHUNK_SIZE_BYTES must be between 1024 and 33554432 bytes")]
    InvalidChunkSize,
    #[error("PATWARI_UPLOAD_CHUNK_SIZE_BYTES must not exceed PATWARI_MAX_REQUEST_BODY_BYTES")]
    ChunkExceedsRequestBody,
    #[error("PATWARI_MAX_ARTIFACT_STORED_BYTES must be between 1 and 8589934592 bytes")]
    InvalidStoredArtifactLimit,
    #[error("PATWARI_MAX_ARTIFACT_ORIGINAL_BYTES must be between 1 and 8589934592 bytes")]
    InvalidOriginalArtifactLimit,
    #[error("PATWARI_MAX_ARTIFACT_COUNT must be between 1 and 1024")]
    InvalidArtifactCountLimit,
    #[error("PATWARI_MAX_SNAPSHOT_STORED_BYTES must be between 1 and 68719476736 bytes")]
    InvalidStoredSnapshotLimit,
    #[error("PATWARI_MAX_SNAPSHOT_ORIGINAL_BYTES must be between 1 and 68719476736 bytes")]
    InvalidOriginalSnapshotLimit,
    #[error("configured stored artifact limit would require too many chunks")]
    TooManyChunks,
    #[error("PATWARI_UPLOAD_EXPIRY must be a duration between 60s and 30d")]
    InvalidUploadExpiry,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            bind_addr: DEFAULT_BIND_ADDR
                .parse()
                .expect("default bind address is valid"),
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            max_artifact_stored_bytes: DEFAULT_MAX_ARTIFACT_STORED_BYTES,
            max_artifact_original_bytes: DEFAULT_MAX_ARTIFACT_ORIGINAL_BYTES,
            max_artifact_count: DEFAULT_MAX_ARTIFACT_COUNT,
            max_snapshot_stored_bytes: DEFAULT_MAX_SNAPSHOT_STORED_BYTES,
            max_snapshot_original_bytes: DEFAULT_MAX_SNAPSHOT_ORIGINAL_BYTES,
            upload_expiry: DEFAULT_UPLOAD_EXPIRY,
        }
    }
}

impl Config {
    /// Loads bounded service settings from `PATWARI_*` environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicitly configured value is invalid, unsafe,
    /// or incompatible with another configured transfer bound.
    pub fn from_env() -> Result<Self, ConfigError> {
        let values = std::env::vars()
            .filter(|(key, _)| key.starts_with("PATWARI_"))
            .collect();
        Self::from_values(&values)
    }

    fn from_values(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        if let Some(value) = values.get("PATWARI_DATA_DIR") {
            if value.trim().is_empty() {
                return Err(ConfigError::EmptyDataDir);
            }
            config.data_dir = PathBuf::from(value);
        }

        if let Some(value) = values.get("PATWARI_BIND_ADDR") {
            config.bind_addr = value.parse().map_err(|_| ConfigError::InvalidBindAddr)?;
        }

        if let Some(value) = values.get("PATWARI_MAX_REQUEST_BODY_BYTES") {
            config.max_request_body_bytes =
                parse_bounded_usize(value, MIN_REQUEST_BODY_BYTES, MAX_REQUEST_BODY_BYTES)
                    .ok_or(ConfigError::InvalidRequestBodyLimit)?;
        }

        let chunk_was_explicit = values.contains_key("PATWARI_UPLOAD_CHUNK_SIZE_BYTES");
        if let Some(value) = values.get("PATWARI_UPLOAD_CHUNK_SIZE_BYTES") {
            config.chunk_size_bytes =
                parse_bounded_usize(value, MIN_CHUNK_SIZE_BYTES, MAX_CHUNK_SIZE_BYTES)
                    .ok_or(ConfigError::InvalidChunkSize)?;
        } else {
            config.chunk_size_bytes = config.chunk_size_bytes.min(config.max_request_body_bytes);
        }
        if config.chunk_size_bytes > config.max_request_body_bytes {
            return Err(ConfigError::ChunkExceedsRequestBody);
        }
        if !chunk_was_explicit && config.chunk_size_bytes < MIN_CHUNK_SIZE_BYTES {
            return Err(ConfigError::InvalidChunkSize);
        }

        if let Some(value) = values.get("PATWARI_MAX_CONCURRENCY") {
            config.max_concurrency = parse_bounded_usize(value, 1, MAX_CONCURRENCY)
                .ok_or(ConfigError::InvalidConcurrencyLimit)?;
        }

        if let Some(value) = values.get("PATWARI_REQUEST_TIMEOUT") {
            config.request_timeout = parse_duration(value)
                .filter(|duration| *duration <= MAX_REQUEST_TIMEOUT)
                .ok_or(ConfigError::InvalidRequestTimeout)?;
        }

        let stored_limit_was_explicit = values.contains_key("PATWARI_MAX_ARTIFACT_STORED_BYTES");
        if let Some(value) = values.get("PATWARI_MAX_ARTIFACT_STORED_BYTES") {
            config.max_artifact_stored_bytes = parse_bounded_u64(value, 1, MAX_ARTIFACT_BYTES)
                .ok_or(ConfigError::InvalidStoredArtifactLimit)?;
        } else {
            config.max_artifact_stored_bytes = config
                .max_artifact_stored_bytes
                .min(max_stored_bytes_for_chunk_size(config.chunk_size_bytes)?);
        }
        if config.max_artifact_stored_bytes
            > max_stored_bytes_for_chunk_size(config.chunk_size_bytes)?
        {
            return Err(if stored_limit_was_explicit {
                ConfigError::TooManyChunks
            } else {
                ConfigError::InvalidStoredArtifactLimit
            });
        }

        if let Some(value) = values.get("PATWARI_MAX_ARTIFACT_ORIGINAL_BYTES") {
            config.max_artifact_original_bytes = parse_bounded_u64(value, 1, MAX_ARTIFACT_BYTES)
                .ok_or(ConfigError::InvalidOriginalArtifactLimit)?;
        }

        if let Some(value) = values.get("PATWARI_MAX_ARTIFACT_COUNT") {
            config.max_artifact_count = parse_bounded_usize(value, 1, MAX_ARTIFACT_COUNT)
                .ok_or(ConfigError::InvalidArtifactCountLimit)?;
        }

        if let Some(value) = values.get("PATWARI_MAX_SNAPSHOT_STORED_BYTES") {
            config.max_snapshot_stored_bytes = parse_bounded_u64(value, 1, MAX_SNAPSHOT_BYTES)
                .ok_or(ConfigError::InvalidStoredSnapshotLimit)?;
        }

        if let Some(value) = values.get("PATWARI_MAX_SNAPSHOT_ORIGINAL_BYTES") {
            config.max_snapshot_original_bytes = parse_bounded_u64(value, 1, MAX_SNAPSHOT_BYTES)
                .ok_or(ConfigError::InvalidOriginalSnapshotLimit)?;
        }

        if let Some(value) = values.get("PATWARI_UPLOAD_EXPIRY") {
            config.upload_expiry = parse_duration(value)
                .filter(|duration| *duration >= MIN_UPLOAD_EXPIRY && *duration <= MAX_UPLOAD_EXPIRY)
                .ok_or(ConfigError::InvalidUploadExpiry)?;
        }

        config.validate()?;
        Ok(config)
    }

    /// Validates a programmatically constructed configuration against the same
    /// bounds used for environment configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when a direct caller bypassed a required resource or
    /// transfer-layout bound.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::EmptyDataDir);
        }
        if !(MIN_REQUEST_BODY_BYTES..=MAX_REQUEST_BODY_BYTES).contains(&self.max_request_body_bytes)
        {
            return Err(ConfigError::InvalidRequestBodyLimit);
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(ConfigError::InvalidConcurrencyLimit);
        }
        if self.request_timeout.is_zero() || self.request_timeout > MAX_REQUEST_TIMEOUT {
            return Err(ConfigError::InvalidRequestTimeout);
        }
        if !(MIN_CHUNK_SIZE_BYTES..=MAX_CHUNK_SIZE_BYTES).contains(&self.chunk_size_bytes) {
            return Err(ConfigError::InvalidChunkSize);
        }
        if self.chunk_size_bytes > self.max_request_body_bytes {
            return Err(ConfigError::ChunkExceedsRequestBody);
        }
        if self.max_artifact_stored_bytes == 0
            || self.max_artifact_stored_bytes > MAX_ARTIFACT_BYTES
        {
            return Err(ConfigError::InvalidStoredArtifactLimit);
        }
        if self.max_artifact_original_bytes == 0
            || self.max_artifact_original_bytes > MAX_ARTIFACT_BYTES
        {
            return Err(ConfigError::InvalidOriginalArtifactLimit);
        }
        if !(1..=MAX_ARTIFACT_COUNT).contains(&self.max_artifact_count) {
            return Err(ConfigError::InvalidArtifactCountLimit);
        }
        if self.max_snapshot_stored_bytes == 0
            || self.max_snapshot_stored_bytes > MAX_SNAPSHOT_BYTES
        {
            return Err(ConfigError::InvalidStoredSnapshotLimit);
        }
        if self.max_snapshot_original_bytes == 0
            || self.max_snapshot_original_bytes > MAX_SNAPSHOT_BYTES
        {
            return Err(ConfigError::InvalidOriginalSnapshotLimit);
        }
        if self.max_artifact_stored_bytes > max_stored_bytes_for_chunk_size(self.chunk_size_bytes)?
        {
            return Err(ConfigError::TooManyChunks);
        }
        if self.upload_expiry < MIN_UPLOAD_EXPIRY || self.upload_expiry > MAX_UPLOAD_EXPIRY {
            return Err(ConfigError::InvalidUploadExpiry);
        }
        Ok(())
    }
}

fn max_stored_bytes_for_chunk_size(chunk_size_bytes: usize) -> Result<u64, ConfigError> {
    u64::try_from(chunk_size_bytes)
        .ok()
        .and_then(|size| size.checked_mul(MAX_CHUNK_COUNT))
        .ok_or(ConfigError::TooManyChunks)
}

fn parse_bounded_usize(value: &str, minimum: usize, maximum: usize) -> Option<usize> {
    value
        .parse()
        .ok()
        .filter(|parsed: &usize| *parsed >= minimum && *parsed <= maximum)
}

fn parse_bounded_u64(value: &str, minimum: u64, maximum: u64) -> Option<u64> {
    value
        .parse()
        .ok()
        .filter(|parsed: &u64| *parsed >= minimum && *parsed <= maximum)
}

fn parse_duration(value: &str) -> Option<Duration> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix('s') {
        (value, 1_u64)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 60 * 60)
    } else if let Some(value) = value.strip_suffix('d') {
        (value, 24 * 60 * 60)
    } else {
        return None;
    };
    number
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| seconds.checked_mul(multiplier))
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_loopback_only() {
        let config = Config::default();

        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(config.max_request_body_bytes, 32 * 1024 * 1024);
        assert_eq!(config.max_concurrency, 64);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.chunk_size_bytes, 4 * 1024 * 1024);
        assert_eq!(config.max_artifact_count, 128);
        assert_eq!(config.max_snapshot_stored_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(config.upload_expiry, Duration::from_hours(24));
    }

    #[test]
    fn environment_values_override_defaults() {
        let values = HashMap::from([
            ("PATWARI_DATA_DIR".into(), "archive-data".into()),
            ("PATWARI_BIND_ADDR".into(), "127.0.0.1:9000".into()),
            ("PATWARI_MAX_REQUEST_BODY_BYTES".into(), "4096".into()),
            ("PATWARI_UPLOAD_CHUNK_SIZE_BYTES".into(), "4096".into()),
            ("PATWARI_MAX_ARTIFACT_STORED_BYTES".into(), "4096".into()),
            ("PATWARI_MAX_ARTIFACT_ORIGINAL_BYTES".into(), "8192".into()),
            ("PATWARI_MAX_ARTIFACT_COUNT".into(), "2".into()),
            ("PATWARI_MAX_SNAPSHOT_STORED_BYTES".into(), "8192".into()),
            ("PATWARI_MAX_SNAPSHOT_ORIGINAL_BYTES".into(), "16384".into()),
            ("PATWARI_MAX_CONCURRENCY".into(), "2".into()),
            ("PATWARI_REQUEST_TIMEOUT".into(), "5s".into()),
            ("PATWARI_UPLOAD_EXPIRY".into(), "2h".into()),
        ]);
        let config = Config::from_values(&values).expect("configuration should parse");

        assert_eq!(config.data_dir, PathBuf::from("archive-data"));
        assert_eq!(config.max_request_body_bytes, 4096);
        assert_eq!(config.chunk_size_bytes, 4096);
        assert_eq!(config.max_artifact_stored_bytes, 4096);
        assert_eq!(config.max_artifact_original_bytes, 8192);
        assert_eq!(config.max_artifact_count, 2);
        assert_eq!(config.max_snapshot_stored_bytes, 8192);
        assert_eq!(config.max_snapshot_original_bytes, 16384);
        assert_eq!(config.max_concurrency, 2);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert_eq!(config.upload_expiry, Duration::from_hours(2));
    }

    #[test]
    fn rejects_unbounded_or_incompatible_values() {
        let values = HashMap::from([("PATWARI_MAX_CONCURRENCY".into(), "0".into())]);
        let error = Config::from_values(&values).expect_err("zero concurrency is not a safe limit");
        assert_eq!(error, ConfigError::InvalidConcurrencyLimit);

        let values = HashMap::from([
            ("PATWARI_UPLOAD_CHUNK_SIZE_BYTES".into(), "8192".into()),
            ("PATWARI_MAX_REQUEST_BODY_BYTES".into(), "4096".into()),
        ]);
        let error =
            Config::from_values(&values).expect_err("a chunk cannot exceed the request limit");
        assert_eq!(error, ConfigError::ChunkExceedsRequestBody);

        let values = HashMap::from([
            ("PATWARI_UPLOAD_CHUNK_SIZE_BYTES".into(), "1024".into()),
            (
                "PATWARI_MAX_ARTIFACT_STORED_BYTES".into(),
                (u64::from(1024_u32) * (MAX_CHUNK_COUNT + 1)).to_string(),
            ),
        ]);
        let error = Config::from_values(&values).expect_err("chunk bitmap must remain bounded");
        assert_eq!(error, ConfigError::TooManyChunks);
    }
}
