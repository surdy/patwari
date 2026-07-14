use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use thiserror::Error;

pub const DEFAULT_DATA_DIR: &str = "data";
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENCY: usize = 64;
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind_addr: SocketAddr,
    pub max_request_body_bytes: usize,
    pub max_concurrency: usize,
    pub request_timeout: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("PATWARI_DATA_DIR must not be empty")]
    EmptyDataDir,
    #[error("PATWARI_BIND_ADDR must be a socket address")]
    InvalidBindAddr,
    #[error("PATWARI_MAX_REQUEST_BODY_BYTES must be a positive integer")]
    InvalidRequestBodyLimit,
    #[error("PATWARI_MAX_CONCURRENCY must be a positive integer")]
    InvalidConcurrencyLimit,
    #[error("PATWARI_REQUEST_TIMEOUT must be a positive duration such as 30s")]
    InvalidRequestTimeout,
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
        }
    }
}

impl Config {
    /// Loads bounded service settings from `PATWARI_*` environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicitly configured value is invalid or unbounded.
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
                parse_positive(value).ok_or(ConfigError::InvalidRequestBodyLimit)?;
        }

        if let Some(value) = values.get("PATWARI_MAX_CONCURRENCY") {
            config.max_concurrency =
                parse_positive(value).ok_or(ConfigError::InvalidConcurrencyLimit)?;
        }

        if let Some(value) = values.get("PATWARI_REQUEST_TIMEOUT") {
            config.request_timeout =
                parse_duration(value).ok_or(ConfigError::InvalidRequestTimeout)?;
        }

        Ok(config)
    }
}

fn parse_positive(value: &str) -> Option<usize> {
    value.parse().ok().filter(|parsed: &usize| *parsed > 0)
}

fn parse_duration(value: &str) -> Option<Duration> {
    let value = value.strip_suffix('s')?;
    value
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds > 0)
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
    }

    #[test]
    fn environment_values_override_defaults() {
        let values = HashMap::from([
            ("PATWARI_DATA_DIR".into(), "archive-data".into()),
            ("PATWARI_BIND_ADDR".into(), "127.0.0.1:9000".into()),
            ("PATWARI_MAX_REQUEST_BODY_BYTES".into(), "4096".into()),
            ("PATWARI_MAX_CONCURRENCY".into(), "2".into()),
            ("PATWARI_REQUEST_TIMEOUT".into(), "5s".into()),
        ]);
        let config = Config::from_values(&values).expect("configuration should parse");

        assert_eq!(config.data_dir, PathBuf::from("archive-data"));
        assert_eq!(config.max_request_body_bytes, 4096);
        assert_eq!(config.max_concurrency, 2);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
    }

    #[test]
    fn rejects_unbounded_or_invalid_values() {
        let values = HashMap::from([("PATWARI_MAX_CONCURRENCY".into(), "0".into())]);
        let error = Config::from_values(&values).expect_err("zero concurrency is not a safe limit");

        assert_eq!(error, ConfigError::InvalidConcurrencyLimit);
    }
}
