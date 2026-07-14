//! Patwari's durable multi-artifact archive service.
//!
//! The public surface is intentionally small: configuration, versioned API
//! contracts, service bootstrap, maintenance, and serving. Storage and
//! ingestion implementation details remain crate-private.

pub mod config;
pub mod contract;

mod database;
mod error;
mod health;
mod ingestion;
mod retrieval;
mod service;
mod storage;
mod validation;

pub use service::{
    ArchiveIdentity, BootstrapError, MaintenanceError, ReconciliationError, Service, serve,
};

#[cfg(test)]
mod tests;
