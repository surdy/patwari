use std::sync::Arc;

use axum::{
    Router,
    error_handling::HandleErrorLayer,
    routing::{delete, get, post, put},
};
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tower::{ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    limit::RequestBodyLimitLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::{
    config::{Config, ConfigError},
    database::{self},
    deletion, health, ingestion, integrity, retrieval,
    storage::StorageLayout,
};

pub use crate::database::{ArchiveIdentity, BootstrapError};
pub use crate::ingestion::ReconciliationError;
pub use crate::integrity::IntegrityScanError;

#[derive(Debug, Error)]
pub enum MaintenanceError {
    #[error("archive maintenance could not obtain a server timestamp")]
    Clock,
    #[error("archive maintenance could not complete")]
    Operation,
}

/// Number of fixed lock stripes used to serialize completion for a given
/// upload identifier. Every upload ID deterministically hashes onto one of
/// these stripes, so lock storage stays bounded no matter how many uploads
/// are created over the life of the process (unlike a map keyed by upload
/// ID, which would grow without bound).
pub(crate) const UPLOAD_LOCK_STRIPES: usize = 256;

/// Number of fixed lock stripes that serialize deletion against completion
/// coalescence for a session-scoped semantic snapshot identity. A completion
/// that finds a live fingerprint takes this lock before attaching capture
/// provenance, so deletion cannot tombstone that snapshot mid-attachment.
pub(crate) const SNAPSHOT_LOCK_STRIPES: usize = 256;

/// Number of fixed lock stripes used to serialize every operation that
/// promotes, verifies/reuses, creates or references, or conditionally
/// deletes a canonical blob file for a given `(owner_namespace,
/// stored_sha256)` digest. Bounded and stripe-based for the same reason as
/// `UPLOAD_LOCK_STRIPES`: lock storage must not grow with the number of
/// distinct blobs ever seen.
pub(crate) const BLOB_LOCK_STRIPES: usize = 256;

pub(crate) struct AppState {
    pub(crate) database: SqlitePool,
    pub(crate) storage: StorageLayout,
    pub(crate) identity: ArchiveIdentity,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) chunk_size_bytes: u64,
    pub(crate) max_artifact_stored_bytes: u64,
    pub(crate) max_artifact_original_bytes: u64,
    pub(crate) max_artifact_count: usize,
    pub(crate) max_snapshot_stored_bytes: u64,
    pub(crate) max_snapshot_original_bytes: u64,
    pub(crate) upload_expiry: std::time::Duration,
    pub(crate) admin_deletion_enabled: bool,
    pub(crate) blob_gc_grace: std::time::Duration,
    pub(crate) integrity_scan_concurrency: usize,
    pub(crate) integrity_scan_buffer_bytes: usize,
    /// Held by each response body, rather than only by its request handler,
    /// so the cap covers clients that read slowly or stop reading entirely.
    pub(crate) download_permits: Arc<Semaphore>,
    /// Tower's request timeout ends once a streaming response is constructed;
    /// the download stream finishes that same deadline after headers are sent.
    pub(crate) download_timeout: std::time::Duration,
    upload_locks: [Arc<AsyncMutex<()>>; UPLOAD_LOCK_STRIPES],
    snapshot_locks: [Arc<AsyncMutex<()>>; SNAPSHOT_LOCK_STRIPES],
    blob_locks: [Arc<AsyncMutex<()>>; BLOB_LOCK_STRIPES],
    #[cfg(test)]
    pub(crate) test_hooks: TestHooks,
}

/// Test-only synchronization points used to deterministically land one
/// task's execution inside a specific window of another task's execution,
/// so concurrency regression tests do not depend on real thread-scheduling
/// timing. Compiled only for `cargo test`; absent from release builds.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestHooks {
    /// Paused immediately before a completion opens its database
    /// transaction to insert a blob row and attempt to commit its snapshot.
    /// Lets a test force a specific winner/loser ordering between two
    /// completions racing on the same session + fingerprint, by pausing one
    /// side here while the other runs to completion.
    before_snapshot_commit: std::sync::Mutex<Option<Arc<Checkpoint>>>,
    /// Paused immediately after a losing snapshot-race transaction rolls
    /// back and before any conditional canonical-blob-file deletion runs:
    /// exactly the historical vulnerable window between releasing the
    /// `SQLite` lock and deleting a promoted file. A test can wait for
    /// arrival, run a third party's completion, then resume, to
    /// deterministically exercise that window instead of hoping for it.
    after_losing_rollback: std::sync::Mutex<Option<Arc<Checkpoint>>>,
    /// Signaled (never paused) immediately before a completion attempts to
    /// acquire the per-digest blob lock. Lets a test confirm a third party
    /// has reached that exact attempt, so a subsequent "did it block"
    /// assertion is not confounded by the third party simply not having
    /// been scheduled yet.
    before_blob_lock_attempt: std::sync::Mutex<Option<Arc<Checkpoint>>>,
    /// Paused before a completion acquires its first digest lock. Lets a test
    /// land GC between a tombstone's committed orphan candidate and a
    /// rearchive's file promotion.
    before_blob_lock_acquire: std::sync::Mutex<Option<Arc<Checkpoint>>>,
    /// Paused after a deletion transaction has removed Artifact
    /// relationships but before it commits the Tombstone and projection
    /// update. Lets a test force completion to observe the post-delete state.
    before_snapshot_deletion_commit: std::sync::Mutex<Option<Arc<Checkpoint>>>,
}

#[cfg(test)]
impl TestHooks {
    pub(crate) fn set_before_snapshot_commit(&self, checkpoint: Arc<Checkpoint>) {
        *self
            .before_snapshot_commit
            .lock()
            .expect("test hook mutex is not poisoned") = Some(checkpoint);
    }

    pub(crate) fn clear_before_snapshot_commit(&self) {
        *self
            .before_snapshot_commit
            .lock()
            .expect("test hook mutex is not poisoned") = None;
    }

    pub(crate) fn before_snapshot_commit(&self) -> Option<Arc<Checkpoint>> {
        self.before_snapshot_commit
            .lock()
            .expect("test hook mutex is not poisoned")
            .clone()
    }

    pub(crate) fn set_after_losing_rollback(&self, checkpoint: Arc<Checkpoint>) {
        *self
            .after_losing_rollback
            .lock()
            .expect("test hook mutex is not poisoned") = Some(checkpoint);
    }

    pub(crate) fn after_losing_rollback(&self) -> Option<Arc<Checkpoint>> {
        self.after_losing_rollback
            .lock()
            .expect("test hook mutex is not poisoned")
            .clone()
    }

    pub(crate) fn set_before_blob_lock_attempt(&self, checkpoint: Arc<Checkpoint>) {
        *self
            .before_blob_lock_attempt
            .lock()
            .expect("test hook mutex is not poisoned") = Some(checkpoint);
    }

    pub(crate) fn before_blob_lock_attempt(&self) -> Option<Arc<Checkpoint>> {
        self.before_blob_lock_attempt
            .lock()
            .expect("test hook mutex is not poisoned")
            .clone()
    }

    pub(crate) fn set_before_blob_lock_acquire(&self, checkpoint: Arc<Checkpoint>) {
        *self
            .before_blob_lock_acquire
            .lock()
            .expect("test hook mutex is not poisoned") = Some(checkpoint);
    }

    pub(crate) fn before_blob_lock_acquire(&self) -> Option<Arc<Checkpoint>> {
        self.before_blob_lock_acquire
            .lock()
            .expect("test hook mutex is not poisoned")
            .clone()
    }

    pub(crate) fn set_before_snapshot_deletion_commit(&self, checkpoint: Arc<Checkpoint>) {
        *self
            .before_snapshot_deletion_commit
            .lock()
            .expect("test hook mutex is not poisoned") = Some(checkpoint);
    }

    pub(crate) fn before_snapshot_deletion_commit(&self) -> Option<Arc<Checkpoint>> {
        self.before_snapshot_deletion_commit
            .lock()
            .expect("test hook mutex is not poisoned")
            .clone()
    }
}

/// A single-use two-party rendezvous: one task arrives at the checkpoint and
/// blocks until resumed; another task waits for that arrival and then
/// resumes it. Used to make interleavings deterministic in tests without
/// sleeps or timing-dependent retries.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Checkpoint {
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
impl Checkpoint {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Called by the task that reaches the checkpoint: signals arrival and
    /// waits to be resumed.
    pub(crate) async fn arrive_and_wait(&self) {
        self.arrived.notify_one();
        self.resume.notified().await;
    }

    /// Called by a task that reaches a checkpoint but does not itself need
    /// to be paused: signals arrival for any test awaiting
    /// `wait_for_arrival`, without blocking the caller.
    pub(crate) fn mark_reached(&self) {
        self.arrived.notify_one();
    }

    /// Called by the test: waits until the paused task has arrived.
    pub(crate) async fn wait_for_arrival(&self) {
        self.arrived.notified().await;
    }

    /// Called by the test: releases the paused task.
    pub(crate) fn resume(&self) {
        self.resume.notify_one();
    }
}

impl AppState {
    /// Returns the fixed stripe lock for `upload_id`. Two different upload
    /// IDs may share a stripe (and therefore briefly contend), but the
    /// number of distinct locks never exceeds `UPLOAD_LOCK_STRIPES`.
    pub(crate) fn upload_lock(&self, upload_id: &str) -> Arc<AsyncMutex<()>> {
        self.upload_locks[upload_lock_stripe(upload_id)].clone()
    }

    /// Returns the fixed lock stripe for a session-scoped semantic snapshot
    /// identity. This closes the completion/deletion race without retaining a
    /// lock per historical fingerprint.
    pub(crate) fn snapshot_lock(
        &self,
        session_id: &str,
        fingerprint_sha256: &str,
    ) -> Arc<AsyncMutex<()>> {
        self.snapshot_locks[snapshot_lock_stripe(session_id, fingerprint_sha256)].clone()
    }

    /// Returns each lock stripe needed for a set of blob digests exactly once,
    /// in ascending deterministic order. Acquiring a mutex once per digest
    /// could recursively acquire the same collision stripe, while a common
    /// stripe order prevents cycles between overlapping digest sets.
    pub(crate) fn blob_locks_for_digests(
        &self,
        owner_namespace: &str,
        stored_sha256s: &[String],
    ) -> Vec<Arc<AsyncMutex<()>>> {
        let mut stripes = stored_sha256s
            .iter()
            .map(|digest| blob_lock_stripe(owner_namespace, digest))
            .collect::<Vec<_>>();
        stripes.sort_unstable();
        stripes.dedup();
        stripes
            .into_iter()
            .map(|stripe| self.blob_locks[stripe].clone())
            .collect()
    }
}

/// Deterministically hashes `upload_id` onto a fixed stripe index using
/// FNV-1a, independent of process-specific hasher randomization.
fn upload_lock_stripe(upload_id: &str) -> usize {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in upload_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    usize::try_from(hash % UPLOAD_LOCK_STRIPES as u64).unwrap_or(0)
}

fn snapshot_lock_stripe(session_id: &str, fingerprint_sha256: &str) -> usize {
    stable_lock_stripe(
        session_id
            .as_bytes()
            .iter()
            .chain(std::iter::once(&0_u8))
            .chain(fingerprint_sha256.as_bytes()),
        SNAPSHOT_LOCK_STRIPES,
    )
}

/// Deterministically hashes `owner_namespace` and `stored_sha256` (the
/// caller must pass the bare hex digest, without a `sha256:` prefix, so that
/// prefixed and unprefixed callers agree on a stripe) onto a fixed stripe
/// index using FNV-1a, independent of process-specific hasher randomization.
///
/// Where a path needs more than one family, its ordering is upload -> semantic
/// snapshot -> blob. Deletion takes semantic snapshot -> blob, and GC takes
/// only blob locks; completion's new-snapshot path needs only upload -> blob.
/// No code path waits on an earlier family after acquiring a later one, so
/// completion, deletion, and GC cannot cycle. A `SQLite` transaction may be
/// opened, committed, rolled back, and followed by conditional cleanup while
/// holding blob locks; no transaction is ever left open while waiting to
/// acquire a blob lock.
fn blob_lock_stripe(owner_namespace: &str, stored_sha256: &str) -> usize {
    stable_lock_stripe(
        owner_namespace
            .as_bytes()
            .iter()
            .chain(std::iter::once(&0_u8))
            .chain(stored_sha256.as_bytes()),
        BLOB_LOCK_STRIPES,
    )
}

fn stable_lock_stripe<'a>(bytes: impl Iterator<Item = &'a u8>, stripe_count: usize) -> usize {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    usize::try_from(hash % stripe_count as u64).unwrap_or(0)
}

pub struct Service {
    pub(crate) state: Arc<AppState>,
}

impl Service {
    /// Creates persistent storage, applies schema migrations, recovers durable
    /// upload state, and loads archive identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the persistent volume, metadata schema, or upload
    /// recovery state cannot be used safely.
    pub async fn bootstrap(config: &Config) -> Result<(Self, ArchiveIdentity), BootstrapError> {
        Self::bootstrap_with_recovery(config, true).await
    }

    /// Creates an archive service for an offline integrity observation.
    ///
    /// Unlike normal server bootstrap, this intentionally does not run
    /// destructive upload/blob recovery. A maintenance scan must be able to
    /// report an unexpected canonical blob file rather than erase that
    /// evidence before the scan begins.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration, persistent storage, or metadata
    /// schema initialization cannot safely complete.
    pub async fn bootstrap_for_integrity(
        config: &Config,
    ) -> Result<(Self, ArchiveIdentity), BootstrapError> {
        Self::bootstrap_with_recovery(config, false).await
    }

    async fn bootstrap_with_recovery(
        config: &Config,
        recover_uploads: bool,
    ) -> Result<(Self, ArchiveIdentity), BootstrapError> {
        config.validate().map_err(BootstrapError::Configuration)?;
        let storage = StorageLayout::create(&config.data_dir).await?;
        let (database, identity) = database::connect(config).await?;
        let chunk_size_bytes = u64::try_from(config.chunk_size_bytes)
            .map_err(|_| BootstrapError::Configuration(ConfigError::InvalidChunkSize))?;
        let state = Arc::new(AppState {
            database,
            storage,
            identity: identity.clone(),
            max_request_body_bytes: config.max_request_body_bytes,
            chunk_size_bytes,
            max_artifact_stored_bytes: config.max_artifact_stored_bytes,
            max_artifact_original_bytes: config.max_artifact_original_bytes,
            max_artifact_count: config.max_artifact_count,
            max_snapshot_stored_bytes: config.max_snapshot_stored_bytes,
            max_snapshot_original_bytes: config.max_snapshot_original_bytes,
            upload_expiry: config.upload_expiry,
            admin_deletion_enabled: config.admin_deletion_enabled,
            blob_gc_grace: config.blob_gc_grace,
            integrity_scan_concurrency: config.integrity_scan_concurrency,
            integrity_scan_buffer_bytes: config.integrity_scan_buffer_bytes,
            download_permits: Arc::new(Semaphore::new(config.max_download_concurrency)),
            download_timeout: config.request_timeout,
            upload_locks: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            snapshot_locks: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            blob_locks: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            #[cfg(test)]
            test_hooks: TestHooks::default(),
        });

        if recover_uploads {
            ingestion::recover_uploads(&state)
                .await
                .map_err(|_| BootstrapError::Recovery)?;
            ingestion::expire_uploads_at(&state, time::OffsetDateTime::now_utc())
                .await
                .map_err(|_| BootstrapError::Recovery)?;
        }

        Ok((Self { state }, identity))
    }

    pub fn router(&self, config: &Config) -> Router {
        let api = Router::new()
            .route("/clients/{client_id}", put(ingestion::register_client))
            .route("/uploads", post(ingestion::create_upload))
            .route(
                "/uploads/{upload_id}",
                get(ingestion::get_upload_status),
            )
            .route(
                "/uploads/{upload_id}/artifacts/{artifact_index}/chunks/{chunk_index}",
                put(ingestion::put_artifact_chunk),
            )
            .route(
                "/uploads/{upload_id}/abandon",
                post(ingestion::abandon_upload),
            )
            .route(
                "/uploads/{upload_id}/complete",
                post(ingestion::complete_upload),
            )
            .route(
                "/uploads/{upload_id}/capture",
                get(retrieval::get_capture_by_upload),
            )
            .route("/sessions", get(retrieval::list_sessions))
            .route("/sessions/{session_id}", get(retrieval::get_session))
            .route(
                "/sessions/{session_id}/captures",
                get(retrieval::list_session_captures),
            )
            .route(
                "/sessions/{session_id}/snapshots",
                get(retrieval::list_session_snapshots),
            )
            .route("/captures", get(retrieval::captures))
            .route("/captures/{capture_record_id}", get(retrieval::get_capture))
            .route("/snapshots", get(retrieval::list_snapshots))
            .route(
                "/snapshots/{snapshot_id}",
                get(retrieval::get_snapshot),
            )
            .route(
                "/snapshots/{snapshot_id}/captures",
                get(retrieval::get_snapshot_captures),
            )
            .route(
                "/snapshots/{snapshot_id}/manifest",
                get(retrieval::get_snapshot_manifest),
            )
            .route("/manifests", get(retrieval::list_manifests))
            .route(
                "/manifests/{manifest_id}",
                get(retrieval::get_manifest),
            )
            .route("/artifacts", get(retrieval::list_artifacts))
            .route(
                "/artifacts/{artifact_id}",
                get(retrieval::get_artifact_metadata),
            )
            .route(
                "/artifacts/{artifact_id}/content",
                get(retrieval::download_artifact),
            )
            .route(
                "/admin/snapshots/{snapshot_id}",
                delete(deletion::delete_snapshot),
            )
            .route("/admin/tombstones", get(deletion::list_tombstones))
            .route(
                "/admin/tombstones/{snapshot_id}",
                get(deletion::get_tombstone),
            )
            .route("/admin/blob-gc", post(deletion::run_blob_gc))
            .fallback(health::api_not_found)
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(health::handle_timeout))
                    .layer(TimeoutLayer::new(config.request_timeout))
                    .layer(tower::limit::ConcurrencyLimitLayer::new(
                        config.max_concurrency,
                    ))
                    .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes))
                    .layer(
                        TraceLayer::new_for_http()
                            .make_span_with(|request: &axum::http::Request<_>| {
                                tracing::info_span!("http_request", method = %request.method())
                            })
                            .on_response(DefaultOnResponse::new().level(Level::INFO)),
                    ),
            );

        Router::new()
            .route("/healthz", get(health::liveness))
            .route("/readyz", get(health::readiness))
            .nest("/api/v1", api)
            .with_state(self.state.clone())
    }

    /// Expires active uploads whose server-assigned expiry time has passed.
    ///
    /// It is safe to call repeatedly; every terminal upload is redacted once and
    /// its upload-scoped temporary directory is removed idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when expiry maintenance cannot safely update metadata or
    /// remove temporary upload storage.
    pub async fn expire_uploads(&self) -> Result<usize, MaintenanceError> {
        ingestion::expire_uploads_at(&self.state, time::OffsetDateTime::now_utc()).await
    }

    /// Compares one snapshot's canonical manifest with its normalized
    /// Artifact/Blob projection without attempting broad integrity scanning.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot is absent, its immutable manifest
    /// cannot be read, or its normalized metadata has drifted from it.
    pub async fn reconcile_snapshot(&self, snapshot_id: &str) -> Result<(), ReconciliationError> {
        ingestion::reconcile_snapshot(&self.state.database, snapshot_id).await
    }

    /// Runs one trusted-operator blob-GC pass. It is safe to call while the
    /// server is serving requests: each candidate is rechecked against live
    /// relationship rows under its deterministic digest lock.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata or blob storage cannot be safely
    /// reconciled and removed.
    pub async fn collect_blob_garbage(
        &self,
    ) -> Result<crate::contract::BlobGcResponse, MaintenanceError> {
        deletion::collect_orphaned_blobs_at(&self.state, time::OffsetDateTime::now_utc()).await
    }

    /// Observes complete archive integrity and persists immutable findings.
    ///
    /// The scan never changes snapshot completion evidence, receipts, or
    /// blob-candidate state. It can run offline through the maintenance CLI
    /// or while this service is serving requests.
    ///
    /// # Errors
    ///
    /// Returns an error when scanner metadata, storage inventory, or durable
    /// finding persistence cannot safely complete.
    pub async fn verify_integrity(
        &self,
    ) -> Result<crate::contract::IntegrityReport, IntegrityScanError> {
        integrity::scan_archive(&self.state).await
    }

    /// Alias for callers that name the operation after its implementation
    /// rather than its maintenance command.
    ///
    /// # Errors
    ///
    /// Propagates any integrity scanner failure from [`Self::verify_integrity`].
    pub async fn scan_integrity(
        &self,
    ) -> Result<crate::contract::IntegrityReport, IntegrityScanError> {
        self.verify_integrity().await
    }

    /// Compatibility-friendly spelling for maintenance callers.
    ///
    /// # Errors
    ///
    /// Propagates any integrity scanner failure from [`Self::verify_integrity`].
    pub async fn verify_archive(
        &self,
    ) -> Result<crate::contract::IntegrityReport, IntegrityScanError> {
        self.verify_integrity().await
    }

    /// Returns current archive health from the latest completed integrity
    /// observation, without mutating historical snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error when integrity history cannot be read safely.
    pub async fn latest_integrity_health(
        &self,
    ) -> Result<Option<crate::contract::IntegrityRunSummary>, IntegrityScanError> {
        integrity::latest_health(&self.state).await
    }

    /// Returns bounded immutable integrity-run history, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error when integrity history cannot be read safely.
    pub async fn list_integrity_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::contract::IntegrityRunSummary>, IntegrityScanError> {
        integrity::list_runs(&self.state, limit).await
    }

    /// Returns bounded finding history for one immutable run, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error when integrity history cannot be read safely.
    pub async fn list_integrity_findings(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::contract::IntegrityFindingSummary>, IntegrityScanError> {
        integrity::list_findings(&self.state, run_id, limit).await
    }

    #[cfg(test)]
    pub(crate) async fn expire_uploads_at(
        &self,
        now: time::OffsetDateTime,
    ) -> Result<usize, MaintenanceError> {
        ingestion::expire_uploads_at(&self.state, now).await
    }
}

/// Binds and serves the archive HTTP endpoints.
///
/// # Errors
///
/// Returns an error when bootstrap or binding the configured listener fails.
pub async fn serve(config: Config) -> Result<(), BootstrapError> {
    let (service, identity) = Service::bootstrap(&config).await?;
    tracing::info!(
        archive_instance_id = %identity.archive_instance_id,
        owner_namespace = %identity.owner_namespace,
        "archive service initialized"
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(BootstrapError::Bind)?;
    tracing::info!("archive service listening");
    axum::serve(listener, service.router(&config))
        .await
        .map_err(BootstrapError::Serve)
}
