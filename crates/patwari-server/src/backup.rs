//! Self-contained archive backup, verification, and restore.
//!
//! A backup is an archive-only unit: an online `SQLite` backup plus every
//! authoritative canonical Blob row/file. Temporary uploads are deliberately
//! excluded, so creation refuses to run while any upload remains active.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use rusqlite::{Connection, OpenFlags, backup::Backup};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    ConnectOptions, FromRow, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    Service,
    config::Config,
    contract::{IntegrityReport, IntegrityRunStatus},
    maintenance::{self, MaintenanceGateError},
};

const BACKUP_FORMAT_VERSION: u16 = 1;
const MANIFEST_FILE_NAME: &str = "manifest.json";
const DATABASE_FILE_NAME: &str = "patwari.db";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("backup output must be a new named directory")]
    InvalidOutput,
    #[error("backup source archive is unavailable")]
    Source,
    #[error("backup metadata operation failed")]
    Metadata,
    #[error("an active upload must complete or be abandoned before backup")]
    ActiveUploads,
    #[error("backup is blocked by archive maintenance")]
    MaintenanceBusy,
    #[error("backup maintenance coordination is unavailable")]
    Maintenance,
    #[error("backup filesystem operation failed")]
    Storage,
    #[error("backup manifest is invalid")]
    Manifest,
    #[error("backup integrity verification found actionable conditions")]
    Integrity,
    #[error("restore destination must be absent or an empty directory")]
    DestinationNotEmpty,
    #[error(
        "restore destination's parent directory must be writable and on the same filesystem \
         as the destination; mount the persistent volume at a writable root and restore into \
         a subdirectory of it, not at the mount root itself"
    )]
    UnsafeDestination,
    #[error("restored archive could not bootstrap")]
    Bootstrap,
    #[error("restored archive integrity scan failed")]
    Scan,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackupCreateResult {
    pub backup_format_version: u16,
    pub archive_instance_id: String,
    pub owner_namespace: String,
    pub created_at: String,
    pub blob_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackupVerification {
    pub backup_format_version: u16,
    pub archive_instance_id: String,
    pub owner_namespace: String,
    pub blob_count: u64,
    pub integrity: IntegrityReport,
}

#[derive(Clone, Debug, Serialize)]
pub struct BackupRestoreResult {
    pub archive_instance_id: String,
    pub owner_namespace: String,
    pub blob_count: u64,
    pub integrity: IntegrityReport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupManifest {
    backup_format_version: u16,
    created_at: String,
    archive_instance_id: String,
    owner_namespace: String,
    archive_created_at: String,
    application_version: String,
    schema_version: i64,
    sqlite: BackupFile,
    blobs: Vec<BlobInventoryEntry>,
    latest_integrity_health: Option<LatestIntegrityHealth>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupFile {
    filename: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobInventoryEntry {
    filename: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LatestIntegrityHealth {
    run_id: String,
    status: String,
    started_at: String,
    completed_at: String,
    finding_count: u64,
    info_count: u64,
    warning_count: u64,
    error_count: u64,
}

#[derive(FromRow)]
struct ArchiveRow {
    owner_namespace: String,
    archive_instance_id: String,
    created_at: String,
}

#[derive(FromRow)]
struct BlobRow {
    stored_sha256: String,
    stored_size_bytes: i64,
}

#[derive(FromRow)]
struct IntegrityHealthRow {
    id: String,
    status: String,
    started_at: String,
    completed_at: String,
    finding_count: i64,
    info_count: i64,
    warning_count: i64,
    error_count: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileDigest {
    size_bytes: u64,
    sha256: String,
}

struct StagingDirectory {
    path: PathBuf,
    finalized: bool,
}

impl StagingDirectory {
    fn create(parent: &Path, purpose: &str) -> Result<Self, BackupError> {
        fs::create_dir_all(parent).map_err(|_| BackupError::Storage)?;
        for _ in 0..8 {
            let path = parent.join(format!(".patwari-{purpose}-{}", Uuid::now_v7()));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        finalized: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(BackupError::Storage),
            }
        }
        Err(BackupError::Storage)
    }

    fn finalize(mut self, destination: &Path) -> Result<(), BackupError> {
        if destination.exists() {
            return Err(BackupError::InvalidOutput);
        }
        sync_directory(&self.path)?;
        fs::rename(&self.path, destination).map_err(|_| BackupError::Storage)?;
        let parent = parent_or_current(destination).ok_or(BackupError::InvalidOutput)?;
        sync_directory(parent)?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if !self.finalized {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Creates a self-contained, atomically finalized backup directory.
///
/// The maintenance lease rejects new API operations, then the exclusive file
/// lock waits for any pre-existing request or integrity scan to finish. While
/// it is held no blob can be promoted, removed, or inventoried concurrently.
///
/// # Errors
///
/// Returns an error if the source archive cannot be safely quiesced, an active
/// upload remains, or the staged backup cannot be written and finalized.
pub async fn create(
    config: &Config,
    output: impl AsRef<Path>,
) -> Result<BackupCreateResult, BackupError> {
    let output = output.as_ref().to_path_buf();
    validate_new_output(&output)?;
    if output_is_within_data_dir(&output, &config.data_dir)? {
        return Err(BackupError::InvalidOutput);
    }
    let (database_path, blob_root, maintenance_dir) = source_paths(config)?;
    let database = connect_source_database(&database_path).await?;
    let permit = maintenance::ExclusivePermit::acquire(&database, &maintenance_dir)
        .await
        .map_err(map_gate_error)?;
    let result = create_locked(&database, &database_path, &blob_root, &output).await;
    let release = permit.release().await.map_err(map_gate_error);
    match (result, release) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

async fn create_locked(
    database: &SqlitePool,
    database_path: &Path,
    blob_root: &Path,
    output: &Path,
) -> Result<BackupCreateResult, BackupError> {
    let active_uploads: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM uploads
         WHERE status IN ('created', 'artifact_uploaded')",
    )
    .fetch_one(database)
    .await
    .map_err(|_| BackupError::Metadata)?;
    if active_uploads.0 != 0 {
        return Err(BackupError::ActiveUploads);
    }

    let archive: ArchiveRow = sqlx::query_as(
        "SELECT owner_namespace, archive_instance_id, created_at
         FROM archive_metadata WHERE singleton = 1",
    )
    .fetch_one(database)
    .await
    .map_err(|_| BackupError::Metadata)?;
    let schema_version: (Option<i64>,) =
        sqlx::query_as("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(database)
            .await
            .map_err(|_| BackupError::Metadata)?;
    let schema_version = schema_version.0.ok_or(BackupError::Metadata)?;
    let rows = sqlx::query_as::<_, BlobRow>(
        "SELECT stored_sha256, stored_size_bytes
         FROM blobs ORDER BY stored_sha256 ASC",
    )
    .fetch_all(database)
    .await
    .map_err(|_| BackupError::Metadata)?;
    let latest_integrity_health = load_latest_integrity_health(database).await?;

    // A passive checkpoint reduces the WAL work before the SQLite backup API
    // takes its own consistent online snapshot. The API remains authoritative
    // if a reader prevents a full checkpoint.
    sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_all(database)
        .await
        .map_err(|_| BackupError::Metadata)?;

    let output_parent = parent_or_current(output).ok_or(BackupError::InvalidOutput)?;
    let staging = StagingDirectory::create(output_parent, "backup")?;
    fs::create_dir_all(staging.path.join("blobs").join("sha256"))
        .map_err(|_| BackupError::Storage)?;
    let sqlite_destination = staging.path.join(DATABASE_FILE_NAME);
    online_sqlite_backup(database_path, &sqlite_destination).await?;
    let sqlite_digest = hash_regular_file(&sqlite_destination)?;

    let mut blobs = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_digest = validate_raw_digest(&row.stored_sha256).ok_or(BackupError::Metadata)?;
        let size_bytes = u64::try_from(row.stored_size_bytes).map_err(|_| BackupError::Metadata)?;
        let filename = blob_relative_filename(raw_digest);
        let source = blob_root
            .join("sha256")
            .join(&raw_digest[..2])
            .join(raw_digest);
        let destination = staging.path.join(&filename);
        copy_blob(&source, &destination, size_bytes, raw_digest)?;
        blobs.push(BlobInventoryEntry {
            filename,
            sha256: digest_document(raw_digest),
            size_bytes,
        });
    }

    let created_at = format_timestamp(OffsetDateTime::now_utc())?;
    let manifest = BackupManifest {
        backup_format_version: BACKUP_FORMAT_VERSION,
        created_at: created_at.clone(),
        archive_instance_id: archive.archive_instance_id.clone(),
        owner_namespace: archive.owner_namespace.clone(),
        archive_created_at: archive.created_at,
        application_version: env!("CARGO_PKG_VERSION").to_owned(),
        schema_version,
        sqlite: BackupFile {
            filename: DATABASE_FILE_NAME.to_owned(),
            sha256: digest_document(&sqlite_digest.sha256),
            size_bytes: sqlite_digest.size_bytes,
        },
        blobs,
        latest_integrity_health,
    };
    write_manifest(&staging.path.join(MANIFEST_FILE_NAME), &manifest)?;
    staging.finalize(output)?;

    Ok(BackupCreateResult {
        backup_format_version: BACKUP_FORMAT_VERSION,
        archive_instance_id: archive.archive_instance_id,
        owner_namespace: archive.owner_namespace,
        created_at,
        blob_count: u64::try_from(manifest.blobs.len()).map_err(|_| BackupError::Metadata)?,
    })
}

/// Verifies immutable backup bytes, boots a clean staged archive copy, and
/// runs the full integrity scanner without modifying the supplied backup.
///
/// # Errors
///
/// Returns an error if the backup layout, hashes, database identity, staged
/// bootstrap, or integrity scanner cannot be validated safely.
pub async fn verify(
    backup: impl AsRef<Path>,
    scanner_config: &Config,
) -> Result<BackupVerification, BackupError> {
    let backup = backup.as_ref().to_path_buf();
    let manifest = validate_backup(&backup).await?;
    let staging_parent = parent_or_current(&backup).ok_or(BackupError::Manifest)?;
    let staging = StagingDirectory::create(staging_parent, "verify")?;
    copy_backup_to_data_dir(&backup, &manifest, &staging.path).await?;
    let integrity = scan_staged_archive(&staging.path, scanner_config, &manifest).await?;

    Ok(BackupVerification {
        backup_format_version: manifest.backup_format_version,
        archive_instance_id: manifest.archive_instance_id,
        owner_namespace: manifest.owner_namespace,
        blob_count: u64::try_from(manifest.blobs.len()).map_err(|_| BackupError::Manifest)?,
        integrity,
    })
}

/// Restores only a completely verified archive into an absent or empty
/// destination. The final rename is within the destination's parent
/// filesystem, so callers never see a partially populated archive directory.
///
/// # Errors
///
/// Returns an error if verification fails, the destination is non-empty, or
/// the staged restored archive does not pass a full integrity scan.
pub async fn restore(
    backup: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    scanner_config: &Config,
) -> Result<BackupRestoreResult, BackupError> {
    let backup = backup.as_ref().to_path_buf();
    let data_dir = data_dir.as_ref().to_path_buf();
    let verification = verify(&backup, scanner_config).await?;
    if verification.integrity.status != IntegrityRunStatus::Healthy {
        return Err(BackupError::Integrity);
    }

    validate_restore_destination(&data_dir)?;
    let manifest = validate_backup(&backup).await?;
    let parent = parent_or_current(&data_dir).ok_or(BackupError::DestinationNotEmpty)?;
    let staging = StagingDirectory::create(parent, "restore")?;
    copy_backup_to_data_dir(&backup, &manifest, &staging.path).await?;
    let integrity = scan_staged_archive(&staging.path, scanner_config, &manifest).await?;
    if integrity.status != IntegrityRunStatus::Healthy {
        return Err(BackupError::Integrity);
    }

    // Recheck immediately before replacing an empty directory so a concurrent
    // writer cannot turn this into a destructive overwrite.
    if data_dir.exists() {
        ensure_empty_directory(&data_dir)?;
        fs::remove_dir(&data_dir).map_err(|_| BackupError::DestinationNotEmpty)?;
    }
    staging.finalize(&data_dir)?;

    Ok(BackupRestoreResult {
        archive_instance_id: verification.archive_instance_id,
        owner_namespace: verification.owner_namespace,
        blob_count: verification.blob_count,
        integrity,
    })
}

async fn connect_source_database(database_path: &Path) -> Result<SqlitePool, BackupError> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .create_if_missing(false)
        .foreign_keys(true)
        .disable_statement_logging();
    SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .map_err(|_| BackupError::Source)
}

fn source_paths(config: &Config) -> Result<(PathBuf, PathBuf, PathBuf), BackupError> {
    let database = config.data_dir.join(DATABASE_FILE_NAME);
    let blob_root = config.data_dir.join("blobs");
    let maintenance = config.data_dir.join("maintenance");
    if !is_regular_file(&database) || !is_directory(&blob_root) || !is_directory(&maintenance) {
        return Err(BackupError::Source);
    }
    Ok((database, blob_root, maintenance))
}

async fn load_latest_integrity_health(
    database: &SqlitePool,
) -> Result<Option<LatestIntegrityHealth>, BackupError> {
    let row = sqlx::query_as::<_, IntegrityHealthRow>(
        "SELECT id, status, started_at, completed_at, finding_count,
                info_count, warning_count, error_count
         FROM integrity_runs
         WHERE completed_at IS NOT NULL
         ORDER BY completed_at_seq DESC, id DESC
         LIMIT 1",
    )
    .fetch_optional(database)
    .await
    .map_err(|_| BackupError::Metadata)?;
    row.map(|row| {
        Ok(LatestIntegrityHealth {
            run_id: row.id,
            status: row.status,
            started_at: row.started_at,
            completed_at: row.completed_at,
            finding_count: u64::try_from(row.finding_count).map_err(|_| BackupError::Metadata)?,
            info_count: u64::try_from(row.info_count).map_err(|_| BackupError::Metadata)?,
            warning_count: u64::try_from(row.warning_count).map_err(|_| BackupError::Metadata)?,
            error_count: u64::try_from(row.error_count).map_err(|_| BackupError::Metadata)?,
        })
    })
    .transpose()
}

async fn online_sqlite_backup(source: &Path, destination: &Path) -> Result<(), BackupError> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || online_sqlite_backup_blocking(&source, &destination))
        .await
        .map_err(|_| BackupError::Storage)?
}

fn online_sqlite_backup_blocking(source: &Path, destination: &Path) -> Result<(), BackupError> {
    let source = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| BackupError::Source)?;
    let mut destination = Connection::open(destination).map_err(|_| BackupError::Storage)?;
    {
        let backup = Backup::new(&source, &mut destination).map_err(|_| BackupError::Source)?;
        backup
            .run_to_completion(128, Duration::from_millis(10), None)
            .map_err(|_| BackupError::Source)?;
    }
    // The lease is a live coordination implementation detail, not archived
    // history. Clearing it in the detached copy ensures a restore can boot
    // immediately even if the source backup is still in progress.
    destination
        .execute(
            "UPDATE maintenance_gate
             SET exclusive_token = NULL, exclusive_until_unix = NULL
             WHERE singleton = 1",
            [],
        )
        .map_err(|_| BackupError::Storage)?;
    destination
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|_| BackupError::Storage)?;
    Ok(())
}

async fn validate_backup(backup: &Path) -> Result<BackupManifest, BackupError> {
    let backup = backup.to_path_buf();
    tokio::task::spawn_blocking(move || validate_backup_blocking(&backup))
        .await
        .map_err(|_| BackupError::Storage)?
}

fn validate_backup_blocking(backup: &Path) -> Result<BackupManifest, BackupError> {
    if !is_directory(backup) {
        return Err(BackupError::Manifest);
    }
    let manifest_path = backup.join(MANIFEST_FILE_NAME);
    let metadata = fs::metadata(&manifest_path).map_err(|_| BackupError::Manifest)?;
    if metadata.len() > MAX_MANIFEST_BYTES || !is_regular_file(&manifest_path) {
        return Err(BackupError::Manifest);
    }
    let bytes = fs::read(&manifest_path).map_err(|_| BackupError::Manifest)?;
    let manifest: BackupManifest =
        serde_json::from_slice(&bytes).map_err(|_| BackupError::Manifest)?;
    validate_manifest_shape(&manifest)?;
    validate_backup_root_entries(backup)?;

    let database_path = backup.join(DATABASE_FILE_NAME);
    let sqlite_digest = hash_regular_file(&database_path).map_err(|_| BackupError::Manifest)?;
    if sqlite_digest.size_bytes != manifest.sqlite.size_bytes
        || sqlite_digest.sha256
            != digest_value(&manifest.sqlite.sha256).ok_or(BackupError::Manifest)?
    {
        return Err(BackupError::Manifest);
    }

    let expected = manifest
        .blobs
        .iter()
        .map(|entry| {
            let digest = digest_value(&entry.sha256).ok_or(BackupError::Manifest)?;
            if entry.filename != blob_relative_filename(digest) {
                return Err(BackupError::Manifest);
            }
            Ok(digest.to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if expected.len() != manifest.blobs.len() {
        return Err(BackupError::Manifest);
    }
    let actual = backup_blob_inventory(backup)?;
    if actual != expected {
        return Err(BackupError::Manifest);
    }
    for entry in &manifest.blobs {
        let digest = digest_value(&entry.sha256).ok_or(BackupError::Manifest)?;
        let file_digest =
            hash_regular_file(&backup.join(&entry.filename)).map_err(|_| BackupError::Manifest)?;
        if file_digest.size_bytes != entry.size_bytes || file_digest.sha256 != digest {
            return Err(BackupError::Manifest);
        }
    }
    Ok(manifest)
}

fn validate_manifest_shape(manifest: &BackupManifest) -> Result<(), BackupError> {
    if manifest.backup_format_version != BACKUP_FORMAT_VERSION
        || Uuid::parse_str(&manifest.archive_instance_id).is_err()
        || manifest.owner_namespace.is_empty()
        || manifest.archive_created_at.is_empty()
        || manifest.created_at.is_empty()
        || manifest.application_version.is_empty()
        || manifest.schema_version <= 0
        || manifest.sqlite.filename != DATABASE_FILE_NAME
        || digest_value(&manifest.sqlite.sha256).is_none()
    {
        return Err(BackupError::Manifest);
    }
    let mut prior = None;
    for entry in &manifest.blobs {
        let digest = digest_value(&entry.sha256).ok_or(BackupError::Manifest)?;
        if entry.filename != blob_relative_filename(digest)
            || prior.is_some_and(|previous: &str| previous >= digest)
        {
            return Err(BackupError::Manifest);
        }
        prior = Some(digest);
    }
    Ok(())
}

fn validate_backup_root_entries(backup: &Path) -> Result<(), BackupError> {
    let expected = BTreeSet::from([
        MANIFEST_FILE_NAME.to_owned(),
        DATABASE_FILE_NAME.to_owned(),
        "blobs".to_owned(),
    ]);
    let actual = fs::read_dir(backup)
        .map_err(|_| BackupError::Manifest)?
        .map(|entry| {
            entry
                .map_err(|_| BackupError::Manifest)?
                .file_name()
                .into_string()
                .map_err(|_| BackupError::Manifest)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if actual != expected {
        return Err(BackupError::Manifest);
    }
    Ok(())
}

fn backup_blob_inventory(backup: &Path) -> Result<BTreeSet<String>, BackupError> {
    let blobs = backup.join("blobs");
    if !is_directory(&blobs) {
        return Err(BackupError::Manifest);
    }
    let blob_root_entries = fs::read_dir(&blobs)
        .map_err(|_| BackupError::Manifest)?
        .map(|entry| {
            entry
                .map_err(|_| BackupError::Manifest)?
                .file_name()
                .into_string()
                .map_err(|_| BackupError::Manifest)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if blob_root_entries != BTreeSet::from(["sha256".to_owned()]) {
        return Err(BackupError::Manifest);
    }
    let sha256_root = blobs.join("sha256");
    if !is_directory(&sha256_root) {
        return Err(BackupError::Manifest);
    }

    let mut inventory = BTreeSet::new();
    for shard in fs::read_dir(&sha256_root).map_err(|_| BackupError::Manifest)? {
        let shard = shard.map_err(|_| BackupError::Manifest)?;
        let name = shard
            .file_name()
            .into_string()
            .map_err(|_| BackupError::Manifest)?;
        if name.len() != 2
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || !is_directory(&shard.path())
        {
            return Err(BackupError::Manifest);
        }
        for blob in fs::read_dir(shard.path()).map_err(|_| BackupError::Manifest)? {
            let blob = blob.map_err(|_| BackupError::Manifest)?;
            let digest = blob
                .file_name()
                .into_string()
                .map_err(|_| BackupError::Manifest)?;
            if validate_raw_digest(&digest).is_none()
                || !digest.starts_with(&name)
                || !is_regular_file(&blob.path())
                || !inventory.insert(digest)
            {
                return Err(BackupError::Manifest);
            }
        }
    }
    Ok(inventory)
}

async fn copy_backup_to_data_dir(
    backup: &Path,
    manifest: &BackupManifest,
    destination: &Path,
) -> Result<(), BackupError> {
    let backup = backup.to_path_buf();
    let manifest = manifest.clone();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        copy_backup_to_data_dir_blocking(&backup, &manifest, &destination)
    })
    .await
    .map_err(|_| BackupError::Storage)?
}

fn copy_backup_to_data_dir_blocking(
    backup: &Path,
    manifest: &BackupManifest,
    destination: &Path,
) -> Result<(), BackupError> {
    copy_regular_file(
        &backup.join(DATABASE_FILE_NAME),
        &destination.join(DATABASE_FILE_NAME),
    )?;
    for blob in &manifest.blobs {
        let digest = digest_value(&blob.sha256).ok_or(BackupError::Manifest)?;
        copy_blob(
            &backup.join(&blob.filename),
            &destination.join(&blob.filename),
            blob.size_bytes,
            digest,
        )?;
    }
    sync_directory(destination)?;
    Ok(())
}

async fn scan_staged_archive(
    data_dir: &Path,
    scanner_config: &Config,
    manifest: &BackupManifest,
) -> Result<IntegrityReport, BackupError> {
    let mut config = scanner_config.clone();
    config.data_dir = data_dir.to_path_buf();
    let (service, identity) = Service::bootstrap_for_integrity(&config)
        .await
        .map_err(|_| BackupError::Bootstrap)?;
    if identity.archive_instance_id != manifest.archive_instance_id
        || identity.owner_namespace != manifest.owner_namespace
    {
        return Err(BackupError::Manifest);
    }
    let report = service
        .verify_integrity()
        .await
        .map_err(|_| BackupError::Scan)?;
    drop(service);
    Ok(report)
}

fn validate_new_output(output: &Path) -> Result<(), BackupError> {
    if output.file_name().is_none() || output.exists() {
        return Err(BackupError::InvalidOutput);
    }
    let parent = parent_or_current(output).ok_or(BackupError::InvalidOutput)?;
    fs::create_dir_all(parent).map_err(|_| BackupError::Storage)?;
    if !is_directory(parent) {
        return Err(BackupError::InvalidOutput);
    }
    Ok(())
}

fn validate_restore_destination(destination: &Path) -> Result<(), BackupError> {
    let parent = parent_or_current(destination).ok_or(BackupError::DestinationNotEmpty)?;
    if destination.file_name().is_none() {
        return Err(BackupError::DestinationNotEmpty);
    }
    fs::create_dir_all(parent).map_err(|_| BackupError::Storage)?;
    ensure_restorable_destination(parent, destination)?;
    if destination.exists() {
        ensure_empty_directory(destination)?;
    }
    Ok(())
}

/// Finalizing a restore stages a sibling directory beside `destination` and
/// replaces it with a same-filesystem [`fs::rename`]. That only works if
/// `parent` is genuinely writable and, when `destination` already exists
/// (for example an empty directory created ahead of time by mounting a
/// fresh persistent volume there), shares one filesystem with `parent`.
///
/// A read-only container root deployed with the volume mounted directly at
/// `PATWARI_DATA_DIR` fails both checks: `parent` sits on the read-only
/// rootfs, and even if it did not, `destination` would be a distinct mount
/// with no valid same-filesystem rename target beside it. Mounting the
/// volume at a writable root and pointing `PATWARI_DATA_DIR` at a
/// subdirectory of it keeps `destination` and `parent` on one writable
/// filesystem. Detecting this here turns a raw, platform-specific `EROFS` or
/// `EXDEV` from the later rename into one clear, actionable error.
fn ensure_restorable_destination(parent: &Path, destination: &Path) -> Result<(), BackupError> {
    if !is_directory(parent) || !is_directory_writable(parent) {
        return Err(BackupError::UnsafeDestination);
    }
    if destination.exists() && !is_same_filesystem(parent, destination)? {
        return Err(BackupError::UnsafeDestination);
    }
    Ok(())
}

/// Probes real write capability instead of trusting permission bits, since a
/// read-only bind mount or rootfs still reports normally-writable
/// permissions while every write call fails.
fn is_directory_writable(directory: &Path) -> bool {
    let probe = directory.join(format!(".patwari-restore-probe-{}", Uuid::now_v7()));
    match OpenOptions::new().write(true).create_new(true).open(&probe) {
        Ok(_) => {
            let _ = fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

#[cfg(unix)]
fn is_same_filesystem(left: &Path, right: &Path) -> Result<bool, BackupError> {
    use std::os::unix::fs::MetadataExt;

    let left = fs::metadata(left).map_err(|_| BackupError::UnsafeDestination)?;
    let right = fs::metadata(right).map_err(|_| BackupError::UnsafeDestination)?;
    Ok(left.dev() == right.dev())
}

#[cfg(not(unix))]
fn is_same_filesystem(_left: &Path, _right: &Path) -> Result<bool, BackupError> {
    Ok(true)
}

fn output_is_within_data_dir(output: &Path, data_dir: &Path) -> Result<bool, BackupError> {
    let root = fs::canonicalize(data_dir).map_err(|_| BackupError::Source)?;
    let parent = parent_or_current(output).ok_or(BackupError::InvalidOutput)?;
    let parent = fs::canonicalize(parent).map_err(|_| BackupError::Storage)?;
    let filename = output.file_name().ok_or(BackupError::InvalidOutput)?;
    Ok(parent.join(filename).starts_with(root))
}

fn ensure_empty_directory(path: &Path) -> Result<(), BackupError> {
    if !is_directory(path) {
        return Err(BackupError::DestinationNotEmpty);
    }
    if fs::read_dir(path)
        .map_err(|_| BackupError::DestinationNotEmpty)?
        .next()
        .is_some()
    {
        return Err(BackupError::DestinationNotEmpty);
    }
    Ok(())
}

fn parent_or_current(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .or_else(|| path.file_name().map(|_| Path::new(".")))
}

fn copy_blob(
    source: &Path,
    destination: &Path,
    expected_size: u64,
    expected_digest: &str,
) -> Result<(), BackupError> {
    if !is_regular_file(source) {
        return Err(BackupError::Storage);
    }
    let parent = destination.parent().ok_or(BackupError::Storage)?;
    fs::create_dir_all(parent).map_err(|_| BackupError::Storage)?;
    match fs::hard_link(source, destination) {
        Ok(()) => verify_hard_link(source, destination)?,
        Err(_) => copy_regular_file(source, destination)?,
    }
    let digest = hash_regular_file(destination)?;
    if digest.size_bytes != expected_size || digest.sha256 != expected_digest {
        return Err(BackupError::Storage);
    }
    sync_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn verify_hard_link(source: &Path, destination: &Path) -> Result<(), BackupError> {
    use std::os::unix::fs::MetadataExt;

    let source = fs::metadata(source).map_err(|_| BackupError::Storage)?;
    let destination = fs::metadata(destination).map_err(|_| BackupError::Storage)?;
    if source.dev() != destination.dev() || source.ino() != destination.ino() {
        return Err(BackupError::Storage);
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_hard_link(_source: &Path, _destination: &Path) -> Result<(), BackupError> {
    Ok(())
}

fn copy_regular_file(source: &Path, destination: &Path) -> Result<(), BackupError> {
    if !is_regular_file(source) {
        return Err(BackupError::Storage);
    }
    let parent = destination.parent().ok_or(BackupError::Storage)?;
    fs::create_dir_all(parent).map_err(|_| BackupError::Storage)?;
    let mut source = File::open(source).map_err(|_| BackupError::Storage)?;
    let mut destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|_| BackupError::Storage)?;
    io::copy(&mut source, &mut destination).map_err(|_| BackupError::Storage)?;
    destination.sync_all().map_err(|_| BackupError::Storage)?;
    Ok(())
}

fn write_manifest(path: &Path, manifest: &BackupManifest) -> Result<(), BackupError> {
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|_| BackupError::Metadata)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| BackupError::Storage)?;
    file.write_all(&bytes).map_err(|_| BackupError::Storage)?;
    file.write_all(b"\n").map_err(|_| BackupError::Storage)?;
    file.sync_all().map_err(|_| BackupError::Storage)
}

fn hash_regular_file(path: &Path) -> Result<FileDigest, BackupError> {
    if !is_regular_file(path) {
        return Err(BackupError::Storage);
    }
    let mut file = File::open(path).map_err(|_| BackupError::Storage)?;
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|_| BackupError::Storage)?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(u64::try_from(read).map_err(|_| BackupError::Storage)?)
            .ok_or(BackupError::Storage)?;
        hasher.update(&buffer[..read]);
    }
    Ok(FileDigest {
        size_bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

fn is_regular_file(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}

fn is_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

fn sync_directory(path: &Path) -> Result<(), BackupError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| BackupError::Storage)
}

fn blob_relative_filename(raw_digest: &str) -> String {
    format!("blobs/sha256/{}/{}", &raw_digest[..2], raw_digest)
}

fn validate_raw_digest(value: &str) -> Option<&str> {
    (value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    .then_some(value)
}

fn digest_value(value: &str) -> Option<&str> {
    value.strip_prefix("sha256:").and_then(validate_raw_digest)
}

fn digest_document(raw_digest: &str) -> String {
    format!("sha256:{raw_digest}")
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.as_ref() {
        use std::fmt::Write;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn format_timestamp(timestamp: OffsetDateTime) -> Result<String, BackupError> {
    use time::format_description::well_known::Rfc3339;

    timestamp
        .format(&Rfc3339)
        .map_err(|_| BackupError::Metadata)
}

fn map_gate_error(error: MaintenanceGateError) -> BackupError {
    match error {
        MaintenanceGateError::Busy => BackupError::MaintenanceBusy,
        MaintenanceGateError::Unavailable => BackupError::Maintenance,
    }
}
