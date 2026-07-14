//! Cross-process maintenance coordination.
//!
//! The `SQLite` lease prevents fresh requests from entering while a backup is
//! pending. A POSIX advisory lock held for the full operation is the actual
//! exclusion primitive: unlike a database row, it is released automatically
//! if its process dies. Both are required because a lease alone cannot cover
//! filesystem blob promotion or deletion.
//!
//! The lease is written exactly once, when the exclusive holder starts
//! waiting for the filesystem lock, and is never refreshed afterward. An
//! earlier design refreshed it from a periodic background task for the full
//! lifetime of the permit, including while the `SQLite` Online Backup API
//! copy was in progress. Because that heartbeat wrote to the very database
//! file being copied, every refresh could make `SQLite` restart the backup
//! step against a changed source, which is unnecessary risk for zero benefit:
//! the flock is already the sole thing every mutator waits on. A lease that
//! goes stale mid-backup cannot admit a mutator, because
//! [`SharedPermit::acquire`] and [`SharedFilePermit::acquire`] both first
//! take a non-blocking shared flock; that attempt keeps failing with
//! `WouldBlock` for as long as this exclusive holder keeps the flock, no
//! matter what the lease row says. The lease only matters for the narrow
//! window after a crash releases the flock automatically but before the
//! lease's fixed expiry catches up, so [`LEASE_DURATION`] only needs to
//! outlast one worst-case exclusive hold (wait for the flock plus the backup
//! body) and must never be unbounded.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::State,
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use fs2::FileExt;
use sqlx::SqlitePool;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{error::ApiError, service::AppState};

const LOCK_FILE_NAME: &str = "archive-maintenance.lock";
/// Long enough to cover [`EXCLUSIVE_WAIT_LIMIT`] plus a generous backup body
/// (`SQLite` copy, blob hashing, and manifest write) with headroom, while
/// staying bounded rather than treating a crashed holder as busy forever.
const LEASE_DURATION: Duration = Duration::from_hours(6);
const EXCLUSIVE_WAIT_LIMIT: Duration = Duration::from_mins(15);

#[derive(Clone, Copy, Debug, Error)]
pub(crate) enum MaintenanceGateError {
    #[error("archive maintenance is in progress")]
    Busy,
    #[error("archive maintenance coordination is unavailable")]
    Unavailable,
}

/// A shared filesystem-lock holder. Dropping its file descriptor releases the
/// lock even if request cancellation skips normal application cleanup.
pub(crate) struct SharedPermit {
    _file: SharedFilePermit,
}

/// File-only shared ownership used while the server is bootstrapping its
/// schema. The maintenance table does not exist until migrations finish, but
/// migrations and identity initialization still must not overlap a backup.
pub(crate) struct SharedFilePermit {
    _file: File,
}

impl SharedFilePermit {
    pub(crate) fn acquire(maintenance_dir: &Path) -> Result<Self, MaintenanceGateError> {
        let file = open_lock_file(maintenance_dir)?;
        match FileExt::try_lock_shared(&file) {
            Ok(()) => Ok(Self { _file: file }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Err(MaintenanceGateError::Busy)
            }
            Err(_) => Err(MaintenanceGateError::Unavailable),
        }
    }
}

impl SharedPermit {
    pub(crate) async fn acquire(
        database: &SqlitePool,
        maintenance_dir: &Path,
    ) -> Result<Self, MaintenanceGateError> {
        let file = SharedFilePermit::acquire(maintenance_dir)?;

        // Check after taking the shared lock. This closes the race where a
        // backup records its pending lease between a request's first database
        // observation and lock acquisition.
        if maintenance_is_active(database).await? {
            return Err(MaintenanceGateError::Busy);
        }
        Ok(Self { _file: file })
    }
}

pub(crate) async fn ensure_not_active(database: &SqlitePool) -> Result<(), MaintenanceGateError> {
    if maintenance_is_active(database).await? {
        Err(MaintenanceGateError::Busy)
    } else {
        Ok(())
    }
}

/// An exclusive backup owner. Its database lease is written exactly once by
/// [`Self::acquire`] and is never refreshed for the lifetime of the permit,
/// including while an `SQLite` Online Backup API copy runs: writing to the
/// source database from a heartbeat while that copy is in progress is
/// exactly the kind of concurrent modification that makes `SQLite` restart
/// the backup step, so no code path here may touch `maintenance_gate` again
/// until [`Self::release`] clears it. The filesystem lock, not the lease, is
/// what every mutator actually waits on for the entire hold.
pub(crate) struct ExclusivePermit {
    database: SqlitePool,
    token: String,
    file: File,
}

impl ExclusivePermit {
    pub(crate) async fn acquire(
        database: &SqlitePool,
        maintenance_dir: &Path,
    ) -> Result<Self, MaintenanceGateError> {
        let token = Uuid::now_v7().to_string();
        claim_lease(database, &token).await?;
        let file = match open_lock_file(maintenance_dir) {
            Ok(file) => file,
            Err(error) => {
                let _ = clear_lease(database, &token).await;
                return Err(error);
            }
        };

        let wait_started = Instant::now();
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    let _ = clear_lease(database, &token).await;
                    return Err(MaintenanceGateError::Unavailable);
                }
            }

            if wait_started.elapsed() >= EXCLUSIVE_WAIT_LIMIT {
                let _ = clear_lease(database, &token).await;
                return Err(MaintenanceGateError::Busy);
            }
            // Deliberately not refreshed: the lease claimed above already
            // covers this wait plus the backup body within `LEASE_DURATION`.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(Self {
            database: database.clone(),
            token,
            file,
        })
    }

    pub(crate) async fn release(self) -> Result<(), MaintenanceGateError> {
        clear_lease(&self.database, &self.token).await?;
        // Explicitly unlock so the lease clear and lock release remain close
        // together; dropping the descriptor is still a crash-safe fallback.
        FileExt::unlock(&self.file).map_err(|_| MaintenanceGateError::Unavailable)
    }
}

/// Gates every API request, not only verb-based writes: upload-status GET can
/// terminalize an expired transfer. Read endpoints remain available outside a
/// maintenance window, while a backup gets a true archive-wide quiescence.
pub(crate) async fn gate_api_requests(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match SharedPermit::acquire(&state.database, state.storage.maintenance_dir()).await {
        Ok(_permit) => next.run(request).await,
        Err(MaintenanceGateError::Busy) => ApiError::maintenance().into_response(),
        Err(MaintenanceGateError::Unavailable) => {
            ApiError::maintenance_unavailable().into_response()
        }
    }
}

fn open_lock_file(maintenance_dir: &Path) -> Result<File, MaintenanceGateError> {
    fs::create_dir_all(maintenance_dir).map_err(|_| MaintenanceGateError::Unavailable)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(maintenance_dir.join(LOCK_FILE_NAME))
        .map_err(|_| MaintenanceGateError::Unavailable)
}

async fn maintenance_is_active(database: &SqlitePool) -> Result<bool, MaintenanceGateError> {
    for _ in 0..2 {
        let row: (Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT exclusive_token, exclusive_until_unix
             FROM maintenance_gate WHERE singleton = 1",
        )
        .fetch_one(database)
        .await
        .map_err(|_| MaintenanceGateError::Unavailable)?;
        let Some(token) = row.0 else {
            return Ok(false);
        };
        let now = unix_timestamp();
        let Some(until) = row.1 else {
            return Ok(true);
        };
        if until > now {
            return Ok(true);
        }
        let _ = sqlx::query(
            "UPDATE maintenance_gate
             SET exclusive_token = NULL, exclusive_until_unix = NULL
             WHERE singleton = 1
               AND exclusive_token = ?1
               AND exclusive_until_unix <= ?2",
        )
        .bind(token)
        .bind(now)
        .execute(database)
        .await
        .map_err(|_| MaintenanceGateError::Unavailable)?;
    }
    Ok(true)
}

async fn claim_lease(database: &SqlitePool, token: &str) -> Result<(), MaintenanceGateError> {
    let now = unix_timestamp();
    let updated = sqlx::query(
        "UPDATE maintenance_gate
         SET exclusive_token = ?1, exclusive_until_unix = ?2
         WHERE singleton = 1
           AND (
               exclusive_token IS NULL
               OR exclusive_until_unix <= ?3
           )",
    )
    .bind(token)
    .bind(lease_until())
    .bind(now)
    .execute(database)
    .await
    .map_err(|_| MaintenanceGateError::Unavailable)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(MaintenanceGateError::Busy)
    }
}

async fn clear_lease(database: &SqlitePool, token: &str) -> Result<(), MaintenanceGateError> {
    sqlx::query(
        "UPDATE maintenance_gate
         SET exclusive_token = NULL, exclusive_until_unix = NULL
         WHERE singleton = 1 AND exclusive_token = ?1",
    )
    .bind(token)
    .execute(database)
    .await
    .map_err(|_| MaintenanceGateError::Unavailable)?;
    Ok(())
}

fn unix_timestamp() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

fn lease_until() -> i64 {
    unix_timestamp()
        + i64::try_from(LEASE_DURATION.as_secs())
            .expect("maintenance lease duration fits signed seconds")
}
