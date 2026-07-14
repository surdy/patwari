use std::io;

use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::config::{Config, ConfigError};

pub(crate) const OWNER_NAMESPACE: &str = "v1";

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveIdentity {
    pub owner_namespace: String,
    pub archive_instance_id: String,
    pub created_at: String,
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("archive configuration is invalid")]
    Configuration(#[source] ConfigError),
    #[error("persistent storage could not be initialized")]
    Storage(#[source] io::Error),
    #[error("metadata store could not be initialized")]
    Database(#[source] sqlx::Error),
    #[error("metadata schema could not be initialized")]
    Migration(#[source] sqlx::migrate::MigrateError),
    #[error("archive identity could not be initialized")]
    Identity(#[source] sqlx::Error),
    #[error("archive timestamp could not be generated")]
    Clock(#[source] time::error::Format),
    #[error("durable upload state could not be recovered")]
    Recovery,
    #[error("archive listener could not be bound")]
    Bind(#[source] io::Error),
    #[error("archive HTTP service stopped unexpectedly")]
    Serve(#[source] io::Error),
}

pub(crate) async fn connect(
    config: &Config,
) -> Result<(SqlitePool, ArchiveIdentity), BootstrapError> {
    let database_path = config.data_dir.join("patwari.db");
    let connect_options = SqliteConnectOptions::new()
        .filename(database_path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .disable_statement_logging();
    let database = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(connect_options)
        .await
        .map_err(BootstrapError::Database)?;

    MIGRATOR
        .run(&database)
        .await
        .map_err(BootstrapError::Migration)?;
    let identity = initialize_identity(&database).await?;
    Ok((database, identity))
}

async fn initialize_identity(database: &SqlitePool) -> Result<ArchiveIdentity, BootstrapError> {
    let mut transaction = database.begin().await.map_err(BootstrapError::Identity)?;
    let created_at = now_rfc3339().map_err(BootstrapError::Clock)?;
    let archive_instance_id = Uuid::now_v7().to_string();

    sqlx::query(
        "INSERT INTO archive_metadata (singleton, owner_namespace, archive_instance_id, created_at)
         VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(singleton) DO NOTHING",
    )
    .bind(OWNER_NAMESPACE)
    .bind(archive_instance_id)
    .bind(created_at)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapError::Identity)?;

    let row = sqlx::query_as::<_, (String, String, String)>(
        "SELECT owner_namespace, archive_instance_id, created_at
         FROM archive_metadata WHERE singleton = 1",
    )
    .fetch_one(&mut *transaction)
    .await
    .map_err(BootstrapError::Identity)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapError::Identity)?;

    Ok(ArchiveIdentity {
        owner_namespace: row.0,
        archive_instance_id: row.1,
        created_at: row.2,
    })
}

pub(crate) fn now_rfc3339() -> Result<String, time::error::Format> {
    format_time(OffsetDateTime::now_utc())
}

pub(crate) fn format_time(timestamp: OffsetDateTime) -> Result<String, time::error::Format> {
    timestamp.format(&Rfc3339)
}

pub(crate) fn expiration_at(
    now: OffsetDateTime,
    duration: std::time::Duration,
) -> Result<String, time::error::Format> {
    let seconds = i64::try_from(duration.as_secs()).expect("configuration duration fits i64");
    format_time(now + time::Duration::seconds(seconds))
}
