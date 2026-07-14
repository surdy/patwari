use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::fs;
use uuid::Uuid;

use crate::database::BootstrapError;

const STORAGE_DIRECTORIES: [&str; 3] = ["blobs", "uploads", "maintenance"];

#[derive(Clone)]
pub(crate) struct StorageLayout {
    pub(crate) blobs: PathBuf,
    pub(crate) uploads: PathBuf,
    maintenance: PathBuf,
}

impl StorageLayout {
    pub(crate) async fn create(data_dir: &Path) -> Result<Self, BootstrapError> {
        fs::create_dir_all(data_dir)
            .await
            .map_err(BootstrapError::Storage)?;

        let layout = Self {
            blobs: data_dir.join(STORAGE_DIRECTORIES[0]),
            uploads: data_dir.join(STORAGE_DIRECTORIES[1]),
            maintenance: data_dir.join(STORAGE_DIRECTORIES[2]),
        };
        for directory in [&layout.blobs, &layout.uploads, &layout.maintenance] {
            fs::create_dir_all(directory)
                .await
                .map_err(BootstrapError::Storage)?;
        }
        Ok(layout)
    }

    pub(crate) async fn is_usable(&self) -> bool {
        for directory in [&self.blobs, &self.uploads, &self.maintenance] {
            if !directory_is_writable(directory).await {
                return false;
            }
        }
        true
    }

    pub(crate) fn upload_dir(&self, upload_id: &str) -> PathBuf {
        self.uploads.join(upload_id)
    }

    pub(crate) fn artifact_chunk_dir(&self, upload_id: &str, artifact_index: u32) -> PathBuf {
        self.upload_dir(upload_id)
            .join("artifacts")
            .join(artifact_index.to_string())
            .join("chunks")
    }

    /// Legacy artifact-zero chunk path.
    #[cfg(test)]
    pub(crate) fn chunk_path(&self, upload_id: &str, chunk_index: u64) -> PathBuf {
        self.artifact_chunk_path(upload_id, 0, chunk_index)
    }

    pub(crate) fn artifact_chunk_path(
        &self,
        upload_id: &str,
        artifact_index: u32,
        chunk_index: u64,
    ) -> PathBuf {
        self.artifact_chunk_dir(upload_id, artifact_index)
            .join(chunk_index.to_string())
    }

    pub(crate) fn staged_chunk_path(&self, upload_id: &str, artifact_index: u32) -> PathBuf {
        self.artifact_chunk_dir(upload_id, artifact_index)
            .join(format!(".chunk-{}.partial", Uuid::now_v7()))
    }

    pub(crate) fn assembled_artifact_path(&self, upload_id: &str, artifact_index: u32) -> PathBuf {
        self.upload_dir(upload_id).join(format!(
            ".assembled-{artifact_index}-{}.partial",
            Uuid::now_v7()
        ))
    }

    pub(crate) fn blob_path(&self, stored_sha256: &str) -> PathBuf {
        self.blobs
            .join("sha256")
            .join(&stored_sha256[..2])
            .join(stored_sha256)
    }

    /// Directory reserved for maintenance coordination and local staging.
    /// It is deliberately part of the persistent data volume so independent
    /// server and CLI processes coordinate through the same lock file.
    pub(crate) fn maintenance_dir(&self) -> &Path {
        &self.maintenance
    }

    pub(crate) async fn ensure_chunk_dir(
        &self,
        upload_id: &str,
        artifact_index: u32,
    ) -> Result<(), io::Error> {
        fs::create_dir_all(self.artifact_chunk_dir(upload_id, artifact_index)).await
    }

    pub(crate) async fn remove_upload_dir(&self, upload_id: &str) -> Result<(), io::Error> {
        match fs::remove_dir_all(self.upload_dir(upload_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn remove_file(path: &Path) -> Result<(), io::Error> {
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

pub(crate) async fn directory_is_writable(directory: &Path) -> bool {
    let probe = directory.join(format!(".patwari-ready-{}", Uuid::now_v7()));
    match fs::write(&probe, []).await {
        Ok(()) => fs::remove_file(probe).await.is_ok(),
        Err(_) => false,
    }
}
