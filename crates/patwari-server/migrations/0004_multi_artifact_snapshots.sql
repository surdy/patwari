-- Manifest v1 now represents an ordered set of regular byte streams.  Keep
-- the v3 singleton columns for in-place compatibility while moving all
-- artifact-specific upload state into this normalized projection.
CREATE TABLE upload_artifacts (
    upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
    artifact_index INTEGER NOT NULL CHECK (artifact_index >= 0),
    logical_path TEXT NOT NULL,
    media_type TEXT,
    original_size_bytes INTEGER NOT NULL CHECK (original_size_bytes >= 0),
    original_sha256 TEXT NOT NULL,
    stored_size_bytes INTEGER NOT NULL CHECK (stored_size_bytes >= 0),
    stored_sha256 TEXT NOT NULL,
    compression TEXT NOT NULL CHECK (compression IN ('identity', 'zstd')),
    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
    PRIMARY KEY (upload_id, artifact_index),
    UNIQUE (upload_id, logical_path)
);

ALTER TABLE uploads ADD COLUMN artifact_count INTEGER NOT NULL DEFAULT 1
    CHECK (artifact_count >= 1);
ALTER TABLE uploads ADD COLUMN total_stored_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_stored_size_bytes >= 0);
ALTER TABLE uploads ADD COLUMN total_original_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_original_size_bytes >= 0);
ALTER TABLE uploads ADD COLUMN transfer_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (transfer_bytes >= 0);
ALTER TABLE uploads ADD COLUMN newly_persisted_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (newly_persisted_bytes >= 0);

ALTER TABLE snapshots ADD COLUMN artifact_count INTEGER NOT NULL DEFAULT 1
    CHECK (artifact_count >= 1);
ALTER TABLE snapshots ADD COLUMN total_original_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_original_size_bytes >= 0);
ALTER TABLE snapshots ADD COLUMN total_stored_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_stored_size_bytes >= 0);

ALTER TABLE artifacts ADD COLUMN artifact_index INTEGER NOT NULL DEFAULT 0
    CHECK (artifact_index >= 0);
CREATE UNIQUE INDEX artifacts_snapshot_artifact_index_idx
    ON artifacts(snapshot_id, artifact_index);

ALTER TABLE upload_audits ADD COLUMN artifact_count INTEGER NOT NULL DEFAULT 1
    CHECK (artifact_count >= 1);
ALTER TABLE upload_audits ADD COLUMN total_original_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_original_size_bytes >= 0);
ALTER TABLE upload_audits ADD COLUMN total_stored_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (total_stored_size_bytes >= 0);

-- v3 constrained all chunks to artifact zero. Rebuild this child table with
-- the same keys and durable data but a general non-negative artifact index.
DROP INDEX upload_chunks_upload_idx;
CREATE TABLE upload_chunks_v4 (
    upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
    artifact_index INTEGER NOT NULL CHECK (artifact_index >= 0),
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
    sha256 TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY (upload_id, artifact_index, chunk_index)
);
INSERT INTO upload_chunks_v4 (
    upload_id, artifact_index, chunk_index, byte_length, sha256, accepted_at
)
SELECT upload_id, artifact_index, chunk_index, byte_length, sha256, accepted_at
FROM upload_chunks;
DROP TABLE upload_chunks;
ALTER TABLE upload_chunks_v4 RENAME TO upload_chunks;
CREATE INDEX upload_chunks_upload_idx
    ON upload_chunks(upload_id, artifact_index, chunk_index);

UPDATE snapshots
SET artifact_count = (
        SELECT COUNT(*) FROM artifacts WHERE artifacts.snapshot_id = snapshots.id
    ),
    total_original_size_bytes = COALESCE((
        SELECT SUM(original_size_bytes) FROM artifacts WHERE artifacts.snapshot_id = snapshots.id
    ), 0),
    total_stored_size_bytes = COALESCE((
        SELECT SUM(blobs.stored_size_bytes)
        FROM artifacts JOIN blobs ON blobs.id = artifacts.blob_id
        WHERE artifacts.snapshot_id = snapshots.id
    ), 0);

UPDATE uploads
SET total_original_size_bytes = declared_original_size_bytes,
    total_stored_size_bytes = declared_stored_size_bytes,
    transfer_bytes = CASE
        WHEN status = 'completed' THEN declared_stored_size_bytes
        ELSE 0
    END;

UPDATE upload_audits
SET total_original_size_bytes = declared_original_size_bytes,
    total_stored_size_bytes = declared_stored_size_bytes;
