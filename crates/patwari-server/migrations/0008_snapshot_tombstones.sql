-- Snapshot fingerprints are unique only while a snapshot is live. SQLite
-- cannot remove the original table-level UNIQUE constraint in place, so
-- rebuild the small set of tables that reference snapshots while foreign keys
-- remain enabled. The backup tables intentionally have no constraints; they
-- exist only inside this migration transaction.
CREATE TABLE artifacts_v8_backup AS
SELECT
    id, snapshot_id, blob_id, logical_path, media_type, original_size_bytes,
    original_sha256, created_at, artifact_index, created_at_seq
FROM artifacts;

CREATE TABLE captures_v8_backup AS
SELECT
    id, owner_namespace, capture_id, session_id, client_id, upload_id,
    manifest_id, snapshot_id, source_captured_at, source_cursor,
    source_state_hash, source_metadata_json, project, repository, branch,
    source_agent_version, artifact_set_version, munshi_version,
    server_received_at, server_completed_at, server_completed_at_seq
FROM captures;

DROP TABLE artifacts;
DROP TABLE captures;
DROP TABLE session_latest_context;

CREATE TABLE snapshots_v8 (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    manifest_id TEXT NOT NULL REFERENCES manifests(id),
    fingerprint_sha256 TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    artifact_count INTEGER NOT NULL CHECK (artifact_count >= 1),
    total_original_size_bytes INTEGER NOT NULL CHECK (total_original_size_bytes >= 0),
    total_stored_size_bytes INTEGER NOT NULL CHECK (total_stored_size_bytes >= 0),
    fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version >= 0),
    deleted_at TEXT,
    completed_at_seq INTEGER NOT NULL CHECK (completed_at_seq >= 0)
);

INSERT INTO snapshots_v8 (
    id, owner_namespace, session_id, manifest_id, fingerprint_sha256,
    completed_at, artifact_count, total_original_size_bytes,
    total_stored_size_bytes, fingerprint_version, deleted_at, completed_at_seq
)
SELECT
    id, owner_namespace, session_id, manifest_id, fingerprint_sha256,
    completed_at, artifact_count, total_original_size_bytes,
    total_stored_size_bytes, fingerprint_version, deleted_at, completed_at_seq
FROM snapshots;

DROP TABLE snapshots;
ALTER TABLE snapshots_v8 RENAME TO snapshots;

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
    blob_id TEXT NOT NULL REFERENCES blobs(id),
    logical_path TEXT NOT NULL,
    media_type TEXT,
    original_size_bytes INTEGER NOT NULL CHECK (original_size_bytes >= 0),
    original_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    artifact_index INTEGER NOT NULL CHECK (artifact_index >= 0),
    created_at_seq INTEGER NOT NULL CHECK (created_at_seq >= 0),
    UNIQUE (snapshot_id, logical_path)
);

INSERT INTO artifacts (
    id, snapshot_id, blob_id, logical_path, media_type, original_size_bytes,
    original_sha256, created_at, artifact_index, created_at_seq
)
SELECT
    id, snapshot_id, blob_id, logical_path, media_type, original_size_bytes,
    original_sha256, created_at, artifact_index, created_at_seq
FROM artifacts_v8_backup;
DROP TABLE artifacts_v8_backup;

CREATE TABLE captures (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    capture_id TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    upload_id TEXT NOT NULL UNIQUE REFERENCES uploads(id),
    manifest_id TEXT NOT NULL REFERENCES manifests(id),
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
    source_captured_at TEXT NOT NULL,
    source_cursor TEXT,
    source_state_hash TEXT,
    source_metadata_json TEXT NOT NULL,
    project TEXT,
    repository TEXT,
    branch TEXT,
    source_agent_version TEXT,
    artifact_set_version INTEGER NOT NULL CHECK (artifact_set_version > 0),
    munshi_version TEXT,
    server_received_at TEXT NOT NULL,
    server_completed_at TEXT NOT NULL,
    server_completed_at_seq INTEGER NOT NULL CHECK (server_completed_at_seq >= 0),
    UNIQUE (owner_namespace, client_id, capture_id)
);

INSERT INTO captures (
    id, owner_namespace, capture_id, session_id, client_id, upload_id,
    manifest_id, snapshot_id, source_captured_at, source_cursor,
    source_state_hash, source_metadata_json, project, repository, branch,
    source_agent_version, artifact_set_version, munshi_version,
    server_received_at, server_completed_at, server_completed_at_seq
)
SELECT
    id, owner_namespace, capture_id, session_id, client_id, upload_id,
    manifest_id, snapshot_id, source_captured_at, source_cursor,
    source_state_hash, source_metadata_json, project, repository, branch,
    source_agent_version, artifact_set_version, munshi_version,
    server_received_at, server_completed_at, server_completed_at_seq
FROM captures_v8_backup;
DROP TABLE captures_v8_backup;

-- This is a rebuildable projection. Bootstrap rebuilds it immediately after
-- migration from the preserved immutable snapshot/capture history.
CREATE TABLE session_latest_context (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id),
    snapshot_id TEXT NOT NULL UNIQUE REFERENCES snapshots(id),
    completed_at TEXT NOT NULL,
    source_agent TEXT NOT NULL,
    project TEXT,
    repository TEXT,
    branch TEXT,
    source_agent_version TEXT,
    artifact_set_version INTEGER NOT NULL CHECK (artifact_set_version > 0),
    completed_at_seq INTEGER NOT NULL CHECK (completed_at_seq >= 0)
);

CREATE INDEX snapshots_session_idx ON snapshots(session_id);
CREATE UNIQUE INDEX snapshots_session_live_fingerprint_idx
    ON snapshots(session_id, fingerprint_sha256)
    WHERE deleted_at IS NULL;
CREATE INDEX snapshots_visible_completed_idx
    ON snapshots(completed_at_seq DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX snapshots_session_visible_completed_idx
    ON snapshots(session_id, completed_at_seq DESC, id DESC)
    WHERE deleted_at IS NULL;

CREATE INDEX artifacts_snapshot_idx ON artifacts(snapshot_id);
CREATE UNIQUE INDEX artifacts_snapshot_artifact_index_idx
    ON artifacts(snapshot_id, artifact_index);
CREATE INDEX artifacts_created_idx ON artifacts(created_at_seq DESC, id DESC);

-- `deleted_at` existed as a pre-deletion visibility hook in v6/v7. If an
-- operator used it before this migration, its Artifact relationships are no
-- longer live and must not retain Blob metadata indefinitely.
DELETE FROM artifacts
WHERE snapshot_id IN (
    SELECT id FROM snapshots WHERE deleted_at IS NOT NULL
);

CREATE INDEX captures_session_idx ON captures(session_id);
CREATE INDEX captures_snapshot_idx ON captures(snapshot_id);
CREATE INDEX captures_client_capture_idx ON captures(client_id, capture_id);
CREATE INDEX captures_snapshot_client_idx ON captures(snapshot_id, client_id);
CREATE INDEX captures_session_completed_idx
    ON captures(session_id, server_completed_at_seq DESC, id DESC);
CREATE INDEX captures_completed_idx
    ON captures(server_completed_at_seq DESC, id DESC);
CREATE INDEX captures_manifest_idx ON captures(manifest_id);

CREATE INDEX session_latest_context_activity_idx
    ON session_latest_context(completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_source_agent_activity_idx
    ON session_latest_context(source_agent, completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_repository_activity_idx
    ON session_latest_context(repository, completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_project_activity_idx
    ON session_latest_context(project, completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_branch_activity_idx
    ON session_latest_context(branch, completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_agent_version_activity_idx
    ON session_latest_context(source_agent_version, completed_at_seq DESC, session_id DESC);
CREATE INDEX session_latest_context_artifact_set_version_activity_idx
    ON session_latest_context(artifact_set_version, completed_at_seq DESC, session_id DESC);

ALTER TABLE blobs ADD COLUMN orphaned_at TEXT;
ALTER TABLE blobs ADD COLUMN eligible_after TEXT;
ALTER TABLE blobs ADD COLUMN eligible_after_seq INTEGER
    CHECK (eligible_after_seq IS NULL OR eligible_after_seq >= 0);
CREATE INDEX blobs_gc_eligible_idx
    ON blobs(eligible_after_seq, id)
    WHERE eligible_after_seq IS NOT NULL;

-- Tombstones and their audit events retain only identity, receipt-scale
-- integrity evidence, deletion timing, and an optional bounded reason. In
-- particular, neither table records artifact paths, artifact metadata, or
-- artifact bytes.
CREATE TABLE tombstones (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    snapshot_id TEXT NOT NULL UNIQUE REFERENCES snapshots(id),
    snapshot_fingerprint_sha256 TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    snapshot_completed_at TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    deleted_at_seq INTEGER NOT NULL CHECK (deleted_at_seq >= 0),
    reason TEXT CHECK (reason IS NULL OR length(reason) <= 512),
    rearchived_snapshot_id TEXT UNIQUE REFERENCES snapshots(id)
);

CREATE INDEX tombstones_session_deleted_idx
    ON tombstones(session_id, deleted_at_seq DESC, id DESC);
CREATE INDEX tombstones_fingerprint_idx
    ON tombstones(session_id, snapshot_fingerprint_sha256, deleted_at_seq DESC, id DESC);

CREATE TABLE deletion_audits (
    id TEXT PRIMARY KEY,
    tombstone_id TEXT NOT NULL UNIQUE REFERENCES tombstones(id),
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    snapshot_id TEXT NOT NULL UNIQUE REFERENCES snapshots(id),
    snapshot_fingerprint_sha256 TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    occurred_at_seq INTEGER NOT NULL CHECK (occurred_at_seq >= 0),
    reason TEXT CHECK (reason IS NULL OR length(reason) <= 512)
);

CREATE INDEX deletion_audits_occurred_idx
    ON deletion_audits(occurred_at_seq DESC, id DESC);

ALTER TABLE snapshots
    ADD COLUMN rearchived_from_tombstone_id TEXT REFERENCES tombstones(id);
