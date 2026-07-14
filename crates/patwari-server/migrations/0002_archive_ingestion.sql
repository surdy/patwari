CREATE TABLE clients (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    hostname TEXT,
    display_name TEXT,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    source_agent TEXT NOT NULL,
    source_session_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (owner_namespace, source_agent, source_session_id)
);

CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    client_id TEXT NOT NULL REFERENCES clients(id),
    idempotency_key TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'artifact_uploaded', 'completed')),
    snapshot_id TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (owner_namespace, client_id, idempotency_key)
);

CREATE TABLE manifests (
    id TEXT PRIMARY KEY,
    upload_id TEXT NOT NULL UNIQUE REFERENCES uploads(id),
    canonical_json TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE blobs (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    stored_sha256 TEXT NOT NULL,
    stored_size_bytes INTEGER NOT NULL CHECK (stored_size_bytes >= 0),
    compression TEXT NOT NULL CHECK (compression IN ('identity', 'zstd')),
    created_at TEXT NOT NULL,
    UNIQUE (owner_namespace, stored_sha256)
);

CREATE TABLE snapshots (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    manifest_id TEXT NOT NULL REFERENCES manifests(id),
    fingerprint_sha256 TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    UNIQUE (session_id, fingerprint_sha256)
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
    blob_id TEXT NOT NULL REFERENCES blobs(id),
    logical_path TEXT NOT NULL,
    media_type TEXT,
    original_size_bytes INTEGER NOT NULL CHECK (original_size_bytes >= 0),
    original_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (snapshot_id, logical_path)
);

CREATE INDEX uploads_session_idx ON uploads(session_id);
CREATE INDEX uploads_snapshot_idx ON uploads(snapshot_id);
CREATE INDEX snapshots_session_idx ON snapshots(session_id);
CREATE INDEX artifacts_snapshot_idx ON artifacts(snapshot_id);
