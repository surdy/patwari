ALTER TABLE uploads ADD COLUMN chunk_size_bytes INTEGER NOT NULL DEFAULT 4194304
    CHECK (chunk_size_bytes > 0);
ALTER TABLE uploads ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0
    CHECK (chunk_count >= 0);
ALTER TABLE uploads ADD COLUMN declared_stored_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (declared_stored_size_bytes >= 0);
ALTER TABLE uploads ADD COLUMN declared_original_size_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (declared_original_size_bytes >= 0);
ALTER TABLE uploads ADD COLUMN expires_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z';

CREATE TABLE upload_chunks (
    upload_id TEXT NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
    artifact_index INTEGER NOT NULL CHECK (artifact_index = 0),
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    byte_length INTEGER NOT NULL CHECK (byte_length >= 0),
    sha256 TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY (upload_id, artifact_index, chunk_index)
);

CREATE INDEX upload_chunks_upload_idx ON upload_chunks(upload_id, artifact_index, chunk_index);
CREATE INDEX uploads_expiry_idx ON uploads(expires_at);

-- This table deliberately holds only redacted terminal audit facts. In
-- particular it has no manifest JSON/hash, chunk hash, request body, or path.
CREATE TABLE upload_audits (
    upload_id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL,
    client_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    declared_original_size_bytes INTEGER NOT NULL CHECK (declared_original_size_bytes >= 0),
    declared_stored_size_bytes INTEGER NOT NULL CHECK (declared_stored_size_bytes >= 0),
    chunk_size_bytes INTEGER NOT NULL CHECK (chunk_size_bytes > 0),
    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
    created_at TEXT NOT NULL,
    terminal_at TEXT NOT NULL,
    terminal_reason TEXT NOT NULL CHECK (terminal_reason IN ('abandoned', 'expired')),
    error_code TEXT NOT NULL
);

CREATE INDEX upload_audits_terminal_idx ON upload_audits(terminal_at);
