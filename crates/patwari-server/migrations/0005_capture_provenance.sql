-- A capture is durable provenance for one successfully verified client
-- observation. Uploads remain mutable transfer attempts and terminal upload
-- audits remain compact.
ALTER TABLE uploads ADD COLUMN capture_id TEXT NOT NULL DEFAULT '';
UPDATE uploads SET capture_id = idempotency_key WHERE capture_id = '';
CREATE UNIQUE INDEX uploads_owner_client_capture_id_idx
    ON uploads(owner_namespace, client_id, capture_id);

-- Existing terminal audits may predate capture IDs. New terminal audits retain
-- the opaque ID and canonical manifest digest without retaining manifests,
-- paths, chunk checksums, or content.
ALTER TABLE upload_audits ADD COLUMN capture_id TEXT NOT NULL DEFAULT '';
ALTER TABLE upload_audits ADD COLUMN manifest_sha256 TEXT;
CREATE INDEX upload_audits_capture_idx
    ON upload_audits(owner_namespace, client_id, capture_id, terminal_at);

-- Version zero identifies fingerprints produced before artifact_set_version
-- became a required stable capture-context field. Bootstrap upgrades them
-- before accepting requests while preserving snapshot IDs and manifest hashes.
ALTER TABLE snapshots ADD COLUMN fingerprint_version INTEGER NOT NULL DEFAULT 0
    CHECK (fingerprint_version >= 0);

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
    UNIQUE (owner_namespace, client_id, capture_id)
);

CREATE INDEX captures_session_idx ON captures(session_id);
CREATE INDEX captures_snapshot_idx ON captures(snapshot_id);
CREATE INDEX captures_client_capture_idx ON captures(client_id, capture_id);
