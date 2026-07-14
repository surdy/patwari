-- Integrity observations are append-only maintenance history. They are
-- intentionally separate from immutable snapshot completion evidence: a
-- later scan can report current storage health without rewriting a receipt.
CREATE TABLE integrity_runs (
    id TEXT PRIMARY KEY,
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    started_at TEXT NOT NULL,
    started_at_seq INTEGER NOT NULL CHECK (started_at_seq >= 0),
    completed_at TEXT,
    completed_at_seq INTEGER CHECK (completed_at_seq IS NULL OR completed_at_seq >= 0),
    status TEXT NOT NULL CHECK (
        status IN ('running', 'healthy', 'action_required', 'failed')
    ),
    finding_count INTEGER NOT NULL DEFAULT 0 CHECK (finding_count >= 0),
    info_count INTEGER NOT NULL DEFAULT 0 CHECK (info_count >= 0),
    warning_count INTEGER NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
    error_count INTEGER NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    CHECK (
        (completed_at IS NULL AND completed_at_seq IS NULL AND status = 'running')
        OR
        (completed_at IS NOT NULL AND completed_at_seq IS NOT NULL AND status != 'running')
    )
);

CREATE INDEX integrity_runs_latest_idx
    ON integrity_runs(owner_namespace, completed_at_seq DESC, id DESC)
    WHERE completed_at_seq IS NOT NULL;

-- Target IDs deliberately have no foreign-key constraints. A finding must
-- remain readable after blob GC, and a foreign-key violation is itself an
-- integrity condition that may refer to a missing target row.
CREATE TABLE integrity_findings (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES integrity_runs(id),
    owner_namespace TEXT NOT NULL REFERENCES archive_metadata(owner_namespace),
    kind TEXT NOT NULL CHECK (length(kind) BETWEEN 1 AND 96),
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
    snapshot_id TEXT,
    artifact_id TEXT,
    blob_id TEXT,
    detected_at TEXT NOT NULL,
    detected_at_seq INTEGER NOT NULL CHECK (detected_at_seq >= 0),
    detail_code TEXT NOT NULL CHECK (length(detail_code) BETWEEN 1 AND 128)
);

CREATE INDEX integrity_findings_run_idx
    ON integrity_findings(run_id, detected_at_seq ASC, id ASC);
CREATE INDEX integrity_findings_snapshot_idx
    ON integrity_findings(snapshot_id, detected_at_seq DESC, id DESC)
    WHERE snapshot_id IS NOT NULL;
CREATE INDEX integrity_findings_blob_idx
    ON integrity_findings(blob_id, detected_at_seq DESC, id DESC)
    WHERE blob_id IS NOT NULL;
