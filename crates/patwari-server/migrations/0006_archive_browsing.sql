-- A completed snapshot is the visibility boundary for archive browsing.
-- Deletion is intentionally not implemented yet; this nullable hook lets
-- every normal read exclude a future tombstoned snapshot without reshaping
-- immutable history.
ALTER TABLE snapshots ADD COLUMN deleted_at TEXT;

-- This is a rebuildable projection, not mutable session history. Each row
-- carries the stable context of one session's latest visible completed
-- snapshot. Startup backfill rebuilds it from immutable manifests and
-- completion updates it in the same transaction as its capture provenance.
CREATE TABLE session_latest_context (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id),
    snapshot_id TEXT NOT NULL UNIQUE REFERENCES snapshots(id),
    completed_at TEXT NOT NULL,
    source_agent TEXT NOT NULL,
    project TEXT,
    repository TEXT,
    branch TEXT,
    source_agent_version TEXT,
    artifact_set_version INTEGER NOT NULL CHECK (artifact_set_version > 0)
);

CREATE INDEX snapshots_visible_completed_idx
    ON snapshots(completed_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX snapshots_session_visible_completed_idx
    ON snapshots(session_id, completed_at DESC, id DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX captures_snapshot_client_idx ON captures(snapshot_id, client_id);
CREATE INDEX captures_session_completed_idx
    ON captures(session_id, server_completed_at DESC, id DESC);
CREATE INDEX captures_completed_idx
    ON captures(server_completed_at DESC, id DESC);
CREATE INDEX captures_manifest_idx ON captures(manifest_id);
CREATE INDEX artifacts_created_idx ON artifacts(created_at DESC, id DESC);
CREATE INDEX session_latest_context_activity_idx
    ON session_latest_context(completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_source_agent_activity_idx
    ON session_latest_context(source_agent, completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_repository_activity_idx
    ON session_latest_context(repository, completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_project_activity_idx
    ON session_latest_context(project, completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_branch_activity_idx
    ON session_latest_context(branch, completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_agent_version_activity_idx
    ON session_latest_context(source_agent_version, completed_at DESC, session_id DESC);
CREATE INDEX session_latest_context_artifact_set_version_activity_idx
    ON session_latest_context(artifact_set_version, completed_at DESC, session_id DESC);
