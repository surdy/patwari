-- RFC 3339 timestamps have a variable-precision fractional-second
-- component (for example `.12Z` sorts after `.123Z` as TEXT even though it
-- is chronologically earlier), so comparing the receipt/API text does not
-- preserve chronological order for keyset pagination or activity filters.
-- Every table that participates in newest-first pagination gets an
-- immutable numeric ordering key: signed 64-bit microseconds since the
-- Unix epoch (UTC), paired with each row's already-monotonic UUIDv7 id as
-- the tie-breaker. The RFC 3339 text columns and receipts are unchanged;
-- zero is a safe "not yet backfilled" sentinel because no row is ever
-- completed at the Unix epoch.
ALTER TABLE snapshots ADD COLUMN completed_at_seq INTEGER NOT NULL DEFAULT 0
    CHECK (completed_at_seq >= 0);
ALTER TABLE captures ADD COLUMN server_completed_at_seq INTEGER NOT NULL DEFAULT 0
    CHECK (server_completed_at_seq >= 0);
ALTER TABLE artifacts ADD COLUMN created_at_seq INTEGER NOT NULL DEFAULT 0
    CHECK (created_at_seq >= 0);

-- This projection is fully rebuilt by one set-based statement at every
-- startup (see rebuild_session_latest_context) and transactionally
-- maintained on every completion, so it carries the same key but needs no
-- historical backfill of its own.
ALTER TABLE session_latest_context ADD COLUMN completed_at_seq INTEGER NOT NULL DEFAULT 0
    CHECK (completed_at_seq >= 0);

-- Replace the TEXT-ordered indexes with numeric-ordered ones so both the
-- query planner and the returned row order match true chronological order.
DROP INDEX snapshots_visible_completed_idx;
CREATE INDEX snapshots_visible_completed_idx
    ON snapshots(completed_at_seq DESC, id DESC)
    WHERE deleted_at IS NULL;
DROP INDEX snapshots_session_visible_completed_idx;
CREATE INDEX snapshots_session_visible_completed_idx
    ON snapshots(session_id, completed_at_seq DESC, id DESC)
    WHERE deleted_at IS NULL;
DROP INDEX captures_session_completed_idx;
CREATE INDEX captures_session_completed_idx
    ON captures(session_id, server_completed_at_seq DESC, id DESC);
DROP INDEX captures_completed_idx;
CREATE INDEX captures_completed_idx
    ON captures(server_completed_at_seq DESC, id DESC);
DROP INDEX artifacts_created_idx;
CREATE INDEX artifacts_created_idx
    ON artifacts(created_at_seq DESC, id DESC);
DROP INDEX session_latest_context_activity_idx;
CREATE INDEX session_latest_context_activity_idx
    ON session_latest_context(completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_source_agent_activity_idx;
CREATE INDEX session_latest_context_source_agent_activity_idx
    ON session_latest_context(source_agent, completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_repository_activity_idx;
CREATE INDEX session_latest_context_repository_activity_idx
    ON session_latest_context(repository, completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_project_activity_idx;
CREATE INDEX session_latest_context_project_activity_idx
    ON session_latest_context(project, completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_branch_activity_idx;
CREATE INDEX session_latest_context_branch_activity_idx
    ON session_latest_context(branch, completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_agent_version_activity_idx;
CREATE INDEX session_latest_context_agent_version_activity_idx
    ON session_latest_context(source_agent_version, completed_at_seq DESC, session_id DESC);
DROP INDEX session_latest_context_artifact_set_version_activity_idx;
CREATE INDEX session_latest_context_artifact_set_version_activity_idx
    ON session_latest_context(artifact_set_version, completed_at_seq DESC, session_id DESC);
