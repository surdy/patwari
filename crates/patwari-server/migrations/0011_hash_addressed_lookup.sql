-- Hash-addressed artifact lookup (ADR 0004) resolves a content hash to the
-- artifacts carrying it without walking sessions and snapshots. The original
-- digest lives on the artifact row; the stored digest lives on the shared,
-- per-owner deduplicated blob. Back both equality filters with an index so the
-- resolution is a keyed lookup rather than a scan.
CREATE INDEX artifacts_original_sha256_idx ON artifacts(original_sha256);
CREATE INDEX blobs_stored_sha256_idx ON blobs(stored_sha256);
-- The stored-hash filter drives from blobs into artifacts; without a keyed
-- blob_id path the planner falls back to scanning every artifact row.
CREATE INDEX artifacts_blob_id_idx ON artifacts(blob_id);
