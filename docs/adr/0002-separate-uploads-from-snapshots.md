# Separate uploads from snapshots

Uploads are mutable, resumable transfer attempts that can fail or expire; snapshots are immutable,
self-contained states that exist only after verification. Keeping them separate preserves failed-attempt
provenance without treating incomplete data as archived and allows concurrent or repeated uploads to
resolve to the same completed snapshot.

For the initial single-artifact protocol, an upload records a server-assigned fixed chunk layout.
Accepted chunk bytes and metadata are durable only while the upload is active; completion verifies a
gap-free assembled artifact before creating a snapshot. Abandonment and expiry remove partial content
and chunk detail while retaining a compact redacted terminal audit record, so failed transfer
provenance never becomes archived artifact content.
