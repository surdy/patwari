# Separate uploads from snapshots

Uploads are mutable, resumable transfer attempts that can fail or expire; snapshots are immutable,
self-contained states that exist only after verification. Keeping them separate preserves failed-attempt
provenance without treating incomplete data as archived and allows concurrent or repeated uploads to
resolve to the same completed snapshot.

For the v1 multi-artifact protocol, an upload records a server-assigned fixed chunk layout for each
canonical artifact index. Accepted chunk bytes and metadata are durable only while the upload is
active; completion verifies every gap-free assembled artifact before one transaction creates the
snapshot and its complete artifact set. Abandonment and expiry remove partial content and chunk
detail while retaining a compact redacted terminal audit record, so failed transfer provenance never
becomes archived artifact content.
