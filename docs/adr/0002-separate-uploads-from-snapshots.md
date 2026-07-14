# Separate uploads from snapshots

Uploads are mutable, resumable transfer attempts that can fail or expire; snapshots are immutable,
self-contained states that exist only after verification. Keeping them separate preserves failed-attempt
provenance without treating incomplete data as archived and allows concurrent or repeated uploads to
resolve to the same completed snapshot.
