# Separate semantic snapshot identity from storage representation

Patwari identifies a snapshot within a session from stable capture context (project, repository,
branch, source-agent version, and artifact-set version) and the canonical logical-path-ordered set
of verified original artifact content. Stored compressed bytes are represented by owner-scoped blobs.
Tracking original and stored hashes separately allows compression settings and repeated captures to
change without inventing new session states, while still supporting exact transfer verification and
blob deduplication. Client/capture IDs, source and server times, source cursor/state hash, Munshi
version, opaque source metadata, and transfer metrics are provenance rather than snapshot identity.

Deletion does not make a semantic fingerprint reusable as the same historical resource: it records a
Tombstone for the deleted snapshot ID. A later capture of the same fingerprint creates a new snapshot
ID linked to that Tombstone, while Blob liveness remains derived from live Artifact relationships
rather than semantic identity or a cached count.
