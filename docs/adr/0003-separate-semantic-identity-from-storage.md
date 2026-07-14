# Separate semantic snapshot identity from storage representation

Patwari identifies a snapshot from stable capture context and the canonical logical-path-ordered set
of verified original artifact content, while stored compressed bytes are represented by owner-scoped
blobs. Tracking original and stored hashes separately allows compression settings and repeated
captures to change without inventing new session states, while still supporting exact transfer
verification and blob deduplication.
