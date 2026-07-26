# Keep blobs plaintext-verifiable; no client-side encryption in v1

Patwari's integrity story depends on seeing original content: ingestion decompresses stored blobs to
verify the declared original sha256 before issuing a receipt, and integrity scans repeat that
verification for the life of the archive. Client-side encryption, considered in the planning handoff,
would blind both — the server could vouch only for ciphertext, and a receipt would no longer prove
that the original session bytes were received intact.

v1 therefore stores unencrypted (identity or zstd) blobs and keeps end-to-end original-content
verification, consistent with the trusted-LAN, no-authentication posture: anyone who can reach the
server can already read the archive, so application-layer encryption adds key-management risk without
a matching threat model. Encryption at rest belongs to the host volume. If Patwari ever crosses the
trusted network boundary, encryption is revisited together with authentication, not before it.
