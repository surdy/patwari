# Look up artifacts by original content hash

Patwari's artifact listing accepts equality filters on `original_sha256` and `stored_sha256`, backed
by supporting indexes, alongside the existing snapshot and session filters. Every other lookup path
remains UUID-based; the filter adds a resolution step from a content hash to the artifacts that carry
it, so a client holding only a hash — for example a Munshi claim ticket embedded in a summary — can
reach the artifact metadata and content URL without walking sessions and snapshots.

A hash filter is identity, not interpretation: Patwari still never searches within content, ranks
results, or decompresses blobs for readers. Downloads continue to return stored bytes with both
representations described in headers, and clients verify and decompress locally. Because blobs are
deduplicated per owner, one hash may resolve to multiple artifacts across snapshots; the listing
returns all of them and the client chooses, typically the newest.
