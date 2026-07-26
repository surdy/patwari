# Patwari Archive

Patwari preserves verified, immutable captures of coding-agent sessions. It owns archival identity,
provenance, and integrity; it does not interpret session content or curate human-readable knowledge.

## Language

**Owner**:
The archive namespace within which sessions and stored content are identified and deduplicated. Patwari
has one owner initially, even before authentication or multiple owners are supported.
_Avoid_: Account, user, tenant

**Client**:
A persistent Munshi installation that submits captures to Patwari. A hostname or display name describes
a client but does not identify it.
_Avoid_: Device, machine, uploader

**Session**:
One open-ended logical coding-agent conversation, identified within an owner by its source agent and
opaque source session ID. Moving the conversation between clients does not create a new session.
_Avoid_: Upload, run, archive

**Capture**:
A client's observation of a session, identified by a client-generated capture ID and source-reported
time/cursor. Successful captures are retained as provenance even when they resolve to the same
snapshot; expired or abandoned transfer attempts are not captures.
_Avoid_: Snapshot, backup

**Upload**:
A resumable attempt to submit one capture's proposed manifest and artifact bytes. An upload may fail,
expire, or resolve to an existing snapshot.
_Avoid_: Snapshot, artifact

**Snapshot**:
An immutable, self-contained, verified session state. A snapshot is client-neutral and may be the result
of multiple captures from different clients.
_Avoid_: Upload, capture, revision

**Capture context**:
The stable source facts that distinguish a snapshot beyond its artifact contents, including repository,
project, branch, source-agent version, and artifact-set version. Capture provenance such as client,
source/server times, source cursor/state hash, Munshi version, and opaque source metadata is excluded.
_Avoid_: Session metadata, upload metadata

**Manifest**:
The canonical, authoritative description of a snapshot's capture context and complete artifact set.
Normalized query data is a projection of the manifest.
_Avoid_: Index, receipt

**Snapshot fingerprint**:
Patwari's canonical identity for a snapshot within a session, derived from verified original artifact
content and stable capture context while excluding capture/client provenance and storage
representation. It is never a client capture ID.
_Avoid_: Source state hash, manifest hash, blob hash

**Artifact**:
One regular byte stream at a unique normalized logical path within a snapshot. An artifact describes
original content and refers to a stored blob.
_Avoid_: Blob, file object, chunk

**Artifact role**:
What an artifact means within its snapshot — transcript, summary, extracted tool output — conveyed by
its logical path under the manifest's artifact-set version. Roles are a client convention; Patwari
records the paths but never branches on them.
_Avoid_: Content kind, media type, artifact type

**Blob**:
A verified stored representation of artifact content, deduplicated within an owner by its stored bytes.
Different snapshots and artifacts may refer to the same blob.
_Avoid_: Artifact, snapshot

**Receipt**:
A reproducible, versioned document proving that Patwari accepted and verified a snapshot. It identifies
the issuing archive instance but is not an independently mutable record. It is snapshot-level, so
per-upload transfer metrics belong in a completion envelope rather than the receipt.
_Avoid_: Certificate, snapshot

**Tombstone**:
The durable, minimal record that an archived Snapshot was explicitly deleted, including its identity,
canonical manifest hash, server deletion time, and linkage to any later rearchive. Artifact
relationships are removed at deletion; an unreferenced Blob becomes only a grace-delayed candidate
whose live relationship rows remain authoritative. Re-archiving the same state creates a new Snapshot
linked to the Tombstone rather than silently reviving the deleted ID.
_Avoid_: Soft delete, archived snapshot

**Integrity run**:
One immutable server-time maintenance observation of archive health. It records bounded aggregate
counts and owns its time-stamped findings; it does not alter Snapshot completion or receipts.
_Avoid_: Repair job, snapshot verification status

**Integrity finding**:
A time-stamped observation of a snapshot or blob's current health. It does not rewrite the historical
fact that a snapshot passed verification when it was completed. Findings belong to an immutable
integrity scan run and current health is projected from the latest completed run; earlier findings
remain history.
_Avoid_: Snapshot status, upload failure
