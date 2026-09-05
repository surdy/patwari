# Patwari domain model and product decisions

Why the archive is shaped the way it is: what it promises, what it refuses, which tool owns which
job, and the objects it stores. The wire-level contract is in [`docs/api.md`](api.md); the canonical
vocabulary is in [`CONTEXT.md`](../CONTEXT.md); individual trade-offs are recorded in
[`docs/adr/`](adr/).

## Product decisions

| Area | Decision |
| --- | --- |
| Purpose | Archive complete coding-agent sessions |
| Summaries | Remain in Notesmith; Patwari does not store them |
| User interface | API and CLI integrations only; no web UI in v1 |
| Artifact visibility | Server-readable compressed files |
| Encryption | No application-level encryption initially |
| Authentication | None initially; deployment must remain inside a trusted network boundary |
| Initial deployment | Single-user container on a private network (see [`self-hosting.md`](self-hosting.md)) |
| Metadata store | SQLite |
| Blob store | Dedicated local filesystem volume |
| Additional backups | Patwari creates verified archive-only backup sets; external filesystem replication is the durability boundary |
| Archive representation | Versioned manifest plus individually compressed files |
| Source cleanup | Manual, previewable Munshi command only |
| Initial source | GitHub Copilot CLI through Munshi |

## Goals

- Accept immutable, versioned snapshots of logical coding-agent sessions.
- Retain every successfully verified client capture as durable provenance, even when repeated
  captures resolve to one semantic snapshot.
- Preserve every declared artifact with size and checksum verification.
- Make upload creation and completion retries idempotent.
- Expose stable metadata and artifact APIs for Munshi and future analysis tools.
- Keep agent-specific interpretation in Munshi source adapters.
- Return a durable archival receipt only after a complete snapshot has been verified.
- Support exact artifact download and independent integrity verification.
- Keep the storage layout operable with normal filesystem backup tools.

## Non-goals for v1

- Storing or rendering Markdown summaries.
- Replacing Notesmith as the knowledge store.
- Searching transcript contents inside Patwari.
- Generating learnings, patterns, or skills inside the server.
- Restoring sessions into an installed coding agent.
- Automatically deleting source sessions.
- A browser interface.
- Multi-user accounts, quotas, or application-level authorization.
- Client-side encryption.
- PostgreSQL, S3, or distributed operation.

Patwari supplies archives to future analysis tools; it does not become an agent or knowledge
extraction system itself.

## Responsibility boundary

### Munshi

- Discovers a source session and its files.
- Produces stable logical paths and agent-specific metadata.
- Takes a consistent capture of files that may still be changing.
- Compresses each artifact, initially with Zstandard.
- Computes checksums and creates the versioned manifest.
- Uploads or resumes the snapshot.
- Records Patwari IDs and archival receipts.
- Downloads, decompresses, and interprets artifacts.
- Eventually performs agent-specific restore validation.
- Offers a manual, previewable prune command after successful archival.

### Patwari

- Owns remote session and snapshot identity.
- Validates manifests and upload state.
- Negotiates fixed chunks and streams every declared artifact to temporary storage with bounded
  resource use.
- Verifies stored and decompressed original sizes and checksums.
- Atomically promotes verified blobs into immutable storage.
- Indexes normalized metadata without interpreting transcript contents.
- Returns manifests, snapshots, focused capture provenance, and artifacts.
- Records archival and deletion events.
- Never claims that an artifact is restorable by a particular agent.

### Qanungo

- Reads the archive through the public v1 API as an ordinary consumer.
- Pages sessions, snapshots, captures, and manifests with the returned opaque cursors.
- Downloads and decompresses artifacts to mine transcripts for patterns and findings.
- Interprets session content — the interpretation Patwari deliberately does not do.
- Has no privileged access, no write path, and is not a runtime dependency of Patwari.

### Notesmith

- Stores the latest curated Markdown summary.
- May include a `patwari_session_id`, snapshot ID, or stable Patwari URI.
- Is not a runtime dependency of Patwari.

## Domain model

```text
Client
  |
  +-- Upload (mutable transfer attempt)
        |
        +-- Capture (successful durable provenance)
              |
              +-- Session
                    |
                    +-- Snapshot
                          |
                          +-- Artifact
                          +-- Artifact
                          +-- Archival receipt
```

### Client

Identifies the Munshi installation that uploaded data. This is operational identity, not an
authentication principal.

```text
id (client-generated UUID)
hostname (mutable)
display_name (mutable)
metadata (mutable)
created_at
updated_at
```

### Session

A logical coding-agent conversation that may be resumed and captured more than once.

The natural key is:

```text
source_agent + source_session_id
```

Repository, branch, machine, and timestamps are attributes rather than identity components.

```text
id
source_agent
source_session_id
created_at
updated_at
```

Stable capture context belongs to the canonical manifest rather than mutable session identity.
Project, repository, branch, source-agent version, and artifact-set version participate in snapshot
identity. Source time, source cursor/state hash, Munshi version, and selected opaque source metadata
are retained as capture provenance instead.

For browsing, Patwari maintains a rebuildable `latest completed snapshot` projection per session.
It is updated in the same transaction that records successful capture provenance and is rebuilt from
immutable snapshots and manifests during startup. It is a query accelerator, not a rewrite of
historical context: older snapshot and capture responses always retain their own immutable values.

### Capture and upload

A `capture_id` is generated by the client for one source observation and is distinct from the
server-derived snapshot fingerprint. An upload is a resumable transfer attempt for that capture; it
can expire or be abandoned without creating a capture record. Only successful completion atomically
creates a capture linked to its owner, client, session, upload, immutable manifest, and resulting
snapshot.

```text
capture_id
client_id
session_id
upload_id
snapshot_id
manifest_id
source_captured_at
source_cursor
source_state_hash
source_metadata
munshi_version
server_received_at
server_completed_at
```

Repeated or concurrent captures from distinct clients may link to the same session-scoped snapshot,
but each successful observation retains its own provenance.

### Snapshot

An immutable, internally consistent verified state of a session. Source cursor or source state
identifies capture provenance rather than snapshot identity.

```text
id
session_id
snapshot_fingerprint
manifest_hash
completed_at
```

Only completed snapshots exist. Uploads are separate mutable transfer attempts, while snapshots,
their canonical manifests, and their artifacts are immutable after verification.

An enabled administrative deletion creates a durable Tombstone and a separate deletion audit event,
then marks the snapshot tombstoned and removes its `Artifact` relationships in one SQLite
transaction. Normal reads exclude the tombstoned snapshot, its captures, manifests, and artifact
resources. Captures and canonical manifests remain linked as internal history; the admin Tombstone
representation deliberately exposes only receipt-scale identity/integrity facts and a capture count,
never artifact paths or content. If the same fingerprint is archived later, a new snapshot ID is
created and linked to the prior Tombstone; the deleted ID is never revived. Deleting the latest
snapshot transactionally projects the next newest live snapshot, or clears the session from normal
browsing when none remains.

### Artifact

One regular byte stream belonging to a snapshot. A snapshot contains an ordered, canonical set of
artifacts, sorted by logical path.

```text
id
snapshot_id
logical_path
media_type
original_size_bytes
original_sha256
blob_id
```

Logical paths are portable normalized relative paths and unique case-insensitively within a snapshot.
They use only ASCII letters, digits, `.`, `_`, `-`, and `/`; empty, traversal, absolute, drive,
device-name, trailing-dot/space, backslash, and non-regular-file notions are rejected. Inputs are
byte streams only: the API has no file-kind, symlink, device, or filesystem-object field.

### Blob

A blob is the stored representation of an artifact. It records the stored SHA-256, stored size, and
compression. Blob bytes are content-addressed on disk and deduplicated within an owner; the artifact
retains the original-content SHA-256 and size. A stored digest whose size or compression conflicts
with its canonical blob metadata is an integrity error, never a deduplication hit.

### Archival receipt and completion

The receipt is deterministic snapshot-level evidence that Patwari received and verified the
declared snapshot. All captures resolving to that snapshot receive byte-for-byte equivalent receipt
fields.

```text
snapshot_id
manifest_hash
snapshot_fingerprint
archive_instance_id
artifact_count
total_original_bytes
total_stored_bytes
completed_at
receipt_version
```

`POST /uploads/{upload_id}/complete` returns a completion envelope with this `receipt`, a separate
per-upload `transfer` object (`upload_transfer_bytes` and
`newly_persisted_physical_bytes`), and the resulting capture provenance. Transfer facts do not
belong in the receipt because valid captures of one state can use different stored representations.
This receipt confirms Patwari integrity only. Filesystem replication and backup remain external
operational responsibilities.

`total_original_bytes` and `total_stored_bytes` are logical sums across the immutable snapshot. A
snapshot response also includes `artifact_count`, `total_original_bytes`, `total_stored_bytes`, and
the focused capture-provenance relation URL.

## Network and trust model

Patwari v1 has no application authentication. Therefore:

- it must not be exposed directly to the public internet;
- its listener is configurable and defaults to loopback;
- deployment must place it behind a private network or an authenticated network boundary;
- administrative deletion endpoints remain disabled by default;
- request bodies, artifact contents, and local paths are not written to normal logs;
- size, count, concurrency, and request-duration limits are mandatory.

Adding authentication later must not change session, snapshot, or artifact identifiers. The schema
should leave room for an owning principal without requiring one in v1.

## Retrieval and analysis access

Programmatic consumers can:

- filter the latest completed session context by source agent, repository, project, branch, client,
  and activity time;
- enumerate complete snapshots, captures, canonical manifests, and artifact metadata;
- stream individual compressed artifacts;
- verify checksums from the archival receipt;
- maintain an exact incremental traversal with returned opaque cursors.

Content search and learning extraction belong in separate tools. Qanungo is that tool: it downloads
and streams archived transcripts, derives findings, and writes curated results elsewhere without
coupling that workflow to Patwari.

## Local cleanup safety

Patwari does not delete source files, and **no local prune command is built**. The shape sketched
here is a requirement, not a shipped interface — Munshi has `archive` commands but no `prune`:

```text
munshi prune --archived --older-than <duration> --dry-run   # not built
munshi prune --archived --older-than <duration>             # not built
```

Were it built, the non-dry-run prune command would have to require:

- a locally stored archival receipt;
- a matching complete snapshot fetched from Patwari;
- matching manifest and artifact checksums;
- an explicit confirmation showing files and bytes to be removed;
- a configurable minimum age;
- no active or resumable local session state.

Automatic cleanup is out of scope for v1. Patwari cannot verify that its filesystem has been backed
up elsewhere, so the operator remains responsible for that durability decision.

## Implementation stack

Recommended stack:

- Rust stable;
- Axum and Tokio for HTTP and streaming;
- SQLx with SQLite migrations;
- Serde for protocol types;
- SHA-256 for manifests, chunks, and stored artifacts;
- tracing with structured redacted logs;
- a checked-in HTTP reference, [`docs/api.md`](api.md), as the public contract;
- a single OCI container image for deployment.

Rust aligns with Munshi, supports bounded streaming well, and allows protocol fixtures to be shared
through that written contract without sharing internal models.

## Delivery roadmap

> The plan as written in 2026-07, kept for its exit criteria. **Where each phase actually stands is
> the [Status table in the README](../README.md#status).** Where the plan below says OpenAPI, read
> [`docs/api.md`](api.md): no OpenAPI document was ever checked in, and the written reference is the
> contract instead.

### Phase 0: Source compatibility spike

In Munshi, inspect Copilot CLI's documented session references and local files. Prove that Munshi
can take a consistent copy without relying on a mutable live database. Define the first
agent-specific artifact set and create sanitized fixtures.

Exit criteria:

- one completed Copilot session can be represented by manifest v1;
- repeated capture produces stable logical paths and hashes;
- capture of a resumed or changing session fails safely or yields a consistent snapshot.

### Phase 1: Contract and server foundation

- Check in OpenAPI v1 and JSON examples.
- Create the Rust workspace, configuration, migrations, and health endpoint.
- Implement session upsert, snapshot creation, and state transitions.
- Add request limits, structured errors, and redacted tracing.

Exit criteria:

- protocol fixtures validate;
- schema migrations work from an empty volume;
- duplicate session and snapshot requests are idempotent.

### Phase 2: Reliable artifact ingestion

- Implement chunk negotiation and resumable upload.
- Verify chunk, artifact, and manifest checksums.
- Promote completed content atomically into the blob store.
- Return stable archival receipts.
- Garbage-collect abandoned temporary uploads.

Exit criteria:

- an interrupted upload resumes without retransmitting completed chunks;
- conflicting retries fail explicitly;
- corrupt or incomplete artifacts can never produce a complete receipt.

### Phase 3: Retrieval and archive inspection

- Filtered session listing, snapshot/capture history, canonical manifests, and artifact metadata are
  available through the versioned browsing resources.
- Artifact downloads stream checksum and size headers.
- Add an administrative archive verification command.

Exit criteria:

- every uploaded artifact can be downloaded byte-for-byte;
- a full archive scan reconciles SQLite metadata with filesystem blobs;
- programmatic consumers can incrementally discover new snapshots.

### Phase 4: Munshi integration

- Generate or implement the Patwari API client from OpenAPI.
- Add archive, retry, status, and download commands.
- Persist upload progress and archival receipts locally.
- Add manual prune dry-run and guarded deletion.
- Add the Patwari reference fields used by Notesmith summaries.

Exit criteria:

- a real Copilot session is captured, uploaded, listed, downloaded, and verified end to end;
- retries survive client and server restarts;
- a dry-run accurately identifies reclaimable local bytes;
- manual pruning refuses sessions without a verified complete snapshot.

### Phase 5: Host operations

- Add container units, a dedicated volume, health checks, and resource limits.
- Document trusted-network exposure.
- Add online SQLite backup, blob inventory, restore, and full verification procedures.
- Exercise filesystem-level backup and disaster recovery.

Exit criteria:

- a clean host can restore Patwari from the external filesystem backup;
- restored metadata and blobs pass a complete integrity scan;
- upgrades preserve existing archives through tested migrations.

### Phase 6: Analysis-tool support

- Stabilize pagination and incremental snapshot discovery.
- Provide efficient manifest and artifact streaming for batch consumers.
- Build analysis as a separate client that writes curated findings to Notesmith.

This phase does not add model execution or derived knowledge to Patwari itself.

## v1 completion definition

Patwari v1 is complete when Munshi can archive a real Copilot CLI session to the server, resume an
interrupted upload, receive a verified immutable receipt, list and download the snapshot, reproduce
all compressed files byte-for-byte, and safely preview manual removal of the original local files.

## Deferred decisions

- Exact Copilot CLI artifact set and consistency strategy, pending the Phase 0 spike.
- Retention policy beyond explicit administrative snapshot deletion.
- Authentication if Patwari crosses the trusted network boundary.
- Restore compatibility contracts for each coding agent.
- PostgreSQL or object-storage backends if single-node operation becomes insufficient.
