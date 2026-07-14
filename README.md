# Patwari

Patwari is a self-hosted archive for complete coding-agent sessions.

**Munshi writes the record; Patwari keeps the archive.**

Munshi captures session files on developer machines, compresses them, and submits an immutable
proposed manifest plus stored artifact bytes to Patwari. Notesmith remains the home for
human-readable summaries. A Notesmith summary may reference its Patwari session ID when full context
is needed.

## Running the archive

The service establishes a durable archive identity and provides the initial single-artifact archival
path. A Munshi installation registers its client UUID, creates an upload with one canonical manifest,
streams the declared stored bytes, completes verification, and then fetches the immutable snapshot or
stored artifact.

```sh
cargo run -p patwari-server
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/readyz
```

An empty data directory is initialized transactionally with one `v1` owner namespace, a generated
archive instance ID, the SQLite schema, and this persistent-volume layout:

```text
data/
├── patwari.db
├── blobs/
├── uploads/
└── maintenance/
```

Restarting with the same `PATWARI_DATA_DIR` retains the owner namespace and archive instance ID.
`/healthz` is process liveness and stays available when dependencies fail. `/readyz` returns `200`
only after SQLite accepts a query and every storage directory accepts a write-and-remove probe; it
returns `503` otherwise.

Configuration is environment-based and bounded by default:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PATWARI_DATA_DIR` | `data` | Dedicated persistent volume directory |
| `PATWARI_BIND_ADDR` | `127.0.0.1:8080` | Listener; loopback is the safe default |
| `PATWARI_MAX_REQUEST_BODY_BYTES` | `33554432` | Infrastructure request-body limit |
| `PATWARI_MAX_CONCURRENCY` | `64` | Maximum concurrent requests |
| `PATWARI_REQUEST_TIMEOUT` | `30s` | Maximum request duration |

Normal output is structured JSON and records only operational fields such as HTTP method, status,
and duration. It does not log request bodies, archived content, credentials, or filesystem paths.

Patwari is designed primarily for programmatic use:

- reliable archival of complete session transcripts and related source files;
- retrieval of original artifacts for inspection or future restoration;
- batch access by tools that mine past sessions for learnings, reusable patterns, and skills;
- verified archival receipts that allow Munshi to offer manual local cleanup.

The canonical domain vocabulary is defined in [`CONTEXT.md`](CONTEXT.md). Architectural trade-offs are
recorded in [`docs/adr/`](docs/adr/).

## Product decisions

| Area | Decision |
| --- | --- |
| Purpose | Archive complete coding-agent sessions |
| Summaries | Remain in Notesmith; Patwari does not store them |
| User interface | API and CLI integrations only; no web UI in v1 |
| Artifact visibility | Server-readable compressed files |
| Encryption | No application-level encryption initially |
| Authentication | None initially; deployment must remain inside a trusted network boundary |
| Initial deployment | Single-user Podman Quadlet on quadhost |
| Metadata store | SQLite |
| Blob store | Dedicated local filesystem volume |
| Additional backups | Managed outside Patwari at the filesystem/host level |
| Archive representation | Versioned manifest plus individually compressed files |
| Source cleanup | Manual, previewable Munshi command only |
| Initial source | GitHub Copilot CLI through Munshi |

## Goals

- Accept immutable, versioned snapshots of logical coding-agent sessions.
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
- Streams the one declared artifact to temporary storage with bounded resource use.
- Verifies stored and decompressed original sizes and checksums.
- Atomically promotes verified blobs into immutable storage.
- Indexes normalized metadata without interpreting transcript contents.
- Lists and returns manifests, snapshots, and artifacts.
- Records archival and deletion events.
- Never claims that an artifact is restorable by a particular agent.

### Notesmith

- Stores the latest curated Markdown summary.
- May include a `patwari_session_id`, snapshot ID, or stable Patwari URI.
- Is not a runtime dependency of Patwari.

## Domain model

```text
Client
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

Capture context such as project, repository, branch, source cursor, and source-version facts belongs
to the canonical manifest rather than mutable session identity.

### Snapshot

An immutable, internally consistent capture of a session at a source cursor or source state.

```text
id
session_id
snapshot_fingerprint
manifest_hash
completed_at
```

Only completed snapshots exist. Uploads are separate mutable transfer attempts, while snapshots,
their canonical manifests, and their artifacts are immutable after verification.

### Artifact

One compressed source file belonging to a snapshot.

```text
id
snapshot_id
logical_path
media_type
original_size_bytes
original_sha256
blob_id
```

Logical paths are relative, normalized, and unique within a snapshot. Absolute source paths must
not be used as artifact identity.

### Blob

A blob is the stored representation of an artifact. It records the stored SHA-256, stored size, and
compression. Blob bytes are content-addressed on disk; the artifact retains the original-content
SHA-256 and size.

### Archival receipt

The completion response is evidence that Patwari received and verified the declared snapshot.

```text
snapshot_id
manifest_hash
snapshot_fingerprint
archive_instance_id
artifact_count (currently one)
total_original_bytes
total_stored_bytes
completed_at
receipt_version
```

This receipt confirms Patwari integrity only. Filesystem replication and backup remain external
operational responsibilities.

## Single-artifact manifest v1

```json
{
  "schema_version": 1,
  "session": {
    "source_agent": "copilot-cli",
    "source_session_id": "abc123"
  },
  "capture": {
    "captured_at": "2026-07-11T19:42:00Z",
    "source_cursor": "184",
    "project": "munshi",
    "repository": "surdy/munshi",
    "branch": "main",
    "source_agent_version": "1.0.70",
    "munshi_version": "0.1.0"
  },
  "artifact": {
    "logical_path": "events.jsonl",
    "media_type": "application/x-ndjson",
    "original_size_bytes": 504321,
    "original_sha256": "sha256:...",
    "stored_size_bytes": 91234,
    "stored_sha256": "sha256:...",
    "compression": "zstd"
  }
}
```

Hashes are exactly `sha256:` followed by 64 lowercase hexadecimal digits. The server normalizes and
serializes this document before persisting it; this canonical form is authoritative. Version 1
accepts exactly one normalized relative regular-file logical path.

## API v1

The OpenAPI document will be the cross-repository contract. Patwari must not expose database models
as its public protocol.

```text
GET    /healthz

PUT    /api/v1/clients/{client_id}

POST   /api/v1/uploads
PUT    /api/v1/uploads/{upload_id}/artifacts/0/chunks/0
POST   /api/v1/uploads/{upload_id}/complete
GET    /api/v1/snapshots/{snapshot_id}
GET    /api/v1/artifacts/{artifact_id}/content
```

Deletion is not part of the normal upload flow. It is an explicit administrative operation,
disabled by default until retention semantics and CLI confirmation are implemented.

### Idempotency

- Client registration is an idempotent `PUT` keyed by its client-generated UUID. Hostname, display
  name, and metadata are mutable attributes.
- Upload creation requires a client idempotency key. Repeating it with the same canonical manifest
  returns the existing upload; reusing it for a different manifest returns `409 Conflict`.
- Session creation is atomic with upload creation and is keyed within the owner namespace by
  `source_agent + source_session_id`.
- The sole artifact is addressed as chunk index `0`; multi-chunk resume is deliberately deferred.
- Retrying completion returns byte-for-byte the same versioned receipt, including the persistent
  archive instance ID.

## Storage design

```text
/data/
├── patwari.db
├── blobs/
│   └── sha256/
│       └── ab/
│           └── abcdef...
├── uploads/
│   └── <snapshot-id>/
└── maintenance/
```

- SQLite stores metadata, state transitions, idempotency records, and audit events.
- Completed blobs are content-addressed by the checksum of the stored compressed bytes.
- Snapshot artifacts reference blobs, allowing safe deduplication.
- Temporary uploads and completed blobs are on the same filesystem so promotion can use atomic
  rename.
- Database and blob paths live on one dedicated persistent volume.
- Backup tooling must capture SQLite and blobs consistently. Patwari will provide a maintenance
  command that creates a SQLite online backup and a blob inventory for filesystem-level backup.
- Patwari must not reuse a disposable application cache volume.

Reference counts are transactional metadata. Blob garbage collection deletes only content with no
snapshot references and only after a grace period.

## Network and trust model

Patwari v1 has no application authentication. Therefore:

- it must not be exposed directly to the public internet;
- its listener is configurable and defaults to loopback;
- quadhost deployment must place it behind the trusted LAN or an authenticated network boundary;
- administrative deletion endpoints remain disabled by default;
- request bodies, artifact contents, and local paths are not written to normal logs;
- size, count, concurrency, and request-duration limits are mandatory.

Adding authentication later must not change session, snapshot, or artifact identifiers. The schema
should leave room for an owning principal without requiring one in v1.

## Retrieval and analysis access

Programmatic consumers can:

- filter sessions by source agent, repository, project, client, and activity time;
- enumerate complete snapshots and inspect manifests;
- stream individual compressed artifacts;
- verify checksums from the archival receipt;
- maintain their own analysis cursor using snapshot creation time and ID.

Content search and learning extraction belong in separate tools. A future tool can download or
stream archived transcripts, derive learnings, and write curated results to Notesmith without
coupling that workflow to Patwari.

## Local cleanup safety

Patwari does not delete source files. Munshi may later provide:

```text
munshi archive <session-id>
munshi archive --all
munshi prune --archived --older-than <duration> --dry-run
munshi prune --archived --older-than <duration>
```

The non-dry-run prune command must require:

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
- utoipa or a checked-in OpenAPI document for the public contract;
- Podman Quadlet for deployment.

Rust aligns with Munshi, supports bounded streaming well, and allows protocol fixtures and generated
types to be shared through the OpenAPI contract without sharing internal models.

## Delivery roadmap

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

- Implement filtered session listing.
- Return manifests and snapshot history.
- Stream artifact downloads with checksum and size headers.
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

### Phase 5: Quadhost operations

- Add Quadlet units, dedicated volume, health checks, and resource limits.
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

Patwari v1 is complete when Munshi can archive a real Copilot CLI session to quadhost, resume an
interrupted upload, receive a verified immutable receipt, list and download the snapshot, reproduce
all compressed files byte-for-byte, and safely preview manual removal of the original local files.

## Deferred decisions

- Exact Copilot CLI artifact set and consistency strategy, pending the Phase 0 spike.
- Chunk size and maximum artifact/snapshot limits, informed by real session measurements.
- Retention and explicit remote deletion policy.
- Authentication if Patwari crosses the trusted network boundary.
- Restore compatibility contracts for each coding agent.
- PostgreSQL or object-storage backends if single-node operation becomes insufficient.
