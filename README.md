# Patwari

Patwari is a self-hosted archive for complete coding-agent sessions.

**Munshi writes the record; Patwari keeps the archive.**

Munshi captures session files on developer machines, compresses them, and submits an immutable
proposed manifest plus stored artifact bytes to Patwari. Notesmith remains the home for
human-readable summaries. A Notesmith summary may reference its Patwari session ID when full context
is needed.

## Running the archive

The service establishes a durable archive identity and archives complete multi-artifact snapshots. A
Munshi installation registers its client UUID, creates an upload for one client-generated
`capture_id` and canonical manifest, receives a server-assigned chunk size, streams resumable
stored-byte chunks for every declared artifact, completes verification of the complete set, and
then fetches the immutable snapshot, capture provenance, or an individual stored artifact.

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

## Deployment, backup, and recovery

The production image is defined by [`Containerfile`](Containerfile), and the
quadhost Quadlet template, immutable-image installer, and environment example
live in [`deploy/quadhost/`](deploy/quadhost/). The host listener is
deliberately restricted to `192.168.16.169:8787`; v1 has no authentication, so
it must remain on a trusted LAN and never be published on `0.0.0.0`.

Use the archive-only maintenance CLI for a consistent online backup:

```sh
PATWARI_DATA_DIR=/var/lib/patwari-volume/data patwari-server backup create --output /backups/patwari-20260714
patwari-server backup verify /backups/patwari-20260714
patwari-server backup restore /backups/patwari-20260714 --data-dir /empty/patwari-data
```

Backup creation gates API work and integrity scans across processes, refuses
active uploads, uses SQLite's online backup API, inventories authoritative
Blob rows in deterministic digest order, hashes copied blobs, and atomically
finalizes a self-contained directory. Verification checks the manifest,
database, blob inventory and hashes, then boots a staged copy and runs the
full integrity scanner. Restore verifies before touching its destination,
refuses a non-empty destination, stages atomically, preserves archive
identity, and scans again.

The persistent archive volume and local backup directory are **not**
disposable cache. External replication of verified finalized backup
directories to independent storage is the durability boundary. See
[`docs/operations/quadhost.md`](docs/operations/quadhost.md) for deployment,
health checks, schedules, SELinux, update/rollback, trusted-network, and
clean-host disaster-recovery procedures.

Configuration is environment-based and bounded by default:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PATWARI_DATA_DIR` | `data` | Dedicated persistent volume directory |
| `PATWARI_BIND_ADDR` | `127.0.0.1:8080` | Listener; loopback is the safe default |
| `PATWARI_MAX_REQUEST_BODY_BYTES` | `33554432` | Infrastructure request-body limit |
| `PATWARI_MAX_CONCURRENCY` | `64` | Maximum concurrent requests |
| `PATWARI_MAX_DOWNLOAD_CONCURRENCY` | `8` | Maximum concurrent artifact response bodies |
| `PATWARI_REQUEST_TIMEOUT` | `30s` | Maximum request duration |
| `PATWARI_UPLOAD_CHUNK_SIZE_BYTES` | `4194304` | Fixed server-assigned stored-byte chunk size |
| `PATWARI_MAX_ARTIFACT_STORED_BYTES` | `1073741824` | Maximum declared compressed artifact size |
| `PATWARI_MAX_ARTIFACT_ORIGINAL_BYTES` | `4294967296` | Maximum declared decompressed artifact size |
| `PATWARI_MAX_ARTIFACT_COUNT` | `128` | Maximum artifacts in one snapshot |
| `PATWARI_MAX_SNAPSHOT_STORED_BYTES` | `4294967296` | Maximum stored-byte sum in one snapshot |
| `PATWARI_MAX_SNAPSHOT_ORIGINAL_BYTES` | `17179869184` | Maximum original-byte sum in one snapshot |
| `PATWARI_UPLOAD_EXPIRY` | `24h` | Server-time lifetime of an unfinished upload |
| `PATWARI_ADMIN_DELETION_ENABLED` | `false` | Enables trusted-boundary administrative deletion and GC HTTP endpoints |
| `PATWARI_BLOB_GC_GRACE` | `7d` | Server-time delay before an unreferenced blob is GC eligible |
| `PATWARI_INTEGRITY_SCAN_CONCURRENCY` | `4` | Maximum concurrent checksum/decompression workers used by `verify` |
| `PATWARI_INTEGRITY_SCAN_BUFFER_BYTES` | `65536` | Per-worker scan read/decompression buffer |

The body limit is 1 KiB–64 MiB, request concurrency is 1–256, download concurrency is 1–64,
timeout is 1s–5m, chunk size is
1 KiB–32 MiB, each artifact limit is 1 byte–8 GiB, a snapshot is limited to 1,024 artifacts and
64 GiB per stored/original aggregate, and unfinished-upload expiry is 1m–30d.
Chunk size must fit the request-body limit, and the configured stored-artifact limit must fit at
most 65,536 chunks per artifact; a snapshot is additionally capped at 262,144 chunks. Durations
accept `s`, `m`, `h`, or `d`. The
service verifies both stored and decompressed sizes while streaming; it never trusts a declared
checksum or size alone. Integrity scan concurrency is 1–32 workers and its buffer is 4 KiB–1 MiB.

Administrative deletion is opt-in: `PATWARI_ADMIN_DELETION_ENABLED` accepts only `true` or `false`
and defaults to `false`. Blob-GC grace is 1m–365d; it defaults to 7d. Operators must enable the
administrative surface only behind the same trusted network boundary required by the unauthenticated
v1 service.

Normal output is structured JSON and records only operational fields such as HTTP method, status,
and duration. It does not log request bodies, archived content, credentials, or filesystem paths.

## Integrity verification

Run a complete maintenance scan without starting the HTTP listener:

```sh
PATWARI_DATA_DIR=/srv/patwari/data cargo run -p patwari-server -- verify
```

`verify` writes exactly one JSON report to stdout and diagnostic structured logs to stderr. It exits
`0` when no action is required, `1` for actionable findings, and `2` when configuration, bootstrap,
or the scanner itself cannot complete safely. The report includes the UUIDv7 run ID, server
start/end times, final status, counts by severity and stable finding kind, and a bounded list of
redacted finding summaries. It never includes paths, manifest content, artifact bytes, or raw
capture metadata.

Every scan creates an immutable `Integrity Run` and append-only `Integrity Findings`. `info`
findings describe intentional states; `warning` and `error` findings are actionable. Current health
is the newest completed run, while prior runs and findings remain available through the in-process
`Service` methods `latest_integrity_health`, `list_integrity_runs`, and
`list_integrity_findings`. A finding never changes a Snapshot's completed timestamp, receipt, or
historical verification fact.

The scan validates canonical manifest existence, parsing, and hash; compares every live normalized
Snapshot projection with its manifest; checks SQLite foreign keys; recomputes canonical blob sizes
and SHA-256 digests; validates original artifact digests after bounded identity/Zstandard streaming;
and inventories only `blobs/sha256/<two-hex>/<sha256>`. It does not recurse through symlinks.
Files under `uploads/` and dot/partial temporary entries are intentionally not treated as canonical
blob files. A canonical-looking file without a Blob row is actionable.

Scans are observational and may run while the server accepts uploads, deletion, or GC. They page
metadata, use fixed digest locks shared with promotion and GC, and revalidate observed rows before
persisting a corruption finding. A detected in-process change is recorded as non-actionable
`transient_change` rather than false corruption. Offline `verify` is the strongest consistency
mode because no server-side writer is concurrently active. It also deliberately bypasses normal
restart cleanup so an unexpected canonical blob remains evidence for the scan rather than being
removed before it can be reported.

Blob liveness remains authoritative only through live `Artifact -> Blob` references. A Tombstoned
Snapshot is reported as informational, as is an unreferenced Blob still inside its configured GC
grace period. A due GC candidate and a candidate that regained a live reference are actionable
maintenance conditions; an unreferenced Blob with no valid candidate state is an accidental orphan.

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

## Multi-artifact manifest v1

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
    "source_state_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "source_metadata": {
      "adapter_cursor_kind": "event"
    },
    "project": "munshi",
    "repository": "surdy/munshi",
    "branch": "main",
    "source_agent_version": "1.0.70",
    "artifact_set_version": 1,
    "munshi_version": "0.1.0"
  },
  "artifacts": [
    {
      "logical_path": "events.jsonl",
      "media_type": "application/x-ndjson",
      "original_size_bytes": 504321,
      "original_sha256": "sha256:...",
      "stored_size_bytes": 91234,
      "stored_sha256": "sha256:...",
      "compression": "zstd"
    },
    {
      "logical_path": "metadata.json",
      "media_type": "application/json",
      "original_size_bytes": 128,
      "original_sha256": "sha256:...",
      "stored_size_bytes": 128,
      "stored_sha256": "sha256:...",
      "compression": "identity"
    }
  ]
}
```

Hashes are exactly `sha256:` followed by 64 lowercase hexadecimal digits. The server normalizes and
sorts this document by `logical_path` before persisting it; this canonical `artifacts[]` form is
authoritative and client order does not affect snapshot identity. Version 1 accepts a legacy
singleton `artifact` input only for compatibility, but never persists that shape for new uploads.
The snapshot fingerprint is scoped to a session and contains project, repository, branch,
source-agent version, required `artifact_set_version`, and each canonical logical path plus
verified original content. It excludes client and capture/upload IDs; source/server times; source
cursor/state hash; Munshi version; opaque source metadata; transfer metrics; compression; and stored
representation.

## API v1

The OpenAPI document will be the cross-repository contract. Patwari must not expose database models
as its public protocol.

```text
GET    /healthz

PUT    /api/v1/clients/{client_id}

POST   /api/v1/uploads
GET    /api/v1/uploads/{upload_id}
PUT    /api/v1/uploads/{upload_id}/artifacts/{artifact_index}/chunks/{chunk_index}
POST   /api/v1/uploads/{upload_id}/abandon
POST   /api/v1/uploads/{upload_id}/complete
GET    /api/v1/uploads/{upload_id}/capture

GET    /api/v1/sessions
GET    /api/v1/sessions/{session_id}
GET    /api/v1/sessions/{session_id}/captures
GET    /api/v1/sessions/{session_id}/snapshots

GET    /api/v1/captures
GET    /api/v1/captures?client_id={client_id}&capture_id={capture_id}
GET    /api/v1/captures/{capture_record_id}

GET    /api/v1/snapshots
GET    /api/v1/snapshots/{snapshot_id}
GET    /api/v1/snapshots/{snapshot_id}/captures
GET    /api/v1/snapshots/{snapshot_id}/manifest

GET    /api/v1/manifests
GET    /api/v1/manifests/{manifest_id}

GET    /api/v1/artifacts
GET    /api/v1/artifacts/{artifact_id}
GET    /api/v1/artifacts/{artifact_id}/content

DELETE /api/v1/admin/snapshots/{snapshot_id}
GET    /api/v1/admin/tombstones
GET    /api/v1/admin/tombstones/{snapshot_id}
POST   /api/v1/admin/blob-gc
```

`POST /uploads` requires `capture_id` and reports it with the assigned `chunk_size_bytes`.
`idempotency_key` remains a deprecated compatibility alias when `capture_id` is omitted; when both
are supplied, they must be the same value. `GET /uploads/{upload_id}` reports the same capture ID
and, for
each upload artifact, its stable `artifact_index`, logical path, artifact-specific chunk URL
template, `chunk_count`, `accepted_chunk_bitmap`, and missing indexes. Bitmap byte zero represents
chunk indexes 0–7 with the least-significant bit representing index 0. Each `PUT` must include
`Content-Type: application/octet-stream`, `X-Patwari-Chunk-Length`, and
`X-Patwari-Chunk-SHA256` (`sha256:` plus 64 lowercase hex digits). The server derives the only
valid length for each index, including the final chunk. For practical compatibility, a headerless
`chunks/0` request is accepted only when the negotiated artifact has exactly one chunk; its
canonical manifest supplies the equivalent persisted length and checksum contract.

### Verified artifact content

`GET /api/v1/artifacts/{artifact_id}/content` returns the **stored** bytes from the canonical blob;
it never decompresses an artifact. Before it returns `200`, Patwari reads the opened blob descriptor
in fixed 64 KiB chunks, rejects anything other than a regular file, verifies its exact stored size
and SHA-256, and checks the Artifact/Blob projection against the snapshot's canonical manifest.
The verified descriptor is rewound and used for the response, so a pathname replacement cannot
substitute a different blob after preflight on Unix. Blob files are immutable by storage contract;
the preflight also compares descriptor metadata before and after hashing to detect ordinary
in-place mutation. No SQLite transaction or blob lock remains held while a client reads the body.

If an artifact ID is absent, the endpoint returns `404 artifact_not_found`. If an existing artifact
has missing, non-regular, truncated, corrupt, or projection-drifted storage, it returns
`409 artifact_integrity_failure` before sending a success response. It never emits a
success-shaped partial body for a blob that fails preflight.

Every successful content response has this stable header contract. Stored digest headers and
`X-Patwari-Stored-SHA256` cover bytes exactly as stored; original fields cover the decompressed
original bytes. Patwari hash values use `sha256:` followed by 64 lowercase hexadecimal digits.

| Header | Meaning |
| --- | --- |
| `Content-Type` | Declared artifact media type, or `application/octet-stream` when none was declared |
| `Content-Length` | Exact stored-byte size |
| `Content-Encoding: zstd` | Present only for Zstandard stored bytes; omitted for identity storage |
| `Digest: SHA-256=<base64>` and `Content-Digest: sha-256=:<base64>:` | Standard digest forms for the exact stored response bytes |
| `X-Patwari-Logical-Path` | Canonical logical path encoded as unpadded URL-safe Base64 |
| `X-Patwari-Logical-Path-Encoding` | Always `base64url` |
| `X-Patwari-Media-Type` | Declared media type when one was supplied; omitted otherwise |
| `X-Patwari-Compression` | Canonical `identity` or `zstd` storage encoding |
| `X-Patwari-Original-Size-Bytes`, `X-Patwari-Original-SHA256` | Verified uncompressed/original size and hash |
| `X-Patwari-Stored-Size-Bytes`, `X-Patwari-Stored-SHA256` | Verified stored size and hash; these agree with `Content-Length` and the digest headers |

`Cache-Control: no-transform` accompanies the response so intermediaries must not alter the
stored-byte representation. An HTTP client that automatically decodes `Content-Encoding` must
disable that behavior when it needs byte-for-byte blob verification. A client can verify the stored
length/hash while streaming, then use `X-Patwari-Compression` to stream-decompress Zstandard bytes
(or pass identity bytes through) and verify `X-Patwari-Original-Size-Bytes` and
`X-Patwari-Original-SHA256`.

The download concurrency cap is independent of general request concurrency. A permit remains held
for the entire response body, including slow reads, and is released on completion, error, timeout,
or disconnect. `PATWARI_REQUEST_TIMEOUT` also applies as a whole-body deadline because the generic
Tower request timeout ends once a streaming response is constructed; the download deadline starts
when that request begins.

Capture provenance is available after successful completion by upload, by the `(client_id,
capture_id)` query pair, by immutable capture-record ID, as a paginated archive-wide collection, or
as the focused relation for one snapshot. Capture responses return the opaque `source_metadata`
document unchanged alongside independently useful stable fields such as `project`, `repository`,
`branch`, `source_agent_version`, and `artifact_set_version`. Metadata keys are deliberately not
queryable or logged.

### Archive browsing and pagination

The collection endpoints return:

```json
{
  "items": [],
  "next_cursor": "opaque-or-null",
  "high_watermark": {"timestamp": "2026-07-13T20:00:00Z", "id": "uuidv7"}
}
```

`limit` defaults to 50 and must be 1–100. Collections are always descending by an immutable server
timestamp and UUIDv7 ID. The timestamp is the projected latest-snapshot completion for sessions,
snapshot completion for snapshots, capture completion for captures and canonical-manifest summaries,
and artifact creation for artifacts. Cursors are opaque, are bound to their collection and filters,
and carry the first page's high-watermark. Continue a traversal only with its returned cursor:
newer records will not be interleaved, duplicated, or cause older records to be skipped.

`GET /sessions` lists only sessions with a visible completed snapshot. It supports independent
`source_agent`, `repository`, `project`, `branch`, `source_agent_version`,
`artifact_set_version`, `client_id`, `activity_from`, and `activity_to` filters. Activity bounds are
inclusive RFC 3339 instants over the projected latest snapshot's server completion time. Repository,
project, branch, and agent-version filters use that same latest snapshot context. A `client_id`
matches when that client contributed **any successful capture associated with the projected latest
snapshot**; it intentionally does not match a client that contributed only an older snapshot.

`GET /snapshots` accepts `session_id` plus the same stable context, client, and activity filters;
its activity time is snapshot completion. `GET /captures` accepts `client_id`, `session_id`,
`snapshot_id`, and the same stable context filters; its activity time is capture completion.
`GET /sessions/{session_id}/snapshots` and `/captures` expose the full historical timeline. The
focused `/snapshots/{snapshot_id}/captures` relation accepts the same bounded pagination parameters
and returns `captures` plus `next_cursor` and `high_watermark` for compatibility.
`GET /manifests` lists the immutable canonical document retained for each completed capture,
including captures coalesced onto an existing snapshot. Snapshot inspection embeds its selected
canonical manifest and artifact metadata; the manifest and artifact resources provide separately
addressable inspection documents. Manifest summaries accept `session_id`; artifact metadata accepts
`snapshot_id` or `session_id`. Normal list and inspection resources exclude tombstoned snapshots and
their related captures, manifests, and artifacts.

`POST /uploads/{upload_id}/abandon` explicitly discards resumable bytes. The callable server
maintenance operation expires uploads by server time. Both paths remove temporary files, chunk
records, and manifests, retaining only redacted audit facts: client/session IDs, the opaque capture
ID, canonical manifest digest, declared sizes and chunk count, timestamps, terminal reason, and
error code. They do not retain request bodies, paths, chunk checksums, manifest contents, or
artifact content.

### Administrative deletion and blob GC

The `/api/v1/admin/*` surface returns `403` unless
`PATWARI_ADMIN_DELETION_ENABLED=true`. It is an operator contract, not v1 authentication: expose it
only inside the trusted boundary. `DELETE /api/v1/admin/snapshots/{snapshot_id}` requires an exact
confirmation bound to the resource:

```text
delete-snapshot:<snapshot-id>:sha256:<snapshot-fingerprint-without-prefix>
```

Supply it in `X-Patwari-Delete-Confirmation` or in the optional JSON body's `confirmation` field;
if both are supplied they must match. The optional JSON `reason` is bounded to 512 non-control
bytes and must be non-sensitive. Repeating the same confirmed deletion returns the existing
Tombstone/audit record without creating another event. `GET /admin/tombstones` is bounded and
cursor-paginated; it is the only API representation that reissues a deleted snapshot's historical
receipt. A rearchive is a new capture observation, so the client must submit a new `capture_id`;
its new receipt names a new snapshot ID even when the fingerprint is identical.

Deletion removes `Artifact` rows but does not immediately remove blobs. It records
`orphaned_at`/`eligible_after` using server time only. `POST /admin/blob-gc` and the in-process
maintenance call process a bounded batch after grace. Immediately before metadata and file removal,
GC joins live `Artifact` → `Snapshot` relationships inside its transaction; an optional cached count
can never authorize deletion. Digest-striped locks serialize completion, deletion, and GC. GC removes
the metadata in an uncommitted transaction, removes the file while holding that digest lock, then
commits, so a new reference cannot gain a missing file concurrently.

### Capture identity and idempotency

- Client registration is an idempotent `PUT` keyed by its client-generated UUID. Hostname, display
  name, and metadata are mutable attributes.
- Upload creation requires a client-generated capture ID. Repeating it for the same client with the
  same canonical manifest returns the existing upload; reusing it with a different manifest returns
  `409 Conflict`.
- Session creation is atomic with upload creation and is keyed within the owner namespace by
  `source_agent + source_session_id`.
- Artifact indexes are the canonical logical-path order in the manifest, and each has independent
  server-negotiated chunk indexes. Retrying a chunk with the same artifact/index, length, and
  checksum is idempotent; a different length or checksum returns `409 Conflict` without replacing
  accepted bytes.
- Retrying completion returns the same receipt. Simultaneous distinct captures of the same verified
  semantic state safely coalesce to one session-scoped snapshot while each atomically gains a
  successful capture record.

## Storage design

```text
/data/
├── patwari.db
├── blobs/
│   └── sha256/
│       └── ab/
│           └── abcdef...
├── uploads/
│   └── <upload-id>/
│       └── artifacts/<artifact-index>/chunks/<chunk-index>
└── maintenance/
```

- SQLite stores metadata, state transitions, idempotency records, and audit events.
- Completed blobs are content-addressed by the checksum of the stored compressed bytes.
- Snapshot artifacts reference blobs, allowing safe deduplication, including multiple artifacts in
  the same snapshot that share one representation.
- Temporary uploads and completed blobs are on the same filesystem so promotion can use atomic
  hard links and cleanup can be recovered after a crash.
- Chunk files are synced and linked before their metadata record is committed. Restart recovery
  removes file-only remnants and makes metadata-only chunks retryable; completion assembles and
  independently verifies every declared artifact with bounded streaming before one atomic metadata
  commit makes the snapshot visible.
- Database and blob paths live on one dedicated persistent volume.
- Backup tooling must capture SQLite and blobs consistently. Patwari will provide a maintenance
  command that creates a SQLite online backup and a blob inventory for filesystem-level backup.
- Patwari must not reuse a disposable application cache volume.

`Artifact` relationship rows are authoritative for blob liveness. Any operational count is
rebuildable only and cannot authorize deletion. Blob GC deletes only content with no **live**
SnapshotArtifact relationship after the persisted server-time grace period; rearchiving clears a
pending candidate in the same transaction that creates the new relationship.

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

- filter the latest completed session context by source agent, repository, project, branch, client,
  and activity time;
- enumerate complete snapshots, captures, canonical manifests, and artifact metadata;
- stream individual compressed artifacts;
- verify checksums from the archival receipt;
- maintain an exact incremental traversal with returned opaque cursors.

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
- Retention policy beyond explicit administrative snapshot deletion.
- Authentication if Patwari crosses the trusted network boundary.
- Restore compatibility contracts for each coding agent.
- PostgreSQL or object-storage backends if single-node operation becomes insufficient.
