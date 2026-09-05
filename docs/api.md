# Patwari HTTP API and CLI reference

This document is the cross-repository contract for Patwari's HTTP surface and its command-line
entry points. There is no OpenAPI document: this checked-in reference is the contract for now.
Patwari does not expose database models as its public protocol.

Domain vocabulary is defined in [`CONTEXT.md`](../CONTEXT.md); the model behind these resources is
in [`docs/domain.md`](domain.md). Everything here assumes the trust posture in the
[README](../README.md#trust-and-authentication): no authentication, private network only.

## Command-line interface

The `patwari-server` binary matches a fixed set of positional argument forms exactly — there is no
option parser and no abbreviation. The accepted forms are:

```text
patwari-server                       # same as serve
patwari-server serve
patwari-server verify
patwari-server backup create --output <backup-dir>
patwari-server backup verify <backup-dir>
patwari-server backup restore <backup-dir> --data-dir <empty-data-dir>
patwari-server --help                # also -h, or help
patwari-server --version
```

`--help`, `-h`, and `help` print the usage block to **stdout** and exit `0`; `--version` prints
`patwari-server <version>` to stdout and exits `0`.

Anything else logs `unknown archive command`, prints `{"error_code":"invalid_command"}` to stdout,
prints the same usage block to **stderr after it**, and exits `2`. The JSON line stays first on
stdout and the exit code stays `2`, so a script that parses `error_code` is unaffected by the
usage text.

Every command reads its configuration from the environment (below). Backup and restore procedure,
scheduling, and the durability boundary are in [`docs/self-hosting.md`](self-hosting.md);
`verify`, `backup`, and the CLI's own error codes are covered there too.

## Configuration

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
| `PATWARI_BLOB_GC_GRACE` | `90d` | Server-time delay before an unreferenced blob is GC eligible |
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
and defaults to `false`. Blob-GC grace is 1m–365d; it defaults to 90d — deletion is the suite's only irreversible act, so the default gives a mistaken tombstone a whole season to be noticed before its bytes can go. Operators must enable the
administrative surface only behind the same trusted network boundary required by the unauthenticated
v1 service.

Normal output is structured JSON and records only operational fields such as HTTP method, status,
and duration. It does not log request bodies, archived content, credentials, or filesystem paths.

`PATWARI_BLOB_GC_GRACE`, and the administrative surface it delays, are explained operationally in
[`docs/self-hosting.md`](self-hosting.md#blob-garbage-collection).

## Health and observability

`/healthz` is process liveness and stays available when dependencies fail. `/readyz` returns `200`
only after SQLite accepts a query and every storage directory accepts a write-and-remove probe; it
returns `503` otherwise, and is the deployment and monitoring check.

## API v1

```text
GET    /healthz

PUT    /api/v1/clients/{client_id}

POST   /api/v1/uploads
GET    /api/v1/uploads/{upload_id}
PUT    /api/v1/uploads/{upload_id}/artifacts/{artifact_index}/chunks/{chunk_index}
POST   /api/v1/uploads/{upload_id}/abandon
POST   /api/v1/uploads/{upload_id}/complete
GET    /api/v1/uploads/{upload_id}/capture

GET    /api/v1/stats
GET    /api/v1/clients

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
GET    /api/v1/artifacts?original_sha256={hex}
GET    /api/v1/artifacts?stored_sha256={hex}
GET    /api/v1/artifacts/{artifact_id}
GET    /api/v1/artifacts/{artifact_id}/content

DELETE /api/v1/admin/snapshots/{snapshot_id}
GET    /api/v1/admin/tombstones
GET    /api/v1/admin/tombstones/{snapshot_id}
POST   /api/v1/admin/blob-gc
```

`GET /api/v1/artifacts` also accepts `original_sha256` and `stored_sha256` equality filters (bare
64-character lowercase hex), composable with the `snapshot_id` and `session_id` filters. This is
the hash-addressed lookup from ADR 0004: a client holding only a content hash — for example a
Munshi claim ticket — resolves the artifacts carrying it without walking sessions and snapshots.
Because blobs are deduplicated per owner, one hash may resolve to multiple artifacts across
snapshots; the listing returns all of them through normal pagination.

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
`chunks/0` request is accepted only when the whole upload is a single artifact of exactly one
chunk; its canonical manifest supplies the equivalent persisted length and checksum contract. Any
multi-artifact upload always sends the headers.

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

### Inventory

Two unpaginated read resources answer "how much is in the archive" and "which clients have written
to it" without walking a collection 50 rows at a time.

`GET /api/v1/stats` returns archive-wide totals as of the moment the query ran:

```json
{
  "schema_version": 1,
  "generated_at": "2026-09-05T06:24:49.892048Z",
  "archive_instance_id": "01a0703d-6348-7251-9d35-fdfa83ee8978",
  "sessions": 1,
  "snapshots": 1,
  "captures": 1,
  "artifacts": 1,
  "blobs": 1,
  "stored_bytes": 68,
  "original_bytes": 68,
  "blob_stored_bytes": 68,
  "clients": 1,
  "tombstones": 0,
  "last_ingest_at": "2026-09-05T06:24:45.840285Z",
  "oldest_activity_at": "2026-09-05T06:24:45.840285Z",
  "newest_activity_at": "2026-09-05T06:24:45.840285Z"
}
```

Every count is of **live** rows, so a tombstoned snapshot leaves `sessions`, `snapshots`,
`captures`, and `artifacts` and is counted in `tombstones` instead. Two byte figures are reported
because they answer different questions: `stored_bytes` and `original_bytes` are the sums of the
same per-snapshot totals `GET /snapshots/{id}` returns, so a blob shared by several snapshots is
counted once per snapshot, while `blob_stored_bytes` is the deduplicated size of the authoritative
blob rows — what the blob store actually occupies. `blobs` counts those same rows, which survive a
tombstone until blob GC collects them after grace. `last_ingest_at` is the newest live capture's
server completion time; `oldest_activity_at` and `newest_activity_at` bound exactly the session
activity time the `GET /sessions` `activity_from` / `activity_to` filters compare against, so a
consumer can size a window before it asks for one. `schema_version` is the version of this
document and is pinned the way a manifest schema version is.

`GET /api/v1/clients` lists every registered client with the fields
`PUT /api/v1/clients/{client_id}` stored:

```json
{
  "items": [
    {
      "client_id": "96c976cd-0025-47f6-bab1-5948d84ce11f",
      "hostname": "workstation",
      "display_name": "Workstation",
      "first_seen_at": "2026-09-05T06:24:36.616757Z",
      "last_seen_at": "2026-09-05T06:24:36.616757Z",
      "capture_count": 1,
      "last_capture_at": "2026-09-05T06:24:45.840285Z"
    }
  ],
  "next_cursor": null,
  "high_watermark": null
}
```

`first_seen_at` and `last_seen_at` are that registration's creation and last update, so
`last_seen_at` moves only when a client re-registers; `last_capture_at` is the completion time of
its newest live capture and is the stronger "is this machine still reporting" signal.
`capture_count` counts that client's live captures. The client registry is bounded by the number
of machines an owner runs, so this listing is deliberately unpaginated: `next_cursor` is always
`null` and reserved for a future page boundary. Per-client `metadata` is not returned here — it is
an opaque document, available from the registration response.

Both routes sit outside `/api/v1/admin/*` and inside the maintenance gate: they need no
`PATWARI_ADMIN_DELETION_ENABLED`, and they return `503 maintenance_in_progress` during a backup
like every other read. Both stay inside the ADR 0001 boundary by construction — every field is a
count, a byte total, a timestamp, or an identifier the archive already stores, and nothing is
derived from artifact content.

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

## Multi-artifact manifest v1

`source_agent` is free-form archival metadata (a non-empty string, at most 128 bytes, no control
characters), not an enum: Patwari never branches on it. Munshi currently submits `"copilot-cli"`
and `"claude-code"` sessions; new agents need no Patwari change.

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

## Storage layout

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
- Backup tooling must capture SQLite and blobs consistently. The `backup create` command provides
  this: a SQLite online backup plus a blob inventory, finalized as a self-contained directory for
  filesystem-level backup.
- Patwari must not reuse a disposable application cache volume.

`Artifact` relationship rows are authoritative for blob liveness. Any operational count is
rebuildable only and cannot authorize deletion. Blob GC deletes only content with no **live**
SnapshotArtifact relationship after the persisted server-time grace period; rearchiving clears a
pending candidate in the same transaction that creates the new relationship.
