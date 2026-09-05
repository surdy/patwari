# Ingesting a snapshot: one worked upload

The narrative version of this protocol is in the [README](../README.md#quick-start) and the
complete reference in [`docs/api.md`](api.md). This page is its executable twin: every request and
response below was run against a scratch `patwari-server` on `127.0.0.1:8080`, and the responses
are the ones it actually returned (trimmed for width, never edited).

**No request here carries a credential.** Patwari has no authentication; anything that can reach
the listener can write to the archive. Read [Trust and
authentication](../README.md#trust-and-authentication) before you point anything at a server you
did not start yourself.

An upload is five steps:

```text
PUT  /api/v1/clients/{client_id}                                    register the writer (idempotent)
POST /api/v1/uploads                                                declare a manifest, get a chunk size
PUT  /api/v1/uploads/{id}/artifacts/{index}/chunks/{chunk}          send stored bytes, one chunk at a time
POST /api/v1/uploads/{id}/complete                                  verify everything, get a receipt
GET  /api/v1/uploads/{id}/capture                                   read the capture provenance back
```

You need `curl`, `jq`, `zstd`, and `shasum` (or `sha256sum`). Start a server first:

```sh
PATWARI_DATA_DIR=./scratch-data cargo run -p patwari-server
BASE=http://127.0.0.1:8080
```

## 0. Two tiny artifacts

The example snapshot carries two artifacts. Stored bytes may be `zstd` or `identity` — **the
protocol does not require compression** — so this uses one of each, which is also what a real
producer does when compression fails to shrink a small input.

```sh
printf '{"type":"user","text":"hello"}\n{"type":"assistant","text":"hi"}\n' > events.jsonl
printf '{"harness":"demo","turns":2}\n'                                    > metadata.json
zstd -3 -q -f events.jsonl -o events.jsonl.zst

for f in events.jsonl events.jsonl.zst metadata.json; do
  printf '%s %s %s\n' "$f" "$(wc -c < $f | tr -d ' ')" "$(shasum -a 256 $f | cut -d' ' -f1)"
done
```

```text
events.jsonl      64  eaced853e9414cec7944df81f2e7522f22e567d9fb432633704d38bc970ee6ae
events.jsonl.zst  67  2f1febbdab00d32e9281c52d35f64daefbfd0e8d67f80ae39d03e3f4c2aebbf4
metadata.json     29  aea26bf837a0d323f6c9f93e4e00b93950f46e80cdbcdf7b321635a35b418545
```

Note that the compressed form is *larger* here — 67 bytes against 64. That is fine; Patwari never
requires stored bytes to be smaller than original. It is also why a real producer compresses first
and keeps the compressed representation only when it wins.

`events.jsonl` and `metadata.json` are arbitrary names. **Patwari never branches on a logical
path**; roles are a convention between producer and consumer (ADR
[0005](adr/0005-artifact-roles-by-logical-path.md)). The convention Munshi and Qanungo actually
use — `summary.md`, `transcript.jsonl`, `outputs/<sha256>`, `sidecar/<path>` — is written down in
[`docs/consumers.md`](consumers.md#the-artifact-role-convention).

## 1. Register the client

The client ID is a UUID the client generates and keeps. `PUT` is idempotent: hostname, display
name, and metadata are mutable attributes of it.

```sh
CLIENT=11111111-2222-4333-8444-555555555555
curl -sS -X PUT "$BASE/api/v1/clients/$CLIENT" \
  -H 'Content-Type: application/json' \
  -d '{"hostname":"patwari.example.net","display_name":"worked-example"}'
```

```json
HTTP 200
{"client_id":"11111111-2222-4333-8444-555555555555","hostname":"patwari.example.net",
 "display_name":"worked-example","metadata":{},
 "created_at":"2026-09-05T06:06:14.942711Z","updated_at":"2026-09-05T06:06:14.942711Z"}
```

## 2. Create the upload

The body is the client ID, a client-generated `capture_id`, and the
[manifest v1](api.md#multi-artifact-manifest-v1) document. Every artifact declares **both**
representations: the original (decompressed) size and hash, and the stored size and hash of the
bytes you are about to send. Hashes are `sha256:` plus 64 lowercase hex digits.

```sh
cat > upload-request.json <<'JSON'
{
  "client_id": "11111111-2222-4333-8444-555555555555",
  "capture_id": "worked-example-0001",
  "manifest": {
    "schema_version": 1,
    "session": {
      "source_agent": "demo-cli",
      "source_session_id": "demo-session-1"
    },
    "capture": {
      "captured_at": "2026-09-05T05:00:00Z",
      "source_cursor": "2",
      "source_state_hash": "sha256:eaced853e9414cec7944df81f2e7522f22e567d9fb432633704d38bc970ee6ae",
      "source_metadata": {"hostname": "patwari.example.net", "utc_offset": "+00:00"},
      "project": "demo",
      "repository": "example/demo",
      "branch": "main",
      "source_agent_version": "0.1.0",
      "artifact_set_version": 1,
      "munshi_version": "0.1.0"
    },
    "artifacts": [
      {
        "logical_path": "events.jsonl",
        "media_type": "application/x-ndjson",
        "original_size_bytes": 64,
        "original_sha256": "sha256:eaced853e9414cec7944df81f2e7522f22e567d9fb432633704d38bc970ee6ae",
        "stored_size_bytes": 67,
        "stored_sha256": "sha256:2f1febbdab00d32e9281c52d35f64daefbfd0e8d67f80ae39d03e3f4c2aebbf4",
        "compression": "zstd"
      },
      {
        "logical_path": "metadata.json",
        "media_type": "application/json",
        "original_size_bytes": 29,
        "original_sha256": "sha256:aea26bf837a0d323f6c9f93e4e00b93950f46e80cdbcdf7b321635a35b418545",
        "stored_size_bytes": 29,
        "stored_sha256": "sha256:aea26bf837a0d323f6c9f93e4e00b93950f46e80cdbcdf7b321635a35b418545",
        "compression": "identity"
      }
    ]
  }
}
JSON

curl -sS -X POST "$BASE/api/v1/uploads" \
  -H 'Content-Type: application/json' --data @upload-request.json | jq .
```

```json
HTTP 201
{
  "upload_id": "01a0702c-bbb1-7371-85b7-c9542a90a657",
  "capture_id": "worked-example-0001",
  "session_id": "01a0702c-bbb1-7371-85b7-c949256a7fcf",
  "status": "created",
  "manifest_sha256": "sha256:657a7fd19e8d8e8bdd5dc55bf565892c8efae8fdad4a867e9f28ad39d8bf7cd6",
  "chunk_size_bytes": 4194304,
  "artifacts": [
    {
      "artifact_index": 0,
      "logical_path": "events.jsonl",
      "stored_size_bytes": 67,
      "stored_sha256": "sha256:2f1febbdab00d32e9281c52d35f64daefbfd0e8d67f80ae39d03e3f4c2aebbf4",
      "compression": "zstd",
      "chunk_count": 1,
      "chunk_upload_url": "/api/v1/uploads/01a0702c-…/artifacts/0/chunks/{chunk_index}",
      "accepted_chunk_bitmap": "00",
      "missing_chunk_indexes": [0]
    },
    {
      "artifact_index": 1,
      "logical_path": "metadata.json",
      "stored_size_bytes": 29,
      "compression": "identity",
      "chunk_count": 1,
      "chunk_upload_url": "/api/v1/uploads/01a0702c-…/artifacts/1/chunks/{chunk_index}",
      "accepted_chunk_bitmap": "00",
      "missing_chunk_indexes": [0]
    }
  ],
  "status_url": "…", "abandon_url": "…", "completion_url": "…", "capture_url": "…"
}
```

Three things the server decided, not you:

- **`artifact_index` is canonical order**, ascending by `logical_path` — `events.jsonl` is 0 and
  `metadata.json` is 1 regardless of the order you sent them in. The server normalises and sorts
  `artifacts[]` before persisting; client order never affects snapshot identity.
- **`chunk_size_bytes` is server-assigned** (`PATWARI_UPLOAD_CHUNK_SIZE_BYTES`, default 4 MiB), and
  the per-index chunk length is derived from it, final chunk included. You do not get to choose.
- **`session_id` already exists.** Session creation is atomic with upload creation, keyed by
  `source_agent + source_session_id`; a second upload for the same pair joins the same session.

## 3. Send the chunks

Each `PUT` needs exactly three headers, and the bytes are the **stored** representation — the
zstd frame for artifact 0, the plain file for artifact 1.

```sh
UP=01a0702c-bbb1-7371-85b7-c9542a90a657

curl -sS -X PUT "$BASE/api/v1/uploads/$UP/artifacts/0/chunks/0" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Patwari-Chunk-Length: 67' \
  -H 'X-Patwari-Chunk-SHA256: sha256:2f1febbdab00d32e9281c52d35f64daefbfd0e8d67f80ae39d03e3f4c2aebbf4' \
  --data-binary @events.jsonl.zst          # HTTP 204

curl -sS -X PUT "$BASE/api/v1/uploads/$UP/artifacts/1/chunks/0" \
  -H 'Content-Type: application/octet-stream' \
  -H 'X-Patwari-Chunk-Length: 29' \
  -H 'X-Patwari-Chunk-SHA256: sha256:aea26bf837a0d323f6c9f93e4e00b93950f46e80cdbcdf7b321635a35b418545' \
  --data-binary @metadata.json             # HTTP 204
```

A successful chunk is `204 No Content` with no body. The failure modes are all `422
validation_error`, each with a distinct message, and every one of these was provoked against the
running server:

| What you did | Message |
| --- | --- |
| Omitted `Content-Type: application/octet-stream` | `artifact upload requires application/octet-stream content type` |
| Omitted the `X-Patwari-Chunk-*` headers | `chunk checksum and length headers are required` |
| Declared a length the layout does not allow | `chunk length does not match the negotiated chunk layout` |
| Declared a checksum the body does not hash to | `chunk body length or checksum does not match its headers` |
| Used a chunk index past `chunk_count` | `chunk index is outside the negotiated artifact range` |

The headerless compatibility form mentioned in the reference is narrower than it sounds: it applies
only when the **whole upload** is one artifact of exactly one chunk. A multi-artifact upload always
sends the headers.

## 4. Check what landed

`GET /api/v1/uploads/{id}` is the resume surface — safe to poll, and the only thing you need after
a crash.

```sh
curl -sS "$BASE/api/v1/uploads/$UP" | jq '{status, artifacts: [.artifacts[]
  | {artifact_index, logical_path, accepted_chunk_bitmap, missing_chunk_indexes}]}'
```

```json
HTTP 200
{
  "status": "artifact_uploaded",
  "artifacts": [
    {"artifact_index": 0, "logical_path": "events.jsonl",
     "accepted_chunk_bitmap": "01", "missing_chunk_indexes": []},
    {"artifact_index": 1, "logical_path": "metadata.json",
     "accepted_chunk_bitmap": "01", "missing_chunk_indexes": []}
  ]
}
```

`accepted_chunk_bitmap` is lowercase hex; **byte zero holds chunk indexes 0–7 with the
least-significant bit as index 0**, so `01` means chunk 0 is in. `missing_chunk_indexes` is the
same fact spelled out, and is what a resuming client actually re-sends.

## 5. Complete

```sh
curl -sS -X POST "$BASE/api/v1/uploads/$UP/complete" | jq .
```

```json
HTTP 200
{
  "receipt": {
    "receipt_version": 2,
    "archive_instance_id": "01a0702c-7332-7ae1-bc26-8735f50104cd",
    "owner_namespace": "v1",
    "snapshot_id": "01a0702c-bc18-7f81-bf0e-3b10292c3417",
    "session_id": "01a0702c-bbb1-7371-85b7-c949256a7fcf",
    "snapshot_fingerprint": "sha256:772ca740d392caad21ff229064de6cae4c3d56de0f985beb308c240a4421ad3b",
    "manifest_sha256": "sha256:657a7fd19e8d8e8bdd5dc55bf565892c8efae8fdad4a867e9f28ad39d8bf7cd6",
    "artifact_count": 2,
    "total_original_bytes": 93,
    "total_stored_bytes": 96,
    "completed_at": "2026-09-05T06:06:15.064012Z"
  },
  "transfer": {
    "upload_id": "01a0702c-bbb1-7371-85b7-c9542a90a657",
    "capture_id": "worked-example-0001",
    "upload_transfer_bytes": 96,
    "newly_persisted_physical_bytes": 96
  },
  "capture": { "capture_record_id": "01a0702c-bc19-7c73-ba28-1a547759e146", … }
}
```

Completion is where the archive earns the word: it reassembles every declared artifact, streams it
under bounded memory, verifies the stored size and hash **and** the decompressed original size and
hash, and only then makes the snapshot visible in one atomic metadata commit. Nothing is half
archived, and a declared checksum is never trusted on its own.

The receipt separates three kinds of fact deliberately: **immutable snapshot evidence**
(`receipt`), **mutable transfer accounting** (`transfer`), and **this capture's provenance**
(`capture`). Only the first is snapshot identity.

Completing an upload that is missing a chunk is `409 artifact_incomplete` —
`all negotiated chunks must be accepted before completion`.

## 6. Read the capture back

```sh
curl -sS "$BASE/api/v1/uploads/$UP/capture" | jq .
```

```json
HTTP 200
{
  "capture_record_id": "01a0702c-bc19-7c73-ba28-1a547759e146",
  "capture_id": "worked-example-0001",
  "client_id": "11111111-2222-4333-8444-555555555555",
  "session_id": "01a0702c-bbb1-7371-85b7-c949256a7fcf",
  "snapshot_id": "01a0702c-bc18-7f81-bf0e-3b10292c3417",
  "manifest_id": "01a0702c-bbb2-7f10-8916-67a0e1ffca34",
  "manifest_sha256": "sha256:657a7fd19e8d8e8bdd5dc55bf565892c8efae8fdad4a867e9f28ad39d8bf7cd6",
  "source_captured_at": "2026-09-05T05:00:00Z",
  "source_cursor": "2",
  "source_metadata": {"hostname": "patwari.example.net", "utc_offset": "+00:00"},
  "project": "demo", "repository": "example/demo", "branch": "main",
  "source_agent_version": "0.1.0", "artifact_set_version": 1, "munshi_version": "0.1.0",
  "server_received_at": "2026-09-05T06:06:14.960937Z",
  "server_completed_at": "2026-09-05T06:06:15.064012Z",
  "capture_url": "/api/v1/captures/01a0702c-bc19-7c73-ba28-1a547759e146",
  "manifest_url": "/api/v1/manifests/01a0702c-bbb2-7f10-8916-67a0e1ffca34"
}
```

`source_metadata` comes back exactly as it was supplied. Patwari stores it and never queries,
indexes, or logs its keys.

## 7. And it is in the archive

```sh
curl -sS "$BASE/api/v1/sessions?limit=100&activity_from=2026-09-01T00:00:00Z" \
  | jq '[.items[] | {session_id, source_agent, source_session_id,
                     latest: .latest_snapshot.snapshot_id,
                     artifact_set_version: .latest_snapshot.artifact_set_version}]'
```

```json
[
  {"session_id": "01a0702c-bbb1-7371-85b7-c949256a7fcf", "source_agent": "demo-cli",
   "source_session_id": "demo-session-1",
   "latest": "01a0702c-bc18-7f81-bf0e-3b10292c3417", "artifact_set_version": 1}
]
```

From here the read side takes over: [`docs/consumers.md`](consumers.md) walks from this listing to
the artifact bytes and back.

## Idempotency, conflicts, and abandon

Two identities are at work and it is worth keeping them apart:

- **`capture_id`** is *yours*. It names one durable capture observation, is scoped to
  `(owner, client, capture_id)`, and is unrelated to content.
- **`snapshot_fingerprint`** is *the server's*. It is derived from the session plus the verified
  content: project, repository, branch, source-agent version, `artifact_set_version`, and each
  canonical logical path with its verified original content. It deliberately excludes client and
  capture/upload IDs, source and server times, source cursor and state hash, Munshi version,
  `source_metadata`, transfer metrics, compression, and the stored representation.

That split is what makes the flow safe to retry:

```sh
# Same capture_id, byte-identical manifest -> the existing upload, no new capture.
curl -sS -X POST "$BASE/api/v1/uploads" -H 'Content-Type: application/json' \
  --data @upload-request.json                                              # HTTP 200

# Same capture_id, changed manifest (branch: main -> other) -> refused.
jq '.manifest.capture.branch = "other"' upload-request.json > changed.json
curl -sS -X POST "$BASE/api/v1/uploads" -H 'Content-Type: application/json' --data @changed.json
# HTTP 409
# {"error":{"code":"capture_id_conflict",
#           "message":"capture identifier was already used for a different manifest"}}

# Retrying completion returns the same receipt, not a second snapshot.
curl -sS -X POST "$BASE/api/v1/uploads/$UP/complete" | jq -r .receipt.snapshot_id
# 01a0702c-bc18-7f81-bf0e-3b10292c3417
```

And a genuinely new capture of unchanged content **coalesces** rather than duplicating. Uploading
the same two artifacts under `capture_id: worked-example-0002` produced:

```json
{"snapshot": "01a0702c-bc18-7f81-bf0e-3b10292c3417",
 "fp": "sha256:772ca740d392caad21ff229064de6cae4c3d56de0f985beb308c240a4421ad3b",
 "transferred": 96, "newly_persisted_physical_bytes": 0, "capture": "worked-example-0002"}
```

— the same snapshot ID and fingerprint as the first upload, a second capture record against it
(`capture_count: 2` on that session's snapshot listing), 96 bytes transferred and **zero newly
persisted**, because both blobs deduplicated onto the ones already stored.

### What a 409 means

A `409` is never "try again harder". It always means *an identity you asserted is already bound to
something else*, and the fix is on the client:

| Code | Meaning | Fix |
| --- | --- | --- |
| `capture_id_conflict` | This `capture_id` already exists for this client with a different canonical manifest | Mint a new `capture_id` for the changed content. Reusing one is a client bug. |
| `chunk_conflict` | This artifact/chunk index was already accepted with a different length or checksum | Your bytes changed under the upload. Abandon and start a new capture; accepted bytes are never replaced. |
| `artifact_incomplete` | Completion was called with chunks still missing | Read `missing_chunk_indexes` from `GET /uploads/{id}` and send them. |
| `upload_completion_contended` | Metadata was busy completing a concurrent request | Retry the completion; this one *is* transient. |

### The deprecated `idempotency_key` alias

`capture_id` used to be called `idempotency_key`. The old name still works when `capture_id` is
omitted, and supplying **both** is allowed only when the values are identical — otherwise `422
validation_error`, `capture_id and idempotency_key must be identical when both are supplied`. New
clients should send `capture_id` only.

### Abandon

`POST /api/v1/uploads/{id}/abandon` explicitly discards an unfinished upload's resumable bytes; the
same thing happens by server time after `PATWARI_UPLOAD_EXPIRY` (default 24h). Both paths remove
the temporary files, chunk records, and the manifest, leaving only redacted audit facts — client
and session IDs, the opaque capture ID, the manifest digest, declared sizes and chunk count,
timestamps, terminal reason, and error code. Request bodies, logical paths, chunk checksums,
manifest contents, and artifact bytes are not retained.

```sh
curl -sS -X POST "$BASE/api/v1/uploads/$UP/abandon" | jq '{status, artifacts, manifest_sha256}'
# HTTP 200
# {"status": "abandoned", "artifacts": [], "manifest_sha256": null}
```

Abandon what you will not finish. An abandoned upload is terminal — it cannot be resumed. Its
`capture_id` may be used again by a fresh upload, but only under the **same** canonical manifest;
re-declaring it with changed content is still `409 capture_id_conflict`. Abandoning releases the
transfer, not the capture identity.
