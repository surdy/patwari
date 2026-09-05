# Consuming the archive

**Trust posture, first and plainly: Patwari has no authentication, so a consumer sends no
credential, and any client that can reach the listener can read the whole archive — the archive's
only protection is the private network it is on** (see [Trust and
authentication](../README.md#trust-and-authentication)).

This page is the read side of the API as a *consumer* meets it: a tool that reads somebody else's
archive, never writes to it, and has to get the same answer on every run. The producing side — how
bytes get in — is [`docs/ingest.md`](ingest.md); the complete route, header, and limit reference is
[`docs/api.md`](api.md). Domain vocabulary is in [`CONTEXT.md`](../CONTEXT.md).

The worked consumer is [Qanungo](https://github.com/surdy/qanungo), which mirrors a window of the
archive and folds the transcripts into a coaching report. Everything below is stated from the
archive's side, with the consumer's choices named where they are instructive rather than required.

## What a consumer needs, and what it does not

Four routes are enough to mirror an archive:

```text
GET /api/v1/sessions?activity_from=…&limit=…&cursor=…   which sessions are in my window
GET /api/v1/snapshots/{snapshot_id}                     one snapshot's manifest + artifact list
GET /api/v1/sessions/{session_id}/snapshots             that session's own history, newest first
GET /api/v1/artifacts/{artifact_id}/content             the stored bytes, with verification headers
```

Everything else in [`docs/api.md`](api.md) is available and none of it is required for this shape.
A consumer never `POST`s, never `PUT`s, and never touches `/api/v1/admin/*`.

Budget your client against what the server actually is: **one process, `PATWARI_MAX_CONCURRENCY`
requests in flight (default 64) and `PATWARI_MAX_DOWNLOAD_CONCURRENCY` response bodies (default
8)**. Qanungo runs a fixed pool of four workers and refuses to be configured above eight, because
a reader that opens more download slots than the archive has does not go faster — it occupies
every slot and starves the archive's other clients. A read-side client that *retries* into a busy
archive turns a slow server into an unavailable one, so Qanungo does not retry at all: a session
that fails is recorded as a skip and named in the report.

## How much is in the archive

Before mirroring a window, ask what the window can contain. `GET /api/v1/stats` returns the whole
archive as integers and timestamps in one request — `sessions`, `snapshots`, `captures`,
`artifacts`, `blobs`, `clients`, `tombstones`, the stored and original byte totals, `last_ingest_at`,
and `oldest_activity_at` / `newest_activity_at`. Those two bounds are the **same** session activity
time `activity_from` and `activity_to` filter on, so they tell a consumer the real extent of the
archive before it picks a window, and a window that starts before `oldest_activity_at` will not
find anything older. `GET /api/v1/clients` is its companion: every registered client with its
`hostname`, `capture_count`, and `last_capture_at`, which is how a reader answers "are all my
machines still reporting" without inferring it from its own mirror. Neither route paginates,
neither needs the admin surface, and both are cheap enough to call on every refresh — but neither
is a substitute for the cursors below: they are a snapshot of totals, not a traversal. Both are
documented in full in [`docs/api.md`](api.md#inventory).

## Discovering sessions incrementally

`GET /api/v1/sessions` lists **only sessions that have a visible completed snapshot**, and projects
that session's *latest* completed snapshot onto the row as `latest_snapshot`. Tombstoned snapshots
and their related captures, manifests, and artifacts are excluded from normal listings, so a
session whose only snapshot has been deleted is simply not there.

```json
{
  "items": [ { "session_id": "…", "source_agent": "…", "latest_snapshot": { … } } ],
  "next_cursor": "opaque-or-null",
  "high_watermark": {"timestamp": "2026-09-05T06:06:27.393693Z", "id": "01a07002-…"}
}
```

### `activity_from` / `activity_to`

Both are **inclusive** RFC 3339 instants (`>=` and `<=` in the query), and both compare against
**the projected latest snapshot's server completion time** — the archive's clock, not the
transcript's. Two consequences worth internalising:

- A session that is re-captured *moves*. Its projected completion time becomes the new snapshot's,
  so a session first archived months ago re-enters a window that only covers this week. This is
  usually what you want (you get the newest capture of it) and is never what a transcript-time
  filter would have done.
- Selecting a window by archive time and then reporting by transcript time are two different
  clocks. Say which one a number is on.

`activity_from` later than `activity_to` is rejected with `422 validation_error`. The same
inclusive bounds exist on `/snapshots` (snapshot completion) and `/captures` (capture completion).

### Cursors and `high_watermark`

Collections are ordered **descending by their documented server timestamp, then by UUIDv7 id**.
`limit` defaults to 50 and must be 1–100.

- The **first page** (no `cursor`) sets the traversal's high watermark: the newest row on that page.
  It is echoed as `high_watermark` on that page and every later page of the same traversal.
- `next_cursor` is present **only when the server saw at least one more row** beyond `limit`. A
  page that ends the collection returns `null`, and that is the traversal's terminator — the
  presence of a cursor, not a full page, is the signal.
- A cursor is opaque and **carries the first page's high watermark plus the last row returned**.
  Later pages are bounded on both sides: at or below the high watermark, and strictly past the last
  row. So records completed *after* the traversal began never interleave, nothing is duplicated,
  and nothing older is skipped past.
- A cursor is **bound to its collection and its exact filter set**. Reusing one with different
  filters is `422 validation_error` (`"cursor does not match this collection and filters"`), as is
  a tampered or truncated cursor. Never construct one from the `high_watermark` fields; those are
  informational.
- An empty collection returns `items: []` with `next_cursor` and `high_watermark` both `null`.

### Incremental across runs

A cursor is exact **within one traversal**; it is not a resume token between runs. There are two
honest ways to be incremental, and Qanungo chose the second:

1. Persist the previous run's `high_watermark.timestamp` and pass it as the next run's
   `activity_from`. Because the bound is inclusive you will see the boundary row again — which is
   correct, since timestamps can tie and the tiebreak is the id you are no longer carrying.
2. Re-list the whole window every run and let the local cache decide what to fetch. Listing is the
   cheap part (a window is a handful of 100-row pages); the expensive part is downloading content,
   and content is content-addressed, so a re-listed session whose `original_sha256` is already held
   costs nothing. This also self-heals: a session that was re-captured since the last run is
   re-listed with its new snapshot and picked up without any bookkeeping.

## Picking the right snapshot for a session

`latest_snapshot` is a *projection*, not a guarantee about content. **A snapshot that carries only
some of the artifact set still projects onto the session and shadows a complete sibling.** This is
not hypothetical: a Munshi backfill (munshi #78) produced a run of summary-only snapshots, each
of which became the projected latest snapshot of a session whose transcript was already safely
archived in an earlier one.

So the rule a consumer needs is:

1. Read `latest_snapshot.snapshot_id` from the listing row.
2. `GET /api/v1/snapshots/{id}` and look for the artifact you need, by logical path.
3. If it is not there, `GET /api/v1/sessions/{session_id}/snapshots` (newest first) and take the
   **newest sibling that does carry it**. One page is enough in practice; Qanungo reads 50 and does
   not chase further.
4. Then be careful about which snapshot each fact came from. The provenance on the row
   (`repository`, `project`, `branch`) belongs to the *projection*; `source_agent` and
   `artifact_set_version` — the two facts that decide how the transcript may be interpreted at all
   — must come from **the snapshot that actually carries the artifact**. They can differ, and
   believing a degenerate projection's provenance over the sibling's is exactly the mistake this
   walk exists to avoid.

Absence is a real archive state in both directions: a capture whose summariser never returned has
a transcript and no summary, exactly as the #78 captures have a summary and no transcript. Neither
is an error; both are a reason to look at the siblings.

## Reading a manifest

`GET /api/v1/snapshots/{id}` embeds the snapshot's selected canonical manifest and its complete,
**unpaginated** artifact list — one request per snapshot, which is why the walk above is cheap.
`GET /api/v1/snapshots/{id}/manifest` and `GET /api/v1/manifests/{manifest_id}` return the same
immutable document as separately addressable resources, with its `sha256`.

The manifest is where capture provenance is *stated*: `manifest.session.source_agent`,
`manifest.capture.artifact_set_version`, and the opaque `manifest.capture.source_metadata` map that
the producer filled in (Munshi writes `hostname` and `utc_offset` there, among others). Patwari
stores `source_metadata` unchanged and never queries, indexes, or logs its keys — treat it as
provenance you may read, not a filter you may push down.

A canonical manifest is immutable, and a snapshot's identity is derived from it. That is what lets
a consumer cache the whole snapshot document by id and never fetch it twice; Qanungo keeps a
snapshot index beside its blob cache for exactly this, which turned a warm sync from ~700 requests
into the listing pages alone.

## Fetching artifact content

`GET /api/v1/artifacts/{artifact_id}/content` returns the **stored** bytes. Patwari never
decompresses for a reader. Before it sends a byte it re-verifies the blob — size, SHA-256, and the
artifact's projection against the canonical manifest — so a `200` means the archive has just
proven what it is about to send you. The full header contract is in
[`docs/api.md`](api.md#verified-artifact-content); the four that matter to a verifying client are:

| Header | Use |
| --- | --- |
| `X-Patwari-Compression` | `identity` or `zstd` — how to decode the body |
| `X-Patwari-Stored-Size-Bytes`, `X-Patwari-Stored-SHA256` | check the bytes off the socket |
| `X-Patwari-Original-Size-Bytes`, `X-Patwari-Original-SHA256` | check the decoded bytes |
| `Digest` / `Content-Digest` | the same stored-byte digest in the standard forms |

`Cache-Control: no-transform` accompanies every response, and intermediaries must honour it. **An
HTTP client that automatically decodes `Content-Encoding` must have that behaviour turned off** or
it will hand you decoded bytes that no longer match `Content-Length` or the digest headers. With
`curl`, that is `--raw`.

Three things a consumer should build in, none of which the protocol enforces:

- **Verify as you stream.** Hash the stored bytes on the way in and the decoded bytes on the way
  out; treat what you have written as provisional until both digests agree. Stage the file and
  rename it into place only after that, so bytes that fail verification are unlinked rather than
  cached.
- **Cross-check the listing against the headers.** The snapshot's artifact row and the content
  headers are two renderings of the same manifest row. If they disagree, the archive is
  contradicting itself about this artifact — that is a refusal, not a number to reconcile.
- **Use the declared sizes as the transfer's bound.** Abort the moment actual stored bytes exceed
  the declared stored size, or decoded bytes exceed the declared original size, and cap the zstd
  decompression window explicitly (libzstd's default `windowLogMax` is 128 MiB per decoder, which a
  frame can demand simply by declaring it; nothing an honest capture produces needs more than 8
  MiB). Together these bound memory and disk against a lying or hostile response without capping
  how large a legitimate transcript may be.

**There is no range or resume on content.** `PATWARI_REQUEST_TIMEOUT` (default 30s) is armed as a
**whole-body deadline** when the download request begins — the generic request timeout ends once a
streaming response is constructed, so this one has to cover the body — and a download concurrency
permit is held for the entire body, slow reads included. A transfer the server cuts short simply
comes up short of its declared stored size and fails verification. Plan for restart-from-zero, not
for resumption, and keep a per-artifact cache so a restart is rare.

## Hash-addressed lookup

`GET /api/v1/artifacts?original_sha256=<64 lowercase hex>` (and `stored_sha256=`) resolves a
content hash straight to the artifacts carrying it, without walking sessions and snapshots
(ADR [0004](adr/0004-hash-addressed-artifact-lookup.md)). The value is **bare hex** — no `sha256:`
prefix, unlike the document fields. The filters compose with `snapshot_id` and `session_id` and
page normally.

This is the route for a client holding only a hash: a Munshi claim ticket embedded in a summary, or
a finding that cited a `source_hash` and now needs the bytes back. Because blobs are deduplicated
per owner, **one hash may resolve to several artifacts across snapshots** — the listing returns all
of them and the client chooses, typically the newest.

A hash filter is identity, not interpretation. Patwari still never searches within content, ranks
results, or decompresses for a reader.

## The artifact-role convention

*This section is the convention Munshi writes and Qanungo reads. **Patwari itself does not validate
it** — the server never branches on a logical path, and a manifest declaring any other set is
accepted exactly the same way (ADR
[0005](adr/0005-artifact-roles-by-logical-path.md)). It is recorded here because two tools already
depend on it and it was written down nowhere.*

A snapshot's artifacts carry no role or kind field. What an artifact *means* is conveyed by its
logical path, under a convention that `capture.artifact_set_version` versions. Consumers read the
convention for the version they understand; adding a role is a client-side change under a bumped
version, not a server migration.

The convention Munshi writes (`crates/munshi/src/patwari.rs`):

| Logical path | Media type | Role |
| --- | --- | --- |
| `summary.md` | `text/markdown` | The rendered session summary for this revision — YAML frontmatter plus Markdown. **Required.** |
| `transcript.jsonl` | `application/jsonl` | The verbatim harness transcript, byte for byte as the harness wrote it. **Required.** |
| `outputs/<sha256>` | per output (`text/plain; charset=utf-8` in practice) | One oversized tool/message output lifted out of the transcript and content-addressed by the lowercase hex SHA-256 of its own bytes. Zero or more. |
| `sidecar/<relative-path>` | by extension (`.md` → `text/markdown`, `.json` → `application/json`, `.yaml` → `application/yaml`, else `text/plain; charset=utf-8`) | Harness sidecar state staged at archive time, under its own relative path. Zero or more; **artifact set v2 only.** |

`artifact_set_version` values:

- **1** — `summary.md`, `transcript.jsonl`, and `outputs/<sha256>`. Manifests recorded before the
  field became a required input are interpreted as version 1.
- **2** — everything in 1, plus optional `sidecar/<relative-path>` artifacts (munshi #23). Presence
  is per-adapter conditional — Copilot stages an allowlisted set, Claude Code and Codex stage
  nothing — so **a consumer must tolerate an absent kind rather than treat it as damage.**
  Transcript interpretation is unchanged from v1: a v2 snapshot's transcript reads exactly as a
  v1 snapshot's does.

Four properties of the set that a consumer can rely on, and one it cannot:

- **Both required paths are always present, or the snapshot should not exist.** Munshi has one
  artifact-assembly path and it refuses to assemble a reduced set (munshi ADR 0009, #47), so a
  snapshot missing one of them predates that guarantee — which is precisely the #78 shape the
  sibling walk above exists for.
- **`outputs/<sha256>` is derived from the transcript in the same snapshot**, re-extracted from the
  exact bytes being uploaded, so the set is always consistent with `transcript.jsonl` and
  byte-identical across retries of one `capture_id`.
- **The stem of an `outputs/` path is the content address**, so `?original_sha256=<that stem>`
  resolves it directly — that is what a claim ticket in a summary points at.
- **The canonical order is ascending by logical path**, because that is how Patwari normalises
  `artifacts[]` before persisting: `outputs/…` before `sidecar/…` before `summary.md` before
  `transcript.jsonl`. Client order does not affect snapshot identity.
- **What you cannot rely on is the server.** A logical path is a string to Patwari. Match on it
  exactly, tolerate paths you do not recognise, and never assume a set you did not read.

## A minimal walk

Executed against a scratch `patwari-server` on `127.0.0.1:8080` holding one session whose latest
snapshot is summary-only — the #78 shape — and one earlier sibling with the full set.

```sh
BASE=http://127.0.0.1:8080

# 1. The window. Inclusive lower bound on the projected latest snapshot's completion time.
curl -sS "$BASE/api/v1/sessions?limit=100&activity_from=2026-09-01T00:00:00Z" \
  | jq '{next_cursor, high_watermark,
         items: [.items[] | {session_id, source_agent,
                             latest: .latest_snapshot.snapshot_id,
                             completed_at: .latest_snapshot.completed_at}]}'
```

```json
{
  "next_cursor": null,
  "high_watermark": {
    "timestamp": "2026-09-05T06:06:27.393693Z",
    "id": "01a0702c-eb37-71c2-b333-741aedc22e58"
  },
  "items": [
    {
      "session_id": "01a0702c-eb37-71c2-b333-741aedc22e58",
      "source_agent": "claude-code",
      "latest": "01a0702c-ec41-7c30-b8f1-a2c4e9c90977",
      "completed_at": "2026-09-05T06:06:27.393693Z"
    },
    {
      "session_id": "01a0702c-bbb1-7371-85b7-c949256a7fcf",
      "source_agent": "demo-cli",
      "latest": "01a0702c-bc18-7f81-bf0e-3b10292c3417",
      "completed_at": "2026-09-05T06:06:15.064012Z"
    }
  ]
}
```

Two sessions: the `claude-code` one this walk follows, and the `demo-cli` one left behind by the
upload in [`docs/ingest.md`](ingest.md). Note the ordering — descending by that projected
completion time — and that `high_watermark` names the newest row, which is the first item.

```sh
# 2. The projected latest snapshot — and it does not carry a transcript.
curl -sS "$BASE/api/v1/snapshots/01a0702c-ec41-7c30-b8f1-a2c4e9c90977" \
  | jq '{source_agent: .manifest.session.source_agent,
         artifact_set_version: .manifest.capture.artifact_set_version,
         paths: [.artifacts[].logical_path]}'
# => {"source_agent":"claude-code","artifact_set_version":2,"paths":["summary.md"]}

# 3. So walk the session's own snapshots, newest first, and take the newest complete sibling.
curl -sS "$BASE/api/v1/sessions/01a0702c-eb37-71c2-b333-741aedc22e58/snapshots?limit=50" \
  | jq '[.items[] | {snapshot_id, completed_at, artifact_count}]'
# => [{"snapshot_id":"01a0702c-ec41-…","artifact_count":1},
#     {"snapshot_id":"01a0702c-ebb6-7de0-97af-cc36bb98ef2e","artifact_count":2}]

# 4. The sibling's artifact set, and the row for the artifact we want.
curl -sS "$BASE/api/v1/snapshots/01a0702c-ebb6-7de0-97af-cc36bb98ef2e" \
  | jq '.artifacts[] | select(.logical_path == "transcript.jsonl")
        | {artifact_id, original_sha256, original_size_bytes, stored_size_bytes,
           compression, content_url}'
```

```json
{
  "artifact_id": "01a0702c-ebb7-76c0-be82-193caed0e995",
  "original_sha256": "sha256:f695a72ded1024170ddddbcca28e143ca4ffcb6c057319eddd8ff78a1630e51c",
  "original_size_bytes": 90,
  "stored_size_bytes": 85,
  "compression": "zstd",
  "content_url": "/api/v1/artifacts/01a0702c-ebb7-76c0-be82-193caed0e995/content"
}
```

```sh
# 5. The bytes. --raw keeps curl from decoding Content-Encoding under you.
curl -sS --raw -D headers.txt -o stored.bin \
  "$BASE/api/v1/artifacts/01a0702c-ebb7-76c0-be82-193caed0e995/content"
grep -i '^x-patwari-\|^cache-control\|^content-length' headers.txt
```

```text
content-length: 85
cache-control: no-transform
x-patwari-logical-path: dHJhbnNjcmlwdC5qc29ubA
x-patwari-logical-path-encoding: base64url
x-patwari-media-type: application/jsonl
x-patwari-compression: zstd
x-patwari-original-size-bytes: 90
x-patwari-original-sha256: sha256:f695a72ded1024170ddddbcca28e143ca4ffcb6c057319eddd8ff78a1630e51c
x-patwari-stored-size-bytes: 85
x-patwari-stored-sha256: sha256:8257062063da2f1360e345e5ee25270f9a845391f0a226e1f45d9243a6962baf
```

```sh
# 6. Verify both representations, then decode.
shasum -a 256 stored.bin        # 8257062063da2f13…  == x-patwari-stored-sha256
zstd -d -q -c stored.bin | shasum -a 256
                                # f695a72ded102417…  == x-patwari-original-sha256

# 7. And the same artifact by content hash alone — bare hex, no sha256: prefix.
curl -sS "$BASE/api/v1/artifacts?original_sha256=f695a72ded1024170ddddbcca28e143ca4ffcb6c057319eddd8ff78a1630e51c" \
  | jq '[.items[] | {artifact_id, snapshot_id, logical_path}]'
# => [{"artifact_id":"01a0702c-ebb7-…","snapshot_id":"01a0702c-ebb6-…",
#      "logical_path":"transcript.jsonl"}]
```

Paging behaves the same way at any limit — with `limit=1` against the same two-session archive, the
first page carried a `next_cursor` and the second returned the older session with `next_cursor:
null`, both echoing the first page's `high_watermark`.
