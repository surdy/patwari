# Self-hosting Patwari

One Rust process, SQLite metadata, a local blob store: one container, one
persistent volume, no external dependencies.

**Read [Trust and authentication](../README.md#trust-and-authentication) first.**
Patwari has no authentication. Everything below assumes a private network you
control.

## Build the image

[`Containerfile`](../Containerfile) builds the production image with any OCI
builder (Podman, Docker, Buildah, BuildKit). Tag it with the exact Git commit
and deploy by immutable digest rather than a moving tag:

```sh
podman build --build-arg VCS_REF="$(git rev-parse HEAD)" \
  --tag patwari:git-"$(git rev-parse --short HEAD)" .
```

The image runs as UID/GID `10001`, exposes `8080/tcp`, declares
`/var/lib/patwari-volume` as its volume, and ships a `/readyz` health check. It
runs fine with a read-only root filesystem plus a small `/tmp`.

## Storage layout

Mount **one** persistent volume and point `PATWARI_DATA_DIR` at a
**subdirectory of it** — not at the mount root:

```text
/var/lib/patwari-volume          <- volume mount point (writable root)
└── data                         <- PATWARI_DATA_DIR
    ├── patwari.db
    ├── blobs/  uploads/  maintenance/
```

This is enforced, not stylistic. `backup restore` stages a sibling directory
next to its destination and renames it into place atomically, so the
destination's parent must be writable and on the same filesystem. A volume
mounted directly at `PATWARI_DATA_DIR` puts that sibling on the (usually
read-only) container root — `EROFS`, or a cross-device rename (`EXDEV`).
Restore refuses that layout up front with a clear error.

The volume is the archive, not a cache. Keep it on local durable storage; never
a network filesystem, an image layer, `/tmp`, or a build cache — that applies to
backup staging and replication targets too.

## Configuration

Configuration is entirely `PATWARI_*` environment variables. The full table of
variables, defaults, and bounds is in the
[API and CLI reference](api.md#configuration). The ones that matter on
a first deployment are `PATWARI_DATA_DIR` (above), `PATWARI_BIND_ADDR`
(defaults to `127.0.0.1:8080`), `PATWARI_ADMIN_DELETION_ENABLED` (`false`), and
`PATWARI_BLOB_GC_GRACE` (`90d`).

## Network posture

**Patwari has no authentication and no authorisation.** Any client that can
reach the listener can read and write the whole archive. So:

- bind loopback (the default), or a single private interface;
- run it on a private network — a home or office LAN, a VPN, or an overlay
  network such as Tailscale or WireGuard;
- put whatever TLS-terminating reverse proxy you already use in front of it,
  and add authentication there if you want any;
- never expose it to the public internet, directly or by port-forward;
- treat every device that can reach the listener as fully trusted.

In a container the process must bind a non-loopback address (`0.0.0.0:8080`) so
the runtime can publish it — but publish it on one private host address, never
on all interfaces.

`GET /healthz` is process liveness. `GET /readyz` also probes SQLite and every
storage directory, and is the deployment and monitoring check.

## Backups

`backup create` is a consistent online backup, not a file copy: it takes a
maintenance lease, refuses to run while an upload is active, checkpoints and
copies SQLite through its online backup API, inventories authoritative blob rows
in deterministic digest order, hashes every staged file, and atomically
finalises a self-contained directory with a versioned `manifest.json`. The
output directory must be outside `PATWARI_DATA_DIR`. Run it as a one-shot
container against the **same volume**; the shared lock and maintenance lease
coordinate across processes, so the server can stay up.

```sh
patwari-server backup create --output <backup-dir>/patwari-<timestamp>
patwari-server backup verify <backup-dir>/patwari-<timestamp>
```

`backup verify` checks the manifest, database checksum and archive identity,
every blob checksum and inventory entry, then boots a clean staged copy and runs
the full integrity scanner. It exits `1` for an unhealthy scan, `2` for
malformed or missing input. Verify before you replicate. Manifest hashes are
archive metadata; protect backup directories as you protect the archive.

A generic timer — substitute your own image reference and paths:

```ini
# <unit-dir>/patwari-backup.service
[Unit]
Description=Patwari online archive backup
After=patwari.service

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'stamp=$$(date -u +%%Y%%m%%dT%%H%%M%%SZ); exec <container-run-command> <image-ref> backup create --output <backup-dir>/patwari-$$stamp'
```

```ini
# <unit-dir>/patwari-backup.timer
[Timer]
OnCalendar=*-*-* 03:15:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

### Durability boundary

The archive volume and any backup directory on the same machine are operational
copies, not the durability boundary. **Replicating verified, finalised backup
directories to independent storage off the machine is the durability
boundary** — and retrieval from there should be tested periodically. A
successful local backup is no protection against losing the machine or its
filesystem. Patwari cannot verify that its bytes exist anywhere else; that
decision stays with you.

## Restore

On a clean host, install the image and volume but do not start the service.
Create a fresh, empty volume and restore into a subdirectory of its root:

```sh
patwari-server backup restore <backup-dir>/patwari-<timestamp> \
  --data-dir /var/lib/patwari-volume/data
```

Restore verifies the backup before touching the destination, refuses a non-empty
destination, refuses a destination whose parent is not writable or not on the
same filesystem, stages a full data directory, runs another complete integrity
scan, and installs it atomically only when that scan is healthy. It preserves
`archive_instance_id` and the owner namespace. Afterwards check `/readyz`, list
sessions, and re-fetch a known artifact to compare bytes. Never erase a damaged
volume until you hold both a forensic copy of it and a known-good backup.

## Integrity verification

```sh
patwari-server verify
```

Runs the complete maintenance scan without starting the listener and writes one
JSON report to stdout: `0` when no action is required, `1` for actionable
findings, `2` when it cannot complete safely. It may run against a live server,
but offline is the strongest consistency mode.

## Blob garbage collection

Deletion is the only irreversible operation in the suite, so it is opt-in and
delayed. An unreferenced blob becomes GC-eligible only after
`PATWARI_BLOB_GC_GRACE` (default `90d`, range `1m`–`365d`) of server time — long
enough for a mistaken tombstone to be noticed. Re-archiving the same content
clears a pending candidate in the same transaction that recreates the reference.
The administrative surface, GC included, returns `403` unless
`PATWARI_ADMIN_DELETION_ENABLED=true`:

```sh
curl -X POST <base-url>/api/v1/admin/blob-gc
```

Enable it only behind the same private boundary the unauthenticated service
already requires, and prefer turning it back off afterwards.

## Disk full

**The symptom is `/readyz` going `503`, while `/healthz` stays `200`.** Readiness
probes SQLite *and* does a write-and-remove probe in every storage directory; a
full volume fails that probe, so the archive advertises itself as not ready while
the process stays alive. API writes fail alongside it with `500 storage_error`.
That split is deliberate: a full disk is an operator problem, not a crash, and
restarting the process fixes nothing.

Find out what is actually using the volume. With any OCI runtime (substitute
`docker` for `podman` throughout):

```sh
# Where the volume lives on the host, then what is in it.
podman volume inspect <volume> --format '{{.Mountpoint}}'
du -sh <mountpoint>/data/blobs <mountpoint>/data/uploads <mountpoint>/data/patwari.db*

# Or from inside the running container, against the mount itself.
podman exec <container> df -h /var/lib/patwari-volume

# Runtime-wide accounting, including volumes and image layers.
podman system df -v
```

Three things occupy the volume and they behave completely differently:

- **`blobs/` is permanent.** It only ever grows, and nothing but administrative
  deletion followed by GC after grace removes anything from it.
- **`uploads/` is temporary and self-clearing.** Unfinished uploads expire by
  server time after `PATWARI_UPLOAD_EXPIRY` (default `24h`), and restart recovery
  removes file-only remnants left by a crash. A large `uploads/` means clients
  are starting transfers they never finish or abandon — fix that at the client;
  `POST /api/v1/uploads/{id}/abandon` clears one immediately.
- **`maintenance/` holds a lock file.** It is never the problem.

### Reclaiming permanent space

Only one path frees blob bytes, and it is deliberately slow:

```sh
# 0. Enable the administrative surface. It is 403 by default.
#    PATWARI_ADMIN_DELETION_ENABLED=true

# 1. Get the snapshot's fingerprint, and build the exact confirmation for it.
FP=$(curl -sS <base-url>/api/v1/snapshots/<snapshot-id> | jq -r .snapshot_fingerprint)
CONFIRMATION="delete-snapshot:<snapshot-id>:$FP"

# 2. Tombstone it. The confirmation is bound to this resource; a wrong one is
#    409 deletion_confirmation_mismatch, and no snapshot is deleted by accident.
curl -sS -X DELETE <base-url>/api/v1/admin/snapshots/<snapshot-id> \
  -H "X-Patwari-Delete-Confirmation: $CONFIRMATION"

# 3. Collect. Bounded batch; repeat until deleted_blobs is 0.
curl -sS -X POST <base-url>/api/v1/admin/blob-gc
# {"inspected_blobs":0,"deleted_blobs":0}

# 4. Turn the administrative surface back off.
```

**Step 3 frees nothing until the grace period has passed.** Deletion removes
`Artifact` rows and records `orphaned_at`/`eligible_after` on the blobs; GC only
touches a blob once `PATWARI_BLOB_GC_GRACE` (default `90d`) of server time has
elapsed since then. Run immediately after a tombstone, GC reports zero inspected
and zero deleted — that is correct, not a failure.

So **a full disk is not an emergency you can delete your way out of today.** Size
the volume for the archive you intend to keep, and alert on utilisation well
before it is full. If you genuinely need the bytes sooner, the grace is
configurable down to `1m` — but shortening it is a decision to give up the window
in which a mistaken deletion can still be noticed, and deletion is the only
irreversible act in the suite. Prefer growing the volume.

Re-archiving the same content clears a pending candidate in the same transaction
that recreates the reference, so a tombstone made in error is recoverable right
up until GC collects it.

### Backup retention

A backup directory is a *full* copy — the SQLite database plus every blob — so
each retained generation costs roughly what the archive costs. `backup create`
refuses an output directory inside `PATWARI_DATA_DIR`; also keep it off the
archive volume entirely, or a backup will be the thing that fills the disk it is
protecting.

Retain by generation, not by whim: keep N verified, replicated generations
off-box, and delete the oldest only after a newer one has passed
`backup verify` *and* landed on the independent storage. A backup you have not
verified is not a generation, and deleting a good one to make room for an
unverified one is the one retention mistake that loses data.

## Reading logs

Everything the process says goes to **stderr as one JSON object per line**:

```json
{"timestamp":"2026-09-05T05:17:54.042405Z","level":"INFO",
 "fields":{"message":"archive service listening"},"target":"patwari_server::service"}
```

It records operational fields only — HTTP method, status, duration, and stable
messages. It never logs request bodies, archived content, credentials, or
filesystem paths, so a log file is not archive content and a log excerpt is safe
to paste into an issue.

Verbosity is `RUST_LOG`, a standard `tracing` env filter, defaulting to `info`:

```sh
RUST_LOG=warn                      # quieter
RUST_LOG=patwari_server=debug      # this crate only
```

Reading it back:

```sh
podman logs -f <container>                 # container runtime
journalctl -u <unit> -f                    # systemd, following
journalctl -u <unit> --since '1 hour ago' -p err
journalctl -u <unit> -o cat | jq 'select(.level != "INFO")'   # lines are JSON, so filter them
```

The CLI commands split their streams on purpose: `verify`, `backup create`,
`backup verify`, and `backup restore` write exactly one JSON report to **stdout**
and diagnostics to **stderr**. Redirect them separately — `> report.json` keeps
the report machine-readable while the diagnostics still reach the journal.

## What a `503` means

Two error codes are answered with `503`, and they mean different things.

**`maintenance_in_progress` is expected and temporary.** `backup create` takes an
exclusive maintenance lease so it can copy SQLite and inventory blobs against a
quiescent archive, and while that lease is held **every** `/api/v1` route answers
`503 maintenance_in_progress` — reads included, because even an upload-status
`GET` can terminalize expired temporary state. `/healthz` and `/readyz` stay
available throughout, which is why they are the right monitoring checks: a
backup window must not page you.

```json
{"error":{"code":"maintenance_in_progress",
          "message":"archive operations are temporarily paused for maintenance"}}
```

The window is bounded at both ends. Backup waits up to 15 minutes for in-flight
requests to drain before it claims the lease, and the lease itself expires after
6 hours, so a crashed backup process cannot wedge the archive indefinitely.
`backup create` also refuses to start while any upload is still active — "an
active upload must complete or be abandoned before backup" — so the fix for a
backup that will not start is usually a stuck client, not the server.

Clients should treat this as back-off-and-retry. Munshi's uploader already does;
a read-side consumer should too, rather than hammering.

**`maintenance_unavailable` is not a maintenance window.** It means the
coordination itself failed — the lock file or the gate row could not be read or
written. Check `/readyz` and the volume; this usually accompanies a full or
read-only filesystem.

## Error codes

Every API error is `{"error": {"code": …, "message": …}}`. The codes are stable;
the messages are for humans. The ones a client can actually see:

| Code | Status | Meaning |
| --- | --- | --- |
| `validation_error` | 422 | The request is malformed, out of bounds, or contradicts a negotiated layout. The message says which. |
| `endpoint_not_found` | 404 | No such route under `/api/v1`. |
| `request_timeout` | 408 | The request exceeded `PATWARI_REQUEST_TIMEOUT`. |
| `client_not_found`, `session_not_found`, `snapshot_not_found`, `capture_not_found`, `manifest_not_found`, `artifact_not_found`, `upload_not_found`, `tombstone_not_found` | 404 | That resource does not exist, or is tombstoned and therefore excluded from normal listings. |
| `capture_id_conflict` | 409 | This `capture_id` already exists for this client under a *different* canonical manifest. Mint a new one. |
| `chunk_conflict` | 409 | That chunk index was already accepted with different bytes. Accepted bytes are never replaced. |
| `artifact_incomplete` | 409 | Completion was called with chunks still missing. |
| `upload_state_conflict`, `upload_completed`, `upload_expired`, `snapshot_deleted` | 409 | The upload or snapshot is in a state that does not admit this operation. |
| `upload_completion_contended`, `snapshot_deletion_contended` | 409 | Metadata was busy with a concurrent request. **The only 409 that is worth retrying as-is.** |
| `capture_provenance_conflict`, `upload_artifact_projection_invalid`, `blob_integrity_conflict` | 409 | The submitted capture contradicts what the archive already holds. |
| `chunk_missing`, `chunk_checksum_mismatch`, `chunk_layout_invalid`, `artifact_stored_checksum_mismatch`, `artifact_original_checksum_mismatch` | 422 | Transferred bytes did not match what was declared. The archive verified rather than trusted. |
| `artifact_integrity_failure` | 409 | The artifact exists but its stored blob could not be proven against the canonical manifest. **This is corruption, not absence** — deliberately distinct from a 404 so a client never mistakes damaged storage for an empty download. Run `patwari-server verify`. |
| `admin_deletion_disabled` | 403 | `PATWARI_ADMIN_DELETION_ENABLED` is `false`. |
| `deletion_confirmation_mismatch` | 409 | The `X-Patwari-Delete-Confirmation` value does not match this snapshot's fingerprint. |
| `maintenance_in_progress`, `maintenance_unavailable` | 503 | See above. |
| `metadata_error`, `storage_error`, `internal_error` | 500 | SQLite, the blob store, or the service itself could not complete the operation. Check `/readyz` and the logs. |

The CLI reports failures in the same shape on stdout, with its own codes —
`invalid_command`, `configuration_error`, `bootstrap_error`, `scan_error`,
`report_serialization_error`, `backup_create_failed`, `backup_verify_failed`,
`backup_restore_failed` — and exit `2`.

## Updating

Back up, verify, and replicate before every update. Migrations are forward-only:
do not start an older binary against a migrated volume. A code-only rollback is
safe only when you know the schema is unchanged; otherwise restore the
pre-upgrade verified backup into a fresh volume and run the matching image.
