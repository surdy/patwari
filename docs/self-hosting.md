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

## Updating

Back up, verify, and replicate before every update. Migrations are forward-only:
do not start an older binary against a migrated volume. A code-only rollback is
safe only when you know the schema is unchanged; otherwise restore the
pre-upgrade verified backup into a fresh volume and run the matching image.
