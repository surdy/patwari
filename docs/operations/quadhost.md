# Operating Patwari on quadhost

This is the v1 single-host deployment runbook. quadhost is
`core@192.168.16.169` (`x86_64`, Podman 5.8.1); its `/var` filesystem is local
XFS. Patwari has no authentication in v1. Treat every client able to reach the
listener as trusted.

## Network boundary

The checked-in Quadlet publishes **only** `192.168.16.169:8787`:

```text
Munshi on trusted LAN -> 192.168.16.169:8787 -> container 0.0.0.0:8080
```

The server must bind `0.0.0.0:8080` inside its network namespace so Podman can
publish it, but never publish it on `0.0.0.0` at the host. Do not add an
internet-facing reverse proxy, port-forward, or firewall exception. Maintain a
host firewall rule permitting only the trusted LAN.

The quadlet additionally carries caddy-docker-proxy labels publishing
`patwari.clusterfault.com` through the host's Caddy on 443. This is the
reviewed Tailscale/LAN publish: DNS for the name is managed declaratively by
dnscontrol in the quadhost repository and resolves only to the LAN address
(UniFi, for home clients) and the Tailscale CGNAT address `100.81.17.63`
(Cloudflare, unroutable off the tailnet). Because Patwari v1 is
unauthenticated, every device on the tailnet is inside the trust boundary, and
the name must never resolve to a publicly routable address.

## Build and install

Build on quadhost (or produce an `linux/amd64` image), tag it with the exact
Git commit, and record its immutable digest:

```sh
git rev-parse HEAD
sudo podman build --arch amd64 --build-arg VCS_REF="$(git rev-parse HEAD)" \
  --tag localhost/patwari:git-<commit> .
sudo podman image inspect localhost/patwari:git-<commit> \
  --format '{{index .RepoDigests 0}}'
```

Push the image to the chosen private registry if needed. Deploy a reference
with both the commit tag and digest—never `latest`:

```sh
sudo deploy/quadhost/install.sh \
  --image registry.example/patwari:git-<commit>@sha256:<digest>
sudoedit /etc/containers/systemd/patwari.env
sudo systemctl restart patwari.service
```

The script installs the Quadlets under
`/etc/containers/systemd/`, preserves an existing environment file, reloads
systemd, then restarts `patwari.service` if it is already active (a plain
`start` is a no-op on an active unit and would silently keep the old image
running) or starts it if inactive. It then polls the container's own health
check and exits non-zero without reporting success if the update leaves the
service unhealthy. The generated named volume is
`systemd-patwari-data`; rootful Podman places it beneath
`/var/lib/containers/storage/volumes/` on local `/var` XFS. It is the archive,
not a cache:

```sh
sudo podman volume inspect systemd-patwari-data
sudo systemctl --no-pager status patwari.service
curl --fail http://192.168.16.169:8787/readyz
```

`ReadOnly=true`, a small `/tmp` tmpfs, dropped capabilities,
`NoNewPrivileges`, a non-root image user, cgroup resource limits, journald
logging, graceful `SIGTERM`, and a `/readyz` container health check are all
set in `patwari.container`. The `:z,U` volume option gives the named volume a
shared SELinux label and gives its contents to the image UID. The lowercase
`:z` is required because the running service and maintenance containers mount
the same volume concurrently; private `:Z` relabeling can revoke the running
container's access and trigger a health restart. Retain the shared label when
adapting the Quadlet; direct bind mounts need the equivalent SELinux label and
must be on local durable storage, never NFS or disposable cache.

The volume is mounted at the writable root `/var/lib/patwari-volume`, not at
`PATWARI_DATA_DIR` itself; `patwari.env` points `PATWARI_DATA_DIR` at the
`data` subdirectory beneath it (the server creates that subdirectory on first
bootstrap). Restore stages a same-filesystem sibling directory next to its
destination before an atomic rename; mounting the volume directly at
`PATWARI_DATA_DIR` would put that sibling on the read-only container root
(`EROFS`) or make the rename cross filesystems (`EXDEV`). Keep this
volume-root-plus-subdirectory layout when adapting the Quadlet.

`/healthz` means the process is alive. `/readyz` additionally probes SQLite
and all persistent storage directories and is the deployment and monitoring
check.

## Online backup

Backups contain `patwari.db`, `blobs/sha256/...`, and a versioned
`manifest.json`. They deliberately exclude `uploads/`: `backup create` refuses
while any upload is active, so archive metadata never references omitted
temporary chunks. The output directory must be outside `PATWARI_DATA_DIR`.

Run the CLI from a one-shot container with the **same named volume**. The
shared lock and SQLite maintenance lease then coordinate with the running
service across processes:

```sh
IMAGE='registry.example/patwari:git-<commit>@sha256:<digest>'
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
sudo install -d -o 10001 -g 10001 -m 0750 /var/backups/patwari
sudo podman run --rm --network=none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user 10001:10001 \
  -v systemd-patwari-data:/var/lib/patwari-volume:z \
  -v /var/backups/patwari:/backups:Z \
  "$IMAGE" backup create --output "/backups/patwari-$STAMP"
```

The procedure is intentionally consistent rather than a filesystem copy:

1. It records a short-lived SQLite maintenance lease, then holds an advisory
   lock in the persistent `maintenance/` directory. New API work fails fast;
   existing API work and integrity scans drain.
2. It refuses active uploads, performs a passive WAL checkpoint, and uses the
   SQLite online backup API for a coherent database copy.
3. While blob promotion, deletion, GC, and scanning are quiesced, it reads
   authoritative `Blob` rows in sorted digest order, copies or safely
   hard-links each canonical blob into staging, and hashes every staged file.
4. It writes the manifest (archive identity, owner, schema/application
   versions, SQLite checksum/size, deterministic blob inventory, and latest
   integrity context), then atomically renames staging to the final backup
   directory. The detached database has its transient maintenance lease
   cleared.

Normal logs and reports do not include logical artifact paths or content.
Hashes in the manifest are still archive metadata; protect backup access
accordingly.

Verify every backup before replication. Verification needs writable scratch
space beside the backup directory but never changes the finalized backup:

```sh
sudo podman run --rm --network=none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user 10001:10001 \
  -v /var/backups/patwari:/backups:Z \
  "$IMAGE" backup verify "/backups/patwari-$STAMP"
```

`backup verify` checks the manifest, database checksum and identity, every
blob checksum and inventory entry, boots a clean staged copy, and runs the
full integrity scanner. A non-healthy scan exits `1`; malformed or missing
backup input exits `2`.

### Scheduled example

Use an immutable image reference in a root-owned service. The command below
is an example; adjust backup retention and replication separately.

```ini
# /etc/systemd/system/patwari-backup.service
[Unit]
Description=Patwari online archive backup
After=patwari.service

[Service]
Type=oneshot
Environment=IMAGE=registry.example/patwari:git-<commit>@sha256:<digest>
ExecStart=/bin/sh -c 'set -eu; stamp=$$(date -u +%%Y%%m%%dT%%H%%M%%SZ); exec /usr/bin/podman run --rm --network=none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m --user 10001:10001 -v systemd-patwari-data:/var/lib/patwari-volume:z -v /var/backups/patwari:/backups:Z "$$IMAGE" backup create --output "/backups/patwari-$$stamp"'
```

```ini
# /etc/systemd/system/patwari-backup.timer
[Unit]
Description=Nightly Patwari backup

[Timer]
OnCalendar=*-*-* 03:15:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

Enable it with `sudo systemctl daemon-reload && sudo systemctl enable --now
patwari-backup.timer`. Schedule `backup verify` and then replicate verified
backup directories to independent storage.

## Durability boundary and disaster recovery

The local Podman volume and `/var/backups/patwari` are operational copies, not
the final durability boundary. Replicate **verified finalized backup
directories** to independent storage outside quadhost (and test retrieval
there). Do not use NFS, a container image layer, `/tmp`, a build cache, or
other disposable cache for the archive volume, backup staging, or replication
target. A successful local backup is not proof against loss of quadhost or
its `/var` filesystem.

For a clean-host restore, first install the Quadlet/image but do not start
Patwari. Create a fresh empty local volume, restore into it, then start the
service:

```sh
IMAGE='registry.example/patwari:git-<commit>@sha256:<digest>'
sudo deploy/quadhost/install.sh --no-start --image "$IMAGE"
sudo podman volume create systemd-patwari-data
sudo podman run --rm --network=none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user 10001:10001 \
  -v systemd-patwari-data:/var/lib/patwari-volume:z,U \
  -v /var/backups/patwari:/backups:Z \
  "$IMAGE" backup restore "/backups/patwari-$STAMP" --data-dir /var/lib/patwari-volume/data
sudo systemctl start patwari.service
curl --fail http://192.168.16.169:8787/readyz
```

Restore refuses a non-empty destination, refuses a destination whose parent
directory is not writable or not on the same filesystem (for example a
volume mounted directly at the data directory instead of at its writable
root), verifies the backup before copying, builds a staged data directory,
runs another full scanner, and atomically installs it only when the scanner
is healthy. It preserves `archive_instance_id` and owner namespace. Validate
recovery operationally:

```sh
curl --fail http://192.168.16.169:8787/api/v1/sessions
# Retry a known completed upload's /api/v1/uploads/<id>/complete URL to
# reproduce its receipt, then GET its artifact content URL and compare bytes.
sudo podman run --rm --network=none --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user 10001:10001 -v systemd-patwari-data:/var/lib/patwari-volume:z \
  "$IMAGE" verify
```

Never erase a damaged production volume until its forensic copy and a known
good backup have been retained.

## Updates and rollback

Before every update, create and verify a backup, replicate it, then run
`install.sh --image` with the new commit-and-digest image. The script
restarts `patwari.service` itself and fails with a non-zero exit if the
service does not report healthy afterward, so a failed update is never
reported as a success. Keep the prior immutable image reference. Database
migrations are forward-only: do not start an older binary against a
migrated volume. A code-only rollback is safe only when its schema
compatibility is known; otherwise restore the pre-upgrade verified backup
into a fresh volume and deploy the matching image.
