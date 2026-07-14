#!/bin/sh
# Install or update the rootful quadhost Quadlet from this checked-in template.
set -eu

usage() {
    echo "usage: $0 --image registry.example/patwari:git-<commit>@sha256:<digest> [--no-pull] [--no-start]" >&2
    exit 64
}

# Polls the container's own HealthCmd result (see patwari.container) instead
# of guessing at network reachability, so this works the same regardless of
# host firewall/publish configuration. Budgeted comfortably past
# HealthStartPeriod + HealthRetries*HealthInterval from the Quadlet so a
# genuinely unhealthy update is detected here instead of being reported as a
# successful install/update.
wait_for_healthy_or_fail() {
    attempts=0
    max_attempts=60
    while [ "$attempts" -lt "$max_attempts" ]; do
        if systemctl is-failed --quiet patwari.service; then
            echo "patwari.service failed to (re)start" >&2
            systemctl --no-pager --full status patwari.service >&2 || true
            exit 1
        fi
        health=$(podman inspect --format '{{.State.Health.Status}}' patwari 2>/dev/null || echo "unknown")
        case "$health" in
            healthy)
                return 0
                ;;
            unhealthy)
                echo "patwari container is unhealthy after update; refusing to report success" >&2
                systemctl --no-pager --full status patwari.service >&2 || true
                podman logs --tail 50 patwari >&2 || true
                exit 1
                ;;
        esac
        attempts=$((attempts + 1))
        sleep 3
    done
    echo "patwari container did not become healthy within $((max_attempts * 3))s" >&2
    systemctl --no-pager --full status patwari.service >&2 || true
    podman logs --tail 50 patwari >&2 || true
    exit 1
}

image=
pull_image=true
start_service=true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --image)
            [ "$#" -ge 2 ] || usage
            image=$2
            shift 2
            ;;
        --no-pull)
            pull_image=false
            shift
            ;;
        --no-start)
            start_service=false
            shift
            ;;
        *)
            usage
            ;;
    esac
done

[ "$(id -u)" -eq 0 ] || {
    echo "run as root on quadhost" >&2
    exit 77
}
[ -n "$image" ] || usage
case "$image" in
    *[!A-Za-z0-9._:/@-]* | *:latest | *:latest@sha256:* | latest | latest@sha256:*)
        echo "image must be a non-latest immutable reference" >&2
        exit 64
        ;;
esac
case "$image" in
    *@sha256:*)
        ;;
    *)
        echo "image must include an immutable sha256 digest" >&2
        exit 64
        ;;
esac

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
quadlet_dir=/etc/containers/systemd

if [ "$pull_image" = true ]; then
    podman pull "$image"
fi

install -d -m 0755 "$quadlet_dir"
install -m 0644 "$script_dir/patwari-data.volume" "$quadlet_dir/patwari-data.volume"
if [ ! -e "$quadlet_dir/patwari.env" ]; then
    install -m 0640 "$script_dir/patwari.env.example" "$quadlet_dir/patwari.env"
fi
sed "s|__PATWARI_IMAGE__|$image|g" "$script_dir/patwari.container" \
    > "$quadlet_dir/patwari.container"
chmod 0644 "$quadlet_dir/patwari.container"

systemctl daemon-reload
if [ "$start_service" = true ]; then
    if systemctl is-active --quiet patwari.service; then
        # `daemon-reload` only reloads unit *definitions*; a plain `start` on
        # an already-active unit is a no-op and would silently leave the old
        # image running. `restart` is required to actually apply the new
        # image/Quadlet on an update or rollback.
        systemctl restart patwari.service
    else
        systemctl start patwari.service
    fi
    wait_for_healthy_or_fail
    systemctl --no-pager --full status patwari.service
fi
