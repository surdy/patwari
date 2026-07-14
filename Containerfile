# syntax=docker/dockerfile:1
#
# Build with a commit-pinned tag, for example:
#   podman build --tag localhost/patwari:git-<commit> .
FROM docker.io/library/rust:1.96.0-bookworm AS build

WORKDIR /src

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libsqlite3-dev \
        libzstd-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --locked --package patwari-server

FROM docker.io/library/debian:12.11-slim

ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Patwari" \
      org.opencontainers.image.description="Self-hosted verified coding-agent session archive" \
      org.opencontainers.image.source="https://github.com/surdy/patwari" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        libsqlite3-0 \
        libzstd1 \
    && groupadd --gid 10001 patwari \
    && useradd --uid 10001 --gid patwari --home-dir /nonexistent --no-create-home \
        --shell /usr/sbin/nologin patwari \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build --chown=10001:10001 \
    /src/target/release/patwari-server /usr/local/bin/patwari-server

USER 10001:10001
ENV PATWARI_DATA_DIR=/var/lib/patwari-volume/data \
    PATWARI_BIND_ADDR=0.0.0.0:8080 \
    PATWARI_ADMIN_DELETION_ENABLED=false

VOLUME ["/var/lib/patwari-volume"]
EXPOSE 8080/tcp
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/bin/curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:8080/readyz"]

ENTRYPOINT ["/usr/local/bin/patwari-server"]
CMD ["serve"]
