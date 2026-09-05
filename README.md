<p align="center">
  <img src="brand/header.svg" alt="patwari — where session records live forever" width="720">
</p>

Patwari is a self-hosted archive for complete coding-agent sessions.

## Where this fits

Patwari is the archive in a three-tool suite:

> **Munshi writes the record; Patwari keeps the archive; Qanungo audits it.**

[Munshi](https://github.com/surdy/munshi) captures session files on developer machines, compresses
them, and submits an immutable proposed manifest plus stored artifact bytes to Patwari. Patwari
verifies those bytes, gives them permanent identity, and hands them back on request — it never
interprets what is inside them. [Qanungo](https://github.com/surdy/qanungo) is the read-side
consumer: it pages the archive through this API, mines the transcripts, and reports on how the
sessions actually went. Notesmith remains the home for human-readable summaries; a Notesmith summary
may reference its Patwari session ID when full context is needed.

**Start at [daftar](https://github.com/surdy/daftar)** — the suite front door: what the three tools
are, install order, and a first end-to-end run.

Patwari is designed primarily for programmatic use:

- reliable archival of complete session transcripts and related source files;
- retrieval of original artifacts for inspection or future restoration;
- batch access by tools that mine past sessions for learnings, reusable patterns, and skills;
- verified archival receipts that allow Munshi to offer manual local cleanup.

## Trust and authentication

Patwari has no authentication and no authorisation. It was built as one
person's private archive for their own LAN and tailnet, where every client that
can reach the listener is trusted — and that is still exactly what it assumes.
Run it only on a private network, or behind an authenticating boundary you
provide yourself; never on the public internet. Anyone is welcome to take it
and adapt it. See [`docs/self-hosting.md`](docs/self-hosting.md) for the
network posture, backups, and recovery.

## Quick start

Prerequisites: Rust stable (1.96 or newer) and the `libsqlite3` and `libzstd` development libraries
(`libsqlite3-dev` and `libzstd-dev` on Debian/Ubuntu, as installed by the
[`Containerfile`](Containerfile)).

```sh
cargo run -p patwari-server
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/readyz
```

That listens on `127.0.0.1:8080` and initializes `./data` transactionally on first start: one `v1`
owner namespace, a generated archive instance ID, the SQLite schema, and this persistent-volume
layout:

```text
data/
├── patwari.db
├── blobs/
├── uploads/
└── maintenance/
```

Restarting with the same `PATWARI_DATA_DIR` retains the owner namespace and archive instance
ID. `/healthz` is process liveness and stays available when dependencies fail; `/readyz` returns
`200` only after SQLite accepts a query and every storage directory accepts a write-and-remove
probe.

A Munshi installation then registers its client UUID, creates an upload for one client-generated
`capture_id` and canonical manifest, receives a server-assigned chunk size, streams resumable
stored-byte chunks for every declared artifact, completes verification of the complete set, and then
fetches the immutable snapshot, capture provenance, or an individual stored artifact. Every route,
header, and limit in that exchange is specified in [`docs/api.md`](docs/api.md).

For a real deployment — building the image, the volume layout, network posture, backups, restore,
and blob GC — see [`docs/self-hosting.md`](docs/self-hosting.md).

## Documentation

| Document | What it covers |
| --- | --- |
| [`CONTEXT.md`](CONTEXT.md) | The canonical domain vocabulary. Read this before the others. |
| [`docs/self-hosting.md`](docs/self-hosting.md) | Running it: image, volume, configuration, network posture, backups, restore, GC, updates. |
| [`docs/api.md`](docs/api.md) | The HTTP API and CLI reference: endpoints, upload protocol, pagination, content headers, configuration variables, storage layout. |
| [`docs/domain.md`](docs/domain.md) | The domain model, product decisions, responsibility boundary, and the phased delivery plan with its exit criteria. |
| [`docs/adr/`](docs/adr/) | Architectural decision records — the trade-offs and why they were made. |
| [`patwari-handoff.md`](patwari-handoff.md) | Historical planning document (2026-07). The shipped system differs; kept for provenance. |

## Status

**As of 2026-09-04.** The [phased delivery plan](docs/domain.md#delivery-roadmap) and its exit
criteria live in `docs/domain.md`; this is where each phase stands.

| Phase | Covers | Status |
| --- | --- | --- |
| 0 · Source compatibility spike | Copilot CLI session files, first artifact set, sanitized fixtures | **Shipped** — in Munshi; `claude-code` is a live source too |
| 1 · Contract and server foundation | Workspace, configuration, migrations, health, session/snapshot state, request limits, redacted tracing | **Shipped**, except the OpenAPI document — see below |
| 2 · Reliable artifact ingestion | Chunk negotiation, resumable upload, checksum verification, atomic promotion, archival receipts, upload GC | **Shipped** |
| 3 · Retrieval and archive inspection | Filtered listing, snapshot/capture history, manifests, artifact metadata, verified streaming downloads, `verify` scan | **Shipped** |
| 4 · Munshi integration | Archive, retry, status, download commands; local receipts; Notesmith reference fields | **Shipped** in Munshi — except the guarded local prune, which is **not built** |
| 5 · Host operations | Container image, dedicated volume, health checks, online backup, restore, disaster recovery | **Shipped** — see [`docs/self-hosting.md`](docs/self-hosting.md) |
| 6 · Analysis-tool support | Stable pagination and incremental discovery, manifest and artifact streaming for batch consumers, a separate analysis client | **Partly shipped** — pagination, cursors, and `/manifests` are live and Qanungo consumes them; batch-streaming work continues |

Known gaps, deliberately stated rather than promised:

- **No OpenAPI document.** The checked-in [`docs/api.md`](docs/api.md) is the cross-repository
  contract for now.
- **No `--help`.** The binary matches a fixed set of positional argument forms and prints no usage
  text; the commands are listed in [`docs/api.md`](docs/api.md#command-line-interface).
- **No local prune command in Munshi.** Patwari never deletes source files; the bar such a command
  would have to meet is written down in [`docs/domain.md`](docs/domain.md#local-cleanup-safety).
- Retention beyond explicit administrative deletion, authentication, and non-SQLite backends remain
  [deferred](docs/domain.md#deferred-decisions).

## The name

A *patwari* is the village record keeper of South Asian revenue administration: the official who
maintains the register of holdings, records every change in it, and can produce the entry years
later when someone needs proof. Not an interpreter, not an owner — a keeper of records that stay
exactly as they were entered. That is the job this service does for coding-agent sessions.

The suite is named for that record office and its clerks: a *munshi* writes, a *patwari* keeps, a
*qanungo* audits, and a *daftar* is the office they all worked in.

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
