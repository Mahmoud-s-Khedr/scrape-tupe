# Scrape-Tupe — Agent Instructions

> These instructions are for AI coding assistants (Gemini, Antigravity, etc.) working on this project.

## Project Overview

Scrape-Tupe is migrating to a Cloud Run Jobs and Cloud Storage video-download pipeline. The authoritative migration design is [`docs/gcp-migration-report.md`](docs/gcp-migration-report.md). Firestore is the production control plane. SQLite remains a local-development adapter; see `docs/operations.md` for queue, lease, upload-intent, and retention behavior.

## Architecture

Three logical components:

- **Master** — HTTP API server owning Firestore state, managing execution guards, reconciling artifacts, and starting bounded Cloud Run Job executions.
- **Normalizer** — Accepts channel/playlist/video URLs, expands them into individual video records via yt-dlp metadata extraction.
- **Downloader** — bounded Cloud Run Job task that claims jobs, downloads with yt-dlp, uploads to Cloud Storage, then exits when the queue drains.

## Project Structure

```
cmd/
  master/         — Master server entry point
  downloader/     — Downloader worker entry point
  normalizer/     — Normalizer entry point
internal/
  db/             — SQLite local-development database layer (to be replaced by Firestore)
  master/         — Master HTTP handlers, server, reconciliation
  downloader/     — Downloader task logic (yt-dlp integration, leases, upload abstraction)
  normalizer/     — Discovery, metadata extraction, renewable task leases
  gcp/            — Cloud Storage uploads/inspection and Cloud Run Jobs
  firestore/      — Production transactional control plane
  ytdlp/          — Bounded subprocess diagnostics and cancellation
docs/
  plan.md         — Full build plan with phases and architecture
```

## Tech Stack

- **Language:** Go (module: `github.com/Mahmoud-s-Khedr/scrape-tupe`)
- **Database:** SQLite via `modernc.org/sqlite` (pure Go, no CGo)
- **HTTP:** stdlib `net/http` with Go 1.22+ routing patterns
- **Logging:** `log/slog` with JSON handler
- **External tools:** `yt-dlp`, `ffmpeg` (called via `os/exec`)

## Conventions

- Use stdlib `net/http` — no third-party routers.
- Use `log/slog` structured logging everywhere.
- Database operations live in `internal/db/` with methods on the `*DB` receiver.
- HTTP handlers live in `internal/master/handlers.go`.
- All JSON responses use `sendJSON` / `sendError` helpers.
- Worker communicates with master exclusively via HTTP — no direct DB access.
- Videos are deduplicated by `youtube_id`, sources by `url`.
- State machines: see `docs/plan.md` for video and source state transitions.

## Current Progress

- SQLite development adapter and master HTTP API are implemented.
- Discovery/URL normalization and downloader workers use lease-owned HTTP operations.
- Firestore transactions, counters, guarded Cloud Run executions, GCS uploads, and Terraform resources are implemented.
- Review remediation adds compatible tools, queue scan progress, URL replay deduplication, lease renewal, diagnostics, and retention accounting.
- Local unit/race/emulator/image checks precede live GCP validation and release; see `docs/gcp-migration-next-steps.md`.

## Key Design Decisions

1. **Pull-based job distribution** — Workers pull jobs from master; master never pushes.
2. **Lease-based fault tolerance** — Every claimed job has a renewable lease; expired leases are auto-recovered.
3. **Master launches bounded batches** — Calculates pending work and starts one guarded Cloud Run Job execution capped at `MAX_PARALLELISM`.
4. **Cancellation** — Workers cancel subprocess groups on lost ownership. Lease expiry recovers abandoned work; upload intents keep capacity reserved until artifact reconciliation settles their outcome.

## Build & Run

```bash
# Build
go build ./cmd/master
go build ./cmd/downloader

# Run master
./master -addr :8080 -db scrape-tupe.db

# Run downloader (requires master running)
./downloader -master http://localhost:8080 -id worker-1 -output /tmp/scrape-tupe/downloads
```

## Important Files

- `internal/db/schema.sql` — Database schema (source of truth)
- `docs/plan.md` — Full architecture and build plan
