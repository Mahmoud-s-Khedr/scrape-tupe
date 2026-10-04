# Scrape-Tupe — Agent Instructions

> These instructions are for AI coding assistants (Gemini, Antigravity, etc.) working on this project.

## Project Overview

Scrape-Tupe is migrating to a Cloud Run Jobs and Cloud Storage video-download pipeline. The authoritative migration design is [`docs/gcp-migration-report.md`](docs/gcp-migration-report.md). SQLite remains only a local-development adapter until the Firestore control plane is implemented.

## Architecture

Three logical components:

- **Master** — HTTP API server that will own Firestore state, manage workers/jobs, and start bounded Cloud Run Job executions.
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
  master/         — Master HTTP handlers, server, scaler
  downloader/     — Downloader task logic (yt-dlp integration, leases, upload abstraction)
  normalizer/     — Normalizer logic (placeholder)
  gcp/            — Google Cloud integrations (Cloud Storage today; Run/Firestore next)
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

- **Phase 1 ✅** — SQLite schema + Master HTTP API
- **Phase 2 ✅** — Single local downloader with yt-dlp
- **Phase A (in progress)** — GCS upload integration, bounded downloader tasks, and container images
- **Phase 4 ⬜** — Normalizer (channel/playlist/video expansion)
- **Next** — Firestore transactions/counters, Cloud Run Job execution guard/client, and Terraform deployment

## Key Design Decisions

1. **Pull-based job distribution** — Workers pull jobs from master; master never pushes.
2. **Lease-based fault tolerance** — Every claimed job has a renewable lease; expired leases are auto-recovered.
3. **Master launches bounded batches** — Calculates pending work and starts one guarded Cloud Run Job execution capped at `MAX_PARALLELISM`.
4. **SIGTERM handling** — Downloader releases lease on graceful shutdown; lease expiry is the fallback.

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
