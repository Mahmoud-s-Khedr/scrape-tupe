# Scrape-Tupe — Claude Instructions

> These instructions are for Claude Code working on this project.

## Project Overview

Scrape-Tupe is an elastic video-download pipeline backed by **Bunny Magic Containers**, orchestrated by a central **master** service with **SQLite** as the single source of truth.

## Architecture

Three logical components:

- **Master** — HTTP API server, owns the SQLite database, manages workers and jobs, controls Bunny scaling.
- **Normalizer** — Accepts channel/playlist/video URLs, expands them into individual video records via yt-dlp metadata extraction.
- **Downloader** — Worker process that claims one job at a time, downloads with yt-dlp, uploads to Bunny Storage, reports completion.

## Project Structure

```
cmd/
  master/         — Master server entry point
  downloader/     — Downloader worker entry point
  normalizer/     — Normalizer entry point
internal/
  db/             — SQLite database layer (schema, sources, videos, workers)
  master/         — Master HTTP handlers, server, scaler
  downloader/     — Downloader worker logic (yt-dlp integration, heartbeats)
  normalizer/     — Normalizer logic (placeholder)
  bunny/          — Bunny API clients (containers + storage, placeholders)
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
- **Phase 3 ⬜** — Bunny Storage upload integration
- **Phase 4 ⬜** — Normalizer (channel/playlist/video expansion)
- **Phases 5–10 ⬜** — Leases, Docker, Bunny deployment, autoscaling

## Key Design Decisions

1. **Pull-based job distribution** — Workers pull jobs from master; master never pushes.
2. **Lease-based fault tolerance** — Every claimed job has a renewable lease; expired leases are auto-recovered.
3. **Master is the autoscaler** — Calculates required capacity from the video table, sets Bunny min=max=N.
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
