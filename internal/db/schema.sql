CREATE TABLE IF NOT EXISTS sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT    NOT NULL UNIQUE,
    kind        TEXT    NOT NULL CHECK(kind IN ('channel', 'playlist', 'video')),
    state       TEXT    NOT NULL DEFAULT 'queued' CHECK(state IN ('queued', 'discovering', 'discovered', 'normalizing', 'normalized', 'error', 'quarantined')),
    error       TEXT,
    discovery_cursor INTEGER NOT NULL DEFAULT 0,
    discovered_count INTEGER NOT NULL DEFAULT 0,
    normalized_count INTEGER NOT NULL DEFAULT 0,
    quarantined_count INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT,
    lease_until DATETIME,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    retry_at DATETIME,
    authorization_acknowledged INTEGER NOT NULL DEFAULT 0,
    created_at  DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at  DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS videos (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    youtube_id     TEXT    NOT NULL UNIQUE,
    source_id      INTEGER NOT NULL REFERENCES sources(id),
    title          TEXT,
    state          TEXT    NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'leased', 'downloading', 'uploading', 'completed', 'error')),
    worker_id      TEXT,
    lease_until    DATETIME,
    attempt_count  INTEGER NOT NULL DEFAULT 0,
    storage_path   TEXT,
    error          TEXT,
    created_at     DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at     DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_videos_state ON videos(state);
CREATE INDEX IF NOT EXISTS idx_videos_youtube_id ON videos(youtube_id);
CREATE INDEX IF NOT EXISTS idx_videos_worker_id ON videos(worker_id);

-- A video can be reached by more than one authorized source.  source_id is
-- retained on videos for compatibility with older local databases; this join
-- is the authoritative provenance relation.
CREATE TABLE IF NOT EXISTS video_sources (
    video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    PRIMARY KEY (video_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_video_sources_source ON video_sources(source_id);

CREATE TABLE IF NOT EXISTS raw_urls (
    id TEXT PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    source_index INTEGER NOT NULL,
    raw_url TEXT NOT NULL,
    canonical_url TEXT,
    youtube_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','normalized','quarantined')),
    worker_id TEXT,
    lease_until DATETIME,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    retry_at DATETIME,
    error TEXT,
    error_class TEXT,
    terminal_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_id, raw_url),
    UNIQUE(source_id, source_index)
);
CREATE INDEX IF NOT EXISTS idx_raw_urls_claim ON raw_urls(state, retry_at, created_at);
CREATE INDEX IF NOT EXISTS idx_raw_urls_source ON raw_urls(source_id, source_index);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT    PRIMARY KEY,
    state           TEXT    NOT NULL DEFAULT 'active' CHECK(state IN ('active', 'draining', 'dead')),
    last_heartbeat  DATETIME NOT NULL DEFAULT (datetime('now')),
    registered_at   DATETIME NOT NULL DEFAULT (datetime('now'))
);
