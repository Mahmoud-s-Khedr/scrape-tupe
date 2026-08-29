CREATE TABLE IF NOT EXISTS sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT    NOT NULL UNIQUE,
    kind        TEXT    NOT NULL CHECK(kind IN ('channel', 'playlist', 'video')),
    state       TEXT    NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'normalizing', 'normalized', 'error')),
    error       TEXT,
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

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT    PRIMARY KEY,
    state           TEXT    NOT NULL DEFAULT 'active' CHECK(state IN ('active', 'draining', 'dead')),
    last_heartbeat  DATETIME NOT NULL DEFAULT (datetime('now')),
    registered_at   DATETIME NOT NULL DEFAULT (datetime('now'))
);
