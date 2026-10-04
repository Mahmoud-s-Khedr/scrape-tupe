package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestOpenUpgradesExistingSchemaAndBackfillsProvenance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = old.Exec(`CREATE TABLE sources (id INTEGER PRIMARY KEY, url TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued', error TEXT, discovery_cursor INTEGER NOT NULL DEFAULT 0, discovered_count INTEGER NOT NULL DEFAULT 0, normalized_count INTEGER NOT NULL DEFAULT 0, quarantined_count INTEGER NOT NULL DEFAULT 0, worker_id TEXT, lease_until DATETIME, attempt_count INTEGER NOT NULL DEFAULT 0, retry_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP); CREATE TABLE videos (id INTEGER PRIMARY KEY, youtube_id TEXT UNIQUE NOT NULL, source_id INTEGER NOT NULL, title TEXT, state TEXT NOT NULL DEFAULT 'pending', worker_id TEXT, lease_until DATETIME, attempt_count INTEGER NOT NULL DEFAULT 0, storage_path TEXT, error TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP); INSERT INTO sources(id,url,kind) VALUES(1,'https://example.test','video'); INSERT INTO videos(id,youtube_id,source_id) VALUES(1,'old-video',1);`)
	if err != nil {
		t.Fatal(err)
	}
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var ack, joins int
	if err = database.QueryRowContext(context.Background(), `SELECT authorization_acknowledged FROM sources WHERE id=1`).Scan(&ack); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(context.Background(), `SELECT count(*) FROM video_sources WHERE video_id=1 AND source_id=1`).Scan(&joins); err != nil {
		t.Fatal(err)
	}
	if ack != 0 || joins != 1 {
		t.Fatalf("ack=%d joins=%d", ack, joins)
	}
}
