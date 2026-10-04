package db

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

type DB struct {
	*sql.DB
}

func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}
	if err := upgradeSchema(db); err != nil {
		return nil, fmt.Errorf("failed to upgrade schema: %w", err)
	}

	return &DB{DB: db}, nil
}

// upgradeSchema keeps the embedded schema useful for brand-new databases while
// making releases safe for databases created before a column/table existed.
func upgradeSchema(db *sql.DB) error {
	var acknowledged int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sources') WHERE name='authorization_acknowledged'`).Scan(&acknowledged); err != nil {
		return err
	}
	if acknowledged == 0 {
		if _, err := db.Exec(`ALTER TABLE sources ADD COLUMN authorization_acknowledged INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS video_sources (video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE, source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE, PRIMARY KEY(video_id, source_id))`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_video_sources_source ON video_sources(source_id)`); err != nil {
		return err
	}
	_, err := db.Exec(`INSERT OR IGNORE INTO video_sources(video_id, source_id) SELECT id, source_id FROM videos`)
	return err
}

func (db *DB) Close() error {
	return db.DB.Close()
}
