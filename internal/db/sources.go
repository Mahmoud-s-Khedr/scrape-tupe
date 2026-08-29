package db

import (
	"context"
	"database/sql"
	"time"
)

type Source struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	Kind      string    `json:"kind"`
	State     string    `json:"state"`
	Error     *string   `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (db *DB) CreateSource(ctx context.Context, url, kind string) (int64, error) {
	query := `INSERT INTO sources (url, kind) VALUES (?, ?) ON CONFLICT(url) DO NOTHING RETURNING id`
	var id int64
	err := db.QueryRowContext(ctx, query, url, kind).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			err = db.QueryRowContext(ctx, `SELECT id FROM sources WHERE url = ?`, url).Scan(&id)
		}
	}
	return id, err
}

func (db *DB) GetSource(ctx context.Context, id int64) (*Source, error) {
	query := `SELECT id, url, kind, state, error, created_at, updated_at FROM sources WHERE id = ?`
	var s Source
	err := db.QueryRowContext(ctx, query, id).Scan(
		&s.ID, &s.URL, &s.Kind, &s.State, &s.Error, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *DB) ListSources(ctx context.Context) ([]Source, error) {
	query := `SELECT id, url, kind, state, error, created_at, updated_at FROM sources`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []Source
	for rows.Next() {
		var s Source
		if err := rows.Scan(&s.ID, &s.URL, &s.Kind, &s.State, &s.Error, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		sources = append(sources, s)
	}
	return sources, rows.Err()
}

func (db *DB) UpdateSourceState(ctx context.Context, id int64, state string, errMsg *string) error {
	query := `UPDATE sources SET state = ?, error = ?, updated_at = datetime('now') WHERE id = ?`
	_, err := db.ExecContext(ctx, query, state, errMsg, id)
	return err
}
