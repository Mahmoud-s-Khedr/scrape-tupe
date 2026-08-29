package db

import (
	"context"
	"time"
)

type Worker struct {
	ID            string    `json:"id"`
	State         string    `json:"state"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	RegisteredAt  time.Time `json:"registered_at"`
}

func (db *DB) RegisterWorker(ctx context.Context, id string) error {
	query := `INSERT OR REPLACE INTO workers (id, state, last_heartbeat, registered_at) VALUES (?, 'active', datetime('now'), datetime('now'))`
	_, err := db.ExecContext(ctx, query, id)
	return err
}

func (db *DB) Heartbeat(ctx context.Context, id string) error {
	query := `UPDATE workers SET last_heartbeat = datetime('now') WHERE id = ?`
	_, err := db.ExecContext(ctx, query, id)
	return err
}

func (db *DB) ListWorkers(ctx context.Context) ([]Worker, error) {
	query := `SELECT id, state, last_heartbeat, registered_at FROM workers`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.State, &w.LastHeartbeat, &w.RegisteredAt); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

func (db *DB) MarkDeadWorkers(ctx context.Context, timeout time.Duration) (int, error) {
	cutoff := time.Now().Add(-timeout)
	query := `UPDATE workers SET state = 'dead' WHERE state = 'active' AND last_heartbeat < ?`
	res, err := db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}
	count, err := res.RowsAffected()
	return int(count), err
}
