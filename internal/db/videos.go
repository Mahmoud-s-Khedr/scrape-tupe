package db

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrLeaseNotOwned means the job was released, recovered, or assigned to a
// different worker before a worker-owned state transition completed.
var ErrLeaseNotOwned = errors.New("job lease is not owned by worker")

// ErrInvalidVideoState indicates a worker requested a state that workers may
// not set directly.
var ErrInvalidVideoState = errors.New("invalid worker video state")

type Video struct {
	ID           int64      `json:"id"`
	YoutubeID    string     `json:"youtube_id"`
	SourceID     int64      `json:"source_id"`
	Title        *string    `json:"title,omitempty"`
	State        string     `json:"state"`
	WorkerID     *string    `json:"worker_id,omitempty"`
	LeaseUntil   *time.Time `json:"lease_until,omitempty"`
	AttemptCount int        `json:"attempt_count"`
	StoragePath  *string    `json:"storage_path,omitempty"`
	Error        *string    `json:"error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

func (db *DB) InsertVideo(ctx context.Context, youtubeID string, sourceID int64, title string) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	query := `INSERT INTO videos (youtube_id, source_id, title) VALUES (?, ?, ?) ON CONFLICT(youtube_id) DO NOTHING RETURNING id`
	var id int64
	err = tx.QueryRowContext(ctx, query, youtubeID, sourceID, title).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			err = tx.QueryRowContext(ctx, `SELECT id FROM videos WHERE youtube_id = ?`, youtubeID).Scan(&id)
		}
	}
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO video_sources(video_id, source_id) VALUES (?, ?)`, id, sourceID); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (db *DB) GetVideo(ctx context.Context, id int64) (*Video, error) {
	query := `SELECT id, youtube_id, source_id, title, state, worker_id, lease_until, attempt_count, storage_path, error, created_at, updated_at FROM videos WHERE id = ?`
	var v Video
	err := db.QueryRowContext(ctx, query, id).Scan(
		&v.ID, &v.YoutubeID, &v.SourceID, &v.Title, &v.State, &v.WorkerID, &v.LeaseUntil, &v.AttemptCount, &v.StoragePath, &v.Error, &v.CreatedAt, &v.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (db *DB) ListVideos(ctx context.Context, state *string) ([]Video, error) {
	query := `SELECT id, youtube_id, source_id, title, state, worker_id, lease_until, attempt_count, storage_path, error, created_at, updated_at FROM videos`
	var args []any
	if state != nil {
		query += ` WHERE state = ?`
		args = append(args, *state)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var videos []Video
	for rows.Next() {
		var v Video
		if err := rows.Scan(&v.ID, &v.YoutubeID, &v.SourceID, &v.Title, &v.State, &v.WorkerID, &v.LeaseUntil, &v.AttemptCount, &v.StoragePath, &v.Error, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		videos = append(videos, v)
	}
	return videos, rows.Err()
}

func (db *DB) ClaimJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*Video, error) {
	query := `
		UPDATE videos
		SET state = 'leased',
			worker_id = ?,
			lease_until = ?,
			attempt_count = attempt_count + 1,
			updated_at = datetime('now')
		WHERE id = (
			SELECT id FROM videos WHERE state = 'pending' ORDER BY id LIMIT 1
		)
		RETURNING id, youtube_id, source_id, title, state, worker_id, lease_until, attempt_count, storage_path, error, created_at, updated_at
	`
	leaseUntil := sqliteTimestamp(time.Now().Add(leaseDuration))
	var v Video
	err := db.QueryRowContext(ctx, query, workerID, leaseUntil).Scan(
		&v.ID, &v.YoutubeID, &v.SourceID, &v.Title, &v.State, &v.WorkerID, &v.LeaseUntil, &v.AttemptCount, &v.StoragePath, &v.Error, &v.CreatedAt, &v.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (db *DB) RenewLease(ctx context.Context, videoID int64, workerID string, leaseDuration time.Duration) error {
	leaseUntil := sqliteTimestamp(time.Now().Add(leaseDuration))
	query := `
		UPDATE videos SET lease_until = ?, updated_at = datetime('now')
		WHERE id = ? AND worker_id = ?
		  AND state IN ('leased', 'downloading', 'uploading')
		  AND julianday(lease_until) > julianday('now')
	`
	result, err := db.ExecContext(ctx, query, leaseUntil, videoID, workerID)
	if err != nil {
		return err
	}
	return requireWorkerOwnedUpdate(result)
}

func (db *DB) UpdateVideoState(ctx context.Context, videoID int64, workerID string, state string, errMsg *string) error {
	var allowedFrom string
	var anyActiveState bool
	switch state {
	case "downloading":
		allowedFrom = "leased"
	case "uploading":
		allowedFrom = "downloading"
	case "error":
		// A failure can be recorded from any active worker-owned lease state.
		anyActiveState = true
	default:
		return ErrInvalidVideoState
	}
	query := `
		UPDATE videos SET state = ?, error = ?, updated_at = datetime('now')
		WHERE id = ? AND worker_id = ? AND `
	args := []any{state, errMsg, videoID, workerID}
	if anyActiveState {
		query += `state IN ('leased', 'downloading', 'uploading')`
	} else {
		query += `state = ?`
		args = append(args, allowedFrom)
	}
	query += ` AND julianday(lease_until) > julianday('now')`
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return requireWorkerOwnedUpdate(result)
}

func (db *DB) CompleteVideo(ctx context.Context, videoID int64, workerID string, storagePath string) error {
	query := `
		UPDATE videos SET state = 'completed', storage_path = ?, worker_id = NULL, lease_until = NULL, updated_at = datetime('now')
		WHERE id = ? AND worker_id = ? AND state = 'uploading'
		  AND julianday(lease_until) > julianday('now')
	`
	result, err := db.ExecContext(ctx, query, storagePath, videoID, workerID)
	if err != nil {
		return err
	}
	return requireWorkerOwnedUpdate(result)
}

func requireWorkerOwnedUpdate(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrLeaseNotOwned
	}
	return nil
}

// sqliteTimestamp uses a format SQLite's date functions parse consistently.
// Passing time.Time directly leaves formatting up to the driver, which can
// yield a timestamp SQLite's julianday function does not recognize.
func sqliteTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.999999")
}

func (db *DB) RecoverExpiredLeases(ctx context.Context) (int, error) {
	query := `
		UPDATE videos
		SET state = 'pending',
			worker_id = NULL,
			lease_until = NULL,
			updated_at = datetime('now')
		WHERE state IN ('leased', 'downloading', 'uploading')
		  AND julianday(lease_until) < julianday('now')
	`
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	count, err := res.RowsAffected()
	return int(count), err
}

func (db *DB) CountByState(ctx context.Context) (map[string]int, error) {
	query := `SELECT state, COUNT(*) FROM videos GROUP BY state`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		counts[state] = count
	}
	return counts, rows.Err()
}
