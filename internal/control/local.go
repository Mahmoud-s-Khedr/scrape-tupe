package control

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
	"github.com/google/uuid"
)

// LocalStore adapts the original SQLite database for local development. It is
// intentionally not selected by production configuration.
type LocalStore struct{ DB *db.DB }

func NewLocalStore(database *db.DB) *LocalStore { return &LocalStore{DB: database} }
func sourceID(id string) (int64, error)         { return strconv.ParseInt(id, 10, 64) }
func sourceFrom(s *db.Source) *Source {
	state := s.State
	if state == "pending" {
		state = "queued"
	}
	return &Source{ID: strconv.FormatInt(s.ID, 10), URL: s.URL, Kind: s.Kind, AuthorizationAcknowledged: s.AuthorizationAcknowledged, State: state, Error: deref(s.Error), DiscoveryComplete: s.DiscoveryComplete, DiscoveryCursor: s.DiscoveryCursor, DiscoveredCount: s.DiscoveredCount, NormalizedCount: s.NormalizedCount, QuarantinedCount: s.QuarantinedCount, WorkerID: deref(s.WorkerID), LeaseUntil: s.LeaseUntil, AttemptCount: s.AttemptCount, RetryAt: s.RetryAt, CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt}
}
func (s *LocalStore) videoFrom(ctx context.Context, v *db.Video) (*Video, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT source_id FROM video_sources WHERE video_id=? ORDER BY source_id`, v.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &Video{ID: v.YoutubeID, YoutubeID: v.YoutubeID, SourceIDs: ids, Title: deref(v.Title), State: v.State, WorkerID: deref(v.WorkerID), LeaseUntil: v.LeaseUntil, AttemptCount: v.AttemptCount, StoragePath: deref(v.StoragePath), Error: deref(v.Error), CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt}, nil
}
func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (s *LocalStore) CreateSource(ctx context.Context, url, kind string, ack bool) (*Source, error) {
	if !ack {
		return nil, fmt.Errorf("authorization acknowledgement is required")
	}
	id, err := s.DB.CreateSource(ctx, url, kind)
	if err != nil {
		return nil, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE sources SET authorization_acknowledged=1, state=CASE WHEN kind='video' THEN 'normalizing' ELSE state END, updated_at=datetime('now') WHERE id=?`, id); err != nil {
		return nil, err
	}
	if kind == "video" {
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO raw_urls (id, source_id, source_index, raw_url) VALUES (?, ?, 0, ?)`, uuid.NewString(), id, url)
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	got, err := s.DB.GetSource(ctx, id)
	if err != nil {
		return nil, err
	}
	return sourceFrom(got), nil
}
func (s *LocalStore) GetSource(ctx context.Context, id string) (*Source, error) {
	n, e := sourceID(id)
	if e != nil {
		return nil, ErrNotFound
	}
	x, e := s.DB.GetSource(ctx, n)
	if e == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	return sourceFrom(x), nil
}
func (s *LocalStore) ListSources(ctx context.Context) ([]Source, error) {
	xs, e := s.DB.ListSources(ctx)
	if e != nil {
		return nil, e
	}
	out := make([]Source, 0, len(xs))
	for i := range xs {
		out = append(out, *sourceFrom(&xs[i]))
	}
	return out, nil
}
func (s *LocalStore) RetrySource(ctx context.Context, id string) error {
	n, e := sourceID(id)
	if e != nil {
		return e
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var kind, state string
	var complete bool
	if err = tx.QueryRowContext(ctx, `SELECT kind,state,discovery_complete FROM sources WHERE id=?`, n).Scan(&kind, &state, &complete); err != nil {
		return err
	}
	if state != "error" && state != "quarantined" {
		return ErrInvalidState
	}
	if (kind == "channel" || kind == "playlist") && !complete {
		_, err = tx.ExecContext(ctx, `UPDATE sources SET state='queued', error=NULL,retry_at=NULL,worker_id=NULL,lease_until=NULL WHERE id=?`, n)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE raw_urls SET state='pending', worker_id=NULL, lease_until=NULL, retry_at=NULL, error=NULL, error_class=NULL, terminal_at=NULL, updated_at=datetime('now') WHERE source_id=? AND state='quarantined'`, n); err != nil {
		return err
	}
	if kind == "channel" || kind == "playlist" {
		var raws int
		if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM raw_urls WHERE source_id=?`, n).Scan(&raws); err != nil {
			return err
		}
		if raws == 0 {
			if _, err = tx.ExecContext(ctx, `UPDATE sources SET state='queued', error=NULL, retry_at=NULL, worker_id=NULL, lease_until=NULL, updated_at=datetime('now') WHERE id=?`, n); err != nil {
				return err
			}
			return tx.Commit()
		}
	}
	if err = deriveSourceStateTx(ctx, tx, n); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *LocalStore) ClaimDiscovery(ctx context.Context, worker string, lease time.Duration) (*Source, error) {
	until := sqliteTime(time.Now().UTC().Add(lease))
	row := s.DB.QueryRowContext(ctx, `UPDATE sources SET state='discovering', worker_id=?, lease_until=?, attempt_count=attempt_count+1, updated_at=datetime('now') WHERE id=(SELECT id FROM sources WHERE kind IN ('channel','playlist') AND state='queued' AND (retry_at IS NULL OR julianday(retry_at)<=julianday('now')) ORDER BY created_at LIMIT 1) RETURNING id`, worker, until)
	var id int64
	if err := row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return s.GetSource(ctx, strconv.FormatInt(id, 10))
}
func (s *LocalStore) CheckpointDiscovery(ctx context.Context, id, worker string, index int, raw string) error {
	n, err := sourceID(id)
	if err != nil {
		return ErrNotFound
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var cursor int
	var owner string
	var until *time.Time
	if err = tx.QueryRowContext(ctx, `SELECT discovery_cursor,worker_id,lease_until FROM sources WHERE id=?`, n).Scan(&cursor, &owner, &until); err != nil {
		return err
	}
	if owner != worker || until == nil || !until.After(time.Now()) {
		return ErrLeaseNotOwned
	}
	var existing int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM raw_urls WHERE source_id=? AND raw_url=?`, n, raw).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO raw_urls(id,source_id,source_index,raw_url) VALUES(?,?,?,?)`, uuid.NewString(), n, cursor, raw)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE sources SET discovery_cursor=discovery_cursor+1, discovered_count=discovered_count+1, updated_at=datetime('now') WHERE id=?`, n)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *LocalStore) FinishDiscovery(ctx context.Context, id, worker string) error {
	n, e := sourceID(id)
	if e != nil {
		return ErrNotFound
	}
	r, e := s.DB.ExecContext(ctx, `UPDATE sources SET state=CASE WHEN discovered_count=0 THEN 'normalized' ELSE 'normalizing' END,discovery_complete=1,worker_id=NULL,lease_until=NULL,error=NULL,retry_at=NULL,updated_at=datetime('now') WHERE id=? AND worker_id=? AND state='discovering' AND julianday(lease_until)>julianday('now')`, n, worker)
	if e != nil {
		return e
	}
	k, _ := r.RowsAffected()
	if k == 0 {
		return ErrLeaseNotOwned
	}
	return nil
}
func (s *LocalStore) FailDiscovery(ctx context.Context, id, worker string, retryable bool, class, msg string, retryAt time.Time) error {
	n, e := sourceID(id)
	if e != nil {
		return ErrNotFound
	}
	state := "queued"
	if !retryable {
		state = "quarantined"
	}
	r, e := s.DB.ExecContext(ctx, `UPDATE sources SET state=?,worker_id=NULL,lease_until=NULL,error=?,retry_at=?, updated_at=datetime('now') WHERE id=? AND worker_id=? AND state='discovering' AND julianday(lease_until)>julianday('now')`, state, msg, sqliteTime(retryAt), n, worker)
	if e != nil {
		return e
	}
	k, _ := r.RowsAffected()
	if k == 0 {
		return ErrLeaseNotOwned
	}
	return nil
}
func (s *LocalStore) ClaimRawURL(ctx context.Context, worker string, lease time.Duration) (*RawURL, error) {
	until := sqliteTime(time.Now().UTC().Add(lease))
	row := s.DB.QueryRowContext(ctx, `UPDATE raw_urls SET state='leased',worker_id=?,lease_until=?,attempt_count=attempt_count+1,updated_at=datetime('now') WHERE id=(SELECT r.id FROM raw_urls r JOIN sources s ON s.id=r.source_id WHERE r.state='pending' AND (r.retry_at IS NULL OR julianday(r.retry_at)<=julianday('now')) AND s.state='normalizing' ORDER BY r.created_at LIMIT 1) RETURNING id,source_id,source_index,raw_url,canonical_url,youtube_id,state,worker_id,lease_until,attempt_count,retry_at,error,error_class,terminal_at,created_at,updated_at`, worker, until)
	return scanRaw(row)
}
func scanRaw(row *sql.Row) (*RawURL, error) {
	var x RawURL
	var sid int64
	var canonical, youtube, worker, msg, class sql.NullString
	e := row.Scan(&x.ID, &sid, &x.Index, &x.RawURL, &canonical, &youtube, &x.State, &worker, &x.LeaseUntil, &x.AttemptCount, &x.RetryAt, &msg, &class, &x.TerminalAt, &x.CreatedAt, &x.UpdatedAt)
	if e == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	x.CanonicalURL = canonical.String
	x.YoutubeID = youtube.String
	x.WorkerID = worker.String
	x.Error = msg.String
	x.ErrorClass = class.String
	x.SourceID = strconv.FormatInt(sid, 10)
	return &x, e
}
func (s *LocalStore) GetRawURL(ctx context.Context, id string) (*RawURL, error) {
	return scanRaw(s.DB.QueryRowContext(ctx, `SELECT id,source_id,source_index,raw_url,canonical_url,youtube_id,state,worker_id,lease_until,attempt_count,retry_at,error,error_class,terminal_at,created_at,updated_at FROM raw_urls WHERE id=?`, id))
}
func (s *LocalStore) CompleteRawURL(ctx context.Context, id, worker string, m Metadata) error {
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var sid int64
	var owner string
	var until *time.Time
	e = tx.QueryRowContext(ctx, `SELECT source_id,worker_id,lease_until FROM raw_urls WHERE id=?`, id).Scan(&sid, &owner, &until)
	if e != nil {
		return e
	}
	if owner != worker || until == nil || !until.After(time.Now()) {
		return ErrLeaseNotOwned
	}
	_, e = tx.ExecContext(ctx, `UPDATE raw_urls SET state='normalized',canonical_url=?,youtube_id=?,worker_id=NULL,lease_until=NULL,terminal_at=datetime('now'),updated_at=datetime('now') WHERE id=?`, m.URL, m.YoutubeID, id)
	if e != nil {
		return e
	}
	var videoID int64
	if e = tx.QueryRowContext(ctx, `INSERT INTO videos(youtube_id,source_id,title) VALUES(?,?,?) ON CONFLICT(youtube_id) DO UPDATE SET title=COALESCE(videos.title, excluded.title) RETURNING id`, m.YoutubeID, sid, m.Title).Scan(&videoID); e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO video_sources(video_id,source_id) VALUES(?,?)`, videoID, sid)
	if e != nil {
		return e
	}
	if e = deriveSourceStateTx(ctx, tx, sid); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *LocalStore) ReleaseRawURL(ctx context.Context, id, worker string, retryable bool, class, msg string, retryAt time.Time) error {
	state := "pending"
	if !retryable {
		state = "quarantined"
	}
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var sid int64
	if e = tx.QueryRowContext(ctx, `SELECT source_id FROM raw_urls WHERE id=? AND worker_id=? AND state='leased' AND julianday(lease_until)>julianday('now')`, id, worker).Scan(&sid); e != nil {
		if e == sql.ErrNoRows {
			return ErrLeaseNotOwned
		}
		return e
	}
	r, e := tx.ExecContext(ctx, `UPDATE raw_urls SET state=?,worker_id=NULL,lease_until=NULL,error=?,error_class=?,retry_at=?,terminal_at=CASE WHEN ?='quarantined' THEN datetime('now') ELSE NULL END,updated_at=datetime('now') WHERE id=? AND worker_id=?`, state, msg, class, sqliteTime(retryAt), state, id, worker)
	if e != nil {
		return e
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return ErrLeaseNotOwned
	}
	if e = deriveSourceStateTx(ctx, tx, sid); e != nil {
		return e
	}
	return tx.Commit()
}
func sqliteTime(v time.Time) string { return v.UTC().Format("2006-01-02 15:04:05.999999") }
func (s *LocalStore) GetVideo(ctx context.Context, id string) (*Video, error) {
	row := s.DB.QueryRowContext(ctx, "SELECT id FROM videos WHERE youtube_id=?", id)
	var n int64
	if e := row.Scan(&n); e == sql.ErrNoRows {
		return nil, ErrNotFound
	} else if e != nil {
		return nil, e
	}
	v, e := s.DB.GetVideo(ctx, n)
	if e != nil {
		return nil, e
	}
	return s.videoFrom(ctx, v)
}
func (s *LocalStore) ListVideos(ctx context.Context, state string) ([]Video, error) {
	var p *string
	if state != "" {
		p = &state
	}
	xs, e := s.DB.ListVideos(ctx, p)
	if e != nil {
		return nil, e
	}
	out := make([]Video, 0, len(xs))
	for i := range xs {
		v, err := s.videoFrom(ctx, &xs[i])
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, nil
}
func (s *LocalStore) ClaimVideo(ctx context.Context, w string, d time.Duration) (*Video, error) {
	v, e := s.DB.ClaimJob(ctx, w, d)
	if e == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	}
	if e != nil {
		return nil, e
	}
	return s.videoFrom(ctx, v)
}
func (s *LocalStore) RenewLease(ctx context.Context, id, w string, d time.Duration) error {
	_, e := s.GetVideo(ctx, id)
	if e != nil {
		return e
	}
	e = s.DB.RenewLease(ctx, parseVideoID(ctx, s.DB, id), w, d)
	return localErr(e)
}
func (s *LocalStore) SetVideoState(ctx context.Context, id, w, state, msg string) error {
	e := s.DB.UpdateVideoState(ctx, parseVideoID(ctx, s.DB, id), w, state, stringPtr(msg))
	return localErr(e)
}
func (s *LocalStore) CompleteVideo(ctx context.Context, id, w, path string, size int64) error {
	return localErr(s.DB.CompleteVideo(ctx, parseVideoID(ctx, s.DB, id), w, path))
}
func (s *LocalStore) QuarantineVideo(ctx context.Context, id, w, class string, until time.Time) error {
	return s.SetVideoState(ctx, id, w, "error", class)
}
func (s *LocalStore) RetryVideo(ctx context.Context, id string) error {
	_, e := s.DB.ExecContext(ctx, "UPDATE videos SET state='pending', worker_id=NULL, lease_until=NULL WHERE youtube_id=? AND state='error'", id)
	return e
}
func (s *LocalStore) RecoverExpiredLeases(ctx context.Context) (int, error) {
	n, err := s.DB.RecoverExpiredLeases(ctx)
	if err != nil {
		return n, err
	}
	r, err := s.DB.ExecContext(ctx, `UPDATE raw_urls SET state=CASE WHEN attempt_count>=3 THEN 'quarantined' ELSE 'pending' END, worker_id=NULL, lease_until=NULL, retry_at=CASE WHEN attempt_count>=3 THEN retry_at ELSE datetime('now') END, terminal_at=CASE WHEN attempt_count>=3 THEN datetime('now') ELSE terminal_at END, updated_at=datetime('now') WHERE state='leased' AND julianday(lease_until)<julianday('now')`)
	if err != nil {
		return n, err
	}
	k, _ := r.RowsAffected()
	n += int(k)
	r, err = s.DB.ExecContext(ctx, `UPDATE sources SET state=CASE WHEN attempt_count>=3 THEN 'quarantined' ELSE 'queued' END, worker_id=NULL, lease_until=NULL, retry_at=CASE WHEN attempt_count>=3 THEN retry_at ELSE datetime('now') END, updated_at=datetime('now') WHERE state='discovering' AND julianday(lease_until)<julianday('now')`)
	if err != nil {
		return n, err
	}
	k, _ = r.RowsAffected()
	n += int(k)
	// Raw URL state is the source lifecycle authority after discovery. Recompute
	// every source with raw work so lease recovery cannot strand a source in an
	// obsolete aggregate state.
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return n, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT source_id FROM raw_urls`)
	if err != nil {
		return n, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return n, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return n, err
	}
	for _, id := range ids {
		if err := deriveSourceStateTx(ctx, tx, id); err != nil {
			return n, err
		}
	}
	if err := tx.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

// deriveSourceStateTx makes source status a projection of raw URL work once a
// source has been discovered. Counts are recomputed (rather than incremented)
// so retries and lease recovery remain idempotent.
func deriveSourceStateTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	var kind string
	var complete bool
	if err := tx.QueryRowContext(ctx, `SELECT kind,discovery_complete FROM sources WHERE id=?`, sourceID).Scan(&kind, &complete); err != nil {
		return err
	}
	if kind != "video" && !complete {
		return nil
	}
	var total, normalized, quarantined int
	if err := tx.QueryRowContext(ctx, `SELECT count(*), COALESCE(sum(CASE WHEN state='normalized' THEN 1 ELSE 0 END),0), COALESCE(sum(CASE WHEN state='quarantined' THEN 1 ELSE 0 END),0) FROM raw_urls WHERE source_id=?`, sourceID).Scan(&total, &normalized, &quarantined); err != nil {
		return err
	}
	state := "normalizing"
	if total > 0 && normalized == total {
		state = "normalized"
	}
	if total > 0 && normalized+quarantined == total && quarantined > 0 {
		state = "quarantined"
	}
	_, err := tx.ExecContext(ctx, `UPDATE sources SET state=?, normalized_count=?, quarantined_count=?, error=CASE WHEN ?='quarantined' THEN error ELSE NULL END, updated_at=datetime('now') WHERE id=?`, state, normalized, quarantined, state, sourceID)
	return err
}
func (s *LocalStore) CountByState(ctx context.Context) (map[string]int, error) {
	return s.DB.CountByState(ctx)
}
func (s *LocalStore) WorkCounts(ctx context.Context) (map[string]int, error) {
	o := map[string]int{}
	for _, q := range []struct{ k, q string }{{"discoverable_sources", `SELECT COUNT(*) FROM sources WHERE kind IN ('channel','playlist') AND state='queued' AND (retry_at IS NULL OR julianday(retry_at)<=julianday('now'))`}, {"eligible_raw_urls", `SELECT COUNT(*) FROM raw_urls WHERE state='pending' AND (retry_at IS NULL OR julianday(retry_at)<=julianday('now'))`}, {"pending_downloads", `SELECT COUNT(*) FROM videos WHERE state='pending'`}} {
		var n int
		if e := s.DB.QueryRowContext(ctx, q.q).Scan(&n); e != nil {
			return nil, e
		}
		o[q.k] = n
	}
	return o, nil
}
func (s *LocalStore) CreateExecution(context.Context, Execution) (*Execution, error) {
	return nil, fmt.Errorf("Cloud Run executions require Firestore")
}
func (s *LocalStore) GetActiveDownloaderExecution(context.Context) (*Execution, error) {
	return nil, nil
}
func (s *LocalStore) GetActiveExecution(context.Context, string) (*Execution, error) { return nil, nil }
func (s *LocalStore) UpdateExecution(context.Context, string, string, string) error  { return nil }
func stringPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func parseVideoID(ctx context.Context, d *db.DB, id string) int64 {
	var n int64
	_ = d.QueryRowContext(ctx, "SELECT id FROM videos WHERE youtube_id=?", id).Scan(&n)
	return n
}
func localErr(e error) error {
	if e == db.ErrLeaseNotOwned {
		return ErrLeaseNotOwned
	}
	if e == db.ErrInvalidVideoState {
		return ErrInvalidState
	}
	return e
}
