package control

import (
	"context"
	"time"
)

func (s *LocalStore) RenewDiscovery(ctx context.Context, id, worker string, d time.Duration) error {
	n, err := sourceID(id)
	if err != nil {
		return ErrNotFound
	}
	r, err := s.DB.ExecContext(ctx, `UPDATE sources SET lease_until=?,updated_at=datetime('now') WHERE id=? AND worker_id=? AND state='discovering' AND julianday(lease_until)>julianday('now')`, sqliteTime(time.Now().Add(d)), n, worker)
	if err != nil {
		return err
	}
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrLeaseNotOwned
	}
	return nil
}
func (s *LocalStore) RenewRawURL(ctx context.Context, id, worker string, d time.Duration) error {
	r, err := s.DB.ExecContext(ctx, `UPDATE raw_urls SET lease_until=?,updated_at=datetime('now') WHERE id=? AND worker_id=? AND state='leased' AND julianday(lease_until)>julianday('now')`, sqliteTime(time.Now().Add(d)), id, worker)
	if err != nil {
		return err
	}
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrLeaseNotOwned
	}
	return nil
}
