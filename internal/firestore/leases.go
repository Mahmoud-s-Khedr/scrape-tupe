package firestore

import (
	cloudfs "cloud.google.com/go/firestore"
	"context"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"time"
)

func (s *Store) RenewDiscovery(ctx context.Context, id, worker string, d time.Duration) error {
	return s.renewNormalizer(ctx, s.source(id), worker, "discovering", d)
}
func (s *Store) RenewRawURL(ctx context.Context, id, worker string, d time.Duration) error {
	return s.renewNormalizer(ctx, s.raw(id), worker, "leased", d)
}
func (s *Store) renewNormalizer(ctx context.Context, ref *cloudfs.DocumentRef, worker, state string, d time.Duration) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		owner, _ := snap.Data()["worker_id"].(string)
		current, _ := snap.Data()["state"].(string)
		until, _ := snap.Data()["lease_until"].(time.Time)
		if current != state || owner != worker || !until.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		return tx.Update(ref, []cloudfs.Update{{Path: "lease_until", Value: s.now().Add(d)}, {Path: "updated_at", Value: s.now()}})
	})
}
