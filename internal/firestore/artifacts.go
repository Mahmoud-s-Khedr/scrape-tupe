package firestore

import (
	cloudfs "cloud.google.com/go/firestore"
	"context"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"google.golang.org/api/iterator"
	"time"
)

func (s *Store) PrepareUpload(ctx context.Context, id, worker, path string) error {
	return s.owned(ctx, id, worker, func(v *control.Video) error {
		if v.State != "uploading" {
			return control.ErrInvalidState
		}
		if v.ArtifactState == "intent" && v.StoragePath != path {
			return control.ErrInvalidState
		}
		v.StoragePath = path
		v.ArtifactState = "intent"
		return nil
	})
}

// Page all videos so legacy completed records (without artifact_state) are
// adopted too. Progress is durable and bounded even with a large collection.
func (s *Store) ArtifactCandidates(ctx context.Context, limit int) ([]control.Video, error) {
	if limit <= 0 || limit > 100 {
		limit = 64
	}
	cursorRef := s.client.Collection("queue_scans").Doc("artifacts")
	var out []control.Video
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		out = nil
		cursor, e := tx.Get(cursorRef)
		if e != nil && !isNotFound(e) {
			return e
		}
		q := s.client.Collection("videos").OrderBy(cloudfs.DocumentID, cloudfs.Asc).Limit(limit)
		if e == nil {
			if id, _ := cursor.Data()["video_id"].(string); id != "" {
				q = q.StartAfter(s.video(id))
			}
		}
		it := tx.Documents(q)
		defer it.Stop()
		last := ""
		for i := 0; i < limit; i++ {
			snap, e := it.Next()
			if e == iterator.Done {
				last = ""
				break
			}
			if e != nil {
				return e
			}
			v, e := decodeVideo(snap)
			if e != nil {
				return e
			}
			last = v.ID
			if v.StoragePath != "" && (v.ArtifactState == "intent" || v.State == "completed" && v.ArtifactState != "expired") {
				out = append(out, *v)
			}
		}
		return tx.Set(cursorRef, map[string]any{"video_id": last})
	})
	return out, err
}

func (s *Store) ObserveArtifact(ctx context.Context, expected control.Video, o control.ArtifactObservation) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.video(expected.ID))
		if e != nil {
			return e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return e
		}
		if v.StoragePath != expected.StoragePath || v.ArtifactState != expected.ArtifactState || v.ObjectGeneration != expected.ObjectGeneration || !v.UpdatedAt.Equal(expected.UpdatedAt) {
			return nil
		}
		if v.WorkerID != "" && v.LeaseUntil != nil && v.LeaseUntil.After(s.now()) {
			return nil
		}
		if o.Exists && v.ObjectGeneration != 0 && v.ObjectGeneration != o.Generation {
			return control.ErrInvalidState
		}
		values, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		delta := map[string]int64{}
		if v.ArtifactState == "intent" {
			if o.Exists {
				if o.Size <= 0 || o.Size > v.ReservedSize {
					return control.ErrCapacity
				}
				v.State = "completed"
				v.ActualSize = o.Size
				v.ArtifactState = "stored"
				v.ObjectGeneration = o.Generation
				delta["used_bytes"] = o.Size
				delta["reserved_bytes"] = -v.ReservedSize
				v.ReservedSize = 0
			} else {
				// Allow cancelled upload requests to settle before releasing their budget.
				settled := v.UpdatedAt.Add(2 * time.Minute)
				if v.LeaseUntil != nil && v.LeaseUntil.Add(2*time.Minute).After(settled) {
					settled = v.LeaseUntil.Add(2 * time.Minute)
				}
				if s.now().Before(settled) {
					return nil
				}
				delta["reserved_bytes"] = -v.ReservedSize
				delta["pending_downloads"] = 1
				v.State = "pending"
				v.ArtifactState = ""
				v.StoragePath = ""
				v.ReservedSize = 0
			}
			v.WorkerID = ""
			v.LeaseUntil = nil
			v.Error = ""
			v.ErrorClass = ""
		} else if v.State == "completed" {
			if o.Exists {
				if v.ActualSize > 0 && v.ActualSize != o.Size {
					return control.ErrInvalidState
				}
				if v.ObjectGeneration == o.Generation && v.ArtifactState == "stored" {
					return nil
				}
				v.ObjectGeneration = o.Generation
				v.ArtifactState = "stored"
			} else {
				delta["used_bytes"] = -v.ActualSize
				v.ArtifactState = "expired"
			}
		} else {
			return nil
		}
		v.UpdatedAt = s.now()
		if e := tx.Set(s.video(v.ID), v); e != nil {
			return e
		}
		return s.counterDelta(tx, values, delta)
	})
}
