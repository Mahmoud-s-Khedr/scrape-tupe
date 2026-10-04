// Package firestore implements the production Firestore control plane.
package firestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	cloudfs "cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

const countersDoc = "artifacts"

type Store struct {
	client *cloudfs.Client
	now    func() time.Time
}

func New(ctx context.Context, projectID string) (*Store, error) {
	if projectID == "" {
		return nil, errors.New("Firestore project ID is required")
	}
	c, err := cloudfs.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create Firestore client: %w", err)
	}
	return &Store{client: c, now: func() time.Time { return time.Now().UTC() }}, nil
}
func (s *Store) Close() error                          { return s.client.Close() }
func (s *Store) source(id string) *cloudfs.DocumentRef { return s.client.Collection("sources").Doc(id) }
func (s *Store) raw(id string) *cloudfs.DocumentRef    { return s.client.Collection("raw_urls").Doc(id) }
func (s *Store) video(id string) *cloudfs.DocumentRef  { return s.client.Collection("videos").Doc(id) }
func (s *Store) execution(id string) *cloudfs.DocumentRef {
	return s.client.Collection("executions").Doc(id)
}
func (s *Store) executionGuard(kind string) *cloudfs.DocumentRef {
	return s.client.Collection("execution_guards").Doc(kind)
}
func (s *Store) counter() *cloudfs.DocumentRef {
	return s.client.Collection("counters").Doc(countersDoc)
}

func validKind(k string) bool { return k == "channel" || k == "playlist" || k == "video" }
func (s *Store) CreateSource(ctx context.Context, url, kind string, ack bool) (*control.Source, error) {
	if !ack {
		return nil, errors.New("authorization acknowledgement is required")
	}
	if !validKind(kind) {
		return nil, errors.New("unsupported source kind")
	}
	if url == "" {
		return nil, errors.New("source URL is required")
	}
	// A deterministic URL lookup is intentionally avoided: URLs are not safe
	// document IDs. A transaction reads the indexed URL and preserves the first
	// opaque ID under concurrent submissions.
	q := s.client.Collection("sources").Where("url", "==", url).Limit(1)
	var out *control.Source
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		it := tx.Documents(q)
		defer it.Stop()
		snap, e := it.Next()
		if e == nil {
			v, e := decodeSource(snap)
			out = v
			return e
		}
		if e != iterator.Done {
			return e
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		now := s.now()
		state := "queued"
		if kind == "video" {
			state = "normalizing"
		}
		out = &control.Source{ID: uuid.NewString(), URL: url, Kind: kind, AuthorizationAcknowledged: true, State: state, CreatedAt: now, UpdatedAt: now}
		if e := tx.Create(s.source(out.ID), out); e != nil {
			return e
		}
		if kind == "video" {
			raw := control.RawURL{ID: uuid.NewString(), SourceID: out.ID, Index: 0, RawURL: url, State: "pending", CreatedAt: now, UpdatedAt: now}
			if e := tx.Create(s.raw(raw.ID), raw); e != nil {
				return e
			}
			return s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": 1})
		}
		return s.counterDelta(tx, counters, map[string]int64{"discoverable_sources": 1})
	})
	return out, err
}
func decodeRaw(snap *cloudfs.DocumentSnapshot) (*control.RawURL, error) {
	var out control.RawURL
	if e := snap.DataTo(&out); e != nil {
		return nil, e
	}
	out.ID = snap.Ref.ID
	return &out, nil
}
func decodeSource(snap *cloudfs.DocumentSnapshot) (*control.Source, error) {
	var out control.Source
	if e := snap.DataTo(&out); e != nil {
		return nil, e
	}
	out.ID = snap.Ref.ID
	return &out, nil
}
func decodeVideo(snap *cloudfs.DocumentSnapshot) (*control.Video, error) {
	var out control.Video
	if e := snap.DataTo(&out); e != nil {
		return nil, e
	}
	out.ID = snap.Ref.ID
	if out.YoutubeID == "" {
		out.YoutubeID = out.ID
	}
	return &out, nil
}
func (s *Store) GetSource(ctx context.Context, id string) (*control.Source, error) {
	snap, e := s.source(id).Get(ctx)
	if isNotFound(e) {
		return nil, control.ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	return decodeSource(snap)
}
func (s *Store) ListSources(ctx context.Context) ([]control.Source, error) {
	it := s.client.Collection("sources").OrderBy("created_at", cloudfs.Desc).Documents(ctx)
	defer it.Stop()
	var out []control.Source
	for {
		snap, e := it.Next()
		if e == iterator.Done {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
		x, e := decodeSource(snap)
		if e != nil {
			return nil, e
		}
		out = append(out, *x)
	}
}
func (s *Store) RetrySource(ctx context.Context, id string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.source(id))
		if e != nil {
			return e
		}
		x, e := decodeSource(snap)
		if e != nil {
			return e
		}
		if x.State != "error" && x.State != "quarantined" {
			return control.ErrInvalidState
		}
		if x.QuarantinedUntil != nil && x.QuarantinedUntil.After(s.now()) {
			return control.ErrQuarantined
		}
		// Raw URLs are immutable audit records. Retrying a processed source only
		// requeues terminal quarantines; normalized work and its video stay put.
		it := tx.Documents(s.client.Collection("raw_urls").Where("source_id", "==", id).Where("state", "==", "quarantined"))
		var raws []*control.RawURL
		var retryCount int64
		for {
			rawSnap, next := it.Next()
			if next == iterator.Done {
				break
			}
			if next != nil {
				it.Stop()
				return next
			}
			raw, next := decodeRaw(rawSnap)
			if next != nil {
				it.Stop()
				return next
			}
			raws = append(raws, raw)
			retryCount++
		}
		it.Stop()
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		if retryCount > 0 {
			derived, e := s.deriveSourceStateTx(ctx, tx, id, rawStates(raws, "pending"))
			if e != nil {
				return e
			}
			for _, raw := range raws {
				raw.State, raw.WorkerID, raw.Error, raw.ErrorClass = "pending", "", "", ""
				raw.LeaseUntil, raw.RetryAt, raw.TerminalAt = nil, nil, nil
				raw.UpdatedAt = s.now()
				if e = tx.Set(s.raw(raw.ID), raw); e != nil {
					return e
				}
			}
			if e = tx.Set(s.source(id), derived); e != nil {
				return e
			}
			if e := s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": retryCount}); e != nil {
				return e
			}
			return nil
		}
		x.State = "queued"
		x.Error = ""
		x.QuarantinedUntil = nil
		x.UpdatedAt = s.now()
		return tx.Set(s.source(id), x)
	})
}

// ClaimDiscovery makes source expansion resumable. A task holds a short lease
// and checkpoints every discovered URL; it never writes Firestore directly.
func (s *Store) ClaimDiscovery(ctx context.Context, worker string, lease time.Duration) (*control.Source, error) {
	var out *control.Source
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		q := s.client.Collection("sources").Where("state", "==", "queued").Where("kind", "in", []string{"channel", "playlist"}).OrderBy("created_at", cloudfs.Asc).Limit(10)
		it := tx.Documents(q)
		defer it.Stop()
		var chosen *cloudfs.DocumentSnapshot
		for {
			snap, e := it.Next()
			if e == iterator.Done {
				break
			}
			if e != nil {
				return e
			}
			x, e := decodeSource(snap)
			if e != nil {
				return e
			}
			if x.RetryAt == nil || !x.RetryAt.After(s.now()) {
				chosen = snap
				break
			}
		}
		if chosen == nil {
			return control.ErrNotFound
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		x, e := decodeSource(chosen)
		if e != nil {
			return e
		}
		until := s.now().Add(lease)
		x.State = "discovering"
		x.WorkerID = worker
		x.LeaseUntil = &until
		x.AttemptCount++
		x.UpdatedAt = s.now()
		out = x
		if e := tx.Set(chosen.Ref, x); e != nil {
			return e
		}
		return s.counterDelta(tx, counters, map[string]int64{"discoverable_sources": -1})
	})
	return out, err
}
func (s *Store) CheckpointDiscovery(ctx context.Context, sourceID, worker string, index int, rawURL string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.source(sourceID))
		if e != nil {
			return e
		}
		src, e := decodeSource(snap)
		if e != nil {
			return e
		}
		if src.State != "discovering" || src.WorkerID != worker || src.LeaseUntil == nil || !src.LeaseUntil.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		if index < src.DiscoveryCursor {
			return nil
		}
		if index != src.DiscoveryCursor {
			return control.ErrInvalidState
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		now := s.now()
		raw := control.RawURL{ID: uuid.NewString(), SourceID: sourceID, Index: index, RawURL: rawURL, State: "pending", CreatedAt: now, UpdatedAt: now}
		// The document ID and payload ID must be identical: workers receive the
		// latter and completion looks up the former.
		if e = tx.Create(s.raw(raw.ID), raw); e != nil {
			return e
		}
		src.DiscoveryCursor++
		src.DiscoveredCount++
		src.UpdatedAt = now
		if e = tx.Set(s.source(sourceID), src); e != nil {
			return e
		}
		return s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": 1})
	})
}
func (s *Store) FinishDiscovery(ctx context.Context, sourceID, worker string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.source(sourceID))
		if e != nil {
			return e
		}
		x, e := decodeSource(snap)
		if e != nil {
			return e
		}
		if x.State != "discovering" || x.WorkerID != worker || x.LeaseUntil == nil || !x.LeaseUntil.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		x.State = "normalizing"
		x.WorkerID = ""
		x.LeaseUntil = nil
		x.Error = ""
		x.UpdatedAt = s.now()
		return tx.Set(s.source(sourceID), x)
	})
}
func (s *Store) FailDiscovery(ctx context.Context, sourceID, worker string, retryable bool, class, msg string, retryAt time.Time) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.source(sourceID))
		if e != nil {
			return e
		}
		x, e := decodeSource(snap)
		if e != nil {
			return e
		}
		if x.WorkerID != worker {
			return control.ErrLeaseNotOwned
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		if !retryable || x.AttemptCount >= 3 {
			x.State = "quarantined"
			x.QuarantinedUntil = &retryAt
			x.QuarantinedCount++
		} else {
			x.State = "queued"
			x.RetryAt = &retryAt
		}
		x.WorkerID = ""
		x.LeaseUntil = nil
		x.Error = msg
		x.UpdatedAt = s.now()
		if e := tx.Set(s.source(sourceID), x); e != nil {
			return e
		}
		if !retryable || x.AttemptCount >= 3 {
			return nil
		}
		return s.counterDelta(tx, counters, map[string]int64{"discoverable_sources": 1})
	})
}

func (s *Store) ClaimRawURL(ctx context.Context, worker string, lease time.Duration) (*control.RawURL, error) {
	var out *control.RawURL
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		q := s.client.Collection("raw_urls").Where("state", "==", "pending").OrderBy("created_at", cloudfs.Asc).Limit(25)
		it := tx.Documents(q)
		defer it.Stop()
		var chosen *cloudfs.DocumentSnapshot
		for {
			snap, e := it.Next()
			if e == iterator.Done {
				break
			}
			if e != nil {
				return e
			}
			x, e := decodeRaw(snap)
			if e != nil {
				return e
			}
			if x.RetryAt != nil && x.RetryAt.After(s.now()) {
				continue
			}
			ss, e := tx.Get(s.source(x.SourceID))
			if e != nil {
				return e
			}
			src, e := decodeSource(ss)
			if e != nil {
				return e
			}
			if src.State != "normalizing" {
				continue
			}
			chosen = snap
			break
		}
		if chosen == nil {
			return control.ErrNotFound
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		x, e := decodeRaw(chosen)
		if e != nil {
			return e
		}
		until := s.now().Add(lease)
		x.State = "leased"
		x.WorkerID = worker
		x.LeaseUntil = &until
		x.AttemptCount++
		x.UpdatedAt = s.now()
		out = x
		if e = tx.Set(chosen.Ref, x); e != nil {
			return e
		}
		return s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": -1})
	})
	return out, err
}
func (s *Store) GetRawURL(ctx context.Context, id string) (*control.RawURL, error) {
	snap, e := s.raw(id).Get(ctx)
	if isNotFound(e) {
		return nil, control.ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	return decodeRaw(snap)
}
func (s *Store) CompleteRawURL(ctx context.Context, id, worker string, m control.Metadata) error {
	if m.YoutubeID == "" || m.URL == "" {
		return control.ErrInvalidState
	}
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.raw(id))
		if e != nil {
			return e
		}
		raw, e := decodeRaw(snap)
		if e != nil {
			return e
		}
		if raw.State != "leased" || raw.WorkerID != worker || raw.LeaseUntil == nil || !raw.LeaseUntil.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		vs, e := tx.Get(s.video(m.YoutubeID))
		if e != nil && !isNotFound(e) {
			return e
		}
		videoMissing := isNotFound(e)
		derived, e := s.deriveSourceStateTx(ctx, tx, raw.SourceID, map[string]string{raw.ID: "normalized"})
		if e != nil {
			return e
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		now := s.now()
		raw.State = "normalized"
		raw.CanonicalURL = m.URL
		raw.YoutubeID = m.YoutubeID
		raw.WorkerID = ""
		raw.LeaseUntil = nil
		raw.TerminalAt = &now
		raw.UpdatedAt = now
		if e = tx.Set(s.raw(id), raw); e != nil {
			return e
		}
		if videoMissing {
			v := control.Video{ID: m.YoutubeID, YoutubeID: m.YoutubeID, SourceIDs: []string{raw.SourceID}, Title: m.Title, URL: m.URL, EstimatedSize: m.EstimatedSize, State: "pending", CreatedAt: now, UpdatedAt: now}
			if e = tx.Create(s.video(m.YoutubeID), v); e != nil {
				return e
			}
			if e = s.counterDelta(tx, counters, map[string]int64{"pending_downloads": 1}); e != nil {
				return e
			}
		} else {
			v, e := decodeVideo(vs)
			if e != nil {
				return e
			}
			if !contains(v.SourceIDs, raw.SourceID) {
				v.SourceIDs = append(v.SourceIDs, raw.SourceID)
				v.UpdatedAt = now
				if e = tx.Set(s.video(v.ID), v); e != nil {
					return e
				}
			}
		}
		return tx.Set(s.source(raw.SourceID), derived)
	})
}
func (s *Store) ReleaseRawURL(ctx context.Context, id, worker string, retryable bool, class, msg string, retryAt time.Time) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.raw(id))
		if e != nil {
			return e
		}
		raw, e := decodeRaw(snap)
		if e != nil {
			return e
		}
		if raw.WorkerID != worker {
			return control.ErrLeaseNotOwned
		}
		quarantine := !retryable || raw.AttemptCount >= 3
		state := "pending"
		if quarantine {
			state = "quarantined"
		}
		derived, e := s.deriveSourceStateTx(ctx, tx, raw.SourceID, map[string]string{raw.ID: state})
		if e != nil {
			return e
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		now := s.now()
		if quarantine {
			raw.State = "quarantined"
			raw.TerminalAt = &now
		} else {
			raw.State = "pending"
			raw.RetryAt = &retryAt
		}
		raw.WorkerID = ""
		raw.LeaseUntil = nil
		raw.Error = msg
		raw.ErrorClass = class
		raw.UpdatedAt = now
		if e = tx.Set(s.raw(id), raw); e != nil {
			return e
		}
		if e = tx.Set(s.source(raw.SourceID), derived); e != nil {
			return e
		}
		if !quarantine {
			return s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": 1})
		}
		return nil
	})
}
func (s *Store) GetVideo(ctx context.Context, id string) (*control.Video, error) {
	snap, e := s.video(id).Get(ctx)
	if isNotFound(e) {
		return nil, control.ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	return decodeVideo(snap)
}
func (s *Store) ListVideos(ctx context.Context, state string) ([]control.Video, error) {
	q := s.client.Collection("videos").Query
	if state != "" {
		q = q.Where("state", "==", state)
	}
	it := q.Documents(ctx)
	defer it.Stop()
	var out []control.Video
	for {
		snap, e := it.Next()
		if e == iterator.Done {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return nil, e
		}
		out = append(out, *v)
	}
}
func (s *Store) ClaimVideo(ctx context.Context, worker string, lease time.Duration) (*control.Video, error) {
	var out *control.Video
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		it := tx.Documents(s.client.Collection("videos").Where("state", "==", "pending").OrderBy("created_at", cloudfs.Asc).Limit(10))
		defer it.Stop()
		var selected *cloudfs.DocumentSnapshot
		for {
			snap, e := it.Next()
			if e == iterator.Done {
				break
			}
			if e != nil {
				return e
			}
			selected = snap
			break
		}
		if selected == nil {
			return control.ErrNotFound
		}
		v, e := decodeVideo(selected)
		if e != nil {
			return e
		}
		values, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		used, reserved := values["used_bytes"], values["reserved_bytes"]
		if v.EstimatedSize <= 0 || v.EstimatedSize > control.ArtifactBudgetBytes-used-reserved {
			return control.ErrCapacity
		}
		now := s.now()
		until := now.Add(lease)
		v.State = "leased"
		v.WorkerID = worker
		v.LeaseUntil = &until
		v.AttemptCount++
		v.ReservedSize = v.EstimatedSize
		v.UpdatedAt = now
		out = v
		if e = tx.Set(s.video(v.ID), v); e != nil {
			return e
		}
		return s.counterDelta(tx, values, map[string]int64{"reserved_bytes": v.ReservedSize, "pending_downloads": -1})
	})
	return out, err
}
func (s *Store) RenewLease(ctx context.Context, id, worker string, d time.Duration) error {
	return s.owned(ctx, id, worker, func(v *control.Video) error { u := s.now().Add(d); v.LeaseUntil = &u; return nil })
}
func (s *Store) SetVideoState(ctx context.Context, id, worker, state, msg string) error {
	if state == "error" {
		return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
			snap, err := tx.Get(s.video(id))
			if err != nil {
				return err
			}
			v, err := decodeVideo(snap)
			if err != nil {
				return err
			}
			if v.WorkerID != worker || v.LeaseUntil == nil || !v.LeaseUntil.After(s.now()) {
				return control.ErrLeaseNotOwned
			}
			counters, err := s.counterValues(tx)
			if err != nil {
				return err
			}
			return s.releaseInTx(tx, counters, v, "error", msg, nil)
		})
	}
	return s.owned(ctx, id, worker, func(v *control.Video) error {
		valid := ""
		switch state {
		case "downloading":
			valid = "leased"
		case "uploading":
			valid = "downloading"
		default:
			return control.ErrInvalidState
		}
		if valid != "" && v.State != valid {
			return control.ErrInvalidState
		}
		v.State = state
		v.Error = msg
		return nil
	})
}
func (s *Store) owned(ctx context.Context, id, worker string, change func(*control.Video) error) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.video(id))
		if e != nil {
			return e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return e
		}
		if v.WorkerID != worker || v.LeaseUntil == nil || !v.LeaseUntil.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		if e = change(v); e != nil {
			return e
		}
		v.UpdatedAt = s.now()
		return tx.Set(s.video(id), v)
	})
}
func (s *Store) CompleteVideo(ctx context.Context, id, worker, path string, actual int64) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.video(id))
		if e != nil {
			return e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return e
		}
		if v.WorkerID != worker || v.State != "uploading" || v.LeaseUntil == nil || !v.LeaseUntil.After(s.now()) {
			return control.ErrLeaseNotOwned
		}
		if actual <= 0 || actual > v.ReservedSize {
			return control.ErrCapacity
		}
		values, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		used := values["used_bytes"]
		if actual > v.ReservedSize || used+actual > control.ArtifactBudgetBytes {
			return control.ErrCapacity
		}
		v.State = "completed"
		v.StoragePath = path
		v.ActualSize = actual
		v.WorkerID = ""
		v.LeaseUntil = nil
		reservedSize := v.ReservedSize
		v.ReservedSize = 0
		v.UpdatedAt = s.now()
		if e = tx.Set(s.video(id), v); e != nil {
			return e
		}
		return s.counterDelta(tx, values, map[string]int64{"used_bytes": actual, "reserved_bytes": -reservedSize})
	})
}
func (s *Store) QuarantineVideo(ctx context.Context, id, worker, class string, until time.Time) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.video(id))
		if e != nil {
			return e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return e
		}
		if v.WorkerID != worker {
			return control.ErrLeaseNotOwned
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		return s.releaseInTx(tx, counters, v, "quarantined", class, &until)
	})
}
func (s *Store) RetryVideo(ctx context.Context, id string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.video(id))
		if e != nil {
			return e
		}
		v, e := decodeVideo(snap)
		if e != nil {
			return e
		}
		if v.State != "quarantined" && v.State != "error" {
			return control.ErrInvalidState
		}
		if v.QuarantinedUntil != nil && v.QuarantinedUntil.After(s.now()) {
			return control.ErrQuarantined
		}
		counters, e := s.counterValues(tx)
		if e != nil {
			return e
		}
		v.State = "pending"
		v.Error = ""
		v.ErrorClass = ""
		v.QuarantinedUntil = nil
		v.UpdatedAt = s.now()
		if e = tx.Set(s.video(id), v); e != nil {
			return e
		}
		return s.counterDelta(tx, counters, map[string]int64{"pending_downloads": 1})
	})
}
func (s *Store) releaseInTx(tx *cloudfs.Transaction, counters map[string]int64, v *control.Video, state, msg string, until *time.Time) error {
	reservedSize := v.ReservedSize
	v.State = state
	v.Error = msg
	v.ErrorClass = msg
	v.QuarantinedUntil = until
	v.WorkerID = ""
	v.LeaseUntil = nil
	v.ReservedSize = 0
	v.UpdatedAt = s.now()
	if err := tx.Set(s.video(v.ID), v); err != nil {
		return err
	}
	delta := map[string]int64{"reserved_bytes": -reservedSize}
	if state == "pending" {
		delta["pending_downloads"] = 1
	}
	return s.counterDelta(tx, counters, delta)
}
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int, error) {
	now := s.now()
	it := s.client.Collection("videos").Where("state", "in", []string{"leased", "downloading", "uploading"}).Where("lease_until", "<", now).Documents(ctx)
	defer it.Stop()
	n := 0
	for {
		snap, e := it.Next()
		if e == iterator.Done {
			return s.recoverRawAndSourceLeases(ctx, n)
		}
		if e != nil {
			return n, e
		}
		if e = s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
			fresh, e := tx.Get(snap.Ref)
			if e != nil {
				return e
			}
			v, e := decodeVideo(fresh)
			if e != nil {
				return e
			}
			if v.LeaseUntil == nil || !v.LeaseUntil.Before(s.now()) {
				return nil
			}
			counters, e := s.counterValues(tx)
			if e != nil {
				return e
			}
			return s.releaseInTx(tx, counters, v, "pending", "", nil)
		}); e != nil {
			return n, e
		}
		n++
	}
}
func (s *Store) recoverRawAndSourceLeases(ctx context.Context, n int) (int, error) {
	now := s.now()
	for _, spec := range []struct{ collection, state string }{{"raw_urls", "leased"}, {"sources", "discovering"}} {
		it := s.client.Collection(spec.collection).Where("state", "==", spec.state).Where("lease_until", "<", now).Documents(ctx)
		for {
			snap, e := it.Next()
			if e == iterator.Done {
				break
			}
			if e != nil {
				it.Stop()
				return n, e
			}
			e = s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
				fresh, e := tx.Get(snap.Ref)
				if e != nil {
					return e
				}
				if spec.collection == "raw_urls" {
					x, e := decodeRaw(fresh)
					if e != nil {
						return e
					}
					if x.LeaseUntil == nil || !x.LeaseUntil.Before(s.now()) {
						return nil
					}
					state := "pending"
					if x.AttemptCount >= 3 {
						state = "quarantined"
					}
					derived, e := s.deriveSourceStateTx(ctx, tx, x.SourceID, map[string]string{x.ID: state})
					if e != nil {
						return e
					}
					counters, e := s.counterValues(tx)
					if e != nil {
						return e
					}
					x.WorkerID = ""
					x.LeaseUntil = nil
					if x.AttemptCount >= 3 {
						x.State = "quarantined"
						t := s.now()
						x.TerminalAt = &t
					} else {
						x.State = "pending"
						t := s.now()
						x.RetryAt = &t
					}
					x.UpdatedAt = s.now()
					if e = tx.Set(fresh.Ref, x); e != nil {
						return e
					}
					if e = tx.Set(s.source(x.SourceID), derived); e != nil {
						return e
					}
					if x.State == "pending" {
						return s.counterDelta(tx, counters, map[string]int64{"eligible_raw_urls": 1})
					}
					return nil
				}
				x, e := decodeSource(fresh)
				if e != nil {
					return e
				}
				if x.LeaseUntil == nil || !x.LeaseUntil.Before(s.now()) {
					return nil
				}
				counters, e := s.counterValues(tx)
				if e != nil {
					return e
				}
				x.WorkerID = ""
				x.LeaseUntil = nil
				if x.AttemptCount >= 3 {
					x.State = "quarantined"
				} else {
					x.State = "queued"
					t := s.now()
					x.RetryAt = &t
				}
				x.UpdatedAt = s.now()
				if e = tx.Set(fresh.Ref, x); e != nil {
					return e
				}
				if x.State == "queued" {
					return s.counterDelta(tx, counters, map[string]int64{"discoverable_sources": 1})
				}
				return nil
			})
			if e != nil {
				it.Stop()
				return n, e
			}
			n++
		}
		it.Stop()
	}
	return n, nil
}
func (s *Store) CountByState(ctx context.Context) (map[string]int, error) {
	w, e := s.WorkCounts(ctx)
	if e != nil {
		return nil, e
	}
	return map[string]int{"pending": w["pending_downloads"]}, nil
}
func (s *Store) WorkCounts(ctx context.Context) (map[string]int, error) {
	snap, e := s.counter().Get(ctx)
	if isNotFound(e) {
		return map[string]int{}, nil
	}
	if e != nil {
		return nil, e
	}
	o := map[string]int{}
	for _, k := range []string{"discoverable_sources", "eligible_raw_urls", "pending_downloads"} {
		switch v := snap.Data()[k].(type) {
		case int64:
			o[k] = int(v)
		case int:
			o[k] = v
		}
	}
	return o, nil
}
func (s *Store) CreateExecution(ctx context.Context, in control.Execution) (*control.Execution, error) {
	var out *control.Execution
	e := s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		guard, err := tx.Get(s.executionGuard(in.Kind))
		if err == nil {
			if active, _ := guard.Data()["active"].(bool); active {
				return control.ErrExecutionActive
			}
		} else if !isNotFound(err) {
			return err
		}
		now := s.now()
		in.ID = uuid.NewString()
		in.State = "launching"
		in.CreatedAt = now
		in.ReconciledAt = now
		out = &in
		if err := tx.Create(s.execution(in.ID), in); err != nil {
			return err
		}
		return tx.Set(s.executionGuard(in.Kind), map[string]any{"active": true, "execution_id": in.ID, "updated_at": now})
	})
	return out, e
}
func (s *Store) GetActiveDownloaderExecution(ctx context.Context) (*control.Execution, error) {
	return s.GetActiveExecution(ctx, "downloader")
}
func (s *Store) GetActiveExecution(ctx context.Context, kind string) (*control.Execution, error) {
	guard, e := s.executionGuard(kind).Get(ctx)
	if isNotFound(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if active, _ := guard.Data()["active"].(bool); !active {
		return nil, nil
	}
	id, _ := guard.Data()["execution_id"].(string)
	if id == "" {
		return nil, nil
	}
	snap, e := s.execution(id).Get(ctx)
	if isNotFound(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var x control.Execution
	if e = snap.DataTo(&x); e != nil {
		return nil, e
	}
	x.ID = snap.Ref.ID
	return &x, nil
}
func (s *Store) UpdateExecution(ctx context.Context, id, state, runName string) error {
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *cloudfs.Transaction) error {
		snap, e := tx.Get(s.execution(id))
		if e != nil {
			return e
		}
		var x control.Execution
		if e = snap.DataTo(&x); e != nil {
			return e
		}
		guard, e := tx.Get(s.executionGuard(x.Kind))
		if e != nil && !isNotFound(e) {
			return e
		}
		guardMissing := isNotFound(e)
		x.ID = id
		x.State = state
		x.CloudRunName = runName
		x.ReconciledAt = s.now()
		if e = tx.Set(s.execution(id), x); e != nil {
			return e
		}
		if state == "launching" || state == "running" {
			return nil
		}
		if guardMissing {
			return nil
		}
		if guardID, _ := guard.Data()["execution_id"].(string); guardID == id {
			return tx.Set(s.executionGuard(x.Kind), map[string]any{"active": false, "updated_at": s.now()}, cloudfs.MergeAll)
		}
		return nil
	})
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// counterDelta serializes all counter changes through the counter document and
// clamps at zero. Counters are scheduling hints, never a source of truth, so a
// historical under-count must not turn into a negative value that wedges work.
func (s *Store) counterValues(tx *cloudfs.Transaction) (map[string]int64, error) {
	snap, err := tx.Get(s.counter())
	if isNotFound(err) {
		return map[string]int64{}, nil
	}
	if err != nil {
		return nil, err
	}
	values := map[string]int64{}
	for _, key := range []string{"discoverable_sources", "eligible_raw_urls", "pending_downloads", "used_bytes", "reserved_bytes"} {
		switch v := snap.Data()[key].(type) {
		case int64:
			values[key] = v
		case int:
			values[key] = int64(v)
		}
	}
	return values, nil
}

func (s *Store) counterDelta(tx *cloudfs.Transaction, values map[string]int64, delta map[string]int64) error {
	for key, change := range delta {
		values[key] = max(0, values[key]+change)
	}
	return tx.Set(s.counter(), values, cloudfs.MergeAll)
}

// deriveSourceStateTx performs the source aggregate's complete read phase.
// overrides contains pending raw URL state transitions that have not been
// written yet, so the aggregate can be calculated before any transaction write.
func (s *Store) deriveSourceStateTx(ctx context.Context, tx *cloudfs.Transaction, sourceID string, overrides map[string]string) (*control.Source, error) {
	it := tx.Documents(s.client.Collection("raw_urls").Where("source_id", "==", sourceID))
	defer it.Stop()
	var total, normalized, quarantined int
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		raw, err := decodeRaw(snap)
		if err != nil {
			return nil, err
		}
		total++
		state := raw.State
		if override, ok := overrides[raw.ID]; ok {
			state = override
		}
		if state == "normalized" {
			normalized++
		}
		if state == "quarantined" {
			quarantined++
		}
	}
	snap, err := tx.Get(s.source(sourceID))
	if err != nil {
		return nil, err
	}
	src, err := decodeSource(snap)
	if err != nil {
		return nil, err
	}
	state := "normalizing"
	if total > 0 && normalized == total {
		state = "normalized"
	}
	if total > 0 && normalized+quarantined == total && quarantined > 0 {
		state = "quarantined"
	}
	src.State, src.NormalizedCount, src.QuarantinedCount = state, normalized, quarantined
	if state != "quarantined" {
		src.Error = ""
		src.QuarantinedUntil = nil
	}
	src.UpdatedAt = s.now()
	return src, nil
}

func rawStates(raws []*control.RawURL, state string) map[string]string {
	overrides := make(map[string]string, len(raws))
	for _, raw := range raws {
		overrides[raw.ID] = state
	}
	return overrides
}

func isNotFound(err error) bool { return status.Code(err) == codes.NotFound }
