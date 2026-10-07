//go:build integration

package firestore

import (
	cloudfs "cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func seedQueue(t *testing.T, s *Store, sizes []int64, used int64) {
	t.Helper()
	ctx := context.Background()
	now := s.now()
	batch := s.client.Batch()
	for i, size := range sizes {
		id := fmt.Sprintf("video-%03d", i)
		batch.Set(s.video(id), control.Video{ID: id, YoutubeID: id, EstimatedSize: size, State: "pending", CreatedAt: now.Add(time.Duration(i) * time.Second), UpdatedAt: now})
	}
	batch.Set(s.counter(), map[string]int64{"pending_downloads": int64(len(sizes)), "used_bytes": used, "reserved_bytes": 0})
	if _, e := batch.Commit(ctx); e != nil {
		t.Fatal(e)
	}
}
func counterValue(t *testing.T, s *Store, key string) int64 {
	t.Helper()
	snap, e := s.counter().Get(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	n, _ := snap.Data()[key].(int64)
	return n
}
func TestEmulatorClaimSkipsBlockedPrefixAndRepairsOversized(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	sizes := []int64{control.ArtifactBudgetBytes + 1, 0}
	for i := 0; i < 65; i++ {
		sizes = append(sizes, 2048)
	}
	sizes = append(sizes, 100)
	seedQueue(t, s, sizes, control.ArtifactBudgetBytes-1024)
	if _, e := s.ClaimVideo(ctx, "first", time.Minute); !errors.Is(e, control.ErrCapacity) {
		t.Fatalf("first scan: %v", e)
	}
	v, e := s.ClaimVideo(ctx, "second", time.Minute)
	if e != nil || v.ID != fmt.Sprintf("video-%03d", len(sizes)-1) {
		t.Fatalf("claim=%+v err=%v", v, e)
	}
	large, e := s.GetVideo(ctx, "video-000")
	if e != nil || large.State != "error" || large.ErrorClass != "oversized" {
		t.Fatalf("oversized=%+v err=%v", large, e)
	}
	if got := counterValue(t, s, "reserved_bytes"); got != 100 {
		t.Fatalf("reserved=%d", got)
	}
}
func TestEmulatorUnknownSizeReservationAndConcurrentBudget(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	if e := s.SetUnknownSizeReservation(control.ArtifactBudgetBytes / 2); e != nil {
		t.Fatal(e)
	}
	seedQueue(t, s, []int64{0, 0, 0, 0}, 0)
	var claimed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, e := s.ClaimVideo(ctx, fmt.Sprint(i), time.Minute)
			if e == nil {
				if v.ReservedSize != control.ArtifactBudgetBytes/2 {
					t.Errorf("reservation=%d", v.ReservedSize)
				}
				claimed.Add(1)
			} else if !errors.Is(e, control.ErrCapacity) && !errors.Is(e, control.ErrNotFound) {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	if claimed.Load() != 2 {
		t.Fatalf("claimed=%d", claimed.Load())
	}
	if n := counterValue(t, s, "reserved_bytes"); n != control.ArtifactBudgetBytes {
		t.Fatalf("reserved=%d", n)
	}
	ready, e := s.CanDownload(ctx)
	if e != nil || ready {
		t.Fatalf("ready=%v err=%v", ready, e)
	}
}
func TestEmulatorDiscoveryReplayRetryAndRenewal(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	src, e := s.CreateSource(ctx, "https://youtube.test/playlist", "playlist", true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ClaimDiscovery(ctx, "one", time.Minute); e != nil {
		t.Fatal(e)
	}
	for i, u := range []string{"A", "B"} {
		if e = s.CheckpointDiscovery(ctx, src.ID, "one", i, u); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.FailDiscovery(ctx, src.ID, "one", false, "blocked", "blocked", now.Add(-time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.RetrySource(ctx, src.ID); e != nil {
		t.Fatal(e)
	}
	if got := counterValue(t, s, "discoverable_sources"); got != 1 {
		t.Fatalf("discoverable=%d", got)
	}
	if e = s.RetrySource(ctx, src.ID); !errors.Is(e, control.ErrInvalidState) {
		t.Fatalf("repeated retry=%v", e)
	}
	if _, e = s.ClaimDiscovery(ctx, "two", time.Minute); e != nil {
		t.Fatal(e)
	}
	now = now.Add(30 * time.Second)
	if e = s.RenewDiscovery(ctx, src.ID, "two", time.Minute); e != nil {
		t.Fatal(e)
	}
	now = now.Add(40 * time.Second)
	for i, u := range []string{"B", "C", "A", "C"} {
		if e = s.CheckpointDiscovery(ctx, src.ID, "two", i, u); e != nil {
			t.Fatal(e)
		}
	}
	if got := counterValue(t, s, "eligible_raw_urls"); got != 3 {
		t.Fatalf("raw count=%d", got)
	}
	if e = s.FinishDiscovery(ctx, src.ID, "two"); e != nil {
		t.Fatal(e)
	}
	if e = s.RenewDiscovery(ctx, src.ID, "two", time.Minute); !errors.Is(e, control.ErrLeaseNotOwned) {
		t.Fatalf("terminal renew=%v", e)
	}
	raw, e := s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	now = now.Add(30 * time.Second)
	if e = s.RenewRawURL(ctx, raw.ID, "normalizer", time.Minute); e != nil {
		t.Fatal(e)
	}
	now = now.Add(40 * time.Second)
	if e = s.CompleteRawURL(ctx, raw.ID, "normalizer", control.Metadata{YoutubeID: "normalized", URL: "https://youtu.be/normalized", EstimatedSize: 1}); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetSource(ctx, src.ID)
	if e != nil || got.DiscoveredCount != 3 || got.NormalizedCount != 1 {
		t.Fatalf("source=%+v err=%v", got, e)
	}
}
func TestEmulatorArtifactIntentRecoveryAndRetention(t *testing.T) {
	for _, exists := range []bool{true, false} {
		t.Run(fmt.Sprint(exists), func(t *testing.T) {
			s := emulatorStore(t)
			ctx := context.Background()
			now := time.Now().UTC()
			s.now = func() time.Time { return now }
			seedQueue(t, s, []int64{10}, 0)
			v, e := s.ClaimVideo(ctx, "worker", time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.SetVideoState(ctx, v.ID, "worker", "downloading", ""); e != nil {
				t.Fatal(e)
			}
			if e = s.SetVideoState(ctx, v.ID, "worker", "uploading", ""); e != nil {
				t.Fatal(e)
			}
			if e = s.PrepareUpload(ctx, v.ID, "worker", "gs://bucket/videos/video/file.mp4"); e != nil {
				t.Fatal(e)
			}
			// A failed callback keeps capacity reserved, even after recovery.
			if e = s.SetVideoState(ctx, v.ID, "worker", "error", "callback failed"); e != nil {
				t.Fatal(e)
			}
			if n := counterValue(t, s, "reserved_bytes"); n != 10 {
				t.Fatalf("lost reservation=%d", n)
			}
			if e = s.RetryVideo(ctx, v.ID); !errors.Is(e, control.ErrCapacity) {
				t.Fatalf("unsettled retry=%v", e)
			}
			now = now.Add(4 * time.Minute)
			candidates, e := s.ArtifactCandidates(ctx, 64)
			if e != nil || len(candidates) != 1 {
				t.Fatalf("candidates=%v err=%v", candidates, e)
			}
			o := control.ArtifactObservation{Exists: exists, Size: 8, Generation: 42}
			if e = s.ObserveArtifact(ctx, candidates[0], o); e != nil {
				t.Fatal(e)
			}
			if e = s.ObserveArtifact(ctx, candidates[0], o); e != nil {
				t.Fatal(e)
			}
			if n := counterValue(t, s, "reserved_bytes"); n != 0 {
				t.Fatalf("reserved=%d", n)
			}
			got, e := s.GetVideo(ctx, v.ID)
			if e != nil {
				t.Fatal(e)
			}
			if !exists {
				if got.State != "pending" || counterValue(t, s, "pending_downloads") != 1 {
					t.Fatalf("missing recovery=%+v", got)
				}
				return
			}
			if got.State != "completed" || got.ObjectGeneration != 42 || counterValue(t, s, "used_bytes") != 8 {
				t.Fatalf("adoption=%+v", got)
			}
			if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{Exists: true, Size: 8, Generation: 43}); !errors.Is(e, control.ErrInvalidState) {
				t.Fatalf("replacement=%v", e)
			}
			if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{}); e != nil {
				t.Fatal(e)
			}
			if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{}); e != nil {
				t.Fatal(e)
			}
			if n := counterValue(t, s, "used_bytes"); n != 0 {
				t.Fatalf("used after retention=%d", n)
			}
			expired, e := s.GetVideo(ctx, v.ID)
			if e != nil || expired.ArtifactState != "expired" {
				t.Fatalf("expired=%+v err=%v", expired, e)
			}
		})
	}
}
func TestEmulatorStaleArtifactObservationCannotReleaseActiveUpload(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	seedQueue(t, s, []int64{10}, 0)
	v, e := s.ClaimVideo(ctx, "worker", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	for _, state := range []string{"downloading", "uploading"} {
		if e = s.SetVideoState(ctx, v.ID, "worker", state, ""); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.PrepareUpload(ctx, v.ID, "worker", "gs://bucket/videos/v/file"); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetVideo(ctx, v.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{}); e != nil {
		t.Fatal(e)
	}
	if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{Exists: true, Size: 10, Generation: 1}); e != nil {
		t.Fatal(e)
	}
	if n := counterValue(t, s, "reserved_bytes"); n != 10 {
		t.Fatalf("active reservation=%d", n)
	}
	_, e = s.video(v.ID).Update(ctx, []cloudfs.Update{{Path: "storage_path", Value: "gs://bucket/videos/v/other"}})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ObserveArtifact(ctx, *got, control.ArtifactObservation{}); e != nil {
		t.Fatal(e)
	}
}

func TestEmulatorLegacyQuarantinedSourceRetriesNormalization(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	src, e := s.CreateSource(ctx, "https://fixture/legacy", "playlist", true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ClaimDiscovery(ctx, "discovery", time.Minute); e != nil {
		t.Fatal(e)
	}
	if e = s.CheckpointDiscovery(ctx, src.ID, "discovery", 0, "https://fixture/video"); e != nil {
		t.Fatal(e)
	}
	if e = s.FinishDiscovery(ctx, src.ID, "discovery"); e != nil {
		t.Fatal(e)
	}
	raw, e := s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ReleaseRawURL(ctx, raw.ID, "normalizer", false, "blocked", "blocked", time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.source(src.ID).Update(ctx, []cloudfs.Update{{Path: "discovery_complete", Value: cloudfs.Delete}}); e != nil {
		t.Fatal(e)
	}
	if e = s.RetrySource(ctx, src.ID); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetSource(ctx, src.ID)
	if e != nil || got.State != "normalizing" || !got.DiscoveryComplete {
		t.Fatalf("source=%+v err=%v", got, e)
	}
	if _, e = s.ClaimRawURL(ctx, "retry", time.Minute); e != nil {
		t.Fatal(e)
	}
	if n := counterValue(t, s, "discoverable_sources"); n != 0 {
		t.Fatalf("discovery unnecessarily requeued: %d", n)
	}
}

func TestEmulatorReadinessDoesNotSkipItsEligibleCandidate(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	sizes := []int64{1}
	for i := 0; i < 100; i++ {
		sizes = append(sizes, 10)
	}
	seedQueue(t, s, sizes, control.ArtifactBudgetBytes-2)
	ready, e := s.CanDownload(ctx)
	if e != nil || !ready {
		t.Fatalf("ready=%v err=%v", ready, e)
	}
	v, e := s.ClaimVideo(ctx, "worker", time.Minute)
	if e != nil || v.ID != "video-000" {
		t.Fatalf("readiness hid eligible candidate: video=%+v err=%v", v, e)
	}
}
