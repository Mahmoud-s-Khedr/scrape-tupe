//go:build integration

package firestore

import (
	"context"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

func emulatorStore(t *testing.T) *Store {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is required")
	}
	s, err := New(context.Background(), "scrape-tupe-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func normalizeDirectVideo(t *testing.T, s *Store, url, youtubeID string) *control.Video {
	t.Helper()
	ctx := context.Background()
	src, err := s.CreateSource(ctx, url, "video", true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteRawURL(ctx, raw.ID, "normalizer", control.Metadata{YoutubeID: youtubeID, URL: "https://youtube.test/watch?v=" + youtubeID, EstimatedSize: 2}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, src.ID)
	if err != nil || got.State != "normalized" {
		t.Fatalf("source=%+v err=%v", got, err)
	}
	v, err := s.GetVideo(ctx, youtubeID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func assertCountersNonNegative(t *testing.T, s *Store) {
	t.Helper()
	snap, err := s.counter().Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"discoverable_sources", "eligible_raw_urls", "pending_downloads", "used_bytes", "reserved_bytes"} {
		if value, ok := snap.Data()[key].(int64); ok && value < 0 {
			t.Fatalf("counter %s=%d", key, value)
		}
	}
}

func TestEmulatorDirectVideoLifecycleAndProvenance(t *testing.T) {
	ctx := context.Background()
	s := emulatorStore(t)

	normalizeDirectVideo(t, s, "https://youtu.be/emulator-a", "emulator-video")
	normalizeDirectVideo(t, s, "https://youtu.be/emulator-b", "emulator-video")
	v, err := s.GetVideo(ctx, "emulator-video")
	if err != nil || len(v.SourceIDs) != 2 {
		t.Fatalf("video=%+v err=%v", v, err)
	}

	v, err = s.ClaimVideo(ctx, "downloader", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetVideoState(ctx, v.ID, "downloader", "downloading", ""); err != nil {
		t.Fatal(err)
	}
	if err = s.SetVideoState(ctx, v.ID, "downloader", "uploading", ""); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteVideo(ctx, v.ID, "downloader", "gs://bucket/emulator-video", 2); err != nil {
		t.Fatal(err)
	}
	completed, err := s.GetVideo(ctx, v.ID)
	if err != nil || completed.State != "completed" || completed.ReservedSize != 0 {
		t.Fatalf("video=%+v err=%v", completed, err)
	}
	assertCountersNonNegative(t, s)
}

func TestEmulatorDiscoveryRawRetryAndSourceAggregate(t *testing.T) {
	ctx := context.Background()
	s := emulatorStore(t)
	src, err := s.CreateSource(ctx, "https://youtube.test/@emulator-channel", "channel", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimDiscovery(ctx, "discoverer", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = s.FailDiscovery(ctx, src.ID, "discoverer", true, "temporary", "temporary", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimDiscovery(ctx, "discoverer", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckpointDiscovery(ctx, src.ID, "discoverer", 0, "https://youtu.be/discovered"); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishDiscovery(ctx, src.ID, "discoverer"); err != nil {
		t.Fatal(err)
	}
	raw, err := s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseRawURL(ctx, raw.ID, "normalizer", true, "temporary", "temporary", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	raw, err = s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseRawURL(ctx, raw.ID, "normalizer", false, "blocked", "blocked", time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, src.ID)
	if err != nil || got.State != "quarantined" || got.QuarantinedCount != 1 {
		t.Fatalf("source=%+v err=%v", got, err)
	}
	if err = s.RetrySource(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetSource(ctx, src.ID)
	if err != nil || got.State != "normalizing" || got.QuarantinedCount != 0 {
		t.Fatalf("source=%+v err=%v", got, err)
	}
	raw, err = s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err = s.RecoverExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := s.GetRawURL(ctx, raw.ID)
	if err != nil || recovered.State != "quarantined" {
		t.Fatalf("raw=%+v err=%v", recovered, err)
	}
	assertCountersNonNegative(t, s)
}

func TestEmulatorDiscoveryLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	s := emulatorStore(t)
	src, err := s.CreateSource(ctx, "https://youtube.test/@expired-channel", "channel", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimDiscovery(ctx, "discoverer", time.Minute); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err = s.RecoverExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, src.ID)
	if err != nil || got.State != "queued" || got.WorkerID != "" {
		t.Fatalf("source=%+v err=%v", got, err)
	}
	assertCountersNonNegative(t, s)
}

func TestEmulatorVideoReleaseRetryAndLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	s := emulatorStore(t)
	v := normalizeDirectVideo(t, s, "https://youtu.be/emulator-retry", "emulator-retry")

	claimed, err := s.ClaimVideo(ctx, "downloader-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetVideoState(ctx, claimed.ID, "downloader-a", "error", "temporary"); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryVideo(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = s.ClaimVideo(ctx, "downloader-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.QuarantineVideo(ctx, claimed.ID, "downloader-b", "blocked", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryVideo(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = s.ClaimVideo(ctx, "downloader-c", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err = s.RecoverExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := s.GetVideo(ctx, claimed.ID)
	if err != nil || recovered.State != "pending" || recovered.ReservedSize != 0 {
		t.Fatalf("video=%+v err=%v", recovered, err)
	}
	assertCountersNonNegative(t, s)
}

func TestEmulatorExecutionGuardLifecycle(t *testing.T) {
	ctx := context.Background()
	s := emulatorStore(t)
	exec, err := s.CreateExecution(ctx, control.Execution{Kind: "downloader"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateExecution(ctx, control.Execution{Kind: "downloader"}); err != control.ErrExecutionActive {
		t.Fatalf("second execution error=%v", err)
	}
	if err = s.UpdateExecution(ctx, exec.ID, "completed", "run-emulator"); err != nil {
		t.Fatal(err)
	}
	active, err := s.GetActiveDownloaderExecution(ctx)
	if err != nil || active != nil {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if _, err = s.CreateExecution(ctx, control.Execution{Kind: "downloader"}); err != nil {
		t.Fatal(err)
	}
}
