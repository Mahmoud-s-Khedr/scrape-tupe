package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func testLeasedVideo(t *testing.T) (*DB, context.Context, int64) {
	t.Helper()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	sourceID, err := database.CreateSource(ctx, "https://example.test/source", "video")
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := database.InsertVideo(ctx, "example-video", sourceID, "Example")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ClaimJob(ctx, "worker-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	return database, ctx, videoID
}

func TestWorkerUpdatesRequireCurrentLeaseOwner(t *testing.T) {
	database, ctx, videoID := testLeasedVideo(t)

	if err := database.RenewLease(ctx, videoID, "worker-b", time.Minute); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("RenewLease wrong worker error = %v, want ErrLeaseNotOwned", err)
	}
	if err := database.UpdateVideoState(ctx, videoID, "worker-b", "downloading", nil); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("UpdateVideoState wrong worker error = %v, want ErrLeaseNotOwned", err)
	}
	if err := database.CompleteVideo(ctx, videoID, "worker-b", "gs://bucket/object"); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("CompleteVideo wrong worker error = %v, want ErrLeaseNotOwned", err)
	}
}

func TestWorkerUpdatesRejectExpiredLease(t *testing.T) {
	database, ctx, videoID := testLeasedVideo(t)
	if _, err := database.ExecContext(ctx, `UPDATE videos SET lease_until = datetime('now', '-1 minute') WHERE id = ?`, videoID); err != nil {
		t.Fatal(err)
	}

	for _, operation := range []struct {
		name string
		call func() error
	}{
		{"renew", func() error { return database.RenewLease(ctx, videoID, "worker-a", time.Minute) }},
		{"state", func() error { return database.UpdateVideoState(ctx, videoID, "worker-a", "downloading", nil) }},
		{"complete", func() error { return database.CompleteVideo(ctx, videoID, "worker-a", "gs://bucket/object") }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.call(); !errors.Is(err, ErrLeaseNotOwned) {
				t.Fatalf("error = %v, want ErrLeaseNotOwned", err)
			}
		})
	}
}

func TestWorkerStateTransitions(t *testing.T) {
	database, ctx, videoID := testLeasedVideo(t)

	if err := database.UpdateVideoState(ctx, videoID, "worker-a", "uploading", nil); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("skipped transition error = %v, want ErrLeaseNotOwned", err)
	}
	if err := database.UpdateVideoState(ctx, videoID, "worker-a", "completed", nil); !errors.Is(err, ErrInvalidVideoState) {
		t.Fatalf("invalid requested state error = %v, want ErrInvalidVideoState", err)
	}
	if err := database.UpdateVideoState(ctx, videoID, "worker-a", "downloading", nil); err != nil {
		t.Fatalf("leased -> downloading: %v", err)
	}
	if err := database.UpdateVideoState(ctx, videoID, "worker-a", "uploading", nil); err != nil {
		t.Fatalf("downloading -> uploading: %v", err)
	}
	if err := database.CompleteVideo(ctx, videoID, "worker-a", "gs://bucket/object"); err != nil {
		t.Fatalf("uploading -> completed: %v", err)
	}
	video, err := database.GetVideo(ctx, videoID)
	if err != nil {
		t.Fatal(err)
	}
	if video.State != "completed" || video.StoragePath == nil || *video.StoragePath != "gs://bucket/object" {
		t.Fatalf("completed video = %#v", video)
	}
}

func TestWorkerCanRecordErrorFromActiveLease(t *testing.T) {
	database, ctx, videoID := testLeasedVideo(t)
	message := "download failed"
	if err := database.UpdateVideoState(ctx, videoID, "worker-a", "error", &message); err != nil {
		t.Fatalf("record error: %v", err)
	}
}
