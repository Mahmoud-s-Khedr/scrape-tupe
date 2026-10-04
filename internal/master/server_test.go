package master

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
)

func TestRunLeaseRecoveryRecoversExpiredWorkImmediately(t *testing.T) {
	ctx := context.Background()
	database, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	sourceID, err := database.CreateSource(ctx, "https://example.test/source", "video")
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := database.InsertVideo(ctx, "expired-video", sourceID, "Expired")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ClaimJob(ctx, "old-worker", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `UPDATE videos SET lease_until = datetime('now', '-1 minute') WHERE id = ?`, videoID); err != nil {
		t.Fatal(err)
	}

	server := New(database, slog.New(slog.NewTextHandler(io.Discard, nil)))
	recoveryCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		server.RunLeaseRecovery(recoveryCtx, time.Hour)
		close(done)
	}()

	deadline := time.After(time.Second)
	for {
		video, err := database.GetVideo(ctx, videoID)
		if err != nil {
			t.Fatal(err)
		}
		if video.State == "pending" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("video state = %q, want pending", video.State)
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lease recovery loop did not stop after context cancellation")
	}
}
