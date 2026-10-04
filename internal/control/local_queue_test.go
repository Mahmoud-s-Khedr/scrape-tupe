package control

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
)

func queueStore(t *testing.T) *LocalStore {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return NewLocalStore(d)
}

func TestDirectVideoCreatesDurableRawURLAndDeduplicatesVideo(t *testing.T) {
	ctx := context.Background()
	s := queueStore(t)
	a, err := s.CreateSource(ctx, "https://youtu.be/abc", "video", true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.ClaimRawURL(ctx, "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if raw.SourceID != a.ID || raw.RawURL != a.URL {
		t.Fatalf("raw=%+v source=%+v", raw, a)
	}
	if err = s.CompleteRawURL(ctx, raw.ID, "a", Metadata{YoutubeID: "same", URL: "https://www.youtube.com/watch?v=same", EstimatedSize: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateSource(ctx, "https://youtube.com/watch?v=same", "video", true); err != nil {
		t.Fatal(err)
	}
	raw, err = s.ClaimRawURL(ctx, "b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteRawURL(ctx, raw.ID, "b", Metadata{YoutubeID: "same", URL: "https://www.youtube.com/watch?v=same", EstimatedSize: 1}); err != nil {
		t.Fatal(err)
	}
	vs, err := s.ListVideos(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].YoutubeID != "same" {
		t.Fatalf("videos=%+v", vs)
	}
}

func TestDiscoveryCheckpointIsExactlyOnceAndLeaseOwned(t *testing.T) {
	ctx := context.Background()
	s := queueStore(t)
	src, err := s.CreateSource(ctx, "https://youtube.com/@example", "channel", true)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimDiscovery(ctx, "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != src.ID {
		t.Fatal("wrong source claimed")
	}
	if err = s.CheckpointDiscovery(ctx, src.ID, "other", 0, "https://youtu.be/a"); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("wrong owner: %v", err)
	}
	if err = s.CheckpointDiscovery(ctx, src.ID, "owner", 0, "https://youtu.be/a"); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckpointDiscovery(ctx, src.ID, "owner", 0, "https://youtu.be/a"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, src.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DiscoveryCursor != 1 {
		t.Fatalf("cursor=%d", got.DiscoveryCursor)
	}
	// One raw row proves an interrupted/retried task cannot duplicate index 0.
	var n int
	if err = s.DB.QueryRowContext(ctx, "SELECT count(*) FROM raw_urls WHERE source_id=?", src.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("raw count=%d", n)
	}
}
