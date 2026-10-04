package control

import (
	"context"
	"testing"
	"time"
)

func TestLocalSourceLifecycleAndProvenance(t *testing.T) {
	ctx := context.Background()
	s := queueStore(t)
	a, err := s.CreateSource(ctx, "https://youtu.be/a-one", "video", true)
	if err != nil {
		t.Fatal(err)
	}
	if !a.AuthorizationAcknowledged || a.State != "normalizing" {
		t.Fatalf("created source = %+v", a)
	}
	raw, err := s.ClaimRawURL(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteRawURL(ctx, raw.ID, "worker", Metadata{YoutubeID: "same-video", URL: "https://youtube.test/watch?v=same-video", EstimatedSize: 1}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, a.ID)
	if err != nil || got.State != "normalized" {
		t.Fatalf("completed source = %+v, %v", got, err)
	}

	b, err := s.CreateSource(ctx, "https://youtu.be/a-two", "video", true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = s.ClaimRawURL(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteRawURL(ctx, raw.ID, "worker", Metadata{YoutubeID: "same-video", URL: "https://youtube.test/watch?v=same-video", EstimatedSize: 1}); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetVideo(ctx, "same-video")
	if err != nil {
		t.Fatal(err)
	}
	if len(v.SourceIDs) != 2 || v.SourceIDs[0] != a.ID || v.SourceIDs[1] != b.ID {
		t.Fatalf("provenance = %#v", v.SourceIDs)
	}
}

func TestLocalQuarantineRetryOnlyRequeuesQuarantinedRawURLs(t *testing.T) {
	ctx := context.Background()
	s := queueStore(t)
	src, err := s.CreateSource(ctx, "https://youtu.be/retry", "video", true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.ClaimRawURL(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseRawURL(ctx, raw.ID, "worker", false, "blocked", "blocked", time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSource(ctx, src.ID)
	if err != nil || got.State != "quarantined" {
		t.Fatalf("quarantined source = %+v, %v", got, err)
	}
	if err = s.RetrySource(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetSource(ctx, src.ID)
	if err != nil || got.State != "normalizing" {
		t.Fatalf("retried source = %+v, %v", got, err)
	}
	raw, err = s.ClaimRawURL(ctx, "worker-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if raw.ID == "" {
		t.Fatal("retry did not requeue raw URL")
	}
}
