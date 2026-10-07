package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLocalDiscoveryReplayAndPartialRetry(t *testing.T) {
	s := queueStore(t)
	ctx := context.Background()
	src, e := s.CreateSource(ctx, "https://fixture/playlist", "playlist", true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ClaimDiscovery(ctx, "first", time.Minute); e != nil {
		t.Fatal(e)
	}
	for i, u := range []string{"A", "B"} {
		if e = s.CheckpointDiscovery(ctx, src.ID, "first", i, u); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.RecoverExpiredLeases(ctx); e != nil {
		t.Fatal(e)
	}
	before, e := s.GetSource(ctx, src.ID)
	if e != nil || before.State != "discovering" {
		t.Fatalf("source=%+v err=%v", before, e)
	}
	if e = s.FailDiscovery(ctx, src.ID, "first", false, "blocked", "blocked", time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = s.RetrySource(ctx, src.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.RetrySource(ctx, src.ID); !errors.Is(e, ErrInvalidState) {
		t.Fatalf("retry=%v", e)
	}
	if _, e = s.ClaimDiscovery(ctx, "second", time.Minute); e != nil {
		t.Fatal(e)
	}
	if e = s.RenewDiscovery(ctx, src.ID, "wrong", time.Minute); !errors.Is(e, ErrLeaseNotOwned) {
		t.Fatalf("owner=%v", e)
	}
	if e = s.RenewDiscovery(ctx, src.ID, "second", time.Minute); e != nil {
		t.Fatal(e)
	}
	for i, u := range []string{"B", "C", "A", "C"} {
		if e = s.CheckpointDiscovery(ctx, src.ID, "second", i, u); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.FinishDiscovery(ctx, src.ID, "second"); e != nil {
		t.Fatal(e)
	}
	got, e := s.GetSource(ctx, src.ID)
	if e != nil || got.DiscoveredCount != 3 || !got.DiscoveryComplete {
		t.Fatalf("source=%+v err=%v", got, e)
	}
	raw, e := s.ClaimRawURL(ctx, "normalizer", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RenewRawURL(ctx, raw.ID, "normalizer", time.Minute); e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.ExecContext(ctx, `UPDATE raw_urls SET lease_until=datetime('now','-1 minute') WHERE id=?`, raw.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.RenewRawURL(ctx, raw.ID, "normalizer", time.Minute); !errors.Is(e, ErrLeaseNotOwned) {
		t.Fatalf("expired=%v", e)
	}
}
