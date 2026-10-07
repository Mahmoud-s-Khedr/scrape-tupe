//go:build integration

package firestore

import (
	"context"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/downloader"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/master"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/normalizer"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fixtureArtifacts replaces GCS only; workers still use the real HTTP API,
// real subprocess execution, and emulator-backed Firestore transactions.
type fixtureArtifacts struct {
	mu      sync.Mutex
	objects map[string]control.ArtifactObservation
}

func (f *fixtureArtifacts) Destination(path string) string { return "gs://fixture/" + path }
func (f *fixtureArtifacts) Upload(_ context.Context, path, remote string) (string, error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	destination := f.Destination(remote)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[destination] = control.ArtifactObservation{Exists: true, Size: int64(len(data)), Generation: 42}
	return destination, nil
}
func (f *fixtureArtifacts) InspectArtifact(_ context.Context, path string) (control.ArtifactObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.objects[path], nil
}

func TestEmulatorHTTPPipelineReplayDownloadAndRetention(t *testing.T) {
	s := emulatorStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	seedQueue(t, s, []int64{control.ArtifactBudgetBytes + 1}, 0)
	src, e := s.CreateSource(ctx, "https://fixture/playlist", "playlist", true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ClaimDiscovery(ctx, "interrupted", time.Minute); e != nil {
		t.Fatal(e)
	}
	for i, url := range []string{"https://fixture/A", "https://fixture/B"} {
		if e = s.CheckpointDiscovery(ctx, src.ID, "interrupted", i, url); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.FailDiscovery(ctx, src.ID, "interrupted", false, "blocked", "fixture interruption", time.Now().Add(-time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.RetrySource(ctx, src.ID); e != nil {
		t.Fatal(e)
	}
	artifacts := &fixtureArtifacts{objects: map[string]control.ArtifactObservation{}}
	api := master.New(s, logger)
	api.SetArtifactInspector(artifacts)
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	bin := t.TempDir()
	metadata := filepath.Join(bin, "metadata")
	script := `#!/bin/sh
case "$*" in
 *--flat-playlist*) printf 'https://fixture/B\nhttps://fixture/C\nhttps://fixture/A\n' ;;
 *) for url do :; done; id="${url##*/}"; printf '{"id":"%s","title":"fixture","filesize":16}\n' "$id" ;;
esac
`
	if e = os.WriteFile(metadata, []byte(script), 0755); e != nil {
		t.Fatal(e)
	}
	w := &normalizer.HTTPWorker{MasterURL: server.URL, WorkerID: "normalizer", Binary: metadata}
	if e = w.RunDiscovery(ctx); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if e = w.RunURL(ctx); e != nil {
			t.Fatal(e)
		}
	}
	got, e := s.GetSource(ctx, src.ID)
	if e != nil || got.DiscoveredCount != 3 || got.NormalizedCount != 3 || got.State != "normalized" {
		t.Fatalf("source=%+v err=%v", got, e)
	}
	download := `#!/bin/sh
while [ "$#" -gt 0 ]; do
 if [ "$1" = '-o' ]; then shift; template="$1"; fi
 last="$1"; shift
done
id="${last##*=}"
file=$(printf '%s' "$template" | sed "s/%(id)s/$id/;s/%(ext)s/mp4/")
printf 'fixture\n' > "$file"
printf '%s\n' "$file"
`
	if e = os.WriteFile(filepath.Join(bin, "yt-dlp"), []byte(download), 0755); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	d := downloader.New(downloader.Config{MasterURL: server.URL, WorkerID: "downloader", OutputDir: t.TempDir(), Uploader: artifacts, CleanupTempDir: true, ExitWhenEmpty: true}, logger)
	if e = d.Run(ctx); e != nil {
		t.Fatal(e)
	}
	if n := counterValue(t, s, "used_bytes"); n != 24 {
		t.Fatalf("used=%d", n)
	}
	if n := counterValue(t, s, "reserved_bytes"); n != 0 {
		t.Fatalf("reserved=%d", n)
	}
	vs, e := s.ListVideos(ctx, "completed")
	if e != nil || len(vs) != 3 {
		t.Fatalf("videos=%+v err=%v", vs, e)
	}
	for _, v := range vs {
		if v.ObjectGeneration != 42 || v.ArtifactState != "stored" {
			t.Fatalf("artifact=%+v", v)
		}
	}
	artifacts.mu.Lock()
	artifacts.objects = map[string]control.ArtifactObservation{}
	artifacts.mu.Unlock()
	for i := 0; i < 2; i++ {
		if e = api.Reconcile(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if n := counterValue(t, s, "used_bytes"); n != 0 {
		t.Fatalf("used after deletion=%d", n)
	}
	for _, v := range vs {
		got, e := s.GetVideo(ctx, v.ID)
		if e != nil || got.ArtifactState != "expired" {
			t.Fatalf("expired=%+v err=%v", got, e)
		}
	}
}
