package downloader

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestActualYTDLPErrorIsClassified(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "yt-dlp")
	if e := os.WriteFile(binary, []byte("#!/bin/sh\necho 'HTTP Error 429: Too Many Requests' >&2\nexit 1\n"), 0755); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	w := New(Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, e := w.runYTDLP(context.Background(), "video", dir, 100)
	if e == nil || classifyYTDLPFailure(e) != "rate_limited" {
		t.Fatalf("error=%v", e)
	}
}
func TestBoundedWorkerExitsWhenNothingFits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jobs/claim" {
			http.Error(w, "capacity unavailable", 409)
		}
	}))
	defer server.Close()
	w := New(Config{MasterURL: server.URL, WorkerID: "worker", ExitWhenEmpty: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e := w.Run(context.Background()); e != nil {
		t.Fatal(e)
	}
}

type intentUploader struct {
	prepared *atomic.Bool
	calls    int
}

func (u *intentUploader) Destination(path string) string { return "gs://bucket/" + path }
func (u *intentUploader) Upload(_ context.Context, _, path string) (string, error) {
	if !u.prepared.Load() {
		return "", fmt.Errorf("upload started without intent")
	}
	u.calls++
	return u.Destination(path), nil
}
func TestUploadIntentPrecedesObjectCreationAndFailureIsVisible(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var prepared atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/upload") {
					w.WriteHeader(status)
					if status == 200 {
						prepared.Store(true)
					}
				}
			}))
			defer server.Close()
			dir := t.TempDir()
			artifact := filepath.Join(dir, "video.mp4")
			if e := os.WriteFile(artifact, []byte("artifact"), 0644); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(dir, "yt-dlp"), []byte("#!/bin/sh\nprintf '%s\\n' '"+artifact+"'\n"), 0755); e != nil {
				t.Fatal(e)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			uploader := &intentUploader{prepared: &prepared}
			w := New(Config{MasterURL: server.URL, WorkerID: "worker", OutputDir: dir, Uploader: uploader}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			e := w.processJob(context.Background(), &Video{YoutubeID: "video", ReservedSize: 100})
			if status == 200 {
				if e != nil || uploader.calls != 1 {
					t.Fatalf("err=%v uploads=%d", e, uploader.calls)
				}
			} else if e == nil || uploader.calls != 0 {
				t.Fatalf("err=%v uploads=%d", e, uploader.calls)
			}
		})
	}
}

func TestDownloaderStopsAtLeaseDeadlineAfterRenewalFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/renew") {
			http.Error(w, "unavailable", 503)
		}
	}))
	defer server.Close()
	w := New(Config{MasterURL: server.URL, WorkerID: "worker", LeaseDuration: 100 * time.Millisecond, HeartbeatFreq: 20 * time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var lost atomic.Int32
	w.heartbeatLoopWithLeaseLoss(ctx, "video", func() { lost.Add(1) })
	if lost.Load() != 1 {
		t.Fatalf("lost callback count=%d", lost.Load())
	}
}
