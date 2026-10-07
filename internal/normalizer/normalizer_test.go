package normalizer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureBinary(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "yt-dlp")
	if e := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0755); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestDiscoveryReplayStartsAtZeroAndReportsDiagnostics(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay", true: "blocked"}[fail], func(t *testing.T) {
			var mu sync.Mutex
			var checkpoints []int
			var completed atomic.Bool
			var failureClass string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case strings.HasSuffix(r.URL.Path, "/claim"):
					_ = json.NewEncoder(w).Encode(control.Source{ID: "source", URL: "https://fixture", DiscoveryCursor: 2})
				case strings.HasSuffix(r.URL.Path, "/checkpoint"):
					var req struct {
						Index int `json:"index"`
					}
					_ = json.NewDecoder(r.Body).Decode(&req)
					checkpoints = append(checkpoints, req.Index)
				case strings.HasSuffix(r.URL.Path, "/complete"):
					completed.Store(true)
				case strings.HasSuffix(r.URL.Path, "/failure"):
					var req struct {
						Class string `json:"class"`
					}
					_ = json.NewDecoder(r.Body).Decode(&req)
					failureClass = req.Class
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer server.Close()
			body := "printf 'A\\nB\\nC\\n'"
			if fail {
				body = "echo 'Sign in to confirm access denied' >&2; exit 1"
			}
			w := &HTTPWorker{MasterURL: server.URL, WorkerID: "worker", Binary: fixtureBinary(t, body)}
			err := w.RunDiscovery(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if fail {
				if err == nil || failureClass != "blocked" {
					t.Fatalf("err=%v class=%s", err, failureClass)
				}
			} else {
				if err != nil || !completed.Load() || len(checkpoints) != 3 {
					t.Fatalf("err=%v checkpoints=%v", err, checkpoints)
				}
				for i, n := range checkpoints {
					if n != i {
						t.Fatalf("indexes=%v", checkpoints)
					}
				}
			}
		})
	}
}
func TestMetadataFormatSizesAndFailureClassification(t *testing.T) {
	w := &HTTPWorker{Binary: fixtureBinary(t, `echo '{"id":"video","title":"Test","requested_formats":[{"filesize":20},{"filesize_approx":30}]}'`)}
	m, e := w.metadata(context.Background(), "https://fixture")
	if e != nil || m.EstimatedSize != 50 {
		t.Fatalf("metadata=%+v err=%v", m, e)
	}
	w.Binary = fixtureBinary(t, "echo 'HTTP Error 429: Too Many Requests' >&2; exit 1")
	_, e = w.metadata(context.Background(), "https://fixture")
	if e == nil || classifyFailure(e) != "rate_limited" {
		t.Fatalf("error=%v", e)
	}
}
func TestNormalizerLeaseRenewalAndOwnershipCancellation(t *testing.T) {
	for _, status := range []int{200, 409, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var renewals atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { renewals.Add(1); w.WriteHeader(status) }))
			defer server.Close()
			w := &HTTPWorker{MasterURL: server.URL, WorkerID: "worker", LeaseDuration: time.Second, RenewalInterval: 20 * time.Millisecond}
			until := time.Now().Add(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := w.withLease(ctx, "/normalizer/discovery/source", &until, func(task context.Context) error {
				select {
				case <-task.Done():
					return context.Cause(task)
				case <-time.After(250 * time.Millisecond):
					return nil
				}
			})
			if status == 200 {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, control.ErrLeaseNotOwned) {
				t.Fatalf("error=%v", err)
			}
			if renewals.Load() == 0 {
				t.Fatal("lease was never renewed")
			}
			count := renewals.Load()
			time.Sleep(40 * time.Millisecond)
			if renewals.Load() != count {
				t.Fatal("renewal goroutine survived task")
			}
		})
	}
}
func TestFailureCallbackErrorIsVisible(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer server.Close()
	w := &HTTPWorker{MasterURL: server.URL}
	err := w.reportFailure("/normalizer/urls/raw", errors.New("download failed"))
	if err == nil || !strings.Contains(err.Error(), "release failed") {
		t.Fatalf("error=%v", err)
	}
}
