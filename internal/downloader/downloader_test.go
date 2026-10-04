package downloader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func testWorker(serverURL string) *Worker {
	return New(Config{MasterURL: serverURL, WorkerID: "worker-a"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestMasterNon2xxResponsesAreErrors(t *testing.T) {
	operations := []struct {
		name string
		call func(*Worker) error
	}{
		{"register", func(w *Worker) error { return w.register(context.Background()) }},
		{"heartbeat", func(w *Worker) error { return w.sendHeartbeat(context.Background()) }},
		{"renew", func(w *Worker) error { return w.renewLease(context.Background(), 1) }},
		{"state", func(w *Worker) error { return w.updateJobState(context.Background(), 1, "downloading", nil) }},
		{"complete", func(w *Worker) error { return w.completeJob(context.Background(), 1, "gs://bucket/object") }},
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusInternalServerError} {
		for _, operation := range operations {
			t.Run(operation.name+"-"+http.StatusText(status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					http.Error(rw, "useful master detail", status)
				}))
				defer server.Close()
				err := operation.call(testWorker(server.URL))
				if err == nil {
					t.Fatal("expected error")
				}
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != status || !strings.Contains(err.Error(), "useful master detail") {
					t.Fatalf("error = %v, want HTTP status %d with response text", err, status)
				}
				if status == http.StatusConflict && !errors.Is(err, ErrLeaseConflict) {
					t.Fatalf("conflict error = %v, want ErrLeaseConflict", err)
				}
			})
		}
	}
}

func TestClaimJobRecognizesOnlyNotFoundAsEmptyQueue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		http.Error(rw, "no work", http.StatusNotFound)
	}))
	defer server.Close()
	_, err := testWorker(server.URL).claimJob(context.Background())
	if !errors.Is(err, ErrNoPendingJobs) {
		t.Fatalf("claim error = %v, want ErrNoPendingJobs", err)
	}
}

type recordingUploader struct{ calls atomic.Int32 }

func (u *recordingUploader) Upload(context.Context, string, string) (string, error) {
	u.calls.Add(1)
	return "unused", nil
}

func TestLeaseConflictDoesNotUploadCompleteOrMarkError(t *testing.T) {
	var claimCalls, stateCalls, completeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/workers/register":
			rw.WriteHeader(http.StatusOK)
		case "/jobs/claim":
			if claimCalls.Add(1) == 1 {
				_ = json.NewEncoder(rw).Encode(Video{ID: 7, YoutubeID: "video-id"})
				return
			}
			http.Error(rw, "empty", http.StatusNotFound)
		case "/jobs/7/state":
			stateCalls.Add(1)
			http.Error(rw, "lease belongs to another worker", http.StatusConflict)
		case "/jobs/7/complete":
			completeCalls.Add(1)
			rw.WriteHeader(http.StatusOK)
		default:
			http.NotFound(rw, r)
		}
	}))
	defer server.Close()

	var logs bytes.Buffer
	uploader := &recordingUploader{}
	worker := New(Config{
		MasterURL: server.URL, WorkerID: "worker-a", Uploader: uploader, ExitWhenEmpty: true,
	}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err := worker.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := uploader.calls.Load(); got != 0 {
		t.Fatalf("upload calls = %d, want 0", got)
	}
	if got := completeCalls.Load(); got != 0 {
		t.Fatalf("completion calls = %d, want 0", got)
	}
	if got := stateCalls.Load(); got != 1 {
		t.Fatalf("state calls = %d, want just failed downloading transition", got)
	}
	if strings.Contains(logs.String(), "job completed") {
		t.Fatalf("unexpected completion log: %s", logs.String())
	}
}

func TestNewUsesNoopUploaderWhenNoneIsConfigured(t *testing.T) {
	worker := New(Config{}, nil)
	if worker.cfg.Uploader == nil {
		t.Fatal("New left uploader nil")
	}
	if _, ok := worker.cfg.Uploader.(*NoopUploader); !ok {
		t.Fatalf("default uploader = %T, want *NoopUploader", worker.cfg.Uploader)
	}
}
