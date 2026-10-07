package gcp

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestInspectorTreatsOnlyNotFoundAsDeletion(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(int(status.Load()))
		if status.Load() == 200 {
			_, _ = w.Write([]byte(`{"bucket":"bucket","name":"videos/video/file","size":"8","generation":"42"}`))
		} else {
			_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"denied"}}`, status.Load())
		}
	}))
	defer server.Close()
	client, e := storage.NewClient(context.Background(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	i := &GCSArtifactInspector{client: client, bucket: "bucket"}
	for _, code := range []int{200, 404, 403} {
		status.Store(int32(code))
		o, e := i.InspectArtifact(context.Background(), "gs://bucket/videos/video/file")
		switch code {
		case 200:
			if e != nil || !o.Exists || o.Generation != 42 || o.Size != 8 {
				t.Fatalf("observation=%+v err=%v", o, e)
			}
		case 404:
			if e != nil || o.Exists {
				t.Fatalf("missing=%+v err=%v", o, e)
			}
		case 403:
			if e == nil {
				t.Fatal("authorization failure treated as deletion")
			}
		}
	}
	count := calls.Load()
	for _, path := range []string{"gs://other/videos/video/file", "gs://bucket/private/file", "gs://bucket/videos/../file"} {
		if _, e := i.InspectArtifact(context.Background(), path); e == nil {
			t.Fatalf("accepted path %s", path)
		}
	}
	if calls.Load() != count {
		t.Fatal("invalid destinations made a storage request")
	}
}
