package gcp

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

func TestValidObjectName(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"videos/abc123/video.mp4", "videos/a/file without extension"} {
		if err := validObjectName(name); err != nil {
			t.Fatalf("validObjectName(%q): %v", name, err)
		}
	}
	for _, name := range []string{"", "/videos/a.mp4", "videos/../a.mp4", "../a.mp4"} {
		if err := validObjectName(name); err == nil {
			t.Fatalf("validObjectName(%q) succeeded", name)
		}
	}
}

func TestContentType(t *testing.T) {
	t.Parallel()
	if got := contentType("video.mp4"); !strings.HasPrefix(got, "video/mp4") {
		t.Fatalf("contentType(video.mp4) = %q", got)
	}
	if got := contentType("artifact.unknown"); got != "application/octet-stream" {
		t.Fatalf("contentType(artifact.unknown) = %q", got)
	}
}

func TestGCSPath(t *testing.T) {
	t.Parallel()
	if got, want := gcsPath("private-artifacts", "videos/abc/video.mp4"), "gs://private-artifacts/videos/abc/video.mp4"; got != want {
		t.Fatalf("gcsPath() = %q, want %q", got, want)
	}
}

type fakeObject struct {
	data []byte
	crc  uint32
}

type fakeObjectStore struct {
	objects        map[string]fakeObject
	rejectChecksum bool
	attrCalls      int
}

func (s *fakeObjectStore) NewWriter(_ context.Context, _ string, object, _ string, checksum uint32) io.WriteCloser {
	return &fakeWriter{store: s, object: object, checksum: checksum}
}

func (s *fakeObjectStore) Attrs(_ context.Context, _ string, object string) (objectAttrs, error) {
	s.attrCalls++
	stored, ok := s.objects[object]
	if !ok {
		return objectAttrs{}, errors.New("object not found")
	}
	return objectAttrs{Size: int64(len(stored.data)), CRC32C: stored.crc}, nil
}

type fakeWriter struct {
	store    *fakeObjectStore
	object   string
	checksum uint32
	data     bytes.Buffer
}

func (w *fakeWriter) Write(p []byte) (int, error) { return w.data.Write(p) }

func (w *fakeWriter) Close() error {
	if w.store.rejectChecksum {
		return errors.New("checksum mismatch")
	}
	if _, exists := w.store.objects[w.object]; exists {
		return &googleapi.Error{Code: 412, Message: "precondition failed"}
	}
	w.store.objects[w.object] = fakeObject{data: append([]byte(nil), w.data.Bytes()...), crc: w.checksum}
	return nil
}

func TestGCSUploaderCreateAndDuplicateVerification(t *testing.T) {
	data := []byte("video bytes")
	localPath := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(localPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	checksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	t.Run("creates object", func(t *testing.T) {
		store := &fakeObjectStore{objects: make(map[string]fakeObject)}
		uploader := &GCSUploader{store: store, bucket: "private", logger: slog.Default()}
		got, err := uploader.Upload(context.Background(), localPath, "videos/id/video.mp4")
		if err != nil || got != "gs://private/videos/id/video.mp4" {
			t.Fatalf("Upload() = %q, %v", got, err)
		}
		stored := store.objects["videos/id/video.mp4"]
		if !bytes.Equal(stored.data, data) || stored.crc != checksum {
			t.Fatalf("stored object = %#v", stored)
		}
	})

	t.Run("accepts identical duplicate after metadata read", func(t *testing.T) {
		store := &fakeObjectStore{objects: map[string]fakeObject{"videos/id/video.mp4": {data: data, crc: checksum}}}
		uploader := &GCSUploader{store: store, bucket: "private", logger: slog.Default()}
		if _, err := uploader.Upload(context.Background(), localPath, "videos/id/video.mp4"); err != nil {
			t.Fatalf("Upload duplicate: %v", err)
		}
		if store.attrCalls != 1 {
			t.Fatalf("Attrs calls = %d, want 1", store.attrCalls)
		}
	})

	t.Run("rejects different duplicate", func(t *testing.T) {
		store := &fakeObjectStore{objects: map[string]fakeObject{"videos/id/video.mp4": {data: []byte("other"), crc: 1}}}
		uploader := &GCSUploader{store: store, bucket: "private", logger: slog.Default()}
		if _, err := uploader.Upload(context.Background(), localPath, "videos/id/video.mp4"); err == nil || !strings.Contains(err.Error(), "different content") {
			t.Fatalf("Upload collision error = %v", err)
		}
	})

	t.Run("returns checksum finalization failure", func(t *testing.T) {
		store := &fakeObjectStore{objects: make(map[string]fakeObject), rejectChecksum: true}
		uploader := &GCSUploader{store: store, bucket: "private", logger: slog.Default()}
		if _, err := uploader.Upload(context.Background(), localPath, "videos/id/video.mp4"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("Upload checksum error = %v", err)
		}
	})
}

func TestGCSUploaderRejectsInvalidObjectNameBeforeWriting(t *testing.T) {
	store := &fakeObjectStore{objects: make(map[string]fakeObject)}
	uploader := &GCSUploader{store: store, bucket: "private", logger: slog.Default()}
	path := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.Upload(context.Background(), path, "../escape.mp4"); err == nil {
		t.Fatal("Upload accepted invalid object name")
	}
	if len(store.objects) != 0 {
		t.Fatalf("objects = %v", store.objects)
	}
}
