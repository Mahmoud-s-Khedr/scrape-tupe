package downloader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalUploaderCreatesArtifactOnce(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(source, []byte("video data"), 0o600); err != nil {
		t.Fatal(err)
	}
	uploader, err := NewLocalUploader(root)
	if err != nil {
		t.Fatal(err)
	}
	path, err := uploader.Upload(context.Background(), source, "videos/id/video.mp4")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "video data" {
		t.Fatalf("stored data = %q", data)
	}
	if _, err := uploader.Upload(context.Background(), source, "videos/id/video.mp4"); err != nil {
		t.Fatalf("second upload: %v", err)
	}
}

func TestLocalUploaderRejectsDifferentExistingArtifact(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(t.TempDir(), "video.mp4")
	if err := os.WriteFile(source, []byte("new video data"), 0o600); err != nil {
		t.Fatal(err)
	}
	uploader, err := NewLocalUploader(root)
	if err != nil {
		t.Fatal(err)
	}
	remotePath := "videos/id/video.mp4"
	destination := filepath.Join(root, remotePath)
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("different existing data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.Upload(context.Background(), source, remotePath); err == nil {
		t.Fatal("Upload accepted an existing artifact with different content")
	}
}

func TestLocalUploaderRejectsEscapingPath(t *testing.T) {
	uploader, err := NewLocalUploader(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.Upload(context.Background(), "unused", "../escape"); err == nil {
		t.Fatal("Upload accepted an escaping path")
	}
}
