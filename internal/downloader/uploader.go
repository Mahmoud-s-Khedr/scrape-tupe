package downloader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// Uploader abstracts file uploads so we can swap implementations.
// Upload takes a local file path and a remote path, and returns the storage path
// that should be recorded in the database.
type Uploader interface {
	Upload(ctx context.Context, localPath string, remotePath string) (storagePath string, err error)
}

// NoopUploader skips the upload and returns the local file path as the storage path.
// It is useful for local tests without any storage service.
type NoopUploader struct {
	logger *slog.Logger
}

// LocalUploader copies artifacts to a local directory. It is deliberately only
// for tests and local development; production uses the GCS uploader.
type LocalUploader struct {
	root string
}

func NewLocalUploader(root string) (*LocalUploader, error) {
	if root == "" {
		return nil, fmt.Errorf("local storage directory is required")
	}
	return &LocalUploader{root: root}, nil
}

func (u *LocalUploader) Upload(_ context.Context, localPath string, remotePath string) (string, error) {
	if remotePath == "" || filepath.IsAbs(remotePath) || filepath.Clean(remotePath) != remotePath || strings.HasPrefix(remotePath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid local storage path %q", remotePath)
	}
	destination := filepath.Join(u.root, remotePath)
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return "", fmt.Errorf("create local storage directory: %w", err)
	}
	source, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open upload file: %w", err)
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			matches, verifyErr := filesMatch(source, destination)
			if verifyErr != nil {
				return "", fmt.Errorf("verify existing local storage object: %w", verifyErr)
			}
			if !matches {
				return "", fmt.Errorf("local storage object already exists with different content")
			}
			return destination, nil
		}
		return "", fmt.Errorf("create local storage object: %w", err)
	}
	_, copyErr := io.Copy(target, source)
	closeErr := target.Close()
	if copyErr != nil {
		_ = os.Remove(destination)
		return "", fmt.Errorf("copy local storage object: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(destination)
		return "", fmt.Errorf("close local storage object: %w", closeErr)
	}
	return destination, nil
}

// filesMatch compares source with an existing local object without trusting a
// matching filename. It mirrors the retry safety guarantee of the GCS uploader.
func filesMatch(source *os.File, destination string) (bool, error) {
	sourceInfo, err := source.Stat()
	if err != nil {
		return false, fmt.Errorf("stat source file: %w", err)
	}
	existing, err := os.Open(destination)
	if err != nil {
		return false, fmt.Errorf("open existing object: %w", err)
	}
	defer existing.Close()
	existingInfo, err := existing.Stat()
	if err != nil {
		return false, fmt.Errorf("stat existing object: %w", err)
	}
	if !existingInfo.Mode().IsRegular() || sourceInfo.Size() != existingInfo.Size() {
		return false, nil
	}

	sourceHash := sha256.New()
	if _, err := io.Copy(sourceHash, source); err != nil {
		return false, fmt.Errorf("hash source file: %w", err)
	}
	existingHash := sha256.New()
	if _, err := io.Copy(existingHash, existing); err != nil {
		return false, fmt.Errorf("hash existing object: %w", err)
	}
	return bytes.Equal(sourceHash.Sum(nil), existingHash.Sum(nil)), nil
}

// NewNoopUploader creates an uploader that does nothing (for local testing).
func NewNoopUploader(logger *slog.Logger) *NoopUploader {
	return &NoopUploader{logger: logger}
}

// Upload returns the local path as-is without uploading anything.
func (u *NoopUploader) Upload(_ context.Context, localPath string, _ string) (string, error) {
	u.logger.Info("noop uploader: skipping upload, using local path",
		"local_path", localPath,
	)
	return localPath, nil
}

// BuildRemotePath constructs the remote storage path for a video file.
// Format: videos/{youtube_id}/{filename}
func BuildRemotePath(youtubeID string, localPath string) string {
	filename := filepath.Base(localPath)
	return fmt.Sprintf("videos/%s/%s", youtubeID, filename)
}
