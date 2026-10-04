// Package gcp contains integrations with Google Cloud services.
package gcp

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// GCSUploader uploads immutable downloader artifacts to a private GCS bucket.
// It uses Application Default Credentials, which Cloud Run supplies through the
// job's service account. Call Close when the process is finished.
type GCSUploader struct {
	store  gcsObjectStore
	closer io.Closer
	bucket string
	logger *slog.Logger
}

// gcsObjectStore is intentionally small so upload correctness can be tested
// without credentials or a live bucket.
type gcsObjectStore interface {
	NewWriter(ctx context.Context, bucket, object, contentType string, checksum uint32) io.WriteCloser
	Attrs(ctx context.Context, bucket, object string) (objectAttrs, error)
}

type objectAttrs struct {
	Size   int64
	CRC32C uint32
}

type storageObjectStore struct{ client *storage.Client }

func (s storageObjectStore) NewWriter(ctx context.Context, bucket, object, contentType string, checksum uint32) io.WriteCloser {
	writer := s.client.Bucket(bucket).Object(object).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ContentType = contentType
	writer.CRC32C = checksum
	writer.SendCRC32C = true
	return writer
}

func (s storageObjectStore) Attrs(ctx context.Context, bucket, object string) (objectAttrs, error) {
	attrs, err := s.client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		return objectAttrs{}, err
	}
	return objectAttrs{Size: attrs.Size, CRC32C: attrs.CRC32C}, nil
}

// NewGCSUploader creates an uploader using Application Default Credentials.
func NewGCSUploader(ctx context.Context, bucket string, logger *slog.Logger) (*GCSUploader, error) {
	if bucket == "" {
		return nil, errors.New("GCS bucket is required")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create Cloud Storage client: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &GCSUploader{store: storageObjectStore{client: client}, closer: client, bucket: bucket, logger: logger}, nil
}

// Close releases resources held by the Cloud Storage client.
func (u *GCSUploader) Close() error {
	if u.closer == nil {
		return nil
	}
	return u.closer.Close()
}

// Upload creates remotePath exactly once. A pre-existing object is accepted
// only when its size and CRC32C match the local file, making retries safe.
func (u *GCSUploader) Upload(ctx context.Context, localPath, remotePath string) (string, error) {
	if err := validObjectName(remotePath); err != nil {
		return "", err
	}

	file, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open upload file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat upload file: %w", err)
	}
	checksum, err := fileCRC32C(file)
	if err != nil {
		return "", fmt.Errorf("compute upload checksum: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind upload file: %w", err)
	}

	writer := u.store.NewWriter(ctx, u.bucket, remotePath, contentType(localPath), checksum)

	u.logger.Info("uploading to Cloud Storage", "bucket", u.bucket, "object", remotePath, "size_bytes", info.Size())
	if _, err := io.Copy(writer, file); err != nil {
		_ = writer.Close()
		return "", fmt.Errorf("write Cloud Storage object: %w", err)
	}
	if err := writer.Close(); err != nil {
		if isPreconditionFailed(err) {
			if err := u.verifyExisting(ctx, remotePath, info.Size(), checksum); err != nil {
				return "", err
			}
			u.logger.Info("Cloud Storage object already exists with matching integrity metadata", "bucket", u.bucket, "object", remotePath)
			return gcsPath(u.bucket, remotePath), nil
		}
		return "", fmt.Errorf("finalize Cloud Storage object: %w", err)
	}

	return gcsPath(u.bucket, remotePath), nil
}

func (u *GCSUploader) verifyExisting(ctx context.Context, object string, size int64, checksum uint32) error {
	attrs, err := u.store.Attrs(ctx, u.bucket, object)
	if err != nil {
		return fmt.Errorf("read pre-existing object metadata: %w", err)
	}
	if attrs.Size != size || attrs.CRC32C != checksum {
		return fmt.Errorf("object already exists with different content (size=%d crc32c=%08x)", attrs.Size, attrs.CRC32C)
	}
	return nil
}

func validObjectName(name string) error {
	if name == "" || strings.HasPrefix(name, "/") || path.Clean(name) != name || strings.HasPrefix(name, "../") {
		return fmt.Errorf("invalid Cloud Storage object name %q", name)
	}
	return nil
}

func contentType(filename string) string {
	if typ := mime.TypeByExtension(strings.ToLower(path.Ext(filename))); typ != "" {
		return typ
	}
	return "application/octet-stream"
}

func fileCRC32C(r io.Reader) (uint32, error) {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := io.Copy(hash, r); err != nil {
		return 0, err
	}
	return hash.Sum32(), nil
}

func gcsPath(bucket, object string) string { return "gs://" + bucket + "/" + object }

func isPreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed
}
