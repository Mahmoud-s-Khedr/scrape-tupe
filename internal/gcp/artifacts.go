package gcp

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"strings"
)

type GCSArtifactInspector struct {
	client *storage.Client
	bucket string
}

func NewGCSArtifactInspector(ctx context.Context, bucket string) (*GCSArtifactInspector, error) {
	if bucket == "" {
		return nil, errors.New("GCS_BUCKET is required for artifact accounting")
	}
	c, e := storage.NewClient(ctx)
	if e != nil {
		return nil, e
	}
	return &GCSArtifactInspector{client: c, bucket: bucket}, nil
}
func (i *GCSArtifactInspector) Close() error { return i.client.Close() }
func (i *GCSArtifactInspector) InspectArtifact(ctx context.Context, path string) (control.ArtifactObservation, error) {
	prefix := "gs://" + i.bucket + "/"
	if !strings.HasPrefix(path, prefix) {
		return control.ArtifactObservation{}, fmt.Errorf("artifact is outside configured bucket")
	}
	object := strings.TrimPrefix(path, prefix)
	if !strings.HasPrefix(object, "videos/") {
		return control.ArtifactObservation{}, fmt.Errorf("artifact is outside videos prefix")
	}
	if e := validObjectName(object); e != nil {
		return control.ArtifactObservation{}, e
	}
	a, e := i.client.Bucket(i.bucket).Object(object).Attrs(ctx)
	if errors.Is(e, storage.ErrObjectNotExist) {
		return control.ArtifactObservation{}, nil
	}
	if e != nil {
		return control.ArtifactObservation{}, e
	}
	return control.ArtifactObservation{Exists: true, Size: a.Size, Generation: a.Generation}, nil
}
func (u *GCSUploader) Destination(remotePath string) string { return gcsPath(u.bucket, remotePath) }
