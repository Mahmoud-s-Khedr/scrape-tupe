package master

import (
	"context"
	"errors"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"io"
	"log/slog"
	"testing"
)

type artifactStore struct {
	control.Store
	observed []string
}

func (s *artifactStore) PrepareUpload(context.Context, string, string, string) error { return nil }
func (s *artifactStore) ArtifactCandidates(context.Context, int) ([]control.Video, error) {
	return []control.Video{{ID: "unavailable", StoragePath: "bad"}, {ID: "expired", StoragePath: "missing"}}, nil
}
func (s *artifactStore) ObserveArtifact(_ context.Context, v control.Video, o control.ArtifactObservation) error {
	if o.Exists {
		return errors.New("unexpected live object")
	}
	s.observed = append(s.observed, v.ID)
	return nil
}

type failingInspector struct{}

func (failingInspector) InspectArtifact(_ context.Context, path string) (control.ArtifactObservation, error) {
	if path == "bad" {
		return control.ArtifactObservation{}, errors.New("permission denied")
	}
	return control.ArtifactObservation{}, nil
}
func TestArtifactFailureDoesNotReleaseCapacityOrHideOtherObjects(t *testing.T) {
	store := &artifactStore{}
	s := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetArtifactInspector(failingInspector{})
	if e := s.Reconcile(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(store.observed) != 1 || store.observed[0] != "expired" {
		t.Fatalf("observed=%v", store.observed)
	}
}
