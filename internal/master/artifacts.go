package master

import (
	"context"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

func (s *Server) reconcileArtifacts(ctx context.Context) error {
	store, ok := s.store.(control.ArtifactStore)
	if !ok || s.artifacts == nil {
		return nil
	}
	vs, err := store.ArtifactCandidates(ctx, 64)
	if err != nil {
		return err
	}
	// One inaccessible object must not hide other retention updates.
	for _, v := range vs {
		o, err := s.artifacts.InspectArtifact(ctx, v.StoragePath)
		if err == nil {
			err = store.ObserveArtifact(ctx, v, o)
		}
		if err != nil {
			s.logger.Error("artifact reconciliation failed", "video_id", v.ID, "error", err)
		}
	}
	return nil
}
