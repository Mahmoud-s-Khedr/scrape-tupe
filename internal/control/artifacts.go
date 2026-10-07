package control

import "context"

// ArtifactStore tracks upload intents before object creation. An abandoned
// upload continues consuming its reservation until metadata is reconciled.
type ArtifactStore interface {
	PrepareUpload(context.Context, string, string, string) error
	ArtifactCandidates(context.Context, int) ([]Video, error)
	ObserveArtifact(context.Context, Video, ArtifactObservation) error
}

type ArtifactObservation struct {
	Exists     bool
	Size       int64
	Generation int64
}

type ArtifactInspector interface {
	InspectArtifact(context.Context, string) (ArtifactObservation, error)
}
