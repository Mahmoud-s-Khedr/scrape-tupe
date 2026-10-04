// Package control defines the durable control-plane contract used by the
// master and normalizer.  Production uses Firestore; SQLite is deliberately
// retained only as a local-development implementation.
package control

import (
	"context"
	"errors"
	"time"
)

const ArtifactBudgetBytes int64 = 5 << 30

var (
	ErrNotFound        = errors.New("control-plane document not found")
	ErrLeaseNotOwned   = errors.New("job lease is not owned by worker")
	ErrInvalidState    = errors.New("invalid state transition")
	ErrCapacity        = errors.New("artifact capacity unavailable")
	ErrQuarantined     = errors.New("work is quarantined")
	ErrExecutionActive = errors.New("a downloader execution is already active")
)

type Source struct {
	ID                        string     `json:"id" firestore:"id"`
	URL                       string     `json:"url" firestore:"url"`
	Kind                      string     `json:"kind" firestore:"kind"`
	AuthorizationAcknowledged bool       `json:"authorization_acknowledged" firestore:"authorization_acknowledged"`
	State                     string     `json:"state" firestore:"state"`
	VideoCount                int        `json:"video_count" firestore:"video_count"`
	DiscoveryCursor           int        `json:"discovery_cursor" firestore:"discovery_cursor"`
	DiscoveredCount           int        `json:"discovered_count" firestore:"discovered_count"`
	NormalizedCount           int        `json:"normalized_count" firestore:"normalized_count"`
	QuarantinedCount          int        `json:"quarantined_count" firestore:"quarantined_count"`
	WorkerID                  string     `json:"worker_id,omitempty" firestore:"worker_id,omitempty"`
	LeaseUntil                *time.Time `json:"lease_until,omitempty" firestore:"lease_until,omitempty"`
	AttemptCount              int        `json:"attempt_count" firestore:"attempt_count"`
	RetryAt                   *time.Time `json:"retry_at,omitempty" firestore:"retry_at,omitempty"`
	Error                     string     `json:"error,omitempty" firestore:"error,omitempty"`
	QuarantinedUntil          *time.Time `json:"quarantined_until,omitempty" firestore:"quarantined_until,omitempty"`
	CreatedAt                 time.Time  `json:"created_at" firestore:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at" firestore:"updated_at"`
}

// RawURL is an immutable audit record for each URL observed in a source.  Its
// state changes, but the record is deliberately never deleted.
type RawURL struct {
	ID           string     `json:"id" firestore:"id"`
	SourceID     string     `json:"source_id" firestore:"source_id"`
	Index        int        `json:"index" firestore:"index"`
	RawURL       string     `json:"raw_url" firestore:"raw_url"`
	CanonicalURL string     `json:"canonical_url,omitempty" firestore:"canonical_url,omitempty"`
	YoutubeID    string     `json:"youtube_id,omitempty" firestore:"youtube_id,omitempty"`
	State        string     `json:"state" firestore:"state"`
	WorkerID     string     `json:"worker_id,omitempty" firestore:"worker_id,omitempty"`
	LeaseUntil   *time.Time `json:"lease_until,omitempty" firestore:"lease_until,omitempty"`
	AttemptCount int        `json:"attempt_count" firestore:"attempt_count"`
	RetryAt      *time.Time `json:"retry_at,omitempty" firestore:"retry_at,omitempty"`
	Error        string     `json:"error,omitempty" firestore:"error,omitempty"`
	ErrorClass   string     `json:"error_class,omitempty" firestore:"error_class,omitempty"`
	TerminalAt   *time.Time `json:"terminal_at,omitempty" firestore:"terminal_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at" firestore:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at" firestore:"updated_at"`
}

type Video struct {
	ID               string     `json:"id" firestore:"id"` // YouTube ID; stable and globally deduplicated.
	YoutubeID        string     `json:"youtube_id" firestore:"youtube_id"`
	SourceIDs        []string   `json:"source_ids,omitempty" firestore:"source_ids,omitempty"`
	Title            string     `json:"title,omitempty" firestore:"title,omitempty"`
	URL              string     `json:"url,omitempty" firestore:"url,omitempty"`
	EstimatedSize    int64      `json:"estimated_size_bytes,omitempty" firestore:"estimated_size_bytes,omitempty"`
	ReservedSize     int64      `json:"reserved_size_bytes,omitempty" firestore:"reserved_size_bytes,omitempty"`
	ActualSize       int64      `json:"actual_size_bytes,omitempty" firestore:"actual_size_bytes,omitempty"`
	State            string     `json:"state" firestore:"state"`
	WorkerID         string     `json:"worker_id,omitempty" firestore:"worker_id,omitempty"`
	LeaseUntil       *time.Time `json:"lease_until,omitempty" firestore:"lease_until,omitempty"`
	AttemptCount     int        `json:"attempt_count" firestore:"attempt_count"`
	StoragePath      string     `json:"storage_path,omitempty" firestore:"storage_path,omitempty"`
	Error            string     `json:"error,omitempty" firestore:"error,omitempty"`
	ErrorClass       string     `json:"error_class,omitempty" firestore:"error_class,omitempty"`
	QuarantinedUntil *time.Time `json:"quarantined_until,omitempty" firestore:"quarantined_until,omitempty"`
	CreatedAt        time.Time  `json:"created_at" firestore:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" firestore:"updated_at"`
}

type Metadata struct {
	YoutubeID     string
	Title         string
	URL           string
	EstimatedSize int64
}

type Execution struct {
	ID           string    `json:"id" firestore:"id"`
	Kind         string    `json:"kind" firestore:"kind"`
	SourceID     string    `json:"source_id,omitempty" firestore:"source_id,omitempty"`
	CloudRunName string    `json:"cloud_run_name,omitempty" firestore:"cloud_run_name,omitempty"`
	TaskCount    int       `json:"task_count" firestore:"task_count"`
	State        string    `json:"state" firestore:"state"`
	CreatedAt    time.Time `json:"created_at" firestore:"created_at"`
	ReconciledAt time.Time `json:"reconciled_at" firestore:"reconciled_at"`
}

// Store operations that mutate a lease or capacity must be atomic.  The
// interface makes that correctness requirement explicit instead of allowing
// Cloud Run's ephemeral filesystem to become the source of truth.
type Store interface {
	CreateSource(context.Context, string, string, bool) (*Source, error)
	GetSource(context.Context, string) (*Source, error)
	ListSources(context.Context) ([]Source, error)
	RetrySource(context.Context, string) error
	ClaimDiscovery(context.Context, string, time.Duration) (*Source, error)
	CheckpointDiscovery(context.Context, string, string, int, string) error
	FinishDiscovery(context.Context, string, string) error
	FailDiscovery(context.Context, string, string, bool, string, string, time.Time) error

	ClaimRawURL(context.Context, string, time.Duration) (*RawURL, error)
	CompleteRawURL(context.Context, string, string, Metadata) error
	ReleaseRawURL(context.Context, string, string, bool, string, string, time.Time) error
	GetRawURL(context.Context, string) (*RawURL, error)

	GetVideo(context.Context, string) (*Video, error)
	ListVideos(context.Context, string) ([]Video, error)
	ClaimVideo(context.Context, string, time.Duration) (*Video, error)
	RenewLease(context.Context, string, string, time.Duration) error
	SetVideoState(context.Context, string, string, string, string) error
	CompleteVideo(context.Context, string, string, string, int64) error
	QuarantineVideo(context.Context, string, string, string, time.Time) error
	RetryVideo(context.Context, string) error
	RecoverExpiredLeases(context.Context) (int, error)
	CountByState(context.Context) (map[string]int, error)
	WorkCounts(context.Context) (map[string]int, error)

	CreateExecution(context.Context, Execution) (*Execution, error)
	GetActiveDownloaderExecution(context.Context) (*Execution, error)
	GetActiveExecution(context.Context, string) (*Execution, error)
	UpdateExecution(context.Context, string, string, string) error
}
