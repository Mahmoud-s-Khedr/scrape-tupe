package master

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
	"google.golang.org/api/idtoken"
)

type Server struct {
	store          control.Store
	logger         *slog.Logger
	jobs           JobRunner
	operatorAPIKey string
	workerAudience string
	workerEmails   map[string]bool
	validateToken  TokenValidator
	artifacts      control.ArtifactInspector
}

// TokenValidator verifies a token for the supplied audience and returns the
// verified service-account email claim. It is injected in HTTP tests; the
// default delegates signature and audience verification to Google's library.
type TokenValidator func(context.Context, string, string) (string, error)

// SetAuth configures the two trust boundaries. Empty values intentionally keep
// local development open; production Terraform always supplies both values.
func (s *Server) SetAuth(operatorAPIKey, workerAudience string) {
	s.operatorAPIKey = operatorAPIKey
	s.workerAudience = workerAudience
	s.validateToken = googleTokenValidator
}

func googleTokenValidator(ctx context.Context, token, audience string) (string, error) {
	payload, err := idtoken.Validate(ctx, token, audience)
	if err != nil {
		return "", err
	}
	email, _ := payload.Claims["email"].(string)
	if email == "" {
		return "", errors.New("ID token is missing email claim")
	}
	return email, nil
}

func (s *Server) SetWorkerServiceAccounts(emails ...string) {
	s.workerEmails = make(map[string]bool, len(emails))
	for _, email := range emails {
		if email != "" {
			s.workerEmails[email] = true
		}
	}
}

func (s *Server) SetTokenValidator(v TokenValidator) { s.validateToken = v }

func (s *Server) ValidateAuthConfig() error {
	if s.workerAudience != "" && len(s.workerEmails) == 0 {
		return errors.New("MASTER_AUDIENCE requires WORKER_SERVICE_ACCOUNTS")
	}
	if s.workerAudience == "" && len(s.workerEmails) != 0 {
		return errors.New("WORKER_SERVICE_ACCOUNTS requires MASTER_AUDIENCE")
	}
	return nil
}

type JobRunner interface {
	RunDiscovery(context.Context) (string, error)
	RunURLNormalizer(context.Context, int) (string, error)
	RunDownloader(context.Context, int) (string, error)
}
type ExecutionInspector interface {
	ExecutionDone(context.Context, string) (done, successful bool, err error)
}

// New accepts the SQLite DB for backwards-compatible local development or a
// production control.Store (normally internal/firestore.Store).
func New(backend any, logger *slog.Logger) *Server {
	var store control.Store
	switch v := backend.(type) {
	case *db.DB:
		store = control.NewLocalStore(v)
	case control.Store:
		store = v
	default:
		panic("master.New requires *db.DB or control.Store")
	}
	return &Server{
		store:  store,
		logger: logger,
	}
}

func (s *Server) SetJobRunner(j JobRunner) { s.jobs = j }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /sources", s.handleCreateSource)
	mux.HandleFunc("GET /sources", s.handleListSources)
	mux.HandleFunc("GET /sources/{id}", s.handleGetSource)

	mux.HandleFunc("GET /videos", s.handleListVideos)
	mux.HandleFunc("GET /videos/{id}", s.handleGetVideo)

	mux.HandleFunc("POST /workers/register", s.handleRegisterWorker)
	mux.HandleFunc("POST /workers/heartbeat", s.handleWorkerHeartbeat)
	mux.HandleFunc("GET /workers", s.handleListWorkers)

	mux.HandleFunc("POST /jobs/claim", s.handleClaimJob)
	mux.HandleFunc("POST /jobs/{id}/state", s.handleUpdateJobState)
	mux.HandleFunc("POST /jobs/{id}/upload", s.handlePrepareUpload)
	mux.HandleFunc("POST /jobs/{id}/complete", s.handleCompleteJob)
	mux.HandleFunc("POST /jobs/{id}/renew", s.handleRenewJob)
	mux.HandleFunc("POST /jobs/{id}/quarantine", s.handleQuarantineJob)
	mux.HandleFunc("POST /videos/{id}/retry", s.handleRetryVideo)
	mux.HandleFunc("POST /sources/{id}/retry", s.handleRetrySource)
	mux.HandleFunc("POST /internal/reconcile", s.handleReconcile)
	mux.HandleFunc("POST /normalizer/discovery/claim", s.handleClaimDiscovery)
	mux.HandleFunc("POST /normalizer/discovery/{id}/checkpoint", s.handleDiscoveryCheckpoint)
	mux.HandleFunc("POST /normalizer/discovery/{id}/renew", s.handleDiscoveryRenew)
	mux.HandleFunc("POST /normalizer/urls/{id}/renew", s.handleRawRenew)
	mux.HandleFunc("POST /normalizer/discovery/{id}/complete", s.handleDiscoveryComplete)
	mux.HandleFunc("POST /normalizer/discovery/{id}/failure", s.handleDiscoveryFailure)
	mux.HandleFunc("POST /normalizer/urls/claim", s.handleClaimRawURL)
	mux.HandleFunc("POST /normalizer/urls/{id}/complete", s.handleRawURLComplete)
	mux.HandleFunc("POST /normalizer/urls/{id}/failure", s.handleRawURLFailure)

	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /health", s.handleHealth)

	return s.authorize(mux)
}

func (s *Server) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operator := r.URL.Path == "/sources" || strings.HasPrefix(r.URL.Path, "/sources/") || r.URL.Path == "/internal/reconcile" || strings.HasPrefix(r.URL.Path, "/videos/") && strings.HasSuffix(r.URL.Path, "/retry")
		worker := strings.HasPrefix(r.URL.Path, "/workers/") || strings.HasPrefix(r.URL.Path, "/jobs/") || strings.HasPrefix(r.URL.Path, "/normalizer/")
		if operator && s.operatorAPIKey != "" {
			got := r.Header.Get("X-API-Key")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.operatorAPIKey)) != 1 {
				http.Error(w, "operator API key required", http.StatusUnauthorized)
				return
			}
		}
		// The public submission surface is key-protected, while workers carry a
		// Cloud Run ID token. Verify it here too: route-level IAM cannot express
		// that distinction when both share the same Cloud Run service.
		if worker && s.workerAudience != "" {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == r.Header.Get("Authorization") || token == "" {
				http.Error(w, "worker ID token required", http.StatusUnauthorized)
				return
			}
			validator := s.validateToken
			if validator == nil {
				validator = googleTokenValidator
			}
			email, err := validator(r.Context(), token, s.workerAudience)
			if err != nil || !s.workerEmails[email] {
				http.Error(w, "invalid worker ID token", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// RunLeaseRecovery periodically makes expired jobs available to another
// worker. It also performs one recovery pass immediately, so a restarted
// master does not wait an entire interval before recovering abandoned work.
// The loop stops when ctx is cancelled.
func (s *Server) RunLeaseRecovery(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}

	s.recoverExpiredLeases(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverExpiredLeases(ctx)
		}
	}
}

func (s *Server) recoverExpiredLeases(ctx context.Context) {
	count, err := s.store.RecoverExpiredLeases(ctx)
	if err != nil {
		s.logger.Error("failed to recover expired job leases", "error", err)
		return
	}
	if count > 0 {
		s.logger.Info("recovered expired job leases", "count", count)
	}
}

func (s *Server) SetArtifactInspector(i control.ArtifactInspector) { s.artifacts = i }
