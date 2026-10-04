package master

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
)

type Server struct {
	db     *db.DB
	logger *slog.Logger
}

func New(database *db.DB, logger *slog.Logger) *Server {
	return &Server{
		db:     database,
		logger: logger,
	}
}

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
	mux.HandleFunc("POST /jobs/{id}/complete", s.handleCompleteJob)
	mux.HandleFunc("POST /jobs/{id}/renew", s.handleRenewJob)

	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /health", s.handleHealth)

	return mux
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
	count, err := s.db.RecoverExpiredLeases(ctx)
	if err != nil {
		s.logger.Error("failed to recover expired job leases", "error", err)
		return
	}
	if count > 0 {
		s.logger.Info("recovered expired job leases", "count", count)
	}
}
