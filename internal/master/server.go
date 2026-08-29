package master

import (
	"log/slog"
	"net/http"

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
